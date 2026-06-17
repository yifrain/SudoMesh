"""Minimal Kademlia node: PING/PONG + (iterative) FIND_NODE over UDP.

This is intentionally a teaching-grade implementation: it covers bootstrap,
bucket maintenance, and iterative lookup, which are the parts that matter for
the course. STORE/FIND_VALUE are not needed because we reuse the keyspace only
for *routing* tasks to their closest peers (the actual task data travels by
gossip/TCP).

Owner: Person B.
"""

from __future__ import annotations

import asyncio

from swarmsolve.discovery.node_id import NodeID, xor_distance
from swarmsolve.discovery.routing import Contact, RoutingTable
from swarmsolve.transport.messages import Message, MessageType
from swarmsolve.transport.transport import Transport


class KademliaNode:
    def __init__(self, node_id: NodeID, transport: Transport) -> None:
        self.id = node_id
        self.transport = transport
        self.table = RoutingTable(node_id)
        # Pending FIND_NODE_REPLY futures keyed by msg_id.
        self._pending: dict[str, asyncio.Future] = {}

    # ---- inbound handling (called by Peer's dispatcher) ---------------

    async def handle(self, msg: Message, addr: tuple[str, int]) -> None:
        sender_id = NodeID.from_hex(msg.sender)
        # We learn the sender's listening port from the payload (UDP src port
        # may differ); fall back to the datagram source.
        host = msg.payload.get("host", addr[0])
        port = msg.payload.get("port", addr[1])
        self.table.add(Contact(sender_id, host, port))

        if msg.type == MessageType.PING:
            self._send(host, port, MessageType.PONG)
        elif msg.type == MessageType.FIND_NODE:
            target = NodeID.from_hex(msg.payload["target"])
            closest = self.table.closest(target)
            self._send(
                host,
                port,
                MessageType.FIND_NODE_REPLY,
                {
                    "target": target.hex(),
                    "nodes": [c.to_dict() for c in closest],
                    "reply_to": msg.msg_id,
                },
            )
        elif msg.type == MessageType.FIND_NODE_REPLY:
            for d in msg.payload.get("nodes", []):
                self.table.add(Contact.from_dict(d))
            fut = self._pending.pop(msg.payload.get("reply_to", ""), None)
            if fut and not fut.done():
                fut.set_result(msg.payload.get("nodes", []))

    # ---- outbound helpers --------------------------------------------

    def _send(self, host: str, port: int, mtype: MessageType, payload: dict | None = None) -> Message:
        body = {"host": self.transport.host, "port": self.transport.port}
        if payload:
            body.update(payload)
        msg = Message(type=mtype, sender=self.id.hex(), payload=body)
        self.transport.send_udp(host, port, msg)
        return msg

    # ---- public API ---------------------------------------------------

    async def bootstrap(self, contacts: list[Contact]) -> None:
        """Join the network through one or more known contacts."""
        for c in contacts:
            self.table.add(c)
            self._send(c.host, c.port, MessageType.PING)
        await asyncio.sleep(0.2)
        await self.lookup(self.id)  # populate buckets near ourselves

    async def lookup(self, target: NodeID, *, alpha: int = 3) -> list[Contact]:
        """Iterative FIND_NODE: query progressively closer peers.

        Returns the k closest contacts we converged on.
        """
        queried: set[NodeID] = set()
        shortlist = self.table.closest(target)
        for _ in range(6):  # bounded rounds; each round roughly halves the distance -> O(log n)
            # query the alpha closest not-yet-queried peers in parallel (low latency)
            batch = [c for c in shortlist if c.node_id not in queried][:alpha]
            if not batch:
                break  # converged: no closer un-queried peer remains
            futures = []
            for c in batch:
                queried.add(c.node_id)
                msg = self._send(
                    c.host, c.port, MessageType.FIND_NODE, {"target": target.hex()}
                )
                fut: asyncio.Future = asyncio.get_running_loop().create_future()
                self._pending[msg.msg_id] = fut
                futures.append(fut)
            try:
                await asyncio.wait_for(asyncio.gather(*futures, return_exceptions=True), timeout=1.0)
            except asyncio.TimeoutError:
                pass
            shortlist = self.table.closest(target)  # replies populated buckets with closer peers
        return shortlist

    def closest_to_key(self, key: NodeID, count: int) -> list[Contact]:
        """Who should own a task whose key is ``key``? The closest peers."""
        return self.table.closest(key, count)

    def is_responsible_for(self, key: NodeID, replicas: int = 1) -> bool:
        """Am I among the ``replicas`` peers closest to ``key`` (incl. myself)?"""
        contacts = self.table.closest(key, replicas)
        my_dist = xor_distance(self.id, key)
        return all(my_dist <= xor_distance(c.node_id, key) for c in contacts) or len(contacts) < replicas
