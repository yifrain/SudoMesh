"""Push-based gossip with seen-set de-duplication and TTL flood control.

Each message carries a ``msg_id`` and a ``ttl``. On receipt:
    1. drop if already seen (de-dup),
    2. deliver to the local application handler,
    3. if ttl > 0, decrement and forward to a random fan-out subset of peers.

This bounds traffic while still reaching the whole overlay w.h.p., echoing the
probabilistic-coverage idea from BubbleStorm (Chapter 7).

Owner: Person C.
"""

from __future__ import annotations

import random
from collections import OrderedDict
from collections.abc import Awaitable, Callable

from swarmsolve.discovery.routing import Contact, RoutingTable
from swarmsolve.transport.messages import Message
from swarmsolve.transport.transport import Transport

DeliverFn = Callable[[Message], Awaitable[None]]


class Gossip:
    def __init__(
        self,
        transport: Transport,
        table: RoutingTable,
        *,
        fanout: int = 3,
        seen_capacity: int = 4096,
    ) -> None:
        self.transport = transport
        self.table = table
        self.fanout = fanout
        self._seen: "OrderedDict[str, None]" = OrderedDict()
        self._seen_capacity = seen_capacity
        self.deliver: DeliverFn | None = None

    def _mark_seen(self, msg_id: str) -> bool:
        """Return True if newly seen, False if duplicate."""
        if msg_id in self._seen:
            return False
        self._seen[msg_id] = None
        if len(self._seen) > self._seen_capacity:
            self._seen.popitem(last=False)
        return True

    async def handle(self, msg: Message) -> None:
        """Process an inbound gossip message: de-dup -> deliver -> forward.

        This three-step pipeline makes epidemic spread both complete (reaches the
        whole overlay with high probability) and bounded (no message is delivered
        or relayed twice).
        """
        if not self._mark_seen(msg.msg_id):
            return  # 1. de-dup: already seen this msg_id -> stop (caps traffic)
        if self.deliver:
            await self.deliver(msg)       # 2. deliver: hand to the local app (_on_gossip)
        if msg.ttl > 0:
            await self._forward(msg)      # 3. forward: relay onward while ttl remains

    async def broadcast(self, msg: Message) -> None:
        """Originate a new gossip message from this peer."""
        self._mark_seen(msg.msg_id)
        await self._forward(msg)

    async def _forward(self, msg: Message) -> None:
        """Relay to ``fanout`` random neighbours with ttl decremented (flood
        control): larger fan-out = faster/wider coverage but more traffic."""
        targets = self._pick_targets(exclude=msg.sender)
        relay = Message(
            type=msg.type,
            sender=msg.sender,
            payload=msg.payload,
            msg_id=msg.msg_id,
            ttl=msg.ttl - 1,
            ts=msg.ts,
        )
        for c in targets:
            await self.transport.send_tcp(c.host, c.port, relay)

    def _pick_targets(self, exclude: str) -> list[Contact]:
        peers = [c for c in self.table.all_contacts() if c.node_id.hex() != exclude]
        if len(peers) <= self.fanout:
            return peers
        return random.sample(peers, self.fanout)
