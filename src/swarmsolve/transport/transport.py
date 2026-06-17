"""Asyncio TCP + UDP transport.

* TCP server : newline-delimited JSON, one connection per request (simple &
  robust for the demo). Used for OPEN_TASK / DEAD_END / SOLUTION / claims.
* UDP socket : datagram per message. Used for Kademlia discovery RPCs.

A single async ``handler(msg, addr, kind)`` callback receives every inbound
message. Higher layers (discovery, gossip, peer) register their logic there.

Owner: Person A.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from swarmsolve.transport.messages import Message, decode, encode

Handler = Callable[[Message, tuple[str, int], str], Awaitable[None]]


class _UDPProtocol(asyncio.DatagramProtocol):
    def __init__(self, transport_ref: "Transport") -> None:
        self._owner = transport_ref

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        try:
            msg = decode(data)
        except Exception:
            return
        if self._owner.handler:
            asyncio.create_task(self._owner.handler(msg, addr, "udp"))


class Transport:
    """Owns the TCP server + UDP endpoint for one peer."""

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.handler: Handler | None = None
        self._tcp_server: asyncio.AbstractServer | None = None
        self._udp_transport: asyncio.DatagramTransport | None = None

    # ---- lifecycle ----------------------------------------------------

    async def start(self, handler: Handler) -> None:
        self.handler = handler
        self._tcp_server = await asyncio.start_server(
            self._on_tcp_conn, self.host, self.port
        )
        loop = asyncio.get_running_loop()
        self._udp_transport, _ = await loop.create_datagram_endpoint(
            lambda: _UDPProtocol(self),
            local_addr=(self.host, self.port),
        )

    async def stop(self) -> None:
        if self._tcp_server:
            self._tcp_server.close()
            await self._tcp_server.wait_closed()
        if self._udp_transport:
            self._udp_transport.close()

    # ---- inbound ------------------------------------------------------

    async def _on_tcp_conn(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            line = await reader.readline()
            if not line:
                return
            msg = decode(line)
            peer = writer.get_extra_info("peername")
            if self.handler:
                await self.handler(msg, peer, "tcp")
        except Exception:
            pass
        finally:
            writer.close()

    # ---- outbound -----------------------------------------------------

    async def send_tcp(self, host: str, port: int, msg: Message) -> bool:
        """Fire-and-forget reliable send. Returns False if the peer is down."""
        try:
            reader, writer = await asyncio.open_connection(host, port)
            writer.write(encode(msg))
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return True
        except (OSError, ConnectionError):
            return False

    def send_udp(self, host: str, port: int, msg: Message) -> None:
        """Best-effort datagram send (no delivery guarantee)."""
        if self._udp_transport:
            self._udp_transport.sendto(encode(msg), (host, port))
