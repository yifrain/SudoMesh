"""Transport layer: message types + TCP/UDP send/receive + (de)serialization.

* UDP is used for Kademlia discovery RPCs (lightweight, jitter-tolerant).
* TCP is used for task/solution exchange (reliable, larger payloads).

Owner: Person A (see README "Team split").
"""

from swarmsolve.transport.messages import (
    Message,
    MessageType,
    decode,
    encode,
)

__all__ = ["Message", "MessageType", "decode", "encode"]
