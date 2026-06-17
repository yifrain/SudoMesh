"""Kademlia node/key identifiers and the XOR distance metric.

Fully functional and dependency-free, so it is easy to unit-test.

The XOR metric d(a, b) = a XOR b is symmetric and unidirectional, which is the
property that makes Kademlia routing converge. We expose helpers to:
    * create random / hashed IDs,
    * compute XOR distance and shared-prefix length (the k-bucket index),
    * derive a *task key* from a search-tree path (reusing the same keyspace).
"""

from __future__ import annotations

import hashlib
import secrets

ID_BITS = 160
ID_BYTES = ID_BITS // 8


class NodeID:
    """A 160-bit identifier living in the XOR keyspace."""

    __slots__ = ("value",)

    def __init__(self, value: int) -> None:
        self.value = value & ((1 << ID_BITS) - 1)

    # ---- constructors -------------------------------------------------

    @classmethod
    def random(cls) -> "NodeID":
        return cls(secrets.randbits(ID_BITS))

    @classmethod
    def from_bytes(cls, b: bytes) -> "NodeID":
        return cls(int.from_bytes(b[:ID_BYTES].ljust(ID_BYTES, b"\0"), "big"))

    @classmethod
    def from_string(cls, s: str) -> "NodeID":
        """Deterministic ID via SHA-1 of a string (e.g. host:port or a task path)."""
        digest = hashlib.sha1(s.encode("utf-8")).digest()
        return cls.from_bytes(digest)

    @classmethod
    def from_hex(cls, h: str) -> "NodeID":
        return cls(int(h, 16))

    # ---- representation ----------------------------------------------

    def hex(self) -> str:
        return f"{self.value:040x}"

    def short(self) -> str:
        return self.hex()[:8]

    def __eq__(self, other: object) -> bool:
        return isinstance(other, NodeID) and other.value == self.value

    def __hash__(self) -> int:
        return hash(self.value)

    def __repr__(self) -> str:
        return f"NodeID({self.short()})"


def xor_distance(a: NodeID, b: NodeID) -> int:
    """The XOR distance between two IDs (smaller = closer)."""
    return a.value ^ b.value


def shared_prefix_len(a: NodeID, b: NodeID) -> int:
    """Number of leading bits shared by ``a`` and ``b`` (the bucket index)."""
    d = a.value ^ b.value
    if d == 0:
        return ID_BITS
    return ID_BITS - 1 - (d.bit_length() - 1)


def task_key(path_repr: str) -> NodeID:
    """Map a search-tree path to a key in the SAME XOR space as node IDs.

    This is the bridge between the Solver and the DHT: the peer(s) closest to
    ``task_key(path)`` become the natural owner(s) of that subtask.
    """
    return NodeID.from_string("task:" + path_repr)
