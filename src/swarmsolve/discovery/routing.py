"""Kademlia routing table: k-buckets indexed by shared-prefix length.

Each bucket holds up to ``k`` contacts. Buckets are ordered most-recently-seen
last (LRU): when a bucket is full we keep the *oldest still-alive* contact,
which is what gives Kademlia its eclipse-attack resistance (long-lived peers
are preferred over fresh, possibly-malicious ones).

Owner: Person B.
"""

from __future__ import annotations

from dataclasses import dataclass

from swarmsolve.discovery.node_id import (
    ID_BITS,
    NodeID,
    shared_prefix_len,
    xor_distance,
)

K = 8  # bucket size (Kademlia replication parameter)


@dataclass(frozen=True)
class Contact:
    """A reachable peer: its NodeID + network address."""

    node_id: NodeID
    host: str
    port: int

    def to_dict(self) -> dict:
        return {"id": self.node_id.hex(), "host": self.host, "port": self.port}

    @classmethod
    def from_dict(cls, d: dict) -> "Contact":
        return cls(NodeID.from_hex(d["id"]), d["host"], int(d["port"]))


class RoutingTable:
    """A simple list-of-buckets routing table."""

    def __init__(self, self_id: NodeID, k: int = K) -> None:
        self.self_id = self_id
        self.k = k
        self.buckets: list[list[Contact]] = [[] for _ in range(ID_BITS)]

    def _bucket_index(self, node_id: NodeID) -> int:
        return shared_prefix_len(self.self_id, node_id)

    def add(self, contact: Contact) -> None:
        """Insert/refresh a contact (LRU within its bucket)."""
        if contact.node_id == self.self_id:
            return
        bucket = self.buckets[self._bucket_index(contact.node_id)]
        for i, c in enumerate(bucket):
            if c.node_id == contact.node_id:
                bucket.pop(i)
                bucket.append(contact)  # move to most-recently-seen
                return
        if len(bucket) < self.k:
            bucket.append(contact)
        # If full, Kademlia would ping the LRU contact and evict if dead.
        # For the MVP we simply keep the existing (older, trusted) contacts.

    def remove(self, node_id: NodeID) -> None:
        bucket = self.buckets[self._bucket_index(node_id)]
        self.buckets[self._bucket_index(node_id)] = [
            c for c in bucket if c.node_id != node_id
        ]

    def closest(self, target: NodeID, count: int | None = None) -> list[Contact]:
        """Return up to ``count`` contacts closest to ``target`` by XOR distance."""
        count = count or self.k
        everyone = [c for bucket in self.buckets for c in bucket]
        everyone.sort(key=lambda c: xor_distance(c.node_id, target))
        return everyone[:count]

    def all_contacts(self) -> list[Contact]:
        return [c for bucket in self.buckets for c in bucket]

    def size(self) -> int:
        return sum(len(b) for b in self.buckets)
