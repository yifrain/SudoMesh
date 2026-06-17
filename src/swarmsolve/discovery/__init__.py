"""Decentralized peer discovery via a Kademlia DHT.

Highlights mapped to the course (Chapter 6, Kademlia):
    * 160-bit IDs in a single XOR metric space.
    * k-buckets keep long-lived peers (eclipse-attack resistant).
    * iterative FIND_NODE narrows the distance logarithmically.

We reuse the SAME XOR keyspace for *task IDs*, so each subtask deterministically
maps to the peers closest to its key -> structured, dedup-friendly task placement.

Owner: Person B.
"""

from swarmsolve.discovery.node_id import ID_BITS, NodeID, xor_distance
from swarmsolve.discovery.routing import Contact, RoutingTable

__all__ = ["ID_BITS", "NodeID", "xor_distance", "Contact", "RoutingTable"]
