"""Tests for the Kademlia XOR keyspace + routing table."""

from swarmsolve.discovery.node_id import (
    NodeID,
    shared_prefix_len,
    task_key,
    xor_distance,
)
from swarmsolve.discovery.routing import Contact, RoutingTable


def test_xor_distance_symmetry_and_zero():
    a = NodeID.random()
    b = NodeID.random()
    assert xor_distance(a, b) == xor_distance(b, a)
    assert xor_distance(a, a) == 0


def test_shared_prefix_len_self():
    a = NodeID(0b1010)
    assert shared_prefix_len(a, a) == 160


def test_task_key_deterministic():
    assert task_key("12=4;37=9") == task_key("12=4;37=9")
    assert task_key("12=4") != task_key("12=5")


def test_routing_returns_closest():
    me = NodeID.from_string("me")
    table = RoutingTable(me)
    contacts = [Contact(NodeID.from_string(f"n{i}"), "127.0.0.1", 9000 + i) for i in range(20)]
    for c in contacts:
        table.add(c)
    target = NodeID.from_string("n3")
    closest = table.closest(target, 3)
    # the exact match (if present) should be the very closest
    dists = [xor_distance(c.node_id, target) for c in closest]
    assert dists == sorted(dists)
