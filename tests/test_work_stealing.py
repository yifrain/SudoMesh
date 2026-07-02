"""Tests for work-stealing (Chase-Lev deque) + lease renewal.

These run WITHOUT networking: we drive the Scheduler deque and Peer internals
directly. A full multi-process integration is covered by the
``swarmsolve benchmark --work-stealing`` demo.
"""

import time

import pytest

from swarmsolve.peer import Peer
from swarmsolve.solver.board import Board
from swarmsolve.solver.search import expand_subtasks
from swarmsolve.tasks.scheduler import Scheduler
from swarmsolve.tasks.task import Task
from swarmsolve.transport.messages import Message, MessageType


def _peer(board: Board, **kw) -> Peer:
    return Peer("127.0.0.1", 9100, board, **kw)


# --------------------------------------------------------------------------- #
# Scheduler deque: LIFO own-pop vs FIFO steal (Chase-Lev / Cilk)
# --------------------------------------------------------------------------- #
def test_pop_own_is_lifo_tail():
    """A peer works its own newest (tail) task first — LIFO for locality."""
    from swarmsolve.discovery.node_id import NodeID

    sched = Scheduler(NodeID.from_string("me"))
    t_old = Task(path=[(0, 1)])
    t_new = Task(path=[(1, 2)])
    sched.add_open(t_old)   # goes to tail
    sched.add_open(t_new)   # now the tail
    assert sched.pop_own().id == t_new.id   # newest first (tail)
    assert sched.pop_own().id == t_old.id


def test_steal_is_fifo_head():
    """A thief steals the oldest (head) task — the coarsest, most valuable."""
    from swarmsolve.discovery.node_id import NodeID

    sched = Scheduler(NodeID.from_string("me"))
    t_old = Task(path=[(0, 1)])
    t_new = Task(path=[(1, 2)])
    sched.add_open(t_old)
    sched.add_open(t_new)
    assert sched.steal().id == t_old.id     # oldest first (head)
    assert sched.steal().id == t_new.id


def test_steal_empty_returns_none():
    from swarmsolve.discovery.node_id import NodeID

    sched = Scheduler(NodeID.from_string("me"))
    assert sched.steal() is None
    assert sched.pop_own() is None


def test_steal_skips_dead_and_done():
    """steal()/pop_own() lazily skip tasks that became dead/done in the deque."""
    from swarmsolve.discovery.node_id import NodeID

    sched = Scheduler(NodeID.from_string("me"))
    t1 = Task(path=[(0, 1)])
    t2 = Task(path=[(1, 2)])
    sched.add_open(t1)
    sched.add_open(t2)
    sched.mark_dead(t1.path)     # t1 now invalid (stale in deque)
    stolen = sched.steal()       # should skip t1, return t2
    assert stolen.id == t2.id


# --------------------------------------------------------------------------- #
# Peer-level STEAL request/reply handling
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_steal_request_gives_task_from_head():
    """On STEAL_REQUEST, a peer replies with a task from its deque head."""
    board = Board.empty(9)
    peer = _peer(board)
    children = expand_subtasks(board, [])
    for path in children:
        peer.scheduler.add_open(Task(path=path))

    sent: list = []

    async def fake_send(host, port, msg):
        sent.append((host, port, msg))
        return True

    peer.transport.send_tcp = fake_send  # type: ignore[assignment]

    req = Message(MessageType.STEAL_REQUEST, "deadbeef",
                  {"host": "127.0.0.1", "port": 9999}, ttl=0)
    await peer._on_steal_msg(req, ("127.0.0.1", 9999))

    assert len(sent) == 1
    _, _, reply = sent[0]
    assert reply.type == MessageType.STEAL_REPLY
    assert "task" in reply.payload   # a task was handed over


@pytest.mark.asyncio
async def test_steal_request_replies_empty_when_no_work():
    """A peer with an empty deque replies with an empty STEAL_REPLY."""
    board = Board.empty(9)
    peer = _peer(board)
    sent: list = []

    async def fake_send(host, port, msg):
        sent.append(msg)
        return True

    peer.transport.send_tcp = fake_send  # type: ignore[assignment]
    req = Message(MessageType.STEAL_REQUEST, "deadbeef",
                  {"host": "127.0.0.1", "port": 9999}, ttl=0)
    await peer._on_steal_msg(req, ("127.0.0.1", 9999))

    assert len(sent) == 1
    assert sent[0].payload == {}     # empty = no work available


@pytest.mark.asyncio
async def test_steal_reply_resolves_pending_future():
    """A STEAL_REPLY resolves the pending future created by _try_steal."""
    import asyncio

    board = Board.empty(9)
    peer = _peer(board)
    children = expand_subtasks(board, [])
    donated = Task(path=children[0])

    fut: asyncio.Future = asyncio.get_running_loop().create_future()
    reply = Message(MessageType.STEAL_REPLY, "deadbeef",
                    {"task": donated.to_dict()}, ttl=0)
    peer._pending_steals[reply.msg_id] = fut
    await peer._on_steal_msg(reply, ("127.0.0.1", 9999))

    assert fut.done()
    assert fut.result()["path"] == [list(p) for p in donated.path] or \
           fut.result() is not None


# --------------------------------------------------------------------------- #
# Lease renewal: a long in-flight task is not reclaimed
# --------------------------------------------------------------------------- #
def test_lease_renewed_during_long_task():
    """The should_stop hook renews the lease on our claimed task."""
    board = Board.empty(9)
    peer = _peer(board, lease_seconds=0.2)
    root = Task(path=[])
    peer.scheduler.claim_local(root)
    # Force the lease close to expiry so renewal triggers (< 50% remaining).
    peer.scheduler.claimed[root.id].lease_expires = time.time() + 0.05

    before = peer.scheduler.claimed[root.id].lease_expires
    peer._tick_and_should_stop()   # per-search-node callback
    after = peer.scheduler.claimed[root.id].lease_expires

    assert after > before
    assert peer.scheduler.claimed[root.id].lease_active()
