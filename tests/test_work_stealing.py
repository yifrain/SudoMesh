"""Tests for work-stealing (work-donation) + lease renewal.

These run WITHOUT networking: we drive Peer internals directly to verify the
donation logic and lease-renewal behave correctly. A full multi-process
integration is covered by the `swarmsolve benchmark --work-stealing` demo.
"""

import pytest

from swarmsolve.peer import Peer
from swarmsolve.solver.board import Board
from swarmsolve.solver.search import expand_subtasks
from swarmsolve.tasks.task import Task


def _peer(board: Board, **kw) -> Peer:
    return Peer("127.0.0.1", 9100, board, **kw)


def test_try_donate_splits_inflight_and_keeps_one_child():
    """Donating should: retire the parent, keep one child, return another."""
    board = Board.empty(9)
    peer = _peer(board)
    # Plant a fake in-flight task at the root (depth 0 -> splittable).
    root = Task(path=[])
    peer.scheduler.claim_local(root)
    peer._inflight[root.id] = 0.0

    donated = peer._try_donate()

    assert donated is not None
    # Parent is retired (covered by children) -> no overlap with kept/donated.
    assert root.id in peer.scheduler.done
    # We kept exactly one child in-flight.
    assert len(peer._inflight) == 1
    # Donated child is a genuine child of the root.
    children = expand_subtasks(board, [])
    donated_paths = {tuple(p) for p in children}
    assert tuple(donated.path) in donated_paths


def test_try_donate_returns_none_when_idle():
    """An idle peer (nothing in-flight) has nothing to donate."""
    board = Board.empty(9)
    peer = _peer(board)
    assert peer._try_donate() is None


def test_try_donate_returns_none_for_unsplittable_leaf():
    """A leaf task (no children) cannot be donated."""
    from swarmsolve.puzzles import full_solution

    full = Board.from_grid(full_solution(9, seed=1))
    peer_full = _peer(full)
    leaf = Task(path=[])
    peer_full.scheduler.claim_local(leaf)
    peer_full._inflight[leaf.id] = 0.0
    assert peer_full._try_donate() is None


def test_lease_renewed_during_long_task():
    """The should_stop hook renews the lease on in-flight tasks."""
    board = Board.empty(9)
    peer = _peer(board, lease_seconds=0.1)
    root = Task(path=[])
    peer.scheduler.claim_local(root)
    peer._inflight[root.id] = 0.0

    original_expiry = peer.scheduler.claimed[root.id].lease_expires
    # Simulate the per-node callback that runs during DFS.
    peer._tick_and_should_stop()
    renewed_expiry = peer.scheduler.claimed[root.id].lease_expires

    assert renewed_expiry > original_expiry
    assert peer.scheduler.claimed[root.id].lease_active()


@pytest.mark.asyncio
async def test_work_donate_message_adopts_task():
    """A WORK_DONATE message should add the donated task to our open pool."""
    from swarmsolve.transport.messages import Message, MessageType

    board = Board.empty(9)
    peer = _peer(board)
    children = expand_subtasks(board, [])
    donated_task = Task(path=children[0])

    msg = Message(
        MessageType.WORK_DONATE, "deadbeef",
        {"task": donated_task.to_dict()}, ttl=0,
    )
    await peer._on_work_steal(msg, ("127.0.0.1", 9999))
    assert donated_task.id in peer.scheduler.open


@pytest.mark.asyncio
async def test_work_donate_ignores_already_done_task():
    """A WORK_DONATE for a task we already finished should be ignored."""
    from swarmsolve.transport.messages import Message, MessageType

    board = Board.empty(9)
    peer = _peer(board)
    children = expand_subtasks(board, [])
    donated_task = Task(path=children[0])
    peer.scheduler.mark_done(donated_task.path)  # pretend we finished it

    msg = Message(
        MessageType.WORK_DONATE, "deadbeef",
        {"task": donated_task.to_dict()}, ttl=0,
    )
    await peer._on_work_steal(msg, ("127.0.0.1", 9999))
    # Not re-added to open (already done).
    assert donated_task.id not in peer.scheduler.open
