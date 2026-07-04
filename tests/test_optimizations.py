"""Tests for the optimization features: unsolvable detection, task tree
bookkeeping, and the pull-based scheduler helpers."""

import asyncio

from swarmsolve.discovery.node_id import NodeID
from swarmsolve.peer import Peer
from swarmsolve.puzzles import make_unsolvable
from swarmsolve.solver.board import Board
from swarmsolve.solver.search import solve_local
from swarmsolve.tasks.scheduler import Scheduler
from swarmsolve.tasks.task import Task, TaskStatus


# --------------------------------------------------------------------------- #
# Task model: new tree-bookkeeping fields
# --------------------------------------------------------------------------- #
def test_task_new_fields_roundtrip():
    t = Task(
        path=[(12, 4), (37, 9)],
        parent_id="12=4",
        children=["12=4;37=9;5=1", "12=4;37=9;5=2"],
        children_exhausted=1,
        status=TaskStatus.DONE_SPLIT,
    )
    d = t.to_dict()
    back = Task.from_dict(d)
    assert back.parent_id == "12=4"
    assert back.children == t.children
    assert back.children_exhausted == 1
    assert back.status == TaskStatus.DONE_SPLIT


def test_task_defaults_backward_compatible():
    # Old-style dict without the new fields must still deserialize.
    old = {"path": [[1, 2]], "status": "OPEN", "owner": None, "lease_expires": 0.0}
    t = Task.from_dict(old)
    assert t.children == []
    assert t.children_exhausted == 0
    assert t.parent_id is None


def test_all_children_exhausted_predicate():
    t = Task(path=[], children=["a", "b", "c"])
    assert not t.all_children_exhausted()
    t.children_exhausted = 3
    assert t.all_children_exhausted()
    # a leaf (no children) is never "all children exhausted"
    leaf = Task(path=[])
    assert not leaf.all_children_exhausted()


# --------------------------------------------------------------------------- #
# Scheduler: split / exhaust aggregation
# --------------------------------------------------------------------------- #
def test_scheduler_bottom_up_single_level():
    sched = Scheduler(NodeID.from_string("me"))
    root = Task(path=[], parent_id=None, children=["a", "b"],
                status=TaskStatus.DONE_SPLIT)
    sched.mark_split(root)
    assert root.id == ""  # empty path -> root id
    assert sched.note_child_exhausted("", "a") is False
    assert sched.note_child_exhausted("", "a") is False   # idempotent
    assert sched.note_child_exhausted("", "b") is True     # all exhausted now
    # counter reflects exactly two distinct children
    assert sched.split[""].children_exhausted == 2


def test_scheduler_report_for_unknown_parent_is_noop():
    sched = Scheduler(NodeID.from_string("me"))
    # No split record for this parent yet -> can't complete.
    assert sched.note_child_exhausted("unknown", "child") is False


def test_scheduler_add_open_rejects_exhausted():
    sched = Scheduler(NodeID.from_string("me"))
    task = Task(path=[(1, 2)])
    sched.mark_exhausted(task.path)
    assert sched.add_open(Task(path=[(1, 2)])) is False


def test_scheduler_offer_open_task():
    sched = Scheduler(NodeID.from_string("me"))
    assert sched.offer_open_task() is None
    sched.add_open(Task(path=[(1, 2)]))
    offered = sched.offer_open_task()
    assert offered is not None and offered.id == "1=2"


# --------------------------------------------------------------------------- #
# make_unsolvable: verified no-solution board
# --------------------------------------------------------------------------- #
def test_make_unsolvable_has_no_solution():
    for seed in (0, 1, 2):
        board = make_unsolvable(9, seed=seed)
        assert not solve_local(board).solved


# --------------------------------------------------------------------------- #
# Peer: hierarchical unsolvable declaration (no network needed)
# --------------------------------------------------------------------------- #
def test_peer_declares_unsolvable_two_levels():
    board = Board.empty(9)
    peer = Peer("127.0.0.1", 0, board, detect_unsolvable=True, log=lambda *a: None)

    # Build a 2-level tree entirely in the peer's scheduler:
    #   root("") -> children [A, B]
    #   A        -> children [A1, A2]
    root = Task(path=[], parent_id=None, children=["A", "B"],
                status=TaskStatus.DONE_SPLIT)
    node_a = Task(path=[(1, 1)], parent_id="", children=["A1", "A2"],
                  status=TaskStatus.DONE_SPLIT)
    assert node_a.id == "1=1"
    root.children = [node_a.id, "B"]  # A's real id
    peer.scheduler.mark_split(root)
    peer.scheduler.mark_split(node_a)

    async def drive():
        # exhaust A's two children -> A becomes exhausted -> rolls up to root
        await peer._on_exhausted_report(node_a.id, "A1")
        assert not peer.unsolvable
        await peer._on_exhausted_report(node_a.id, "A2")   # A now exhausted
        assert not peer.unsolvable                          # root still needs B
        await peer._on_exhausted_report("", "B")            # root exhausted
    asyncio.run(drive())

    assert peer.unsolvable is True


def test_peer_root_leaf_exhausted_is_unsolvable():
    board = Board.empty(9)
    peer = Peer("127.0.0.1", 0, board, detect_unsolvable=True, log=lambda *a: None)
    # A task whose parent is None IS the root; exhausting it => unsolvable.
    root_leaf = Task(path=[], parent_id=None)

    async def drive():
        await peer._report_exhausted(root_leaf)
    asyncio.run(drive())

    assert peer.unsolvable is True


# --------------------------------------------------------------------------- #
# Work stealing: idle peers steal from a busy peer's deque
# --------------------------------------------------------------------------- #
def test_steal_from_deque_gives_head_and_keeps_one():
    from collections import deque

    board = Board.empty(9)
    peer = Peer("127.0.0.1", 0, board, steal=True, log=lambda *a: None)

    # Nothing to steal when deque is empty or holds a single (our own) branch.
    assert peer._steal_from_deque() is None
    peer._steal_deque = deque([[(0, 1)]])
    assert peer._steal_from_deque() is None  # keep at least one for ourselves

    # With >1 branches, a thief gets the HEAD (shallowest/coarsest) one, and it
    # is removed from our deque (no duplication).
    peer._steal_deque = deque([[(0, 1)], [(0, 2)], [(0, 3)]])
    stolen = peer._steal_from_deque()
    assert stolen is not None
    assert stolen.path == [(0, 1)]
    assert list(peer._steal_deque) == [[(0, 2)], [(0, 3)]]


def test_handle_pull_offers_stolen_task(monkeypatch):
    from collections import deque

    board = Board.empty(9)
    peer = Peer("127.0.0.1", 0, board, steal=True, log=lambda *a: None)
    peer._steal_deque = deque([[(0, 1)], [(0, 2)]])

    sent = {}

    async def fake_send_tcp(host, port, msg):
        sent["host"] = host
        sent["msg"] = msg
        return True

    monkeypatch.setattr(peer.transport, "send_tcp", fake_send_tcp)

    from swarmsolve.transport.messages import Message, MessageType
    query = Message(MessageType.TASK_QUERY, "thief",
                    {"host": "127.0.0.1", "port": 9999})

    asyncio.run(peer._handle_pull(query))

    assert sent["msg"].type == MessageType.TASK_OFFER
    assert sent["msg"].payload["task"]["path"] == [(0, 1)]
    # the offered branch was removed from our deque
    assert list(peer._steal_deque) == [[(0, 2)]]


# --------------------------------------------------------------------------- #
# Search-space estimation
# --------------------------------------------------------------------------- #
def test_estimate_subtree_size_monotonic():
    from swarmsolve.puzzles import generate
    from swarmsolve.solver.search import estimate_board_size, estimate_subtree_size

    puzzle = generate(9, seed=3, clue_ratio=0.3)
    empty = estimate_board_size(Board.empty(9))
    full = estimate_board_size(puzzle)
    # more clues => fewer free candidates => smaller estimated tree
    assert empty > full >= 0.0
    # fixing a cell can only shrink (or keep) the estimate vs the empty board
    cell = puzzle.most_constrained_cell()
    if cell is not None:
        val = puzzle.candidates(cell)[0]
        assert estimate_subtree_size(puzzle, [(cell, val)]) <= full + 1e-9


def test_steal_prefers_larger_subtree(monkeypatch):
    from collections import deque
    from swarmsolve.solver import search as searchmod

    board = Board.empty(9)
    peer = Peer("127.0.0.1", 0, board, steal=True, steal_scan=8, log=lambda *a: None)
    # three stealable branches (+1 kept); make the middle one "heaviest"
    peer._steal_deque = deque([[(0, 1)], [(0, 2)], [(0, 3)], [(0, 9)]])
    scores = {"0=1": 1.0, "0=2": 5.0, "0=3": 2.0, "0=9": 0.5}

    def fake_estimate(_board, path):
        from swarmsolve.tasks.task import path_repr
        return scores.get(path_repr(path), 0.0)

    monkeypatch.setattr(peer, "board", board)
    monkeypatch.setattr(searchmod, "estimate_subtree_size", fake_estimate)
    # peer.py imported the symbol directly, so patch it there too
    import swarmsolve.peer as peermod
    monkeypatch.setattr(peermod, "estimate_subtree_size", fake_estimate)

    stolen = peer._steal_from_deque()
    assert stolen is not None
    assert stolen.path == [(0, 2)]  # the heaviest branch


# --------------------------------------------------------------------------- #
# Periodic state sync + crash recovery
# --------------------------------------------------------------------------- #
def test_backup_record_and_take():
    sched = Scheduler(NodeID.from_string("me"))
    assert sched.take_backup_frontier("t") is None
    sched.record_backup("t", [[(1, 2)], [(3, 4)]], nodes=7)
    frontier = sched.take_backup_frontier("t")
    assert frontier == [[(1, 2)], [(3, 4)]]
    # consumed -> gone
    assert sched.take_backup_frontier("t") is None


def test_pick_task_resumes_from_backup():
    import time as _time

    board = Board.empty(9)
    peer = Peer("127.0.0.1", 0, board, steal=True, log=lambda *a: None)
    sched = peer.scheduler

    # Simulate: some peer claimed a coarse task, then crashed (lease expired),
    # but we hold a backup snapshot of its unexplored frontier.
    coarse = Task(path=[(0, 5)])
    coarse.owner = "deadpeer"
    coarse.lease_expires = _time.time() - 1.0  # already expired
    sched.note_claim(coarse)                    # -> claimed
    sched.record_backup(coarse.id, [[(0, 5), (1, 1)], [(0, 5), (1, 2)]], nodes=3)

    picked = peer._pick_task()

    # The coarse whole-task must NOT be what we resume; we resume its frontier.
    assert picked is not None
    open_ids = set(sched.open.keys())
    assert "0=5;1=1" in open_ids
    assert "0=5;1=2" in open_ids
    assert coarse.id not in open_ids  # replaced by the finer frontier
    assert sched.take_backup_frontier(coarse.id) is None  # snapshot consumed


