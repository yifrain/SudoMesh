"""Search-tree exploration: local DFS solving + subtree splitting for distribution.

Core idea
---------
The whole solution space is a *search tree*. The root is the initial puzzle.
Each node picks the most-constrained unsolved cell (MRV heuristic) and branches
on its candidate values. A *subtask* is identified by the **path of assignments**
taken from the root, e.g. ``[(cell=12, val=4), (cell=37, val=9)]``.

This module gives three primitives the distributed layer builds on:

* ``solve_subtree``  -> explore ONE subtree (a peer's unit of work).
* ``expand_subtasks`` -> split the root (or any node) into child subtasks.
* ``solve_local``    -> single-machine baseline (for speedup comparison).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from swarmsolve.solver.board import Board, Contradiction

# A path of (cell_index, value) assignments from the root of the search tree.
Assignment = tuple[int, int]
Path = list[Assignment]


@dataclass
class SearchStats:
    nodes_expanded: int = 0
    dead_ends: int = 0
    solutions: int = 0


@dataclass
class SearchResult:
    solved: bool
    board: Board | None = None
    stats: SearchStats = field(default_factory=SearchStats)
    # Frontier of unexplored subtasks discovered while expanding (for splitting).
    frontier: list[Path] = field(default_factory=list)


def apply_path(initial: Board, path: Path) -> Board:
    """Clone the initial board and apply an assignment path. May raise Contradiction."""
    board = initial.clone()
    for idx, val in path:
        board.assign(idx, val)
    return board


def expand_subtasks(initial: Board, path: Path) -> list[Path]:
    """Split the subtree rooted at ``path`` into child subtasks.

    Returns one child path per candidate of the most-constrained cell.
    Candidates that immediately contradict are pruned (not returned).
    This is what the Task layer calls to generate distributable OpenTasks.
    """
    try:
        board = apply_path(initial, path)
    except Contradiction:
        return []  # whole subtree is a dead end
    cell = board.most_constrained_cell()
    if cell is None:
        return []  # already complete; nothing to split
    children: list[Path] = []
    for val in board.candidates(cell):
        # Cheap feasibility check before emitting a child task.
        try:
            board.clone().assign(cell, val)
        except Contradiction:
            continue
        children.append(path + [(cell, val)])
    return children


def solve_subtree(
    initial: Board,
    path: Path,
    *,
    is_dead_end=None,
    record_dead_end=None,
    should_stop=None,
    node_delay: float = 0.0,
    enumerate_all: bool = False,
) -> SearchResult:
    """Explore the subtree rooted at ``path`` with depth-first search.

    Hooks (all optional) let the distributed layer plug in shared pruning:

    * ``is_dead_end(path) -> bool``     : skip subtrees others proved invalid.
    * ``record_dead_end(path)``         : publish a newly found dead end (gossip).
    * ``should_stop() -> bool``         : cooperative cancel (e.g. solution found
                                          elsewhere, or peer is shutting down).

    ``node_delay`` adds an artificial per-node cost (seconds). It is purely a
    DEMO knob: real Sudoku nodes are too cheap to show network effects, so we
    use it as a stand-in for "expensive" work (e.g. 25x25 / jigsaw search) when
    measuring parallel speedup, fault recovery, or live dashboards.
    """
    stats = SearchStats()

    try:
        root = apply_path(initial, path)
    except Contradiction:
        stats.dead_ends += 1
        if record_dead_end:
            record_dead_end(path)
        return SearchResult(solved=False, stats=stats)

    found: dict[str, Board | None] = {"board": None}

    def dfs(board: Board, cur_path: Path) -> Board | None:
        if should_stop and should_stop():
            return None
        if is_dead_end and is_dead_end(cur_path):
            return None
        stats.nodes_expanded += 1
        if node_delay:
            time.sleep(node_delay)  # demo-only stand-in for expensive work

        cell = board.most_constrained_cell()
        if cell is None:
            stats.solutions += 1
            if found["board"] is None:
                found["board"] = board
            if enumerate_all:
                return None  # keep enumerating the rest of the tree
            return board  # complete -> solution

        for val in board.candidates(cell):
            child = board.clone()
            try:
                child.assign(cell, val)
            except Contradiction:
                stats.dead_ends += 1
                continue
            sol = dfs(child, cur_path + [(cell, val)])
            if sol is not None:
                return sol
        # exhausted all candidates with no solution -> this node is a dead end
        stats.dead_ends += 1
        if record_dead_end:
            record_dead_end(cur_path)
        return None

    solution = dfs(root, path)
    if enumerate_all:
        return SearchResult(solved=stats.solutions > 0, board=found["board"], stats=stats)
    return SearchResult(solved=solution is not None, board=solution, stats=stats)


def solve_local(
    initial: Board, *, node_delay: float = 0.0, enumerate_all: bool = False
) -> SearchResult:
    """Single-machine solve from the root. Baseline for measuring P2P speedup."""
    return solve_subtree(initial, [], node_delay=node_delay, enumerate_all=enumerate_all)
