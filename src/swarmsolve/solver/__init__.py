"""Sudoku solving engine: board representation, constraint propagation, DFS search.

This package is fully self-contained and can run **without any networking**,
which makes it the easiest part to unit-test and to demo first.
"""

from swarmsolve.solver.board import Board, Contradiction
from swarmsolve.solver.search import (
    SearchResult,
    expand_subtasks,
    solve_local,
    solve_subtree,
)

__all__ = [
    "Board",
    "Contradiction",
    "SearchResult",
    "expand_subtasks",
    "solve_local",
    "solve_subtree",
]
