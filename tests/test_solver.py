"""Tests for the self-contained solver core (no networking)."""

from swarmsolve.puzzles import generate, parse_grid
from swarmsolve.solver.board import Board
from swarmsolve.solver.search import expand_subtasks, solve_local, solve_subtree

EASY = """
5 3 . . 7 . . . .
6 . . 1 9 5 . . .
. 9 8 . . . . 6 .
8 . . . 6 . . . 3
4 . . 8 . 3 . . 1
7 . . . 2 . . . 6
. 6 . . . . 2 8 .
. . . 4 1 9 . . 5
. . . . 8 . . 7 9
"""

HARD = """
. . . . . . . . .
. . . . . 3 . 8 5
. . 1 . 2 . . . .
. . . 5 . 7 . . .
. . 4 . . . 1 . .
. 9 . . . . . . .
5 . . . . . . 7 3
. . 2 . 1 . . . .
. . . . 4 . . . 9
"""


def _valid_solution(board: Board) -> bool:
    n, box = board.n, board.box
    grid = board.to_grid()
    full = set(range(1, n + 1))
    for r in range(n):
        if set(grid[r]) != full:
            return False
    for c in range(n):
        if {grid[r][c] for r in range(n)} != full:
            return False
    for br in range(0, n, box):
        for bc in range(0, n, box):
            vals = {grid[br + i][bc + j] for i in range(box) for j in range(box)}
            if vals != full:
                return False
    return True


def test_solve_easy_9x9():
    board = Board.from_grid(parse_grid(EASY))
    res = solve_local(board)
    assert res.solved
    assert _valid_solution(res.board)


def test_subtasks_cover_solution():
    """Union of subtree searches must still find the solution."""
    board = Board.from_grid(parse_grid(HARD))  # hard puzzle => real branching
    children = expand_subtasks(board, [])
    assert len(children) >= 2  # root branches into multiple subtasks
    found = any(solve_subtree(board, path).solved for path in children)
    assert found


def test_generate_is_solvable():
    for n in (9, 16):
        puzzle = generate(n, seed=1)
        assert solve_local(puzzle).solved


def test_flat_roundtrip():
    board = Board.from_grid(parse_grid(EASY))
    flat = board.to_flat()
    rebuilt = Board.from_flat(board.n, flat)
    assert rebuilt.to_flat() == flat
