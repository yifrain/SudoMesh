"""Puzzle loading and generation helpers.

* ``parse_grid`` reads a simple text format (rows of numbers, ``0`` or ``.`` =
  empty, whitespace/comma separated).
* ``generate`` builds a solvable puzzle of any size N=k*k by first solving an
  empty board (constraint propagation does the heavy lifting) and then removing
  cells. Handy for demoing 16x16 / 25x25 without shipping huge fixtures.
"""

from __future__ import annotations

import math
import random
import re

from swarmsolve.solver.board import Board


def _token_value(tok: str) -> int:
    """A token is empty if it is 0 or made only of '.'/'_' (e.g. '.', '..')."""
    if tok == "0" or set(tok) <= {".", "_"}:
        return 0
    return int(tok)


def parse_grid(text: str) -> list[list[int]]:
    grid: list[list[int]] = []
    for raw in text.strip().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        tokens = re.split(r"[\s,]+", line)
        row = [_token_value(t) for t in tokens if t]
        grid.append(row)
    return grid


def load_board(path: str) -> Board:
    with open(path, encoding="utf-8") as fh:
        return Board.from_grid(parse_grid(fh.read()))


def _pattern(n: int, b: int, r: int, c: int) -> int:
    """A valid Sudoku 'baseline' value (0-based) for cell (r, c)."""
    return (b * (r % b) + r // b + c) % n


def full_solution(n: int, *, seed: int | None = None) -> list[list[int]]:
    """Build a complete valid N x N grid in O(N^2) via pattern + shuffling.

    This is the standard band/stack/row/col/digit shuffling of a baseline
    pattern grid. It needs no search, so it is instant even for 25x25.
    """
    b = int(round(math.sqrt(n)))
    if b * b != n:
        raise ValueError(f"N={n} is not a perfect square")
    rng = random.Random(seed)

    def shuffled_axis() -> list[int]:
        bands = rng.sample(range(b), b)
        order: list[int] = []
        for band in bands:
            for line in rng.sample(range(b), b):
                order.append(band * b + line)
        return order

    rows, cols = shuffled_axis(), shuffled_axis()
    nums = rng.sample(range(1, n + 1), n)  # digit relabeling
    return [[nums[_pattern(n, b, rows[i], cols[j])] for j in range(n)] for i in range(n)]


def generate(n: int, *, clue_ratio: float = 0.35, seed: int | None = None) -> Board:
    """Generate a solvable N x N puzzle (the full grid is a witness solution)."""
    rng = random.Random(seed)
    full = full_solution(n, seed=seed)

    # poke holes, keeping ~clue_ratio of the cells as clues.
    cells = [(r, c) for r in range(n) for c in range(n)]
    rng.shuffle(cells)
    keep = int(n * n * clue_ratio)
    puzzle = [[0] * n for _ in range(n)]
    for r, c in cells[:keep]:
        puzzle[r][c] = full[r][c]
    return Board.from_grid(puzzle)


def make_unsolvable(
    n: int = 9,
    *,
    clue_ratio: float = 0.45,
    seed: int = 0,
    attempts: int = 500,
) -> Board:
    """Construct a puzzle that is *provably unsolvable* but not trivially so.

    Strategy: keep a subset of a valid full grid as clues, then corrupt ONE of
    them to a different value. Boards whose clues immediately conflict are
    skipped (we want unsolvability that only shows up after search). Each
    candidate is verified with ``solve_local`` so the returned board is
    guaranteed to have no solution. Used to demo/test unsolvable detection.
    """
    from swarmsolve.solver.board import Contradiction
    from swarmsolve.solver.search import solve_local

    for s in range(seed, seed + attempts):
        rng = random.Random((s + 1) * 2654435761 % (2**32))
        full = full_solution(n, seed=s)
        cells = [(r, c) for r in range(n) for c in range(n)]
        rng.shuffle(cells)
        keep = max(1, int(n * n * clue_ratio))
        kept = cells[:keep]
        puzzle = [[0] * n for _ in range(n)]
        for r, c in kept:
            puzzle[r][c] = full[r][c]
        # Corrupt one kept clue to a value different from the true solution.
        r, c = kept[0]
        wrong = rng.choice([v for v in range(1, n + 1) if v != full[r][c]])
        puzzle[r][c] = wrong
        try:
            board = Board.from_grid(puzzle)
        except Contradiction:
            continue  # immediate conflict -> not interesting; try another
        if not solve_local(board).solved:
            return board  # verified: no solution
    raise RuntimeError(
        "could not construct an unsolvable puzzle; increase attempts/clue_ratio"
    )

