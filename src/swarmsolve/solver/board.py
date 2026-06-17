"""Sudoku board with bitmask candidates and constraint propagation.

Supports any N = k*k board (9x9 -> k=3, 16x16 -> k=4, 25x25 -> k=5).

Cells are stored in a flat list of length N*N. Each cell holds a *bitmask*
of still-possible values: bit (v-1) set means value v is still possible.
A cell is "solved" when exactly one bit is set.

This compact representation makes constraint propagation cheap, which is the
foundation that lets us cut the search tree into tasks (see ``search.py``).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field


class Contradiction(Exception):
    """Raised when propagation proves the current partial board is unsolvable."""


def _popcount(x: int) -> int:
    return bin(x).count("1")


def _single_value(mask: int) -> int:
    """Return the value (1-based) of a single-bit mask."""
    return mask.bit_length()


@dataclass
class Board:
    """A Sudoku board of size N x N where N = box * box."""

    n: int
    box: int
    cells: list[int] = field(default_factory=list)  # bitmask per cell
    full_mask: int = 0

    # ---- construction -------------------------------------------------

    @classmethod
    def empty(cls, n: int) -> "Board":
        box = int(round(math.sqrt(n)))
        if box * box != n:
            raise ValueError(f"N={n} is not a perfect square (need N = box*box)")
        full = (1 << n) - 1
        return cls(n=n, box=box, cells=[full] * (n * n), full_mask=full)

    @classmethod
    def from_grid(cls, grid: list[list[int]]) -> "Board":
        """Build from a 2D grid of ints (0 = empty)."""
        n = len(grid)
        board = cls.empty(n)
        for r, row in enumerate(grid):
            for c, val in enumerate(row):
                if val:
                    board.assign(r * n + c, val)
        return board

    def clone(self) -> "Board":
        return Board(n=self.n, box=self.box, cells=list(self.cells), full_mask=self.full_mask)

    # ---- helpers ------------------------------------------------------

    def index(self, row: int, col: int) -> int:
        return row * self.n + col

    def is_solved_cell(self, idx: int) -> bool:
        return _popcount(self.cells[idx]) == 1

    def is_complete(self) -> bool:
        return all(_popcount(m) == 1 for m in self.cells)

    def peers(self, idx: int) -> set[int]:
        """All cells sharing a row, column or box with ``idx`` (excluding itself)."""
        n, box = self.n, self.box
        row, col = divmod(idx, n)
        result: set[int] = set()
        for c in range(n):
            result.add(row * n + c)
        for r in range(n):
            result.add(r * n + col)
        br, bc = (row // box) * box, (col // box) * box
        for r in range(br, br + box):
            for c in range(bc, bc + box):
                result.add(r * n + c)
        result.discard(idx)
        return result

    # ---- core propagation --------------------------------------------

    def assign(self, idx: int, value: int) -> None:
        """Assign ``value`` to cell ``idx`` and propagate constraints.

        Raises ``Contradiction`` if this leads to an impossible state.
        Implements constraint propagation via *elimination* + *naked singles*,
        the standard AC-3-style pruning that shrinks the search tree.
        """
        bit = 1 << (value - 1)
        if not (self.cells[idx] & bit):
            raise Contradiction(f"value {value} not allowed at cell {idx}")
        self.cells[idx] = bit
        # Worklist of cells that just became singletons and must be eliminated.
        queue = [idx]
        while queue:
            cur = queue.pop()
            cur_bit = self.cells[cur]
            for p in self.peers(cur):
                if self.cells[p] & cur_bit:
                    new_mask = self.cells[p] & ~cur_bit
                    if new_mask == 0:
                        raise Contradiction(f"cell {p} has no candidates left")
                    self.cells[p] = new_mask
                    if _popcount(new_mask) == 1:
                        queue.append(p)

    def propagate(self) -> None:
        """Run propagation over all already-solved cells (used after loading)."""
        for idx in range(self.n * self.n):
            if _popcount(self.cells[idx]) == 1:
                # Re-assign to force elimination on peers.
                self.assign(idx, _single_value(self.cells[idx]))

    # ---- search support ----------------------------------------------

    def most_constrained_cell(self) -> int | None:
        """MRV heuristic: return the unsolved cell with the fewest candidates.

        Returns ``None`` if the board is already complete.
        """
        best_idx: int | None = None
        best_count = self.n + 1
        for idx, mask in enumerate(self.cells):
            count = _popcount(mask)
            if count == 1:
                continue
            if count < best_count:
                best_count = count
                best_idx = idx
                if count == 2:  # can't do better among unsolved cells
                    break
        return best_idx

    def candidates(self, idx: int) -> list[int]:
        mask = self.cells[idx]
        return [v for v in range(1, self.n + 1) if mask & (1 << (v - 1))]

    # ---- (de)serialization -------------------------------------------

    def to_grid(self) -> list[list[int]]:
        grid: list[list[int]] = []
        for r in range(self.n):
            row = []
            for c in range(self.n):
                mask = self.cells[r * self.n + c]
                row.append(_single_value(mask) if _popcount(mask) == 1 else 0)
            grid.append(row)
        return grid

    def to_flat(self) -> list[int]:
        """Flat list of assigned values (0 = unsolved). Used for wire transfer."""
        return [
            _single_value(m) if _popcount(m) == 1 else 0
            for m in self.cells
        ]

    @classmethod
    def from_flat(cls, n: int, flat: list[int]) -> "Board":
        board = cls.empty(n)
        for idx, val in enumerate(flat):
            if val:
                board.assign(idx, val)
        return board

    def __str__(self) -> str:
        grid = self.to_grid()
        width = len(str(self.n))
        lines = []
        for r in range(self.n):
            if r and r % self.box == 0:
                lines.append("")
            cells = []
            for c in range(self.n):
                if c and c % self.box == 0:
                    cells.append("|")
                v = grid[r][c]
                cells.append(str(v).rjust(width) if v else "." * width)
            lines.append(" ".join(cells))
        return "\n".join(lines)
