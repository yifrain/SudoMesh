"""Task model: a subtree of the search space identified by an assignment path.

A path like ``[(12, 4), (37, 9)]`` means "from the root puzzle, fix cell 12 = 4
then cell 37 = 9, and explore everything below". The *canonical string* of the
path is hashed into the XOR keyspace (``task_key``) for DHT placement and for
gossip de-duplication.

Beyond placement, a Task also carries the *tree bookkeeping* needed to decide
that a puzzle is **unsolvable**: a task that is expanded into ``children`` moves
to ``DONE_SPLIT``; a leaf branch that is searched to exhaustion with no solution
moves to ``DONE_EXHAUSTED``. A parent counts how many of its children reached
``DONE_EXHAUSTED`` (``children_exhausted``); when that equals ``len(children)``
the parent itself is exhausted and reports up to ``parent_id``. When the root
(``parent_id is None``) becomes exhausted, the whole puzzle has no solution.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum

from swarmsolve.discovery.node_id import NodeID, task_key
from swarmsolve.solver.search import Path


class TaskStatus(str, Enum):
    OPEN = "OPEN"                       # not yet claimed
    CLAIMED = "CLAIMED"                 # leased by some peer, in progress
    DONE = "DONE"                       # (legacy) fully explored, no solution
    DONE_SPLIT = "DONE_SPLIT"          # expanded into children; parent ends lease
    DONE_EXHAUSTED = "DONE_EXHAUSTED"  # branch exhausted -> unsolvable
    DEAD = "DEAD"                      # proven contradictory (pruned)


def path_repr(path: Path) -> str:
    """Canonical, order-independent string for a path (so dups collapse)."""
    return ";".join(f"{idx}={val}" for idx, val in sorted(path))


@dataclass
class Task:
    path: Path
    status: TaskStatus = TaskStatus.OPEN
    owner: str | None = None          # NodeID hex of current lessee (a.k.a. holder)
    lease_expires: float = 0.0        # epoch seconds; 0 = no lease (a.k.a. lease_expire)
    # ---- tree bookkeeping (for unsolvable detection) ------------------
    children: list[str] = field(default_factory=list)   # child task ids ([] = leaf)
    children_exhausted: int = 0                          # children reported EXHAUSTED
    parent_id: str | None = None                         # parent task id (None = root)
    depth: int = field(init=False, default=0)

    def __post_init__(self) -> None:
        self.depth = len(self.path)

    @property
    def id(self) -> str:
        return path_repr(self.path)

    @property
    def key(self) -> NodeID:
        """Position of this task in the shared XOR keyspace."""
        return task_key(self.id)

    def lease_active(self, now: float | None = None) -> bool:
        now = now or time.time()
        return self.status == TaskStatus.CLAIMED and self.lease_expires > now

    def all_children_exhausted(self) -> bool:
        """True once every child has reported DONE_EXHAUSTED (and there is >=1)."""
        return bool(self.children) and self.children_exhausted >= len(self.children)

    # ---- wire format --------------------------------------------------

    def to_dict(self) -> dict:
        return {
            "path": self.path,
            "status": self.status.value,
            "owner": self.owner,
            "lease_expires": self.lease_expires,
            "children": self.children,
            "children_exhausted": self.children_exhausted,
            "parent_id": self.parent_id,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Task":
        return cls(
            path=[tuple(p) for p in d["path"]],
            status=TaskStatus(d.get("status", "OPEN")),
            owner=d.get("owner"),
            lease_expires=d.get("lease_expires", 0.0),
            children=list(d.get("children", [])),
            children_exhausted=int(d.get("children_exhausted", 0)),
            parent_id=d.get("parent_id"),
        )
