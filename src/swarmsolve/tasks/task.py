"""Task model: a subtree of the search space identified by an assignment path.

A path like ``[(12, 4), (37, 9)]`` means "from the root puzzle, fix cell 12 = 4
then cell 37 = 9, and explore everything below". The *canonical string* of the
path is hashed into the XOR keyspace (``task_key``) for DHT placement and for
gossip de-duplication.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum

from swarmsolve.discovery.node_id import NodeID, task_key
from swarmsolve.solver.search import Path


class TaskStatus(str, Enum):
    OPEN = "OPEN"            # not yet claimed
    CLAIMED = "CLAIMED"      # leased by some peer, in progress
    DONE = "DONE"           # fully explored, no solution inside
    DEAD = "DEAD"           # proven contradictory (pruned)


def path_repr(path: Path) -> str:
    """Canonical, order-independent string for a path (so dups collapse)."""
    return ";".join(f"{idx}={val}" for idx, val in sorted(path))


@dataclass
class Task:
    path: Path
    status: TaskStatus = TaskStatus.OPEN
    owner: str | None = None          # NodeID hex of current lessee
    lease_expires: float = 0.0        # epoch seconds; 0 = no lease
    # Parent peer that split & dispatched this task.  When the child proves
    # this path is a dead end, it reports back directly to the parent via TCP
    # (point-to-point) instead of gossiping the dead end to the whole network.
    parent_host: str | None = None
    parent_port: int | None = None
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

    # ---- wire format --------------------------------------------------

    def to_dict(self) -> dict:
        return {
            "path": self.path,
            "status": self.status.value,
            "owner": self.owner,
            "lease_expires": self.lease_expires,
            "parent_host": self.parent_host,
            "parent_port": self.parent_port,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Task":
        return cls(
            path=[tuple(p) for p in d["path"]],
            status=TaskStatus(d.get("status", "OPEN")),
            owner=d.get("owner"),
            lease_expires=d.get("lease_expires", 0.0),
            parent_host=d.get("parent_host"),
            parent_port=d.get("parent_port"),
        )
