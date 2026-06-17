"""Local task bookkeeping: open queue, dead-end set, leases, dedup.

The Scheduler is the per-peer brain that decides *what to work on next* and
keeps shared state consistent with what arrives over gossip:

    * ``open``       : known unclaimed subtasks (dedup by task id).
    * ``dead_ends``  : ids of subtrees proven invalid -> never re-explore.
    * ``leases``     : tasks currently claimed (by us or others) + expiry.

Fairness & dedup: a peer prefers tasks whose ``key`` it is *responsible for*
(closest in XOR space), which spreads work without central coordination.
Fault tolerance: expired leases are reclaimed automatically (peer assumed dead).

Owner: Person C + Person E.
"""

from __future__ import annotations

import time

from swarmsolve.discovery.node_id import NodeID, xor_distance
from swarmsolve.solver.search import Path
from swarmsolve.tasks.task import Task, TaskStatus, path_repr

DEFAULT_LEASE_SECONDS = 10.0


class Scheduler:
    def __init__(self, self_id: NodeID, lease_seconds: float = DEFAULT_LEASE_SECONDS) -> None:
        self.self_id = self_id
        self.lease_seconds = lease_seconds
        self.open: dict[str, Task] = {}
        self.dead_ends: set[str] = set()
        self.claimed: dict[str, Task] = {}
        self.done: set[str] = set()

    # ---- ingest (from local splitting or gossip) ----------------------

    def add_open(self, task: Task) -> bool:
        """Register an open task. Returns False if it's a known dup/dead/done."""
        tid = task.id
        if tid in self.dead_ends or tid in self.done:
            return False
        if tid in self.claimed and self.claimed[tid].lease_active():
            return False
        if tid in self.open:
            return False
        task.status = TaskStatus.OPEN
        self.open[tid] = task
        return True

    def mark_dead(self, path: Path) -> None:
        """Prune a subtree everywhere (idempotent)."""
        tid = path_repr(path)
        self.dead_ends.add(tid)
        self.open.pop(tid, None)
        self.claimed.pop(tid, None)

    def mark_done(self, path: Path) -> None:
        tid = path_repr(path)
        self.done.add(tid)
        self.open.pop(tid, None)
        self.claimed.pop(tid, None)

    def note_claim(self, task: Task) -> None:
        """Record that some peer (maybe us) claimed a task."""
        tid = task.id
        self.open.pop(tid, None)
        task.status = TaskStatus.CLAIMED
        self.claimed[tid] = task

    # ---- selection ----------------------------------------------------

    def reclaim_expired(self, now: float | None = None) -> list[Task]:
        """Move expired leases back to OPEN (the disconnected-peer case)."""
        now = now or time.time()
        reclaimed: list[Task] = []
        for tid, task in list(self.claimed.items()):
            if task.lease_expires <= now:
                task.status = TaskStatus.OPEN
                task.owner = None
                self.claimed.pop(tid, None)
                if tid not in self.dead_ends and tid not in self.done:
                    self.open[tid] = task
                    reclaimed.append(task)
        return reclaimed

    def next_task(self, now: float | None = None) -> Task | None:
        """Pick the best open task to work on, preferring ones we 'own'.

        Ownership preference = smallest XOR distance from our id to the task key.
        This realizes structured, low-collision task placement over the DHT.
        """
        self.reclaim_expired(now)
        if not self.open:
            return None
        best = min(
            self.open.values(),
            key=lambda t: xor_distance(self.self_id, t.key),
        )
        return best

    def claim_local(self, task: Task, now: float | None = None) -> Task:
        """Claim a task for ourselves with a fresh lease."""
        now = now or time.time()
        task.owner = self.self_id.hex()
        task.lease_expires = now + self.lease_seconds
        self.note_claim(task)
        return task

    def renew(self, task: Task, now: float | None = None) -> None:
        now = now or time.time()
        task.lease_expires = now + self.lease_seconds

    # ---- introspection (for the dashboard) ----------------------------

    def stats(self) -> dict:
        return {
            "open": len(self.open),
            "claimed": len(self.claimed),
            "dead_ends": len(self.dead_ends),
            "done": len(self.done),
        }
