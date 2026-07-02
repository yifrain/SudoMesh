"""Local task bookkeeping: deque-based work pool, dead-end set, leases, dedup.

The Scheduler is the per-peer brain that decides *what to work on next* and
keeps shared state consistent with what arrives over gossip:

    * ``task_deque`` : known unclaimed subtasks (deque for O(1) pop/steal).
    * ``dead_ends``  : ids of subtrees proven invalid -> never re-explore.
    * ``leases``     : tasks currently claimed (by us or others) + expiry.

Load balancing via **Work Stealing** (Cilk / ForkJoinPool style):

    * ``pop_own()``  : a peer takes work from the **tail** (LIFO) — this gives
      good cache locality and keeps newly-split (fine-grained) tasks close.
    * ``steal()``    : an idle peer takes work from the **head** (FIFO) — the
      oldest, typically coarsest task — spreading load to the idlest peer.

Both are O(1); no full scan of the pool is needed.

Owner: Person C + Person E.
"""

from __future__ import annotations

import time
from collections import deque

from swarmsolve.discovery.node_id import NodeID
from swarmsolve.solver.search import Path
from swarmsolve.tasks.task import Task, TaskStatus, path_repr

DEFAULT_LEASE_SECONDS = 5.0  # short for fast crash detection; renewed while working


class Scheduler:
    def __init__(self, self_id: NodeID, lease_seconds: float = DEFAULT_LEASE_SECONDS) -> None:
        self.self_id = self_id
        self.lease_seconds = lease_seconds
        # deque for O(1) pop (own, from tail) and steal (from head)
        self.task_deque: deque[Task] = deque()
        # side map for O(1) dedup / status lookups by task id
        self.task_map: dict[str, Task] = {}
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
        if tid in self.task_map:
            return False
        task.status = TaskStatus.OPEN
        self.task_map[tid] = task
        self.task_deque.append(task)
        return True

    def mark_dead(self, path: Path) -> None:
        """Prune a subtree everywhere (idempotent)."""
        tid = path_repr(path)
        self.dead_ends.add(tid)
        self.task_map.pop(tid, None)
        self.claimed.pop(tid, None)
        # Note: we leave stale entries in the deque; they are skipped lazily
        # by pop_own()/steal() to avoid O(N) removal.

    def mark_done(self, path: Path) -> None:
        tid = path_repr(path)
        self.done.add(tid)
        self.task_map.pop(tid, None)
        self.claimed.pop(tid, None)

    def note_claim(self, task: Task) -> None:
        """Record that some peer (maybe us) claimed a task."""
        tid = task.id
        self.task_map.pop(tid, None)
        task.status = TaskStatus.CLAIMED
        self.claimed[tid] = task

    # ---- selection (Work Stealing) ------------------------------------

    def _is_pickable(self, task: Task) -> bool:
        """A task is pickable only if not dead / done / actively claimed."""
        tid = task.id
        if tid in self.dead_ends or tid in self.done:
            return False
        existing = self.claimed.get(tid)
        if existing and existing.owner is not None and existing.lease_active():
            return False
        return True

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
                    self.task_map[tid] = task
                    self.task_deque.append(task)
                    reclaimed.append(task)
        return reclaimed

    def pop_own(self) -> Task | None:
        """Take a task for ourselves: pop from the **tail** (LIFO).

        Newly-split tasks land at the tail, so we work on the finest-grained
        work first — good locality and keeps the deque short.
        """
        while self.task_deque:
            task = self.task_deque.pop()
            tid = task.id
            if not self._is_pickable(task):
                self.task_map.pop(tid, None)
                continue
            self.task_map.pop(tid, None)
            return task
        return None

    def steal(self) -> Task | None:
        """An idle peer steals from the **head** (FIFO): the oldest, typically
        coarsest task. This is the classic Cilk/ForkJoin work-stealing rule:
        the thief grabs the largest available chunk, leaving fine work to the
        owner.
        """
        while self.task_deque:
            task = self.task_deque.popleft()
            tid = task.id
            if not self._is_pickable(task):
                self.task_map.pop(tid, None)
                continue
            self.task_map.pop(tid, None)
            return task
        return None

    def next_task(self, now: float | None = None) -> Task | None:
        """Legacy compatibility: pop from the tail (same as pop_own)."""
        self.reclaim_expired(now)
        return self.pop_own()

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
            "open": len(self.task_deque),
            "claimed": len(self.claimed),
            "dead_ends": len(self.dead_ends),
            "done": len(self.done),
        }
