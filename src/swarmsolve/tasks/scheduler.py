"""Local task bookkeeping: open queue, dead-end set, leases, dedup.

The Scheduler is the per-peer brain that decides *what to work on next* and
keeps shared state consistent with what arrives over gossip:

    * ``open``       : known unclaimed subtasks (dedup by task id).
    * ``dead_ends``  : ids of subtrees proven invalid -> never re-explore.
    * ``claimed``    : tasks currently claimed (by us or others) + expiry.
    * ``done``       : (legacy) subtrees fully explored with no solution.

Unsolvable detection adds three more:

    * ``split``      : tasks we hold the *parent bookkeeping* for (DONE_SPLIT):
                       they carry ``children`` + ``children_exhausted``.
    * ``exhausted``  : ids of subtrees proven to contain no solution.
    * ``_reported``  : parent_id -> set(child_id) already counted, so repeated
                       EXHAUSTED reports (gossip retransmission) are idempotent.

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
        # ---- unsolvable-detection bookkeeping -------------------------
        self.split: dict[str, Task] = {}          # parent tasks in DONE_SPLIT
        self.exhausted: set[str] = set()          # task ids proven no-solution
        self._reported: dict[str, set[str]] = {}  # parent_id -> counted child ids
        # ---- crash-recovery backups (periodic state sync) -------------
        # task_id -> {"frontier": [paths], "nodes": int, "ts": float}. A backup
        # peer keeps the latest snapshot of a busy peer's unexplored frontier so
        # that, if the owner crashes, we resume from the snapshot instead of
        # redoing the whole subtree (only the sync-window's worth is lost).
        self.backups: dict[str, dict] = {}

    # ---- ingest (from local splitting or gossip) ----------------------

    def add_open(self, task: Task) -> bool:
        """Register an open task. Returns False if it's a known dup/dead/done."""
        tid = task.id
        if tid in self.dead_ends or tid in self.done or tid in self.exhausted:
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
        """(Legacy) mark a subtree fully explored (no solution)."""
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

    # ---- unsolvable detection -----------------------------------------

    def mark_split(self, task: Task) -> None:
        """Record that ``task`` was expanded into ``task.children`` (DONE_SPLIT).

        The peer(s) responsible for ``task`` keep this record so they can later
        count DONE_EXHAUSTED reports from the children and roll the result up.
        """
        tid = task.id
        self.open.pop(tid, None)
        self.claimed.pop(tid, None)
        task.status = TaskStatus.DONE_SPLIT
        task.owner = None
        task.lease_expires = 0.0
        # merge counters if we already had a record for this task id
        prev = self.split.get(tid)
        if prev is not None and prev.children_exhausted > task.children_exhausted:
            task.children_exhausted = prev.children_exhausted
        self.split[tid] = task

    def mark_exhausted(self, path: Path) -> None:
        """Record that a subtree was exhaustively searched with no solution."""
        tid = path_repr(path)
        self.exhausted.add(tid)
        self.open.pop(tid, None)
        self.claimed.pop(tid, None)

    def note_child_exhausted(self, parent_id: str, child_id: str) -> bool:
        """Count one child's DONE_EXHAUSTED report against ``parent_id``.

        Idempotent: a repeated report for the same child is ignored. Returns True
        once *all* of the parent's children are exhausted (parent now unsolvable).
        Returns False if we don't hold this parent's bookkeeping yet.
        """
        parent = self.split.get(parent_id)
        if parent is None:
            return False
        seen = self._reported.setdefault(parent_id, set())
        if child_id not in seen:
            seen.add(child_id)
            parent.children_exhausted += 1
        return parent.all_children_exhausted()

    # ---- crash-recovery backups (periodic state sync) -----------------

    def record_backup(self, task_id: str, frontier: list[Path], nodes: int,
                      now: float | None = None) -> None:
        """Store the latest frontier snapshot for a busy peer's task."""
        self.backups[task_id] = {
            "frontier": [list(p) for p in frontier],
            "nodes": nodes,
            "ts": now or time.time(),
        }

    def take_backup_frontier(self, task_id: str) -> list[Path] | None:
        """Consume the snapshot frontier for ``task_id`` (used on recovery)."""
        snap = self.backups.pop(task_id, None)
        if not snap:
            return None
        return [[tuple(a) for a in p] for p in snap["frontier"]]

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
                if (
                    tid not in self.dead_ends
                    and tid not in self.done
                    and tid not in self.exhausted
                ):
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

    def offer_open_task(self, now: float | None = None) -> Task | None:
        """Pick an open task to hand to a probing peer (any active one).

        Used to answer a TASK_QUERY: the probed peer offers one of the open
        tasks it currently holds so the prober can claim it (cold-start help).
        """
        self.reclaim_expired(now)
        for task in self.open.values():
            return task
        return None

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
            "done": len(self.done) + len(self.exhausted),
            "split": len(self.split),
            "exhausted": len(self.exhausted),
        }
