"""Task Guards: the DHT-native task registry for Kademlia non-exclusive mode.

Instead of keeping a task isolated on one node (basic XOR placement) or flooding
its state over gossip, the *Task Guards* model stores each task on its ``k``
nearest peers in the Kademlia keyspace. Those peers are the task's **guards**:
they hold a small tracking record and coordinate the task's life-cycle purely by
point-to-point TCP (``UPDATE_STATUS``), so the intensive state-sync traffic stays
localized to a group of ``k`` peers rather than flooding the whole network.

A guard record tracks exactly the fields from the design spec::

    {
      "task_id": "...", "path": [...],
      "state": "OPEN | CLAIMED | DONE_SPLIT | DONE_EXHAUSTED",
      "holder": "peer_id | None",      "lease_expire": ts | 0,
      "children": ["...", ...],        "children_exhausted": 0,
      "parent_id": "... | None",       "ts": <last-update timestamp>,
    }

The ``ts`` (last-update timestamp) is used to (a) make ``UPDATE_STATUS`` sync
idempotent / last-writer-wins and (b) resolve the *race condition* where two
guards hand the same task to two thieves: the earlier claim wins.

This module is pure bookkeeping (no networking); ``Peer`` drives it and performs
the actual RPCs. That keeps it trivial to unit-test.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from swarmsolve.solver.search import Path

# Guard-tracked task states (superset of the four in the spec).
OPEN = "OPEN"                      # available for stealing
CLAIMED = "CLAIMED"               # leased by a thief (heartbeat active)
DONE_SPLIT = "DONE_SPLIT"        # expanded into children; parent awaits child verdicts
DONE_EXHAUSTED = "DONE_EXHAUSTED"  # branch fully searched, provably no solution


@dataclass
class GuardRecord:
    """One task's tracking record, held by each of its ``k`` guards."""

    task_id: str
    path: Path
    parent_id: str | None = None
    state: str = OPEN
    holder: str | None = None
    lease_expire: float = 0.0
    children: list[str] = field(default_factory=list)
    children_exhausted: int = 0
    ts: float = field(default_factory=time.time)

    def all_children_exhausted(self) -> bool:
        return bool(self.children) and self.children_exhausted >= len(self.children)

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "path": self.path,
            "parent_id": self.parent_id,
            "state": self.state,
            "holder": self.holder,
            "lease_expire": self.lease_expire,
            "children": self.children,
            "children_exhausted": self.children_exhausted,
            "ts": self.ts,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "GuardRecord":
        return cls(
            task_id=d["task_id"],
            path=[tuple(p) for p in d.get("path", [])],
            parent_id=d.get("parent_id"),
            state=d.get("state", OPEN),
            holder=d.get("holder"),
            lease_expire=float(d.get("lease_expire", 0.0)),
            children=list(d.get("children", [])),
            children_exhausted=int(d.get("children_exhausted", 0)),
            ts=float(d.get("ts", time.time())),
        )


class GuardStore:
    """Per-peer store of the task records this peer is a guard for."""

    def __init__(self, lease_seconds: float = 10.0) -> None:
        self.lease_seconds = lease_seconds
        self.records: dict[str, GuardRecord] = {}
        # parent_id -> set(child_id) already counted, so repeated
        # REPORT_CHILD_EXHAUSTED messages are idempotent.
        self._counted: dict[str, set[str]] = {}

    # ---- registration (Kademlia PUT lands here) -----------------------

    def put(self, rec: GuardRecord) -> GuardRecord:
        """Register/refresh a task record, resolving conflicts deterministically.

        Conflict resolution (in order):
          1. **Monotonic progress**: a completed task (DONE_SPLIT/DONE_EXHAUSTED)
             is never regressed to OPEN/CLAIMED by a late/stale message.
          2. **Claim race**: if two guards independently CLAIMED the same task for
             *different* thieves, the **earliest claim wins** (smaller lease_expire
             == earlier claim), so the loser thief aborts -> minimal redundant work.
          3. Otherwise **last-writer-wins** by ``ts`` (merging the monotonic
             children_exhausted counter).
        """
        prev = self.records.get(rec.task_id)
        if prev is None:
            self.records[rec.task_id] = rec
            return rec
        done = (DONE_SPLIT, DONE_EXHAUSTED)
        # 1. monotonic: don't undo a completed task
        if prev.state in done and rec.state not in done:
            if rec.children_exhausted > prev.children_exhausted:
                prev.children_exhausted = rec.children_exhausted
            return prev
        # 2. claim race: earliest claim (smaller lease_expire) wins
        if (prev.state == CLAIMED and rec.state == CLAIMED
                and prev.holder != rec.holder):
            winner = prev if prev.lease_expire <= rec.lease_expire else rec
            self.records[rec.task_id] = winner
            return winner
        # 3. last-writer-wins by timestamp
        if rec.ts >= prev.ts:
            if prev.children_exhausted > rec.children_exhausted:
                rec.children_exhausted = prev.children_exhausted
            self.records[rec.task_id] = rec
            return rec
        return prev

    def get(self, task_id: str) -> GuardRecord | None:
        return self.records.get(task_id)

    # ---- state transitions (a guard mutates then syncs) ---------------

    def open_records(self) -> list[GuardRecord]:
        """All tasks currently OPEN under our guardianship (Opt A / work-steal)."""
        return [r for r in self.records.values() if r.state == OPEN]

    def try_claim(self, task_id: str, holder: str, now: float | None = None
                  ) -> GuardRecord | None:
        """Atomically move an OPEN task to CLAIMED for ``holder`` with a lease.

        Returns the updated record, or ``None`` if it is not OPEN (already taken —
        this is where the timestamp-ordered race resolution kicks in).
        """
        now = now or time.time()
        rec = self.records.get(task_id)
        if rec is None or rec.state != OPEN:
            return None
        rec.state = CLAIMED
        rec.holder = holder
        rec.lease_expire = now + self.lease_seconds
        rec.ts = now
        return rec

    def revert_open(self, task_id: str, now: float | None = None) -> GuardRecord | None:
        """Thief failed / lease expired -> put the task back to OPEN."""
        now = now or time.time()
        rec = self.records.get(task_id)
        if rec is None:
            return None
        rec.state = OPEN
        rec.holder = None
        rec.lease_expire = 0.0
        rec.ts = now
        return rec

    def mark_split(self, task_id: str, children: list[str],
                   now: float | None = None) -> GuardRecord | None:
        """Thief reported it expanded the task into ``children`` (DONE_SPLIT)."""
        now = now or time.time()
        rec = self.records.get(task_id)
        if rec is None:
            return None
        rec.state = DONE_SPLIT
        rec.children = list(children)
        rec.holder = None
        rec.lease_expire = 0.0
        rec.ts = now
        return rec

    def mark_exhausted(self, task_id: str, now: float | None = None
                       ) -> GuardRecord | None:
        """Thief reported the leaf branch is invalid (DONE_EXHAUSTED)."""
        now = now or time.time()
        rec = self.records.get(task_id)
        if rec is None:
            return None
        rec.state = DONE_EXHAUSTED
        rec.holder = None
        rec.lease_expire = 0.0
        rec.ts = now
        return rec

    def note_child_exhausted(self, parent_id: str, child_id: str,
                             now: float | None = None) -> GuardRecord | None:
        """Count one child's exhaustion against ``parent_id`` (idempotent).

        Returns the parent record iff it *just* became fully exhausted (all
        children DONE_EXHAUSTED); otherwise ``None``.
        """
        now = now or time.time()
        rec = self.records.get(parent_id)
        if rec is None:
            return None
        seen = self._counted.setdefault(parent_id, set())
        if child_id not in seen:
            seen.add(child_id)
            rec.children_exhausted += 1
            rec.ts = now
        if rec.all_children_exhausted() and rec.state != DONE_EXHAUSTED:
            rec.state = DONE_EXHAUSTED
            rec.ts = now
            return rec
        return None

    # ---- UPDATE_STATUS sync (last-writer-wins) ------------------------

    def apply_update(self, payload: dict) -> None:
        """Apply a peer guard's UPDATE_STATUS (idempotent, ts-ordered)."""
        self.put(GuardRecord.from_dict(payload))

    # ---- lease monitoring (thief-failure detection) -------------------

    def expired_claims(self, now: float | None = None) -> list[GuardRecord]:
        """CLAIMED records whose lease has lapsed -> holder presumed dead."""
        now = now or time.time()
        return [
            r for r in self.records.values()
            if r.state == CLAIMED and 0 < r.lease_expire <= now
        ]

    def renew(self, task_id: str, now: float | None = None) -> GuardRecord | None:
        """Heartbeat: extend the lease of a CLAIMED task."""
        now = now or time.time()
        rec = self.records.get(task_id)
        if rec is not None and rec.state == CLAIMED:
            rec.lease_expire = now + self.lease_seconds
            rec.ts = now
        return rec

    # ---- introspection ------------------------------------------------

    def stats(self) -> dict:
        by_state: dict[str, int] = {}
        for r in self.records.values():
            by_state[r.state] = by_state.get(r.state, 0) + 1
        return {
            "guarded": len(self.records),
            "guard_open": by_state.get(OPEN, 0),
            "guard_claimed": by_state.get(CLAIMED, 0),
            "guard_split": by_state.get(DONE_SPLIT, 0),
            "guard_exhausted": by_state.get(DONE_EXHAUSTED, 0),
        }
