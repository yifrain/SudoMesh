"""Unit tests for the Task-Guards model (Kademlia non-exclusive mode).

Covers the GuardStore state machine (OPEN/CLAIMED/DONE_SPLIT/DONE_EXHAUSTED),
lease/claim semantics, race resolution, and the bottom-up exhaustion roll-up —
all without networking. The end-to-end multi-process path is exercised by
``swarmsolve demo --guard`` / ``swarmsolve unsolvable --guard``.
"""

from __future__ import annotations

from swarmsolve.tasks.guard import (
    CLAIMED,
    DONE_EXHAUSTED,
    DONE_SPLIT,
    OPEN,
    GuardRecord,
    GuardStore,
)


def _rec(task_id: str, **kw) -> GuardRecord:
    kw.setdefault("path", [])
    return GuardRecord(task_id=task_id, **kw)


# --------------------------------------------------------------------------- #
# registration + claim/lease
# --------------------------------------------------------------------------- #
def test_put_and_open_records():
    gs = GuardStore()
    gs.put(_rec("T1", parent_id="T"))
    gs.put(_rec("T2", parent_id="T"))
    ids = {r.task_id for r in gs.open_records()}
    assert ids == {"T1", "T2"}


def test_try_claim_transitions_open_to_claimed_with_lease():
    gs = GuardStore(lease_seconds=5.0)
    gs.put(_rec("T1"))
    rec = gs.try_claim("T1", holder="P5", now=100.0)
    assert rec is not None
    assert rec.state == CLAIMED
    assert rec.holder == "P5"
    assert rec.lease_expire == 105.0


def test_second_claim_on_claimed_task_is_rejected():
    """The core work-stealing race guard: a task can be claimed only once."""
    gs = GuardStore()
    gs.put(_rec("T1"))
    assert gs.try_claim("T1", "P5") is not None
    assert gs.try_claim("T1", "P9") is None   # already CLAIMED -> rejected


def test_revert_open_on_thief_failure():
    gs = GuardStore()
    gs.put(_rec("T1"))
    gs.try_claim("T1", "P5")
    rec = gs.revert_open("T1")
    assert rec.state == OPEN and rec.holder is None
    # now stealable again
    assert gs.try_claim("T1", "P9") is not None


def test_expired_claims_detected():
    gs = GuardStore(lease_seconds=5.0)
    gs.put(_rec("T1"))
    gs.try_claim("T1", "P5", now=100.0)
    assert gs.expired_claims(now=104.0) == []       # still leased
    expired = gs.expired_claims(now=106.0)
    assert [r.task_id for r in expired] == ["T1"]   # lease lapsed


def test_heartbeat_renews_lease():
    gs = GuardStore(lease_seconds=5.0)
    gs.put(_rec("T1"))
    gs.try_claim("T1", "P5", now=100.0)
    gs.renew("T1", now=104.0)
    assert gs.expired_claims(now=106.0) == []        # renewed to 109
    assert [r.task_id for r in gs.expired_claims(now=110.0)] == ["T1"]


# --------------------------------------------------------------------------- #
# split / exhaustion state transitions
# --------------------------------------------------------------------------- #
def test_mark_split_records_children():
    gs = GuardStore()
    gs.put(_rec("T1"))
    rec = gs.mark_split("T1", ["T11", "T12", "T13"])
    assert rec.state == DONE_SPLIT
    assert rec.children == ["T11", "T12", "T13"]


def test_mark_exhausted():
    gs = GuardStore()
    gs.put(_rec("T11"))
    rec = gs.mark_exhausted("T11")
    assert rec.state == DONE_EXHAUSTED


def test_child_exhaustion_rolls_up_when_all_done():
    """Parent becomes DONE_EXHAUSTED only after ALL children are exhausted."""
    gs = GuardStore()
    gs.put(_rec("T1", state=DONE_SPLIT, children=["T11", "T12", "T13"]))
    assert gs.note_child_exhausted("T1", "T11") is None   # 1/3
    assert gs.note_child_exhausted("T1", "T12") is None   # 2/3
    parent = gs.note_child_exhausted("T1", "T13")         # 3/3 -> exhausted
    assert parent is not None
    assert parent.state == DONE_EXHAUSTED


def test_child_exhaustion_is_idempotent():
    """A duplicated report for the same child is counted once."""
    gs = GuardStore()
    gs.put(_rec("T1", state=DONE_SPLIT, children=["T11", "T12"]))
    assert gs.note_child_exhausted("T1", "T11") is None
    assert gs.note_child_exhausted("T1", "T11") is None   # duplicate ignored
    assert gs.get("T1").children_exhausted == 1
    parent = gs.note_child_exhausted("T1", "T12")
    assert parent is not None and parent.state == DONE_EXHAUSTED


# --------------------------------------------------------------------------- #
# UPDATE_STATUS sync + conflict resolution
# --------------------------------------------------------------------------- #
def test_apply_update_last_writer_wins():
    gs = GuardStore()
    gs.put(_rec("T1", state=OPEN, ts=100.0))
    gs.apply_update(_rec("T1", state=CLAIMED, holder="P5", ts=101.0).to_dict())
    assert gs.get("T1").state == CLAIMED
    # a stale (older ts) update must NOT overwrite the newer state
    gs.apply_update(_rec("T1", state=OPEN, ts=50.0).to_dict())
    assert gs.get("T1").state == CLAIMED


def test_claim_race_earliest_wins():
    """Two guards CLAIMED for different thieves -> earliest claim wins."""
    gs = GuardStore()
    gs.put(_rec("T1", ts=99.0))
    # guard-A claimed for P5 at lease_expire=105 (earlier); we hold that.
    gs.put(_rec("T1", state=CLAIMED, holder="P5", lease_expire=105.0, ts=100.0))
    # guard-B's sync arrives: claimed for P9 at lease_expire=106 (later) -> loses.
    gs.put(_rec("T1", state=CLAIMED, holder="P9", lease_expire=106.0, ts=101.0))
    assert gs.get("T1").holder == "P5"


def test_done_state_not_regressed_by_stale_open():
    """Monotonic progress: a completed task is never reverted by a late OPEN."""
    gs = GuardStore()
    gs.put(_rec("T1", state=DONE_EXHAUSTED, ts=100.0))
    gs.apply_update(_rec("T1", state=OPEN, ts=200.0).to_dict())  # late & newer ts
    assert gs.get("T1").state == DONE_EXHAUSTED


def test_stats_counts_by_state():
    gs = GuardStore()
    gs.put(_rec("A"))
    gs.put(_rec("B"))
    gs.try_claim("B", "P5")
    gs.put(_rec("C", state=DONE_EXHAUSTED))
    st = gs.stats()
    assert st["guarded"] == 3
    assert st["guard_open"] == 1
    assert st["guard_claimed"] == 1
    assert st["guard_exhausted"] == 1
