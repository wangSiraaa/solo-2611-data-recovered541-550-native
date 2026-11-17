"""Reproducible demonstration of the repaired lock service and SQLite store.

Run from this directory:  python3 demo.py
Every check is backed by a second, independent sqlite3 connection so the
printed row values are observations of the real database file, not values
reported back by the code under test.
"""
import json
import sqlite3
import tempfile
from pathlib import Path

from locks import LockManager, ACTIVE, ABORTING, ENDED
from store import Store


def raw_rows(path):
    """Independent connection: bypass the Store object entirely."""
    con = sqlite3.connect(str(path))
    try:
        return dict(con.execute("SELECT key, value FROM user_rows ORDER BY key"))
    finally:
        con.close()


def expect_error(fn, label):
    try:
        fn()
    except ValueError as exc:
        return f"rejected: {exc}"
    raise AssertionError(f"{label} should have been rejected")


# --------------------------------------------------------------------------- #
def incident_cross_writes(root):
    """The original incident: cross X requests, newer txn overwrote b twice."""
    path = Path(root) / "incident.sqlite"
    locks = LockManager()
    store = Store(path, locks)
    store.seed({"a": 10, "b": 20})
    store.begin("T1", 1)
    store.begin("T2", 2)
    assert locks.acquire("T1", "a", "X")
    assert locks.acquire("T2", "b", "X")
    store.write("T1", "a", 11)
    store.write("T2", "b", 21)
    store.write("T2", "b", 22)           # second overwrite of the same key
    assert locks.acquire("T1", "b", "X") is False
    assert locks.acquire("T2", "a", "X") is False

    victim = locks.resolve_one(store)
    out = {"victim": victim,
           "rule": "youngest on the cycle (max age, then max tx id)"}
    out["status_while_aborting"] = store.status(victim)
    out["b_lock_still_held_by_victim"] = locks.holds(victim, "b", "X")
    # The waiting transaction must NOT see the not-yet-undone value.
    out["waiter_read_before_undo"] = expect_error(
        lambda: store.read("T1", "b"), "T1 read while victim aborts")
    out["raw_db_mid_abort"] = raw_rows(path)      # 22 physically present but locked
    out["victim_write_rejected"] = expect_error(
        lambda: store.write(victim, "b", 99), "write by aborting txn")
    out["bypass_release_rejected"] = expect_error(
        lambda: locks.release_all(victim), "release_all on bound txn")

    states = []
    while store.status(victim)["state"] != "ROLLED_BACK":
        states.append(store.rollback_step(victim))
    out["undo_steps"] = states                   # reverse order, barrier at end
    out["raw_db_after_undo"] = raw_rows(path)
    out["victim_lock_state"] = locks.states[victim]

    # Successor can now be granted and must read the restored original value.
    assert locks.acquire("T1", "b", "X")
    out["successor_reads"] = store.read("T1", "b")
    store.commit("T1")
    out["raw_db_after_commit"] = raw_rows(path)
    out["snapshot"] = locks.snapshot()
    store.close()
    return out


def two_s_upgrades(root):
    """Two S holders upgrading to X at once: each waits on the other."""
    path = Path(root) / "upgrades.sqlite"
    locks = LockManager()
    store = Store(path, locks)
    store.seed({"r": 1})
    store.begin("U1", 1)
    store.begin("U2", 2)
    assert locks.acquire("U1", "r", "S")
    assert locks.acquire("U2", "r", "S")
    assert locks.acquire("U1", "r", "X") is False     # upgrade keeps the S
    assert locks.acquire("U2", "r", "X") is False
    out = {"wait_graph": locks.wait_graph(),
           "cycles": locks.cycle_members()}
    victim = locks.resolve_one(store)               # U2 is younger
    out["victim"] = victim
    out["u1_keeps_s_during_abort"] = locks.holds("U1", "r", "S")
    out["victim_keeps_s_during_abort"] = locks.holds(victim, "r", "S")
    store.rollback_all(victim)
    out["after_undo_holder"] = locks.snapshot()["holders"]
    assert locks.acquire("U1", "r", "X")            # upgrade now completes
    out["u1_upgraded"] = locks.holds("U1", "r", "X")
    store.commit("U1")
    store.close()
    return out


def fair_queue_cycle(root):
    """An X waiter blocks a later compatible S, producing a queue-only cycle."""
    locks = LockManager()
    # T1 holds X on r; T2 queues X (wait); T3 queues S behind T2.
    locks.register("T1", 1)
    locks.register("T2", 2)
    locks.register("T3", 3)
    assert locks.acquire("T1", "r", "X")
    assert locks.acquire("T2", "r", "X") is False
    assert locks.acquire("T3", "r", "S") is False    # cannot jump T2's X
    out = {"wait_graph": locks.wait_graph(),
           "cycles": locks.cycle_members(),
           "note": "T3->T2 is a FIFO predecessor edge, not a holder edge"}
    # Build the cycle on a second resource q: T2 and T3 hold/queue crosswise.
    assert locks.acquire("T3", "q", "X")
    assert locks.acquire("T2", "q", "S") is False   # blocked by T3's X
    out["wait_graph_full"] = locks.wait_graph()
    out["cycles_full"] = locks.cycle_members()
    victim = locks.resolve_one()                    # lock-only victim
    # T1 has no outgoing edge; the ring is T2<->T3 and T3 (age 3) is youngest.
    out["victim"] = victim
    assert victim == "T3"
    out["victim_ended_immediately"] = locks.states[victim] == ENDED
    out["after_resolve"] = locks.snapshot()
    return out


def pure_lock_deadlock():
    """No business rows at all: the same sacrifice rules must still work."""
    locks = LockManager()
    locks.register("P1", 1)
    locks.register("P2", 2)
    assert locks.acquire("P1", "x", "X")
    assert locks.acquire("P2", "y", "X")
    assert locks.acquire("P1", "y", "X") is False
    assert locks.acquire("P2", "x", "X") is False
    before = locks.cycle_members()
    victim = locks.resolve_one()                    # no store argument
    return {
        "cycles_before": before,
        "victim": victim,
        "victim_state": locks.states[victim],
        "remaining_cycles": locks.cycle_members(),
        "snapshot": locks.snapshot(),
    }


def two_disjoint_cycles():
    """resolve_one breaks exactly one ring per call."""
    locks = LockManager()
    for tx in ("A1", "A2", "B1", "B2"):
        locks.register(tx, 1)
    assert locks.acquire("A1", "ra", "X")
    assert locks.acquire("A2", "rb", "X")
    assert locks.acquire("A1", "rb", "X") is False
    assert locks.acquire("A2", "ra", "X") is False
    assert locks.acquire("B1", "rc", "X")
    assert locks.acquire("B2", "rd", "X")
    assert locks.acquire("B1", "rd", "X") is False
    assert locks.acquire("B2", "rc", "X") is False
    first = locks.resolve_one()
    between = locks.cycle_members()
    second = locks.resolve_one()
    third = locks.resolve_one()
    return {"first_victim": first,
            "cycles_still_present_after_one_call": between,
            "second_victim": second,
            "third_call_without_cycle": third,
            "final_states": locks.states}


def normal_behaviors(root):
    """Shared reads, fair granting, cancel-wait, sequential commit still work."""
    path = Path(root) / "normal.sqlite"
    locks = LockManager()
    store = Store(path, locks)
    store.seed({"k": 7})
    store.begin("N1", 1)
    store.begin("N2", 2)
    out = {}
    assert locks.acquire("N1", "k", "S")
    assert locks.acquire("N2", "k", "S")            # S/S compatible
    out["shared_reads"] = [store.read("N1", "k"), store.read("N2", "k")]
    assert locks.acquire("N1", "k", "X") is False   # upgrade queues behind...
    store.begin("N3", 3)
    assert locks.acquire("N3", "k", "S") is False   # later S cannot jump the X
    locks.cancel_pending("N1")                      # cancels only the upgrade
    out["n1_keeps_s_after_cancel"] = locks.holds("N1", "k", "S")
    out["n3_granted_after_cancel"] = locks.acquire("N3", "k", "S")
    out["idempotent_s"] = locks.acquire("N1", "k", "S")
    out["x_holder_s_request_no_downgrade"] = (
        locks.acquire("N3", "k", "X") is False)     # blocked by the other S
    locks.cancel_pending("N3")
    store.commit("N1")
    store.commit("N2")
    store.commit("N3")
    out["raw_db"] = raw_rows(path)
    out["snapshot"] = locks.snapshot()
    store.close()
    return out


def rollback_new_key_and_stale_callbacks(root):
    """Created keys disappear on undo; a late write callback cannot revive."""
    path = Path(root) / "newkey.sqlite"
    locks = LockManager()
    store = Store(path, locks)
    store.seed({"existing": 5})
    store.begin("W2", 1)
    store.begin("W1", 2)                            # W1 is the younger victim
    assert locks.acquire("W1", "new", "X")
    store.write("W1", "new", 100)                  # key did not exist before
    assert locks.acquire("W2", "existing", "X")
    assert locks.acquire("W2", "new", "X") is False
    assert locks.acquire("W1", "existing", "X") is False
    victim = locks.resolve_one(store)
    store.rollback_all(victim)
    out = {"victim": victim,
           "raw_db": raw_rows(path),                # 'new' must be gone
           "w2_gets_new": locks.acquire("W2", "new", "X"),
           "w2_reads_new_as_absent": store.read("W2", "new")}
    out["late_write_rejected"] = expect_error(
        lambda: store.write(victim, "new", 1), "late write callback")
    out["double_terminate_rejected"] = expect_error(
        lambda: store.commit(victim), "commit after rollback")
    store.commit("W2")
    store.close()
    return out


def run(root):
    return {
        "1_original_incident_cross_writes": incident_cross_writes(root),
        "2_two_S_holders_upgrading": two_s_upgrades(root),
        "3_fair_queue_predecessor_cycle": fair_queue_cycle(root),
        "4_pure_lock_deadlock_no_rows": pure_lock_deadlock(),
        "5_two_disjoint_cycles": two_disjoint_cycles(),
        "6_normal_shared_reads_fairness_cancel_commit": normal_behaviors(root),
        "7_undo_created_key_and_stale_callbacks":
            rollback_new_key_and_stale_callbacks(root),
    }


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as root:
        print(json.dumps(run(root), indent=2, sort_keys=True, ensure_ascii=False))
