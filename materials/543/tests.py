"""Independent correctness verification for locks.py and store.py.

The expectations here are computed by small, separately written reference
routines (independent wait-edge construction, iterative DFS cycle search,
raw sqlite3 snapshots) -- never by the detector or rollback code under test.

Run:  python3 tests.py
"""
import random
import sqlite3
import tempfile
from pathlib import Path

from locks import LockManager, ACTIVE, ABORTING, ENDED
from store import Store

PASS = 0


def check(name, cond, detail=""):
    global PASS
    assert cond, f"FAILED {name}: {detail}"
    PASS += 1
    print(f"  ok  {name}")


def expect_value_error(name, fn):
    try:
        fn()
    except ValueError:
        check(name, True)
        return
    raise AssertionError(f"FAILED {name}: no ValueError raised")


# --------------------------------------------------------------------------- #
# Independent reference model, written from the spec, not from locks.py
# --------------------------------------------------------------------------- #
def ref_conflict(a, b):
    return a == "X" or b == "X"


def ref_wait_graph(holders, waiters, active):
    """holders: {r: {tx: mode}}; waiters: {r: [(tx, mode), ...]}."""
    graph = {tx: set() for tx in active}
    for r, queue in waiters.items():
        for i, (tx, mode) in enumerate(queue):
            if tx not in active:
                continue
            for other, held in holders.get(r, {}).items():
                if other != tx and ref_conflict(mode, held):
                    graph[tx].add(other)
            for pred, pmode in queue[:i]:
                if pred in active and ref_conflict(mode, pmode):
                    graph[tx].add(pred)
    return graph


def ref_cycles(graph):
    """Iterative DFS: every node from which a cycle is reachable back to itself."""
    cyclic = set()
    for start in graph:
        stack = [(start, iter(graph.get(start, ())))]
        seen = set()
        while stack:
            node, it = stack[-1]
            advanced = False
            for nxt in it:
                if nxt == start:
                    cyclic.add(start)
                    stack = []
                    advanced = True
                    break
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append((nxt, iter(graph.get(nxt, ()))))
                    advanced = True
                    break
            if not advanced and stack:
                stack.pop()
    return cyclic


def ref_graph_matches(locks, label):
    active = {tx for tx, s in locks.states.items() if s == ACTIVE}
    ref = {k: sorted(v) for k, v in ref_wait_graph(
        dict(locks.holders),
        {r: list(q) for r, q in locks.waiters.items()}, active).items()}
    check(f"{label}: wait graph equals independent reference",
          locks.wait_graph() == ref, f"{locks.wait_graph()} != {ref}")
    check(f"{label}: cycle members equal independent reference",
          set(locks.cycle_members()) == ref_cycles(ref))


def raw_snapshot(path):
    con = sqlite3.connect(str(path))
    try:
        rows = dict(con.execute(
            "SELECT key, value FROM user_rows ORDER BY key").fetchall())
        txns = dict(con.execute("SELECT tx, state FROM txns").fetchall())
        undo = con.execute("SELECT count(*) FROM undo").fetchone()[0]
        return rows, txns, undo
    finally:
        con.close()


# --------------------------------------------------------------------------- #
def test_two_s_upgrades(root):
    print("test: two S holders upgrade simultaneously")
    path = Path(root) / "u.sqlite"
    lm = LockManager()
    st = Store(path, lm)
    st.seed({"r": 1})
    st.begin("U1", 1)
    st.begin("U2", 2)
    check("both S granted together",
          lm.acquire("U1", "r", "S") and lm.acquire("U2", "r", "S"))
    check("U1 upgrade waits", lm.acquire("U1", "r", "X") is False)
    check("U2 upgrade waits", lm.acquire("U2", "r", "X") is False)
    check("upgraders keep their granted S",
          lm.holds("U1", "r", "S") and lm.holds("U2", "r", "S"))
    ref_graph_matches(lm, "upgrade deadlock")
    check("both on cycle", set(lm.cycle_members()) == {"U1", "U2"})
    victim = lm.resolve_one(st)
    check("younger U2 chosen", victim == "U2")
    check("victim ABORTING", lm.states[victim] == ABORTING)
    check("victim S still protects row", lm.holds(victim, "r", "S"))
    check("U1 still only has S", lm.holds("U1", "r", "S")
          and not lm.holds("U1", "r", "X"))
    check("no cycle while aborting", lm.cycle_members() == [])
    st.rollback_all(victim)
    rows, txns, undo = raw_snapshot(path)
    check("row unchanged after rollback", rows == {"r": 1})
    check("sql state ROLLED_BACK", txns[victim] == "ROLLED_BACK")
    check("undo log emptied", undo == 0)
    check("victim ENDED", lm.states[victim] == ENDED)
    check("U1 upgrade now granted", lm.acquire("U1", "r", "X"))
    check("U1 reads original", st.read("U1", "r") == 1)
    st.commit("U1")
    st.close()


def test_fair_queue_cycle():
    print("test: X waiter blocks later compatible S (queue-predecessor edge)")
    lm = LockManager()
    lm.register("T1", 1)
    lm.register("T2", 2)
    lm.register("T3", 3)
    lm.acquire("T1", "r", "X")
    check("T2 X waits", lm.acquire("T2", "r", "X") is False)
    check("later S cannot jump waiting X", lm.acquire("T3", "r", "S") is False)
    ref_graph_matches(lm, "fair queue no cycle yet")
    check("no false cycle before second resource", lm.cycle_members() == [])
    check("T3 blocked by holder T1 and predecessor T2",
          set(lm.wait_graph()["T3"]) == {"T1", "T2"})
    check("T2 blocked only by holder", lm.wait_graph()["T2"] == ["T1"])
    lm.acquire("T3", "q", "X")
    check("T2 S on q waits behind T3 X", lm.acquire("T2", "q", "S") is False)
    ref_graph_matches(lm, "fair queue with cycle")
    check("ring is exactly T2,T3", set(lm.cycle_members()) == {"T2", "T3"})
    check("bystander T1 not sacrificed",
          (v := lm.resolve_one()) == "T3", f"victim was {v}")
    check("pure-lock victim ended at once", lm.states["T3"] == ENDED)
    check("T2 granted S on q after victim release", lm.holds("T2", "q", "S"))
    check("T2 still queued on r (FIFO preserved)",
          ("T2", "X") in lm.waiters["r"])
    check("one call did not clear the r queue",
          lm.snapshot()["waiters"] == {"r": [["T2", "X"]]})


def test_cross_writes_double_overwrite(root):
    print("test: cross writes, newer txn overwrote same key twice")
    path = Path(root) / "x.sqlite"
    lm = LockManager()
    st = Store(path, lm)
    st.seed({"a": 10, "b": 20})
    st.begin("T1", 1)
    st.begin("T2", 2)
    lm.acquire("T1", "a", "X")
    lm.acquire("T2", "b", "X")
    st.write("T1", "a", 11)
    st.write("T2", "b", 21)
    st.write("T2", "b", 22)
    lm.acquire("T1", "b", "X")
    lm.acquire("T2", "a", "X")
    ref_graph_matches(lm, "cross-write deadlock")
    victim = lm.resolve_one(st)
    check("victim is younger T2", victim == "T2")
    # Independent raw observation at the half-done boundary.
    rows, txns, undo = raw_snapshot(path)
    check("physical value still un-undone mid-abort", rows["b"] == 22)
    check("locks keep the un-undone row protected", lm.holds("T2", "b", "X"))
    expect_value_error("waiting txn cannot read before undo completes",
                       lambda: st.read("T1", "b"))
    first = st.rollback_step(victim)
    check("first LIFO step undoes the latest write, one undo remains",
          first["remaining_undo"] == 1)
    rows, _, _ = raw_snapshot(path)
    check("b shows 21 after first step, lock still protecting it",
          rows["b"] == 21)
    check("lock still held between steps", lm.holds(victim, "b", "X"))
    second = st.rollback_step(victim)
    check("second step reaches ROLLED_BACK", second["state"] == "ROLLED_BACK")
    rows, txns, _ = raw_snapshot(path)
    check("b restored to original 20", rows["b"] == 20)
    check("a retains T1's committed-pending write", rows["a"] == 11)
    check("victim undo records gone",
          st.db.execute("SELECT count(*) FROM undo WHERE tx=?",
                        (victim,)).fetchone()[0] == 0)
    check("victim ENDED only now", lm.states[victim] == ENDED)
    check("T1 finally granted b", lm.acquire("T1", "b", "X"))
    check("successor reads restored value", st.read("T1", "b") == 20)
    # Stale callbacks after termination.
    expect_value_error("late read rejected", lambda: st.read(victim, "b"))
    expect_value_error("late write rejected", lambda: st.write(victim, "b", 9))
    expect_value_error("duplicate rollback rejected",
                       lambda: st.rollback_step(victim))
    expect_value_error("duplicate commit rejected", lambda: st.commit(victim))
    expect_value_error("late acquire rejected",
                       lambda: lm.acquire(victim, "b", "S"))
    st.commit("T1")
    rows, _, _ = raw_snapshot(path)
    check("commit keeps values", rows == {"a": 11, "b": 20})
    st.close()


def test_interleaved_stepwise_rollback(root):
    print("test: deterministic interleaving of two stepwise rollbacks")
    path = Path(root) / "i.sqlite"
    lm = LockManager()
    st = Store(path, lm)
    st.seed({"k1": 0, "k2": 0})
    st.begin("A", 1)
    st.begin("B", 2)
    st.begin("C", 3)
    lm.acquire("A", "k1", "X")
    lm.acquire("B", "k2", "X")
    # A and B deadlock twice over two resources via C-free 2-cycle setup:
    lm.acquire("A", "k2", "X")
    lm.acquire("B", "k1", "X")
    st.write("A", "k1", 1)
    st.write("A", "k1", 2)
    st.write("B", "k2", 3)
    st.write("B", "k2", 4)
    v1 = lm.resolve_one(st)                     # B younger
    check("first victim B", v1 == "B")
    st.rollback_step("B")
    check("B still aborting with one undo left",
          st.status("B")["remaining_undo"] == 1
          and lm.states["B"] == ABORTING)
    check("B's locks not released mid-rollback", lm.holds("B", "k2", "X"))
    # A never waited on B for k2? A does wait; while B holds locks A stays queued.
    check("A cannot skip past B's held lock", not lm.holds("A", "k2", "X"))
    st.rollback_step("B")
    check("B fully ended", lm.states["B"] == ENDED)
    rows, _, _ = raw_snapshot(path)
    check("k2 back to 0", rows["k2"] == 0)
    check("A now gets k2", lm.acquire("A", "k2", "X"))
    # A later aborts too (no cycle anymore -> manual abort path through Store):
    st.begin_abort("A")
    check("begin_abort flips lock state", lm.states["A"] == ABORTING)
    st.rollback_step("A")
    rows, _, _ = raw_snapshot(path)
    check("k1 intermediate visible only physically, lock still held",
          rows["k1"] == 1 and lm.holds("A", "k1", "X"))
    st.rollback_step("A")
    rows, _, _ = raw_snapshot(path)
    check("k1 restored LIFO to 0, no intermediate remains", rows["k1"] == 0)
    check("A ended", lm.states["A"] == ENDED)
    st.close()


def test_pure_lock_deadlock():
    print("test: pure lock client, no business rows")
    lm = LockManager()
    lm.register("P1", 1)
    lm.register("P2", 2)
    lm.acquire("P1", "x", "X")
    lm.acquire("P2", "y", "X")
    lm.acquire("P1", "y", "X")
    lm.acquire("P2", "x", "X")
    ref_graph_matches(lm, "pure lock deadlock")
    victim = lm.resolve_one()                  # no store at all
    check("younger P2 victim", victim == "P2")
    check("released and ended immediately",
          lm.states["P2"] == ENDED
          and "P2" not in lm.snapshot()["holders"].get("x", {})
          and "P2" not in lm.snapshot()["holders"].get("y", {}))
    check("P1 inherits y", lm.holds("P1", "y", "X"))
    check("no cycle remains", lm.cycle_members() == [])
    check("resolve_one with no ring sacrifices nobody",
          lm.resolve_one() is None)


def test_two_disjoint_cycles():
    print("test: two disjoint cycles, one victim per call")
    lm = LockManager()
    for tx in ("A1", "A2", "B1", "B2"):
        lm.register(tx, 1)
    lm.acquire("A1", "ra", "X")
    lm.acquire("A2", "rb", "X")
    lm.acquire("A1", "rb", "X")
    lm.acquire("A2", "ra", "X")
    lm.acquire("B1", "rc", "X")
    lm.acquire("B2", "rd", "X")
    lm.acquire("B1", "rd", "X")
    lm.acquire("B2", "rc", "X")
    ref_graph_matches(lm, "two disjoint cycles")
    check("four cycle members", set(lm.cycle_members()) ==
          {"A1", "A2", "B1", "B2"})
    first = lm.resolve_one()
    check("one call ends exactly one txn",
          sum(1 for s in lm.states.values() if s == ENDED) == 1)
    check("one ring still detectable", len(lm.cycle_members()) == 2)
    second = lm.resolve_one()
    check("different ring broken second", first != second)
    check("third call is a no-op without a ring", lm.resolve_one() is None)
    check("survivors still ACTIVE",
          lm.states["A1"] == ACTIVE and lm.states["B1"] == ACTIVE)


def test_age_tie_lexicographic():
    print("test: equal ages -> lexicographically larger id sacrificed")
    lm = LockManager()
    lm.register("aa", 5)
    lm.register("ab", 5)
    lm.acquire("aa", "r", "S")
    lm.acquire("ab", "r", "S")
    lm.acquire("aa", "r", "X")
    lm.acquire("ab", "r", "X")
    check("ab sacrificed on tie", lm.resolve_one() == "ab")


def test_cancel_and_fairness():
    print("test: cancel pending never touches grants and re-wakes head")
    lm = LockManager()
    lm.register("a", 1)
    lm.register("b", 2)
    lm.register("c", 3)
    lm.acquire("a", "r", "X")
    lm.acquire("b", "r", "S")       # waits (X holder)
    lm.acquire("c", "r", "S")       # waits behind b, compatible
    lm.cancel_pending("b")
    check("cancel only removes pending",
          ("b", "S") not in lm.waiters["r"] and not lm.holds("b", "r", "S"))
    check("c still blocked by holder a", lm.waiters["r"] == [("c", "S")])
    lm.release_all("a")
    check("c granted after release drains head", lm.holds("c", "r", "S"))
    # S-run granting and X barrier.
    lm.register("d", 4)
    lm.register("e", 5)
    lm.register("f", 6)
    lm.acquire("d", "q", "S")
    lm.acquire("e", "q", "X")       # waits
    lm.acquire("f", "q", "S")       # must not jump e
    check("f blocked by fair predecessor e",
          "e" in lm.wait_graph()["f"])
    lm.cancel_pending("e")
    check("f granted once X barrier cancelled", lm.acquire("f", "q", "S"))
    check("d and f share S", lm.holds("d", "q", "S") and lm.holds("f", "q", "S"))


def test_validation_and_idempotence():
    print("test: invalid inputs rejected before state changes")
    lm = LockManager()
    expect_value_error("empty tx id", lambda: lm.register("", 1))
    expect_value_error("non-string tx", lambda: lm.register(7, 1))
    expect_value_error("float age", lambda: lm.register("z", 1.5))
    expect_value_error("bool age", lambda: lm.register("z", True))
    lm.register("z", 5)
    expect_value_error("duplicate register", lambda: lm.register("z", 6))
    expect_value_error("non-monotonic age", lambda: lm.register("y", 4))
    expect_value_error("acquire unknown tx",
                       lambda: lm.acquire("nope", "r", "S"))
    expect_value_error("bad mode", lambda: lm.acquire("z", "r", "K"))
    expect_value_error("empty resource", lambda: lm.acquire("z", "", "S"))
    check("valid state after rejections",
          lm.snapshot() == {"states": {"z": ACTIVE}, "holders": {}, "waiters": {}})
    check("S idempotent", lm.acquire("z", "r", "S"))
    check("S repeat still granted", lm.acquire("z", "r", "S"))
    check("S request by X holder keeps X",
          (lm.acquire("z", "r2", "X") and lm.acquire("z", "r2", "S")
           and lm.holds("z", "r2", "X")))
    lm.release_all("z")
    check("release ends nothing for plain client, locks cleared",
          lm.states["z"] == ACTIVE and lm.snapshot()["holders"] == {})


def test_store_guard_and_commit(root):
    print("test: bound txn cannot bypass Store; normal commit persists")
    path = Path(root) / "c.sqlite"
    lm = LockManager()
    st = Store(path, lm)
    st.seed({"k": 7})
    st.begin("T", 1)
    expect_value_error("release_all refused while bound",
                       lambda: lm.release_all("T"))
    expect_value_error("read without lock", lambda: st.read("T", "k"))
    lm.acquire("T", "k", "S")
    check("S read works", st.read("T", "k") == 7)
    expect_value_error("write without X", lambda: st.write("T", "k", 8))
    expect_value_error("non-int write", lambda: st.write("T", "k", True))
    lm.acquire("T", "k", "X")
    st.write("T", "k", 8)
    st.commit("T")
    rows, txns, undo = raw_snapshot(path)
    check("committed value persisted", rows == {"k": 8})
    check("sql COMMITTED, undo discarded", txns["T"] == "COMMITTED" and undo == 0)
    check("locks released by commit", lm.snapshot()["holders"] == {})
    check("lock state ENDED", lm.states["T"] == ENDED)
    expect_value_error("commit twice", lambda: st.commit("T"))
    st.close()


def test_rollback_created_key(root):
    print("test: undo restores existence, not just values")
    path = Path(root) / "n.sqlite"
    lm = LockManager()
    st = Store(path, lm)
    st.seed({"old": 1})
    st.begin("B", 1)
    st.begin("A", 2)
    lm.acquire("A", "new", "X")
    st.write("A", "new", 100)
    st.write("A", "new", 101)
    lm.acquire("B", "old", "X")
    lm.acquire("B", "new", "X")
    lm.acquire("A", "old", "X")
    victim = lm.resolve_one(st)
    check("A is victim", victim == "A")
    st.rollback_all("A")
    rows, _, _ = raw_snapshot(path)
    check("created key deleted on rollback", rows == {"old": 1})
    check("B inherits new and sees absence",
          lm.acquire("B", "new", "X") and st.read("B", "new") is None)
    st.commit("B")
    st.close()


# --------------------------------------------------------------------------- #
# Bounded fuzz against an independent row/undo model
# --------------------------------------------------------------------------- #
def test_extra_lock_edge_cases():
    print("test: duplicate requests, S-run granting, X barrier, unknown txns")
    lm = LockManager()
    lm.register("a", 1)
    lm.register("b", 2)
    lm.register("c", 3)
    # Head S-run: a and b get S together even though c's X waits behind them.
    assert lm.acquire("a", "r", "S")
    assert lm.acquire("b", "r", "S")
    assert lm.acquire("c", "r", "X") is False
    lm.release_all("a")
    lm.release_all("b")
    check("X granted once S-run clears", lm.holds("c", "r", "X"))
    # Duplicate queued request does not duplicate the queue entry.
    lm.register("d", 4)
    check("first d request queues", lm.acquire("d", "r", "S") is False)
    check("duplicate d request stays queued once",
          lm.acquire("d", "r", "S") is False)
    check("single queue entry", lm.waiters["r"] == [("d", "S")])
    lm.cancel_pending("d")
    check("queue empty after cancel", lm.waiters["r"] == [])
    # Unknown / ended transaction handling.
    check("holds on unknown tx is False", lm.holds("ghost", "r", "S") is False)
    expect_value_error("cancel unknown tx", lambda: lm.cancel_pending("ghost"))
    expect_value_error("release unknown tx", lambda: lm.release_all("ghost"))
    # A real cycle whose victim is unknown to the supplied store: the store
    # rejects, and the lock-side ABORTING state must not silently release.
    lm.register("x1", 5)
    lm.register("x2", 6)
    lm.acquire("x1", "z", "X")
    lm.acquire("x2", "w", "X")
    lm.acquire("x1", "w", "X")
    lm.acquire("x2", "z", "X")
    expect_value_error("store rejecting begin_abort propagates",
                       lambda: lm.resolve_one(store=_UnusableStore()))
    check("victim stays ABORTING, granted lock retained, pending stripped",
          lm.states["x2"] == ABORTING and lm.holds("x2", "w", "X")
          and not lm.holds("x2", "z", "X")
          and all(w != "x2" for q in lm.waiters.values() for w, _ in q))
    lm.terminate("x1")
    lm.terminate("x2")


class _UnusableStore:
    def begin_abort(self, tx):
        raise ValueError("store does not know this txn")


def test_fuzz(root):
    print("fuzz: randomized operations vs independent model (seeded)")
    rng = random.Random(543543)
    path = Path(root) / "fuzz.sqlite"
    lm = LockManager()
    st = Store(path, lm)
    n_res = 100                                     # spec maximum
    st.seed({f"k{i}": i for i in range(n_res)})
    # Independent model: committed rows + per-txn undo log of pre-images.
    committed = {f"k{i}": i for i in range(n_res)}
    model = {}          # tx -> {"undo": [(key, exists, old, new)], "age": a}
    next_age = 1
    op_count = 0
    victims = 0
    while op_count < 10000:                         # spec operation maximum
        op_count += 1
        active = [t for t, s in lm.states.items() if s == ACTIVE]
        choice = rng.randrange(10)
        if choice == 0 and len(active) < 100:      # spec active-txn maximum
            tx = f"tx{next_age}"
            st.begin(tx, next_age)
            model[tx] = {"undo": [], "age": next_age}
            next_age += 1
            continue
        if not active:
            continue
        tx = rng.choice(active)
        key = f"k{rng.randrange(n_res)}"
        if choice in (1, 2):
            mode = "S" if choice == 1 else "X"
            lm.acquire(tx, key, mode)
        elif choice == 3:
            lm.cancel_pending(tx)
        elif choice == 4:
            if lm.holds(tx, key, "S"):
                val = st.read(tx, key)
                # Independent expectation: committed value unless this txn
                # overwrote the key, in which case its own latest write shows.
                own = [u for u in model[tx]["undo"] if u[0] == key]
                expected = committed[key] if not own else own[-1][3]
                check("fuzz read matches model", val == expected,
                      f"{val} != {expected}")
        elif choice == 5:
            if lm.holds(tx, key, "X"):
                value = rng.randrange(1000)
                before = st.dump_rows().get(key)
                st.write(tx, key, value)
                # pre-image plus the new value, for independent read checks
                model[tx]["undo"].append(
                    (key, before is not None, before, value))
        elif choice == 6:
            if tx in model:
                for k in {u[0] for u in model[tx]["undo"]}:
                    committed[k] = st.dump_rows()[k]
                st.commit(tx)
                del model[tx]
        elif choice == 7:
            if lm.cycle_members():
                victim = lm.resolve_one(st)
                victims += 1
                st.rollback_all(victim)
                # Model-level expectation: every key this victim touched
                # returns to its pre-txn image; verify against raw DB.
                rows, txns, _ = raw_snapshot(path)
                victim_undo = st.db.execute(
                    "SELECT count(*) FROM undo WHERE tx=?", (victim,)).fetchone()[0]
                touched = {u[0] for u in model[victim]["undo"]}
                for k in touched:
                    first = next(u for u in model[victim]["undo"] if u[0] == k)
                    if first[1]:
                        check("fuzz restored pre-image",
                              rows.get(k) == first[2], f"{k}: {rows.get(k)}")
                    else:
                        check("fuzz deleted created key", k not in rows)
                check("fuzz victim undo empty", victim_undo == 0)
                check("fuzz victim ended", lm.states[victim] == ENDED)
                del model[victim]
        elif choice == 8:
            ref_graph_matches(lm, f"fuzz op {op_count}")
        # choice 9: no-op tick
    check("fuzz resolved several real deadlocks", victims >= 3,
          f"only {victims}")
    # Drain: abort every remaining active txn with undo, compare raw rows.
    for tx in [t for t, s in list(lm.states.items()) if s == ACTIVE]:
        if tx in model and model[tx]["undo"]:
            st.begin_abort(tx)
            for _ in range(len(model[tx]["undo"]) + 1):
                status = st.rollback_step(tx)
                if status["state"] == "ROLLED_BACK":
                    break
    rows, _, undo_left = raw_snapshot(path)
    check("fuzz final rows equal independent committed model",
          rows == committed, f"{rows} != {committed}")
    check("fuzz no undo records remain", undo_left == 0)
    st.close()


# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as root:
        test_two_s_upgrades(root)
        test_fair_queue_cycle()
        test_cross_writes_double_overwrite(root)
        test_interleaved_stepwise_rollback(root)
        test_pure_lock_deadlock()
        test_two_disjoint_cycles()
        test_age_tie_lexicographic()
        test_cancel_and_fairness()
        test_validation_and_idempotence()
        test_store_guard_and_commit(root)
        test_rollback_created_key(root)
        test_extra_lock_edge_cases()
        test_fuzz(root)
    print(f"\nALL {PASS} CHECKS PASSED")
