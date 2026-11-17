# Lock service and undo store

Python 3.11, standard library only (real `sqlite3` database files, no other
dependency). From this directory:

```
python3 demo.py     # reproducible incident + every required scenario
python3 tests.py    # 2000+ independent assertions, including a 10k-op fuzz
```

The demo uses temporary real SQLite databases and prints actual database
observations taken through a second, independent sqlite3 connection.

## What was wrong with the starting point

1. `resolve_one` released the victim's granted locks *before* any undo ran, so
   a waiting transaction could read values the victim had not yet undone.
2. Undo applied steps in forward order. A key overwritten twice was restored
   to its *intermediate* value, not the pre-transaction image.
3. `wait_graph` only contained holder edges. Fair-queue predecessors (an
   X-waiter that stops a later compatible S) were invisible, so whole classes
   of real cycles could never be detected.
4. `cancel_pending` never re-drained a queue it had opened; bound logical
   transactions could bypass the Store with `release_all`; input validation
   was partial and could reject only after partial state changes.

## Unified state machine

Locks (`locks.py`) and the SQLite Store (`store.py`, columns of `txns.state`)
share one vocabulary and move in lockstep:

```
ACTIVE ──resolve_one / begin_abort──▶ ABORTING (SQL: ROLLING_BACK)
                                         │  granted locks stay held;
                                         │  pending requests stripped at once;
                                         │  all business undo applied LIFO
                                         ▼
                                     ENDED (SQL: ROLLED_BACK)  ← release barrier
ACTIVE ──commit──▶ ENDED (SQL: COMMITTED), undo log discarded, locks released
```

Rules enforced by both sides:

- A victim's **granted** locks protect its rows for the entire ABORTING phase.
  Only not-yet-granted queue requests are removed immediately, after which
  every affected queue is drained from the head.
- Locks move to a successor only when the Store reaches ROLLED_BACK. Pure
  lock-only clients (`resolve_one(store=None)`) have zero undo steps, so their
  release happens inside the same call — no fake business rows are needed.
- One `resolve_one` call breaks exactly one ring. Disjoint rings require
  repeated calls; with no ring the call returns `None` and never sacrifices a
  transaction merely because it has waited long.
- Victim selection: highest age (youngest); ties broken by the lexicographically
  larger transaction id. Ages are checked for global monotonicity at register.
- Waiting requests never count as held locks: reads need granted S/X, writes
  need granted X. An S→X upgrade keeps the granted S while queued.
- Fair FIFO per resource: a contiguous run of compatible S requests at the
  head is granted together; a waiting X is a barrier no later S can cross.
- Wait edges = conflicting *holders* plus the FIFO predecessors that truly
  block the request (X conflicts with every predecessor; S only with an X
  predecessor). Compatible queue members that do not block add no edge.
  Cycles are computed with Tarjan SCC; `tests.py` cross-checks the graph and
  cycles against an independently written reference model.
- Bound logical transactions cannot call `release_all` while unfinished;
  terminal release is owned by `commit` / rollback completion. Repeated
  termination, late reads/writes/acquires and stale rollback callbacks are
  rejected and never revive a finished transaction.

## Undo log and the two rollback boundaries

Every write records the full pre-image `(key, old_exists, old_value)`, so
repeated overwrites and creation of previously absent keys are both
reversible. Steps are restored in **reverse** order: a key written twice
returns directly to its original content with no intermediate value left
behind. Each physical step is still its own short SQLite transaction.

Two boundaries over the same SQL state machine are offered:

| | `rollback_step` (stepwise + barrier) | `rollback_all` (atomic) |
|---|---|---|
| Observation | one undo per call; ABORTING/locks held between calls; release only on the final call | no half-undone state is ever observable; one SQLite transaction undoes everything |
| Wait time | successors wait until the *last* step; lock grants happen late but interleaving of multiple aborts is explicitly deterministic | successors wait the same total wall time, then receive all grants in one drain |
| Implementation state | caller drives `remaining_undo` to zero; the barrier lives in the status transition to ROLLED_BACK | bookkeeping collapses to a single loop; same `terminate()` barrier afterwards |

Both paths call `LockManager.terminate` only after the SQL state is
ROLLED_BACK, so the safety property (no successor reads an un-restored row)
is identical; the choice only changes how much intermediate state a
same-process observer can step through. `finish_abort` remains as a
compatibility wrapper around the atomic path.

## Scope

Single process, externally serialized call order, no threads or cross-process
coordination. Lock ownership is in-memory and is not recovered after a crash;
the SQLite business rows and undo log themselves are durable in the file.
Validated up to 100 active transactions, 100 resources, 10000 operations.
