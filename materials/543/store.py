"""Real SQLite row store with logical multi-step transactions and undo.

Each SQLite short transaction still protects one physical operation (or one
undo step); the *logical* transaction spans many calls and is tracked in the
txns table with the same state vocabulary as locks.py:

    ACTIVE -> ROLLING_BACK -> ROLLED_BACK
    ACTIVE -> COMMITTED

The undo log stores the full pre-image (existence + value) for every write,
including repeated overwrites of one key and creation of previously absent
keys. Undo applies steps in REVERSE order, so a key overwritten twice is
restored straight to its original content with no intermediate value left
behind. The lock manager never releases a victim's granted X locks until this
Store reaches ROLLED_BACK: the row under restoration stays protected until
the barrier completes.
"""
import sqlite3

from locks import ACTIVE, ABORTING, ENDED

# The SQL txns.state column stores the matching strings ('ACTIVE',
# 'ROLLING_BACK', 'ROLLED_BACK', 'COMMITTED') and is compared as literals.


class Store:
    def __init__(self, path, locks):
        self.path = str(path)
        self.locks = locks
        # Single process, externally serialized call order: the default
        # deferred isolation is enough; each `with self.db:` block is one
        # physical SQLite transaction protecting one logical operation.
        self.db = sqlite3.connect(self.path)
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS user_rows(
                key TEXT PRIMARY KEY, value INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS txns(
                tx TEXT PRIMARY KEY, age INTEGER NOT NULL,
                state TEXT NOT NULL, next_step INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS undo(
                tx TEXT NOT NULL, step INTEGER NOT NULL, key TEXT NOT NULL,
                old_exists INTEGER NOT NULL, old_value INTEGER,
                PRIMARY KEY(tx, step));
        """)
        self.db.commit()

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _check_key(key):
        if not isinstance(key, str) or not key:
            raise ValueError("key must be a non-empty string")

    @staticmethod
    def _check_value(value):
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError("value must be an integer")

    def _sql_state(self, tx):
        row = self.db.execute("SELECT state FROM txns WHERE tx=?", (tx,)).fetchone()
        if row is None:
            raise ValueError(f"unknown logical transaction {tx!r}")
        return row[0]

    def seed(self, rows):
        """Install initial business rows before any transaction starts."""
        if not isinstance(rows, dict):
            raise ValueError("seed expects a {key: int} mapping")
        prepared = []
        for key, value in rows.items():
            self._check_key(key)
            self._check_value(value)
            prepared.append((key, value))
        with self.db:
            self.db.executemany(
                "INSERT INTO user_rows(key, value) VALUES (?,?)", prepared)

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #
    def begin(self, tx, age):
        # All validation happens inside register/bind before any SQL write.
        self.locks.register(tx, age)
        self.locks.bind_store(tx)
        with self.db:
            self.db.execute(
                "INSERT INTO txns(tx, age, state, next_step) VALUES (?,?,'ACTIVE',1)",
                (tx, age))

    def status(self, tx):
        state = self._sql_state(tx)
        remaining = self.db.execute(
            "SELECT count(*) FROM undo WHERE tx=?", (tx,)).fetchone()[0]
        return {
            "state": state,
            "remaining_undo": remaining,
            "lock_state": self.locks.states[tx],
        }

    # ------------------------------------------------------------------ #
    # row operations (waiting requests never count as held locks)
    # ------------------------------------------------------------------ #
    def read(self, tx, key):
        self._check_key(key)
        if self._sql_state(tx) != ACTIVE:
            raise ValueError("read requires an ACTIVE logical transaction")
        if not self.locks.holds(tx, key, "S"):
            raise ValueError("read requires a granted S or X lock")
        row = self.db.execute(
            "SELECT value FROM user_rows WHERE key=?", (key,)).fetchone()
        return None if row is None else row[0]

    def write(self, tx, key, value):
        self._check_key(key)
        self._check_value(value)
        if self._sql_state(tx) != ACTIVE:
            raise ValueError("write requires an ACTIVE logical transaction")
        if not self.locks.holds(tx, key, "X"):
            raise ValueError("write requires a granted X lock")
        with self.db:
            step = self.db.execute(
                "SELECT next_step FROM txns WHERE tx=?", (tx,)).fetchone()[0]
            old = self.db.execute(
                "SELECT value FROM user_rows WHERE key=?", (key,)).fetchone()
            self.db.execute(
                "INSERT INTO undo(tx, step, key, old_exists, old_value) "
                "VALUES (?,?,?,?,?)",
                (tx, step, key, int(old is not None),
                 None if old is None else old[0]))
            self.db.execute(
                "INSERT INTO user_rows(key, value) VALUES (?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value))
            self.db.execute(
                "UPDATE txns SET next_step=next_step+1 WHERE tx=?", (tx,))

    # ------------------------------------------------------------------ #
    # rollback: shared SQL state machine, two boundary choices
    # ------------------------------------------------------------------ #
    def begin_abort(self, tx):
        """Flip ACTIVE -> ROLLING_BACK. Called by LockManager.resolve_one
        (which has already moved the lock-side state to ABORTING) or directly."""
        if self._sql_state(tx) != ACTIVE:
            raise ValueError("abort requires an ACTIVE transaction")
        if self.locks.states.get(tx) == ACTIVE:
            self.locks.states[tx] = ABORTING
            # Direct abort: strip pending requests exactly like resolve_one.
            self.locks.cancel_pending(tx)
        with self.db:
            self.db.execute(
                "UPDATE txns SET state='ROLLING_BACK' WHERE tx=?", (tx,))

    def _restore_one(self, tx):
        """Undo the most recent remaining step (LIFO). Returns False if none."""
        item = self.db.execute(
            "SELECT step, key, old_exists, old_value FROM undo "
            "WHERE tx=? ORDER BY step DESC LIMIT 1", (tx,)).fetchone()
        if item is None:
            return False
        step, key, old_exists, old_value = item
        if old_exists:
            self.db.execute(
                "INSERT INTO user_rows(key, value) VALUES (?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, old_value))
        else:
            self.db.execute("DELETE FROM user_rows WHERE key=?", (key,))
        self.db.execute("DELETE FROM undo WHERE tx=? AND step=?", (tx, step))
        return True

    def rollback_step(self, tx):
        """Stepwise boundary: one undo step per call; release only at the end."""
        if self._sql_state(tx) != "ROLLING_BACK":
            raise ValueError("transaction is not ROLLING_BACK")
        with self.db:
            self._restore_one(tx)
            if not self.db.execute(
                    "SELECT 1 FROM undo WHERE tx=? LIMIT 1", (tx,)).fetchone():
                self.db.execute(
                    "UPDATE txns SET state='ROLLED_BACK' WHERE tx=?", (tx,))
        if self._sql_state(tx) == "ROLLED_BACK":
            # Completion barrier: all business undo is physically done, so the
            # protecting locks may finally move to successors.
            self.locks.terminate(tx)
        return self.status(tx)

    def rollback_all(self, tx):
        """Atomic boundary: undo every remaining step inside one SQLite
        transaction, then release. No half-undone state is observable."""
        if self._sql_state(tx) != "ROLLING_BACK":
            raise ValueError("transaction is not ROLLING_BACK")
        with self.db:
            while self._restore_one(tx):
                pass
            self.db.execute(
                "UPDATE txns SET state='ROLLED_BACK' WHERE tx=?", (tx,))
        self.locks.terminate(tx)
        return self.status(tx)

    def commit(self, tx):
        if self._sql_state(tx) != ACTIVE:
            raise ValueError("commit requires an ACTIVE transaction")
        with self.db:
            self.db.execute("DELETE FROM undo WHERE tx=?", (tx,))
            self.db.execute(
                "UPDATE txns SET state='COMMITTED' WHERE tx=?", (tx,))
        # Values persist; terminal barrier releases the protecting locks.
        self.locks.terminate(tx)

    def finish_abort(self, tx, locks=None):
        """Compatibility entry: drive a ROLLING_BACK txn to its terminal state.

        Kept for callers of the old signature (store/locks argument order
        tolerated); new code should call rollback_step/rollback_all directly.
        """
        state = self._sql_state(tx)
        if state == "ROLLED_BACK":
            if self.locks.states.get(tx) != ENDED:
                self.locks.terminate(tx)
            return "RELEASED"
        if state != "ROLLING_BACK":
            raise ValueError("transaction is not being rolled back")
        self.rollback_all(tx)
        return "RELEASED"

    # ------------------------------------------------------------------ #
    # raw observation through an independent connection
    # ------------------------------------------------------------------ #
    def dump_rows(self):
        return dict(self.db.execute(
            "SELECT key, value FROM user_rows ORDER BY key").fetchall())

    def close(self):
        self.db.close()
