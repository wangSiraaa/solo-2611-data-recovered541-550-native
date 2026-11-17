"""A real SQLite row/undo store behind the logical lock service."""
import sqlite3


class Store:
    def __init__(self, path, locks):
        self.path = str(path)
        self.locks = locks
        self.db = sqlite3.connect(self.path)
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS user_rows(key TEXT PRIMARY KEY, value INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS txns(tx TEXT PRIMARY KEY, age INTEGER NOT NULL,
                                           state TEXT NOT NULL, next_step INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS undo(tx TEXT NOT NULL, step INTEGER NOT NULL,
                                           key TEXT NOT NULL, old_exists INTEGER NOT NULL,
                                           old_value INTEGER, PRIMARY KEY(tx, step));
        """)
        self.db.commit()

    def seed(self, rows):
        with self.db:
            self.db.executemany("INSERT INTO user_rows VALUES (?,?)", rows.items())

    def begin(self, tx, age):
        self.locks.register(tx, age)
        with self.db:
            self.db.execute("INSERT INTO txns VALUES (?,?,'ACTIVE',1)", (tx, age))

    def status(self, tx):
        row = self.db.execute("SELECT state FROM txns WHERE tx=?", (tx,)).fetchone()
        if row is None:
            raise ValueError("unknown transaction")
        count = self.db.execute("SELECT count(*) FROM undo WHERE tx=?", (tx,)).fetchone()[0]
        return {"state": row[0], "remaining_undo": count}

    def read(self, tx, key):
        if self.status(tx)["state"] != "ACTIVE" or not self.locks.holds(tx, key, "S"):
            raise ValueError("read requires an active transaction and a granted lock")
        row = self.db.execute("SELECT value FROM user_rows WHERE key=?", (key,)).fetchone()
        return None if row is None else row[0]

    def write(self, tx, key, value):
        state = self.status(tx)["state"]
        if state in ("COMMITTED", "ROLLED_BACK") or not self.locks.holds(tx, key, "X"):
            raise ValueError("write requires a live transaction and a granted X lock")
        if not isinstance(value, int):
            raise ValueError("integer values only")
        with self.db:
            step = self.db.execute("SELECT next_step FROM txns WHERE tx=?", (tx,)).fetchone()[0]
            old = self.db.execute("SELECT value FROM user_rows WHERE key=?", (key,)).fetchone()
            self.db.execute("INSERT INTO undo VALUES (?,?,?,?,?)",
                            (tx, step, key, int(old is not None), None if old is None else old[0]))
            self.db.execute("INSERT INTO user_rows VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                            (key, value))
            self.db.execute("UPDATE txns SET next_step=next_step+1 WHERE tx=?", (tx,))

    def start_abort(self, tx):
        if self.status(tx)["state"] != "ACTIVE":
            raise ValueError("abort requires active transaction")
        with self.db:
            self.db.execute("UPDATE txns SET state='ROLLING_BACK' WHERE tx=?", (tx,))

    def rollback_step(self, tx):
        if self.status(tx)["state"] != "ROLLING_BACK":
            raise ValueError("transaction is not rolling back")
        with self.db:
            item = self.db.execute("SELECT step,key,old_exists,old_value FROM undo WHERE tx=? ORDER BY step ASC LIMIT 1",
                                   (tx,)).fetchone()
            if item:
                step, key, exists, value = item
                if exists:
                    self.db.execute("INSERT INTO user_rows VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                                    (key, value))
                else:
                    self.db.execute("DELETE FROM user_rows WHERE key=?", (key,))
                self.db.execute("DELETE FROM undo WHERE tx=? AND step=?", (tx, step))
            if not self.db.execute("SELECT 1 FROM undo WHERE tx=? LIMIT 1", (tx,)).fetchone():
                self.db.execute("UPDATE txns SET state='ROLLED_BACK' WHERE tx=?", (tx,))
        return self.status(tx)

    def commit(self, tx):
        if self.status(tx)["state"] != "ACTIVE":
            raise ValueError("commit requires active transaction")
        with self.db:
            self.db.execute("DELETE FROM undo WHERE tx=?", (tx,))
            self.db.execute("UPDATE txns SET state='COMMITTED' WHERE tx=?", (tx,))
        self.locks.states[tx] = "ENDED"
        self.locks.release_all(tx)

    def close(self):
        self.db.close()
