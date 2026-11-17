"""Transaction / per-table write-id directory backed by a real SQLite file.

Two independent numbering spaces live here:

* ``txn_id``        - global, monotonically increasing from 1, never reused.
* ``write_id``      - per table, monotonically increasing from 1, never reused
                      inside that table.  A transaction may write several
                      tables (each gets its own counter) or skip a table, so
                      one numbering must never be used in place of the other.

All allocation, state and identity data is persisted in the SQLite file; a
new process reopening the same file sees the full history.  Only single
process, sequentially ordered callers are required - no distributed commit
and no crash-recovery of in-flight writers.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

TERMINAL_STATES = ("committed", "aborted")
SNAPSHOT_FORMAT = "txn-snapshot/v1"


class DirectoryError(ValueError):
    """Raised for unknown transactions, invalid input or illegal transitions."""


def _is_int(value) -> bool:
    # bool is a subclass of int; the contract explicitly rejects booleans.
    return isinstance(value, int) and not isinstance(value, bool)


def _is_table(value) -> bool:
    return isinstance(value, str) and value != ""


class Directory:
    def __init__(self, path):
        self.path = str(path)
        self.db = sqlite3.connect(self.path)
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute(
            """CREATE TABLE IF NOT EXISTS txns(
                   txn_id INTEGER PRIMARY KEY,
                   state  TEXT NOT NULL CHECK(state IN ('open','committed','aborted'))
               )"""
        )
        self.db.execute(
            """CREATE TABLE IF NOT EXISTS writes(
                   table_name TEXT NOT NULL,
                   write_id   INTEGER NOT NULL,
                   txn_id     INTEGER NOT NULL,
                   PRIMARY KEY(table_name, write_id),
                   UNIQUE(table_name, txn_id)
               )"""
        )
        self.db.commit()

    # ---------------------------------------------------------------- write
    def begin(self):
        """Allocate the next global txn_id (from 1, never reused)."""
        txn_id = self.db.execute(
            "SELECT COALESCE(MAX(txn_id),0)+1 FROM txns"
        ).fetchone()[0]
        with self.db:
            self.db.execute(
                "INSERT INTO txns(txn_id, state) VALUES(?, 'open')", (txn_id,)
            )
        return txn_id

    def allocate(self, txn_id, table):
        """Allocate this table's next write_id for an *open* transaction.

        Retrying with the same (txn_id, table) returns the original id.
        Different tables keep independent counters.
        """
        if not _is_int(txn_id):
            raise DirectoryError("txn_id must be a non-boolean integer")
        if not _is_table(table):
            raise DirectoryError("table must be a non-empty string")
        row = self.db.execute(
            "SELECT state FROM txns WHERE txn_id=?", (txn_id,)
        ).fetchone()
        if row is None:
            raise DirectoryError(f"unknown transaction {txn_id}")
        if row[0] != "open":
            raise DirectoryError(
                f"transaction {txn_id} is {row[0]}, only open transactions allocate write ids"
            )
        old = self.db.execute(
            "SELECT write_id FROM writes WHERE table_name=? AND txn_id=?",
            (table, txn_id),
        ).fetchone()
        if old is not None:  # idempotent retry inside the same transaction
            return old[0]
        next_id = self.db.execute(
            "SELECT COALESCE(MAX(write_id),0)+1 FROM writes WHERE table_name=?",
            (table,),
        ).fetchone()[0]
        with self.db:
            self.db.execute(
                "INSERT INTO writes(table_name, write_id, txn_id) VALUES(?,?,?)",
                (table, next_id, txn_id),
            )
        return next_id

    def finish(self, txn_id, state):
        """Finalize a transaction. Idempotent on identical terminal state."""
        if not _is_int(txn_id):
            raise DirectoryError("txn_id must be a non-boolean integer")
        if state not in TERMINAL_STATES:
            raise DirectoryError(f"state must be one of {TERMINAL_STATES}, got {state!r}")
        row = self.db.execute(
            "SELECT state FROM txns WHERE txn_id=?", (txn_id,)
        ).fetchone()
        if row is None:
            raise DirectoryError(f"unknown transaction {txn_id}")
        current = row[0]
        if current == state:
            return  # identical retry is a no-op success
        if current != "open":
            raise DirectoryError(
                f"transaction {txn_id} already {current}; terminal state cannot be reversed"
            )
        with self.db:
            self.db.execute(
                "UPDATE txns SET state=? WHERE txn_id=?", (state, txn_id)
            )

    # -------------------------------------------------------------- read API
    def snapshot(self, table):
        """Fixed-as-of-now snapshot for one table.

        The exception lists contain *table write ids only* - global txn ids
        never appear there.  The returned plain dict is JSON ready and stays
        valid forever: later commits/aborts cannot mutate it.
        """
        if not _is_table(table):
            raise DirectoryError("table must be a non-empty string")
        txn_high = self.db.execute(
            "SELECT COALESCE(MAX(txn_id),0) FROM txns"
        ).fetchone()[0]
        rows = self.db.execute(
            """SELECT w.write_id, t.state
                 FROM writes w JOIN txns t USING(txn_id)
                WHERE w.table_name=?
                ORDER BY w.write_id""",
            (table,),
        ).fetchall()
        high_write_id = rows[-1][0] if rows else 0
        open_ids = [write_id for write_id, state in rows if state == "open"]
        aborted_ids = [write_id for write_id, state in rows if state == "aborted"]
        invalid_ids = sorted(set(open_ids) | set(aborted_ids))
        return {
            "format": SNAPSHOT_FORMAT,
            "table": table,
            "txn_high_watermark": txn_high,
            "high_write_id": high_write_id,
            "open_write_ids": open_ids,
            "aborted_write_ids": aborted_ids,
            "invalid_write_ids": invalid_ids,
        }

    def export_snapshot(self, table, path):
        """Write the snapshot as UTF-8 JSON so an independent reader can load it."""
        snapshot = self.snapshot(table)
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n",
                        encoding="utf-8")
        return path

    def tables(self):
        return [
            row[0]
            for row in self.db.execute(
                "SELECT DISTINCT table_name FROM writes ORDER BY table_name"
            )
        ]

    def status(self):
        """Directory state check, fully derived from the persisted tables."""
        txn_rows = self.db.execute(
            "SELECT state, COUNT(*) FROM txns GROUP BY state"
        ).fetchall()
        txn_counts = {state: count for state, count in txn_rows}
        per_table = [
            {
                "table": table,
                "write_count": self.db.execute(
                    "SELECT COUNT(*) FROM writes WHERE table_name=?", (table,)
                ).fetchone()[0],
                "high_write_id": self.db.execute(
                    "SELECT COALESCE(MAX(write_id),0) FROM writes WHERE table_name=?",
                    (table,),
                ).fetchone()[0],
                "open_write_ids": [
                    r[0]
                    for r in self.db.execute(
                        """SELECT w.write_id FROM writes w JOIN txns t USING(txn_id)
                           WHERE w.table_name=? AND t.state='open'
                           ORDER BY w.write_id""",
                        (table,),
                    )
                ],
            }
            for (table,) in self.db.execute(
                "SELECT DISTINCT table_name FROM writes ORDER BY table_name"
            )
        ]
        return {
            "path": self.path,
            "txn_high_watermark": self.db.execute(
                "SELECT COALESCE(MAX(txn_id),0) FROM txns"
            ).fetchone()[0],
            "transaction_counts": {
                "open": txn_counts.get("open", 0),
                "committed": txn_counts.get("committed", 0),
                "aborted": txn_counts.get("aborted", 0),
            },
            "tables": per_table,
        }

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
