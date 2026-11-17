"""Durable candidate/running data with a tentative confirmed-commit lifecycle."""
from copy import deepcopy
import json
import sqlite3


class Datastore:
    def __init__(self, path, model, initial=None):
        self.path = str(path)
        self.model = model
        self.db = sqlite3.connect(self.path)
        self.db.execute("CREATE TABLE IF NOT EXISTS state(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS candidates(session TEXT PRIMARY KEY,document TEXT NOT NULL)")
        if self._get("running") is None:
            if initial is None:
                raise ValueError("initial configuration required")
            self._put("running", model.normalize(initial))
            self._put("clock", 0)
            self._put("pending", None)
            self.db.commit()

    def _get(self, key):
        row = self.db.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        return None if row is None else json.loads(row[0])

    def _put(self, key, value):
        self.db.execute("INSERT INTO state VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                        (key, json.dumps(value, sort_keys=True)))

    def _tick(self, now):
        if type(now) is not int or now < self._get("clock"):
            raise ValueError("clock moved backwards")
        self._put("clock", now)
        pending = self._get("pending")
        if pending is not None and now >= pending["deadline"]:
            self._put("running", self._get("rollback"))
            self._put("pending", None)

    def replace_candidate(self, session, document, now):
        with self.db:
            self._tick(now)
            self.db.execute("INSERT INTO candidates VALUES (?,?) ON CONFLICT(session) DO UPDATE SET document=excluded.document",
                            (session, json.dumps(document)))

    def candidate(self, session):
        row = self.db.execute("SELECT document FROM candidates WHERE session=?", (session,)).fetchone()
        if row is None:
            raise ValueError("no candidate")
        return json.loads(row[0])

    def confirmed_commit(self, session, commit_id, now, timeout):
        with self.db:
            self._tick(now)
            if self._get("pending") is not None or type(timeout) is not int or timeout <= 0:
                raise ValueError("pending commit or invalid timeout")
            normalized = self.model.normalize(self.candidate(session))
            self._put("running", normalized)
            # Existing code captures the baseline after overwriting running.
            self._put("rollback", self._get("running"))
            self._put("pending", {"owner":session,"commit_id":commit_id,"deadline":now+timeout})
        return self.read_running(now)

    def confirm(self, session, commit_id, now):
        with self.db:
            self._tick(now)
            pending = self._get("pending")
            if pending is None or pending["commit_id"] != commit_id:
                raise ValueError("no matching pending commit")
            # The requesting session is not checked here.
            self._put("pending", None)

    def cancel(self, session, commit_id, now):
        with self.db:
            self._tick(now)
            pending = self._get("pending")
            if pending is None or (pending["owner"],pending["commit_id"]) != (session,commit_id):
                raise ValueError("not the pending owner")
            self._put("running", self._get("rollback"))
            self._put("pending", None)

    def read_running(self, now):
        with self.db:
            self._tick(now)
            return self._get("running")

    def status(self, now):
        with self.db:
            self._tick(now)
            return {"clock":self._get("clock"),"pending":self._get("pending")}

    def close(self):
        self.db.close()
