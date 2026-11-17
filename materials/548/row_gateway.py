"""Actual SQLite rows with a first version of policy-aware access."""
import sqlite3


class RowGateway:
    def __init__(self, path, catalog):
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.execute('CREATE TABLE IF NOT EXISTS facts(id INTEGER PRIMARY KEY, tenant TEXT NOT NULL, owner TEXT NOT NULL, value INTEGER CHECK(value >= 0))')
        self.db.commit()
        self.catalog = catalog

    def seed(self, rows):
        self.db.executemany('INSERT INTO facts VALUES(:id,:tenant,:owner,:value)', rows)
        self.db.commit()

    def select(self, actor):
        plan = self.catalog.compile(actor, 'SELECT')
        rows = [dict(r) for r in self.db.execute('SELECT * FROM facts ORDER BY id')]
        return {'revision': plan.revision, 'rows': [r for r in rows if plan.allows('using', r)]}

    def update(self, actor, patches):
        plan = self.catalog.compile(actor, 'UPDATE')
        changed = []
        for patch in patches:
            old = self.db.execute('SELECT * FROM facts WHERE id=?', (patch['id'],)).fetchone()
            if old is None or not plan.allows('using', dict(old)):
                continue
            new = dict(old)
            new.update(patch)
            self.db.execute('UPDATE facts SET tenant=:tenant,owner=:owner,value=:value WHERE id=:id', new)
            self.db.commit()
            changed.append(new['id'])
        return {'revision': plan.revision, 'changed_ids': changed}

    def close(self):
        self.db.close()
