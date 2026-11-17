"""Persistent text lookup using the catalogue's current collation."""
import sqlite3


class Store:
    def __init__(self, path, registry, name='name'):
        self.path, self.registry, self.name = path, registry, name
        self.db = sqlite3.connect(path)
        self.activate()
        self.db.execute('CREATE TABLE IF NOT EXISTS docs(id INTEGER PRIMARY KEY, word TEXT NOT NULL)')
        self.db.execute('CREATE INDEX IF NOT EXISTS words_by_name ON docs(word COLLATE dataset_text)')
        self.db.commit()

    def activate(self):
        handle = self.registry.snapshot(self.name)
        self.db.create_collation('dataset_text', handle.compare)
        self.handle = handle

    def add(self, rows):
        self.db.executemany('INSERT INTO docs VALUES (?,?)', rows)
        self.db.commit()

    def equal(self, word):
        return self.db.execute('SELECT id,word FROM docs INDEXED BY words_by_name WHERE word = ? COLLATE dataset_text ORDER BY id', (word,)).fetchall()

    def order(self):
        return self.db.execute('SELECT id,word FROM docs INDEXED BY words_by_name').fetchall()

    def close(self):
        self.db.close()
