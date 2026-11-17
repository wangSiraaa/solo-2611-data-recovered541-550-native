"""Persistent text lookup with versioned comparison structures.

Design in one paragraph
-----------------------
Rows (``id``, original ``word``) live once in ``docs``; identity is always the
independent integer id.  Every comparison version (identified by the content
hash of its definition) owns a *physically separate* key structure in
``doc_keys``.  A structure row records the comparison key, the original word and
the half-open write-sequence interval ``[from_seq, to_seq)`` during which that
row was the visible state of the id.  Reads bind a version and a write-sequence
``snapshot`` and therefore see exactly the committed state at that point --
never a later update, delete or re-interpretation, and never a duplicate.

A migration builds a complete structure for one explicit catalogue version in a
single write transaction (write transactions are briefly serialised), verifies
it against ``docs``, and only then atomically publishes it.  Writes that landed
after the build snapshot are replayed from the small ``outbox`` interval before
publishing, so a successful insert after the build snapshot is never lost.  The
``doc_versions`` catalogue row keeps the target's original name/revision *and*
definition hash, so reopening the store can verify the persisted rule against the
public catalogue and reject a same-revision/different-definition rebinding.

Ordering: the key is compared with SQLite's ``BINARY`` collation, whose UTF-8
byte order is exactly Unicode code point order for valid text.  This is why the
persistent B-tree never needs a collation callback that could be swapped under
it; changing the rule means building a different structure.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import NamedTuple

from collations import (
    DefinitionError,
    Handle,
    InvalidDefinition,
    UnknownVersion,
    definition_id,
)

__all__ = ['Store', 'ReadHandle', 'ReadRow', 'CrashPoint', 'MigrationResult']

DEFAULT_DEFINITION = {'case_insensitive': True, 'accent_sensitive': True}

_BASE_ROWS = 4000          # executemany batch size while building structures

# Process-wide count of active read handles per (database path, version key).
# Reclaim must not destroy a structure another Store instance still reads.
_ACTIVE_READERS = defaultdict(int)
_ACTIVE_LOCK = threading.Lock()


def _reader_acquire(path, version_key):
    with _ACTIVE_LOCK:
        _ACTIVE_READERS[(path, version_key)] += 1


def _reader_release(path, version_key):
    with _ACTIVE_LOCK:
        key = (path, version_key)
        if _ACTIVE_READERS.get(key, 0) > 0:
            _ACTIVE_READERS[key] -= 1
            if _ACTIVE_READERS[key] == 0:
                del _ACTIVE_READERS[key]


def _reader_count(path, version_key):
    with _ACTIVE_LOCK:
        return _ACTIVE_READERS.get((path, version_key), 0)


class CrashPoint(RuntimeError):
    """Raised by an injected failure hook at a named migration boundary."""


class ReadRow(NamedTuple):
    """One visible row: original id, original word and the comparison version."""
    id: int
    word: str
    name: str
    revision: int
    definition_id: str

    def as_dict(self):
        return self._asdict()

    @property
    def version_key(self):
        return '%s\x1f%d\x1f%s' % (self.name, self.revision, self.definition_id)

    @property
    def comparison_version(self):
        return {'name': self.name, 'revision': self.revision,
                'definition_id': self.definition_id}


class MigrationResult(NamedTuple):
    version_key: str
    name: str
    revision: int
    definition_id: str
    built_seq: int
    published_seq: int
    reused_complete: bool
    already_published: bool


def _now():
    return datetime.now(timezone.utc).isoformat(timespec='microseconds')


def version_key_for(name, revision, definition_id_value):
    """Persistent version key: the full ``(name, revision, definition)`` identity.

    Including the revision keeps repeated registrations of the same definition
    as distinct immutable versions -- the same promise the public catalogue
    makes -- while the definition hash lets a reopened store prove which rule it
    built with.  The same revision number carrying a different definition can
    therefore never be silently merged.
    """
    return '%s\x1f%d\x1f%s' % (name, int(revision), definition_id_value)


def _version_parts(version_key):
    parts = version_key.split('\x1f')
    if len(parts) != 3 or not parts[0] or not parts[2]:
        raise DefinitionError('malformed version key %r' % version_key)
    name, rev, did = parts
    try:
        return name, int(rev), did
    except ValueError:
        raise DefinitionError('malformed version key %r' % version_key) from None


class Store:
    """Versioned text index over one SQLite file.

    Public entry points kept from the starting material: ``Store(path,
    registry, name=...)``, ``add``, ``equal``, ``order``, ``close`` and
    ``activate``.  New entry points: ``open_reader`` (snapshot-bound reads),
    ``migrate`` (build + publish one explicit catalogue version) and ``status``.
    """

    def __init__(self, path, registry, name='name', *, busy_timeout=30.0):
        self.path = path
        self.registry = registry
        self.name = name
        self._lock = threading.RLock()
        self._closed = False
        self._readers = set()
        self._active_readers = defaultdict(int)
        self._persisted_handles = {}
        self.db = sqlite3.connect(path, isolation_level=None, check_same_thread=False,
                                  timeout=busy_timeout)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA foreign_keys=OFF')
        self._create_schema()
        self._recover()
        self._resolve_persisted_versions()
        latest = self._latest_published()
        if latest is None:
            self._bootstrap()
            latest = self._latest_published()
        if latest is None:
            raise RuntimeError('store has no published comparison structure')
        self._set_current(latest['version_key'])
        self._ensure_current_uptodate()

    # ------------------------------------------------------------------ setup
    def _create_schema(self):
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS store_meta(
                key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS docs(
                id INTEGER PRIMARY KEY,
                word TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS doc_versions(
                version_key TEXT PRIMARY KEY,       -- == definition_id
                definition_id TEXT NOT NULL,
                name TEXT NOT NULL,
                revision INTEGER NOT NULL,
                definition_json TEXT NOT NULL,
                catalog_state TEXT NOT NULL,        -- unpublished|published
                state TEXT NOT NULL,                -- building|complete|reclaimed
                built_seq INTEGER,
                struct_seq INTEGER,
                published_seq INTEGER,
                created_at TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS doc_versions_version
                ON doc_versions(name, revision);
            CREATE TABLE IF NOT EXISTS doc_keys(
                version_key TEXT NOT NULL,
                id INTEGER NOT NULL,
                from_seq INTEGER NOT NULL,
                to_seq INTEGER,
                ckey TEXT NOT NULL,
                word TEXT NOT NULL,
                PRIMARY KEY(version_key, id, from_seq));
            CREATE UNIQUE INDEX IF NOT EXISTS doc_keys_open
                ON doc_keys(version_key, id) WHERE to_seq IS NULL;
            CREATE INDEX IF NOT EXISTS doc_keys_order
                ON doc_keys(version_key, ckey, id, from_seq);
            CREATE TABLE IF NOT EXISTS outbox(
                seq INTEGER PRIMARY KEY,
                op TEXT NOT NULL,                   -- put|delete
                id INTEGER NOT NULL,
                word TEXT);
            CREATE TABLE IF NOT EXISTS migrations(
                migration_id TEXT PRIMARY KEY,
                target_version_key TEXT NOT NULL,
                name TEXT NOT NULL,
                revision INTEGER NOT NULL,
                definition_id TEXT NOT NULL,
                state TEXT NOT NULL,                -- started|built|published|aborted|failed
                built_seq INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL);
            """)
        if self._get_meta('write_seq') is None:
            self._set_meta('write_seq', 0)

    def _recover(self):
        """Roll forward published metadata, discard incomplete builds.

        A build is committed atomically, so a crash during the build leaves no
        partial ``doc_keys``; only the audit ``migrations`` row can be stale.
        A build that committed but was not published stays ``complete`` and can
        be safely published by retrying ``migrate``.
        """
        with self._lock, self._txn():
            for (vk,) in self.db.execute(
                    "SELECT version_key FROM doc_versions WHERE state='building'").fetchall():
                self.db.execute('DELETE FROM doc_keys WHERE version_key=?', (vk,))
                self.db.execute('DELETE FROM doc_versions WHERE version_key=?', (vk,))
            self.db.execute(
                "UPDATE migrations SET state='aborted', updated_at=?"
                " WHERE state='started'", (_now(),))

    def _resolve_persisted_versions(self):
        handles = {}
        rows = self.db.execute(
            'SELECT version_key, definition_id, name, revision, definition_json,'
            ' catalog_state, state FROM doc_versions ORDER BY created_at').fetchall()
        for vk, did, name, revision, definition_json, catalog_state, state in rows:
            if state == 'reclaimed':
                continue
            definition = _definition_from_json(definition_json, did)
            if vk != version_key_for(name, revision, did):
                raise DefinitionError(
                    'persisted version key %r does not match its identity' % vk)
            # Restore the *exact* audited revision.  A catalogue version with the
            # same (name, revision) but a different definition is rejected here.
            handle = self.registry.restore(name, revision, definition)
            if handle.definition_id != did:
                raise DefinitionError(
                    'persisted index rule %s does not match catalogue handle %s'
                    % (did, handle.definition_id))
            handles[vk] = handle
        self._persisted_handles = handles

    def _bootstrap(self):
        target = None
        try:
            target = self.registry.snapshot(self.name)
        except UnknownVersion:
            target = self.registry.define(self.name, **DEFAULT_DEFINITION)
        if isinstance(target, Handle):
            self._migrate_to(target, crash_after_build=False, crash_after_publish=False,
                             _created_by='bootstrap')

    # -------------------------------------------------------------- meta/seqs
    def _get_meta(self, key, default=None):
        row = self.db.execute('SELECT value FROM store_meta WHERE key=?', (key,)).fetchone()
        return row[0] if row else default

    def _set_meta(self, key, value):
        self.db.execute(
            'INSERT INTO store_meta(key, value) VALUES (?,?)'
            ' ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, str(value)))

    def _write_seq(self):
        return int(self._get_meta('write_seq', 0))

    @contextmanager
    def _txn(self):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
        except BaseException:
            self.db.execute('ROLLBACK')
            raise
        else:
            self.db.execute('COMMIT')

    # -------------------------------------------------------------- versions
    def _version_row(self, version_key):
        cur = self.db.execute(
            'SELECT version_key, definition_id, name, revision, definition_json,'
            ' catalog_state, state, built_seq, struct_seq, published_seq'
            ' FROM doc_versions WHERE version_key=?', (version_key,))
        row = cur.fetchone()
        if row is None:
            return None
        keys = ('version_key', 'definition_id', 'name', 'revision', 'definition_json',
                'catalog_state', 'state', 'built_seq', 'struct_seq', 'published_seq')
        return dict(zip(keys, row))

    def _latest_published(self):
        # A store binds one active comparison version.  Prefer the most recently
        # published structure for ``self.name``; if that name has none (e.g. the
        # store was constructed for a name not yet used), fall back to the most
        # recent published structure overall.
        row = self.db.execute(
            "SELECT version_key FROM doc_versions"
            " WHERE name=? AND catalog_state='published' AND state='complete'"
            " ORDER BY published_seq DESC, created_at DESC LIMIT 1",
            (self.name,)).fetchone()
        if row is None:
            row = self.db.execute(
                "SELECT version_key FROM doc_versions"
                " WHERE catalog_state='published' AND state='complete'"
                " ORDER BY published_seq DESC, created_at DESC LIMIT 1").fetchone()
        if row is None:
            return None
        return self._version_row(row[0])

    def _set_current(self, version_key):
        row = self._version_row(version_key)
        if (row is None or row['catalog_state'] != 'published'
                or row['state'] != 'complete'):
            raise DefinitionError('version %s is not a published complete structure'
                                  % version_key)
        handle = self._persisted_handles.get(version_key)
        if handle is None:
            name, revision, did = _version_parts(version_key)
            definition = _definition_from_json(row['definition_json'], did)
            handle = self.registry.restore(row['name'], row['revision'], definition)
            if version_key_for(name, revision, handle.definition_id) != version_key:
                raise DefinitionError('cannot bind published version %s' % version_key)
            self._persisted_handles[version_key] = handle
        self._current_vk = version_key
        self._current_handle = handle
        self.name = row['name']

    def _ensure_current_uptodate(self):
        """Safety net: the bound version's structure includes every write."""
        row = self._version_row(self._current_vk)
        if row is None:
            raise RuntimeError('current version vanished')
        target_seq = self._write_seq()
        if row['struct_seq'] is None or row['struct_seq'] < target_seq:
            if (row['struct_seq'] or 0) < self._outbox_floor():
                # History needed to replay has been pruned; rebuild instead.
                self._rebuild_catching_up(self._current_vk, self._current_handle)
            else:
                with self._lock, self._txn():
                    self._replay_outbox(row['version_key'], self._current_handle,
                                        row['struct_seq'] or 0, target_seq)
                    self.db.execute(
                        'UPDATE doc_versions SET struct_seq=? WHERE version_key=?',
                        (target_seq, row['version_key']))

    def _adopt_latest_published(self):
        """If another instance published a newer version, adopt it before writing.

        New writes and new read handles must use the latest published rule; old
        read handles keep the version they bound at open time, so this switch
        does not disturb them.
        """
        latest = self._latest_published()
        if latest is not None and latest['version_key'] != self._current_vk:
            self._set_current(latest['version_key'])
            self._ensure_current_uptodate()

    def _rebuild_catching_up(self, vk, handle):
        """Rebuild a complete published structure in one transaction.

        Used only when the replay history has been pruned.  Safe only with no
        active readers on this version -- their historical snapshot would be
        destroyed.  (Pruning guarantees this: a version with active readers keeps
        the history it needs, so this path is never reached for it.)  The old
        structure is replaced in the same transaction that writes the fresh one,
        so a crash never exposes a partial replacement as published.
        """
        if (_reader_count(self._reader_key(), vk) > 0
                or self._active_readers.get(vk, 0) > 0):
            raise RuntimeError('cannot rebuild version %s with active readers' % vk)
        with self._lock, self._txn():
            built_seq = self._write_seq()
            self.db.execute('DELETE FROM doc_keys WHERE version_key=?', (vk,))
            batch = []
            for id_, word in self.db.execute('SELECT id, word FROM docs ORDER BY id'):
                batch.append((vk, id_, built_seq, None, handle.key(word), word))
                if len(batch) >= _BASE_ROWS:
                    self.db.executemany(
                        'INSERT INTO doc_keys(version_key, id, from_seq, to_seq,'
                        ' ckey, word) VALUES (?,?,?,?,?,?)', batch)
                    batch.clear()
            if batch:
                self.db.executemany(
                    'INSERT INTO doc_keys(version_key, id, from_seq, to_seq,'
                    ' ckey, word) VALUES (?,?,?,?,?,?)', batch)
            self._verify_built_structure(handle, vk, built_seq)
            self.db.execute(
                'UPDATE doc_versions SET struct_seq=?, built_seq=? WHERE version_key=?',
                (built_seq, built_seq, vk))

    # ---------------------------------------------------------------- writing
    def add(self, rows):
        """Insert ``(id, word)`` rows; ids are independent identities.

        Two rows with equal comparison keys are kept as distinct records.
        """
        rows = [(int(i), _require_text(w)) for i, w in rows]
        with self._lock:
            self._ensure_writable()
            self._adopt_latest_published()
            with self._txn():
                seq = self._write_seq()
                seen = set()
                for id_, word in rows:
                    if id_ in seen:
                        raise ValueError('duplicate id %r in one add batch' % id_)
                    seen.add(id_)
                    if self.db.execute('SELECT 1 FROM docs WHERE id=?', (id_,)).fetchone():
                        raise sqlite3.IntegrityError('id %r already exists' % id_)
                    seq += 1
                    key = self._current_handle.key(word)
                    self.db.execute('INSERT INTO docs(id, word) VALUES (?,?)', (id_, word))
                    self.db.execute(
                        'INSERT INTO doc_keys(version_key, id, from_seq, to_seq, ckey, word)'
                        ' VALUES (?,?,?,NULL,?,?)',
                        (self._current_vk, id_, seq, key, word))
                    self.db.execute('INSERT INTO outbox(seq, op, id, word) VALUES (?,?,?,?)',
                                    (seq, 'put', id_, word))
                    self.db.execute(
                        'UPDATE doc_versions SET struct_seq=? WHERE version_key=?',
                        (seq, self._current_vk))
                self._set_meta('write_seq', seq)
                self._prune_outbox()

    def update(self, rows):
        """Update words by id, preserving identity and history for old readers."""
        rows = [(int(i), _require_text(w)) for i, w in rows]
        with self._lock:
            self._ensure_writable()
            self._adopt_latest_published()
            with self._txn():
                seq = self._write_seq()
                for id_, word in rows:
                    old = self.db.execute('SELECT word FROM docs WHERE id=?', (id_,)).fetchone()
                    if old is None:
                        raise KeyError('no row with id %r' % (id_,))
                    open_row = self.db.execute(
                        'SELECT 1 FROM doc_keys WHERE version_key=? AND id=?'
                        ' AND to_seq IS NULL', (self._current_vk, id_)).fetchone()
                    if old[0] == word and open_row is not None:
                        continue
                    seq += 1
                    self._close_open(self._current_vk, id_, seq)
                    key = self._current_handle.key(word)
                    self.db.execute(
                        'INSERT INTO doc_keys(version_key, id, from_seq, to_seq, ckey, word)'
                        ' VALUES (?,?,?,NULL,?,?)',
                        (self._current_vk, id_, seq, key, word))
                    self.db.execute('UPDATE docs SET word=? WHERE id=?', (word, id_))
                    self.db.execute('INSERT INTO outbox(seq, op, id, word) VALUES (?,?,?,?)',
                                    (seq, 'put', id_, word))
                    self.db.execute(
                        'UPDATE doc_versions SET struct_seq=? WHERE version_key=?',
                        (seq, self._current_vk))
                self._set_meta('write_seq', seq)
                self._prune_outbox()

    def delete(self, ids):
        """Delete ids from the live table while keeping them visible to old readers."""
        ids = [int(i) for i in ids]
        with self._lock:
            self._ensure_writable()
            self._adopt_latest_published()
            with self._txn():
                seq = self._write_seq()
                for id_ in ids:
                    if self.db.execute('SELECT 1 FROM docs WHERE id=?', (id_,)).fetchone() is None:
                        raise KeyError('no row with id %r' % (id_,))
                    seq += 1
                    self._close_open(self._current_vk, id_, seq)
                    self.db.execute('DELETE FROM docs WHERE id=?', (id_,))
                    self.db.execute('INSERT INTO outbox(seq, op, id, word) VALUES (?,?,?,NULL)',
                                    (seq, 'delete', id_))
                    self.db.execute(
                        'UPDATE doc_versions SET struct_seq=? WHERE version_key=?',
                        (seq, self._current_vk))
                self._set_meta('write_seq', seq)
                self._prune_outbox()

    def _close_open(self, version_key, id_, seq):
        self.db.execute(
            'UPDATE doc_keys SET to_seq=? WHERE version_key=? AND id=? AND to_seq IS NULL',
            (seq, version_key, id_))

    def _prune_outbox(self):
        """Keep only the write history a structure could still need replayed.

        Old readers rely on ``doc_keys`` intervals, not the outbox.  We must keep
        every write from the earliest structure that either (a) is still
        unpublished (a post-build crash awaiting publish), or (b) is published
        but still has active readers and could be caught up by replay rather than
        rebuild.  Structures that are idle and non-current are reclaim/rebuild
        candidates, so their lag does not pin history.
        """
        horizon = None
        for vk, struct_seq, catalog_state, state in self.db.execute(
                'SELECT version_key, struct_seq, catalog_state, state FROM doc_versions'
                " WHERE state='complete' AND struct_seq IS NOT NULL"):
            needs_replay = (catalog_state == 'unpublished'
                            or vk == self._current_vk
                            or self._active_readers.get(vk, 0) > 0
                            or _reader_count(self._reader_key(), vk) > 0)
            if needs_replay:
                horizon = struct_seq if horizon is None else min(horizon, struct_seq)
        if horizon is None:
            horizon = self._write_seq()
        if horizon > 0:
            self.db.execute('DELETE FROM outbox WHERE seq<=?', (horizon,))
            floor = int(self._get_meta('outbox_floor', 0) or 0)
            self._set_meta('outbox_floor', max(floor, horizon))

    def reclaim_unused_structures(self):
        """Explicitly drop non-current published structures with no active readers.

        Reclamation is otherwise performed automatically when the *last* read
        handle of a superseded version is closed (see ``ReadHandle.close``).
        This method exists for callers that want to trigger it directly.  It is
        deliberately not run on every migration: that would race readers held by
        another ``Store`` instance on the same file.
        """
        with self._lock, self._txn():
            self._reclaim_idle_structures()

    def _reclaim_idle_structures(self, protect=None):
        """Reclaim complete, non-current structures with no active readers."""
        for row in self.db.execute(
                "SELECT version_key FROM doc_versions"
                " WHERE state='complete' AND catalog_state='published'"
                " AND version_key<>?", (self._current_vk,)).fetchall():
            vk = row[0]
            if vk == protect:
                continue
            if (_reader_count(self._reader_key(), vk) == 0
                    and self._active_readers.get(vk, 0) == 0):
                self.db.execute('DELETE FROM doc_keys WHERE version_key=?', (vk,))
                self.db.execute(
                    "UPDATE doc_versions SET state='reclaimed', struct_seq=NULL"
                    ' WHERE version_key=?', (vk,))

    def _ensure_writable(self):
        if self._closed:
            raise RuntimeError('store is closed')

    # -------------------------------------------------------------- migrating
    def migrate(self, name=None, revision=None, *, crash_after_build=False,
                crash_after_publish=False):
        """Build and atomically publish one explicit registered version.

        ``revision=None`` selects the current catalogue version.  Passing an
        explicit revision selects exactly that old version.  The previous
        published version stays fully readable throughout; a failure before the
        publish transaction commits leaves it current.
        """
        target_name = name or self.name
        target = self.registry.snapshot(target_name, revision)
        with self._lock:
            return self._migrate_to(target, crash_after_build=crash_after_build,
                                    crash_after_publish=crash_after_publish,
                                    _created_by='migrate')

    def _migrate_to(self, target, *, crash_after_build, crash_after_publish,
                    _created_by):
        if not isinstance(target, Handle):
            raise TypeError('migration target must be a Handle')
        self._ensure_writable()
        vk = version_key_for(target.name, target.revision, target.definition_id)
        # A revision number that already denotes a *different* rule is never
        # used to explain persisted rows.
        conflict = self.db.execute(
            'SELECT version_key FROM doc_versions WHERE name=? AND revision=?'
            ' AND version_key<>?', (target.name, target.revision, vk)).fetchone()
        if conflict:
            raise DefinitionError(
                'catalogue %r revision %r is now %s but persisted index holds %s'
                % (target.name, target.revision, target.definition_id, conflict[0]))

        row = self._version_row(vk)
        if row is not None and row['catalog_state'] == 'published' and row['state'] == 'complete':
            built_seq = row['built_seq']
            lag_seq = row['struct_seq'] or 0
            if lag_seq < self._outbox_floor() and lag_seq < self._write_seq():
                # Replay history for the lag was pruned: rebuild before re-publish.
                self._rebuild_catching_up(vk, target)
                row = self._version_row(vk)
                built_seq, lag_seq = row['built_seq'], row['struct_seq']
            with self._lock, self._txn():
                pub_seq = self._write_seq()
                self._replay_outbox(vk, target, lag_seq, pub_seq)
                self.db.execute(
                    'UPDATE doc_versions SET struct_seq=?, published_seq=?'
                    ' WHERE version_key=?', (pub_seq, pub_seq, vk))
                self._verify_published_snapshot(vk, target, pub_seq)
                self.db.execute(
                    "UPDATE migrations SET state='published', updated_at=?"
                    " WHERE target_version_key=? AND state IN ('started','built')",
                    (_now(), vk))
            self._persisted_handles[vk] = target
            self._set_current(vk)
            return MigrationResult(vk, target.name, target.revision, target.definition_id,
                                   built_seq, pub_seq, True, True)

        migration_id = uuid.uuid4().hex
        with self._lock, self._txn():
            self.db.execute(
                'INSERT INTO migrations(migration_id, target_version_key, name, revision,'
                ' definition_id, state, built_seq, created_at, updated_at)'
                ' VALUES (?,?,?,?,?,?,NULL,?,?)',
                (migration_id, vk, target.name, target.revision, target.definition_id,
                 'started', _now(), _now()))

        reused = row is not None and row['state'] == 'complete' and row['built_seq'] is not None
        if reused:
            built_seq = row['built_seq']
            self._persisted_handles[vk] = target
        else:
            built_seq = self._build_structure(target, vk)
        with self._lock, self._txn():
            self.db.execute(
                "UPDATE migrations SET state='built', built_seq=?, updated_at=?"
                " WHERE migration_id=?", (built_seq, _now(), migration_id))
            self._prune_outbox()
        self._hit_crash('after_build', crash_after_build)

        pub_seq = self._publish(target, vk, built_seq, migration_id)
        self._hit_crash('after_publish', crash_after_publish)

        self._persisted_handles[vk] = target
        with self._lock, self._txn():
            self._set_current(vk)
            self._prune_outbox()
        return MigrationResult(vk, target.name, target.revision, target.definition_id,
                               built_seq, pub_seq, reused, False)

    @staticmethod
    def _hit_crash(label, armed):
        if armed:
            raise CrashPoint(label)

    def _build_structure(self, target, vk):
        """Build and verify a complete key structure in one write transaction.

        The structure is committed under ``unpublished``/``complete``; if either
        the build or the in-transaction verification fails, nothing is published
        and the previous version remains current.  A post-commit verification
        failure marks the structure failed and removes its rows.
        """
        with self._lock:
            try:
                with self._txn():
                    built_seq = self._write_seq()
                    definition_json = _definition_json(target.definition)
                    existing = self._version_row(vk)
                    if existing is None:
                        self.db.execute(
                            'INSERT INTO doc_versions(version_key, definition_id, name,'
                            ' revision, definition_json, catalog_state, state, built_seq,'
                            ' struct_seq, published_seq, created_at)'
                            " VALUES (?,?,?,?,?,'unpublished','building',?,?,NULL,?)",
                            (vk, target.definition_id, target.name, target.revision,
                             definition_json, built_seq, built_seq, _now()))
                    else:
                        self.db.execute('DELETE FROM doc_keys WHERE version_key=?', (vk,))
                        self.db.execute(
                            "UPDATE doc_versions SET state='building',"
                            " catalog_state='unpublished', built_seq=?, struct_seq=?,"
                            ' published_seq=NULL, definition_json=?, name=?, revision=?'
                            ' WHERE version_key=?',
                            (built_seq, built_seq, definition_json, target.name,
                             target.revision, vk))
                    batch = []
                    for id_, word in self.db.execute('SELECT id, word FROM docs ORDER BY id'):
                        batch.append((vk, id_, built_seq, None, target.key(word), word))
                        if len(batch) >= _BASE_ROWS:
                            self.db.executemany(
                                'INSERT INTO doc_keys(version_key, id, from_seq, to_seq,'
                                ' ckey, word) VALUES (?,?,?,?,?,?)', batch)
                            batch.clear()
                    if batch:
                        self.db.executemany(
                            'INSERT INTO doc_keys(version_key, id, from_seq, to_seq,'
                            ' ckey, word) VALUES (?,?,?,?,?,?)', batch)
                    self._verify_built_structure(target, vk, built_seq)
                    self.db.execute(
                        "UPDATE doc_versions SET state='complete', struct_seq=?"
                        ' WHERE version_key=?', (built_seq, vk))
            except BaseException:
                # Best-effort cleanup of a structure that never got published.
                try:
                    with self._lock, self._txn():
                        self.db.execute('DELETE FROM doc_keys WHERE version_key=?', (vk,))
                        self.db.execute(
                            "UPDATE doc_versions SET state='failed', struct_seq=NULL,"
                            " built_seq=NULL WHERE version_key=? AND catalog_state='unpublished'",
                            (vk,))
                finally:
                    raise
        return built_seq

    def _verify_built_structure(self, target, vk, built_seq):
        """Prove the freshly built structure matches ``docs`` exactly."""
        docs = self.db.execute('SELECT id, word FROM docs ORDER BY id').fetchall()
        open_rows = self.db.execute(
            'SELECT id, word, ckey, from_seq, to_seq FROM doc_keys'
            " WHERE version_key=? AND to_seq IS NULL ORDER BY id", (vk,)).fetchall()
        if [r[0] for r in docs] != [r[0] for r in open_rows]:
            raise RuntimeError('built structure does not cover docs exactly')
        for (id_, word), (rid, rword, ckey, fseq, tseq) in zip(docs, open_rows):
            if id_ != rid or word != rword:
                raise RuntimeError('built structure row mismatch at id %r' % id_)
            if fseq != built_seq or tseq is not None:
                raise RuntimeError('built structure interval mismatch at id %r' % id_)
            if ckey != target.key(word):
                raise RuntimeError('built structure key mismatch at id %r' % id_)
        total_open = self.db.execute(
            'SELECT COUNT(*) FROM doc_keys WHERE version_key=? AND to_seq IS NULL',
            (vk,)).fetchone()[0]
        if total_open != len(docs):
            raise RuntimeError('built structure has extra open rows')

    def _publish(self, target, vk, built_seq, migration_id):
        """Atomically replay the build gap, verify, then publish the version."""
        with self._lock, self._txn():
            row = self._version_row(vk)
            if row is None or row['state'] != 'complete':
                raise RuntimeError('cannot publish incomplete structure %s' % vk)
            if row['catalog_state'] == 'published':
                pub_seq = row['published_seq']
            else:
                pub_seq = self._write_seq()
                self._replay_outbox(vk, target, row['struct_seq'] or built_seq, pub_seq)
                self._verify_published_snapshot(vk, target, pub_seq)
                self.db.execute(
                    "UPDATE doc_versions SET catalog_state='published', struct_seq=?,"
                    ' published_seq=? WHERE version_key=?', (pub_seq, pub_seq, vk))
            self.db.execute(
                "UPDATE migrations SET state='published', updated_at=?"
                ' WHERE migration_id=?', (_now(), migration_id))
        return pub_seq

    def _outbox_floor(self):
        return int(self._get_meta('outbox_floor', 0) or 0)

    def _replay_outbox(self, vk, handle, after_seq, upto_seq):
        if after_seq < self._outbox_floor():
            raise RuntimeError(
                'cannot replay %s from seq %r: outbox only retains >%r'
                % (vk, after_seq, self._outbox_floor()))
        rows = self.db.execute(
            'SELECT seq, op, id, word FROM outbox WHERE seq>? AND seq<=? ORDER BY seq',
            (after_seq, upto_seq)).fetchall()
        for seq, op, id_, word in rows:
            self._close_open(vk, id_, seq)
            if op == 'put':
                self.db.execute(
                    'INSERT INTO doc_keys(version_key, id, from_seq, to_seq, ckey, word)'
                    ' VALUES (?,?,?,NULL,?,?)',
                    (vk, id_, seq, handle.key(word), word))

    def _verify_published_snapshot(self, vk, target, seq):
        """Every live doc at *seq* is visible exactly once under *target*."""
        visible = self.db.execute(
            'SELECT id, word, ckey FROM doc_keys WHERE version_key=? AND from_seq<=?'
            ' AND (to_seq IS NULL OR to_seq>?) ORDER BY id', (vk, seq, seq)).fetchall()
        expected = self.db.execute('SELECT id, word FROM docs ORDER BY id').fetchall()
        if len({r[0] for r in visible}) != len(visible):
            raise RuntimeError('published structure shows an id twice')
        visible_docs = [(r[0], r[1]) for r in visible]
        if visible_docs != expected:
            raise RuntimeError('published structure does not match docs at snapshot')
        for id_, word, ckey in visible:
            if ckey != target.key(word):
                raise RuntimeError('published key mismatch at id %r' % id_)

    # ------------------------------------------------------------------ reads
    def open_reader(self):
        """Bind the current published rows and comparison version."""
        with self._lock:
            self._ensure_writable()
            self._adopt_latest_published()
            self._ensure_current_uptodate()
            seq = self._write_seq()
            reader = ReadHandle(self, self._current_vk, self._current_handle, seq)
            self._active_readers[self._current_vk] += 1
            _reader_acquire(self._reader_key(), self._current_vk)
            self._readers.add(reader)
            return reader

    def _reader_key(self):
        return os.path.abspath(self.path)

    def equal(self, word):
        """Convenience equality query on the current version snapshot."""
        with self._lock:
            self._ensure_writable()
            self._adopt_latest_published()
            return self._select_equal(self._current_vk, self._current_handle,
                                      self._write_seq(), word)

    def order(self, start=None, stop=None):
        """Ordered range on the current version; [start, stop) by comparison key."""
        with self._lock:
            self._ensure_writable()
            self._adopt_latest_published()
            return self._select_range(self._current_vk, self._current_handle,
                                      self._write_seq(), start, stop)

    def _select_equal(self, vk, handle, seq, word):
        key = handle.key(word)
        sql = ('SELECT id, word FROM doc_keys INDEXED BY doc_keys_order'
               ' WHERE version_key=? AND ckey=? AND from_seq<=?'
               ' AND (to_seq IS NULL OR to_seq>?) ORDER BY id')
        rows = self.db.execute(sql, (vk, key, seq, seq)).fetchall()
        return [self._read_row(handle, r[0], r[1]) for r in rows]

    def _select_range(self, vk, handle, seq, start, stop):
        clauses = ['version_key=?']
        params = [vk]
        if start is not None:
            clauses.append('ckey>=?')
            params.append(handle.key(_require_text(start)))
        if stop is not None:
            clauses.append('ckey<?')
            params.append(handle.key(_require_text(stop)))
        clauses.append('from_seq<=?')
        params.append(seq)
        clauses.append('(to_seq IS NULL OR to_seq>?)')
        params.append(seq)
        sql = ('SELECT id, word FROM doc_keys INDEXED BY doc_keys_order WHERE '
               + ' AND '.join(clauses) + ' ORDER BY ckey, id')
        rows = self.db.execute(sql, params).fetchall()
        return [self._read_row(handle, r[0], r[1]) for r in rows]

    def _read_row(self, handle, id_, word):
        return ReadRow(id_, word, handle.name, handle.revision, handle.definition_id)

    # ------------------------------------------------------------- evidence
    def _plan(self, sql, params):
        plan = self.db.execute('EXPLAIN QUERY PLAN ' + sql, params).fetchall()
        return [row[3] for row in plan]

    def explain_equal(self, word, reader=None):
        """SQLite plan + index entries examined for an equality lookup."""
        if reader is None:
            vk, handle, seq = self._current_vk, self._current_handle, self._write_seq()
        else:
            vk, handle, seq = reader._version_key, reader._handle, reader._seq
        key = handle.key(word)
        sql = ('SELECT id, word FROM doc_keys INDEXED BY doc_keys_order'
               ' WHERE version_key=? AND ckey=? AND from_seq<=?'
               ' AND (to_seq IS NULL OR to_seq>?) ORDER BY id')
        params = (vk, key, seq, seq)
        returned = self.db.execute(sql, params).fetchall()
        examined = self.db.execute(
            'SELECT COUNT(*) FROM doc_keys INDEXED BY doc_keys_order'
            ' WHERE version_key=? AND ckey=?', (vk, key)).fetchone()[0]
        return {'kind': 'equal', 'sql': sql, 'parameters': params,
                'plan': self._plan(sql, params),
                'index_entries_examined': examined,
                'rows_returned': len(returned),
                'version': {'name': handle.name, 'revision': handle.revision,
                            'definition_id': handle.definition_id}}

    def explain_range(self, start=None, stop=None, reader=None):
        """SQLite plan + index entries examined for a half-open range read."""
        if reader is None:
            vk, handle, seq = self._current_vk, self._current_handle, self._write_seq()
        else:
            vk, handle, seq = reader._version_key, reader._handle, reader._seq
        clauses = ['version_key=?']
        params = [vk]
        count_clauses = ['version_key=?']
        count_params = [vk]
        if start is not None:
            clauses.append('ckey>=?')
            params.append(handle.key(_require_text(start)))
            count_clauses.append('ckey>=?')
            count_params.append(handle.key(_require_text(start)))
        if stop is not None:
            clauses.append('ckey<?')
            params.append(handle.key(_require_text(stop)))
            count_clauses.append('ckey<?')
            count_params.append(handle.key(_require_text(stop)))
        clauses += ['from_seq<=?', '(to_seq IS NULL OR to_seq>?)']
        params += [seq, seq]
        sql = ('SELECT id, word FROM doc_keys INDEXED BY doc_keys_order WHERE '
               + ' AND '.join(clauses) + ' ORDER BY ckey, id')
        returned = self.db.execute(sql, params).fetchall()
        examined = self.db.execute(
            'SELECT COUNT(*) FROM doc_keys INDEXED BY doc_keys_order WHERE '
            + ' AND '.join(count_clauses), count_params).fetchone()[0]
        return {'kind': 'range', 'sql': sql, 'parameters': tuple(params),
                'plan': self._plan(sql, params),
                'index_entries_examined': examined,
                'rows_returned': len(returned),
                'version': {'name': handle.name, 'revision': handle.revision,
                            'definition_id': handle.definition_id}}

    # --------------------------------------------------------------- status
    def status(self):
        """Machine-readable state of the persistent catalogue and structures."""
        with self._lock:
            versions = []
            for row in self.db.execute(
                    'SELECT version_key, name, revision, catalog_state, state,'
                    ' built_seq, struct_seq, published_seq FROM doc_versions'
                    ' ORDER BY created_at, version_key'):
                vk = row[0]
                open_rows = self.db.execute(
                    'SELECT COUNT(*) FROM doc_keys WHERE version_key=? AND to_seq IS NULL',
                    (vk,)).fetchone()[0]
                total_rows = self.db.execute(
                    'SELECT COUNT(*) FROM doc_keys WHERE version_key=?', (vk,)).fetchone()[0]
                versions.append({
                    'version_key': vk, 'name': row[1], 'revision': row[2],
                    'catalog_state': row[3], 'state': row[4], 'built_seq': row[5],
                    'struct_seq': row[6], 'published_seq': row[7],
                    'open_rows': open_rows, 'total_key_rows': total_rows})
            migrations = [dict(zip(
                ('migration_id', 'target_version_key', 'name', 'revision',
                 'definition_id', 'state', 'built_seq', 'created_at', 'updated_at'), r))
                for r in self.db.execute(
                    'SELECT migration_id, target_version_key, name, revision,'
                    ' definition_id, state, built_seq, created_at, updated_at'
                    ' FROM migrations ORDER BY created_at, migration_id')]
            return {
                'path': self.path,
                'write_seq': self._write_seq(),
                'doc_count': self.db.execute('SELECT COUNT(*) FROM docs').fetchone()[0],
                'current': {'version_key': self._current_vk,
                            'name': self._current_handle.name,
                            'revision': self._current_handle.revision,
                            'definition_id': self._current_handle.definition_id},
                'versions': versions,
                'migrations': migrations,
                'active_readers': dict(self._active_readers),
            }

    def inspect_migrations(self):
        return self.status()['migrations']

    # ------------------------------------------------------------- lifecycle
    def activate(self):
        """Re-bind to the latest *published* version (does not rebuild/reindex)."""
        with self._lock:
            latest = self._latest_published()
            if latest is None:
                raise RuntimeError('no published version')
            self._set_current(latest['version_key'])
            self._ensure_current_uptodate()
            return self._current_handle

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
            for reader in list(self._readers):
                reader.close(reclaim=False)
            self._readers.clear()
            self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class ReadHandle:
    """A read handle bound to one version and one committed row snapshot."""

    def __init__(self, store, version_key, handle, seq):
        self._store = store
        self._version_key = version_key
        self._handle = handle
        self._seq = seq
        self._active = True

    # -- comparison semantics of the bound version ------------------------
    @property
    def version(self):
        return {'name': self._handle.name, 'revision': self._handle.revision,
                'definition_id': self._handle.definition_id}

    @property
    def revision(self):
        return self._handle.revision

    @property
    def definition(self):
        return dict(self._handle.definition)

    @property
    def snapshot_seq(self):
        return self._seq

    def key(self, text):
        return self._handle.key(text)

    def compare(self, left, right):
        return self._handle.compare(left, right)

    # -- reads ------------------------------------------------------------
    def equal(self, word):
        self._check()
        with self._store._lock:
            return self._store._select_equal(self._version_key, self._handle,
                                             self._seq, word)

    def order(self, start=None, stop=None):
        self._check()
        with self._store._lock:
            return self._store._select_range(self._version_key, self._handle,
                                             self._seq, start, stop)

    def select_equal(self, word):
        return self.equal(word)

    def select_range(self, start=None, stop=None):
        return self.order(start, stop)

    # -- evidence ---------------------------------------------------------
    def explain_equal(self, word):
        return self._store.explain_equal(word, reader=self)

    def explain_range(self, start=None, stop=None):
        return self._store.explain_range(start, stop, reader=self)

    # -- lifecycle --------------------------------------------------------
    def _check(self):
        if not self._active:
            raise RuntimeError('read handle is closed')

    def _invalidate(self):
        self._active = False

    def close(self, reclaim=True):
        if not self._active:
            return
        store = self._store
        with store._lock:
            self._active = False
            store._readers.discard(self)
            if store._active_readers.get(self._version_key, 0) > 0:
                store._active_readers[self._version_key] -= 1
            _reader_release(store._reader_key(), self._version_key)
            local = store._active_readers.get(self._version_key, 0)
            global_count = _reader_count(store._reader_key(), self._version_key)
            if (reclaim and local == 0 and global_count == 0
                    and self._version_key != store._current_vk):
                self._reclaim(store)
            if local == 0:
                store._active_readers.pop(self._version_key, None)

    def _reclaim(self, store):
        """Release a no-longer-needed dedicated structure."""
        with store._lock, store._txn():
            row = store._version_row(self._version_key)
            if row is None or row['catalog_state'] != 'published':
                return
            if self._version_key == store._current_vk:
                return
            store.db.execute('DELETE FROM doc_keys WHERE version_key=?',
                             (self._version_key,))
            store.db.execute(
                "UPDATE doc_versions SET state='reclaimed', struct_seq=NULL"
                ' WHERE version_key=?', (self._version_key,))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __repr__(self):
        return ('ReadHandle(version=%r, snapshot_seq=%r, active=%r)'
                % (self.version, self._seq, self._active))


def _require_text(value):
    if not isinstance(value, str):
        raise TypeError('text must be a Unicode string, not %r' % type(value).__name__)
    return value


def _definition_json(definition):
    return json.dumps(dict(definition), sort_keys=True, separators=(',', ':'))


def _definition_from_json(text, expected_id):
    definition = json.loads(text)
    if definition_id(definition) != expected_id:
        raise DefinitionError(
            'persisted definition json does not match its identity %s' % expected_id)
    return definition
