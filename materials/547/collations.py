"""Versioned, persistent Unicode collation catalogue.

A catalogue *name* accumulates immutable versions.  Each version is identified
by ``(name, revision)`` and additionally carries a content hash of its validated
definition.  A handle obtained earlier keeps its exact comparison semantics even
after newer versions are defined, and a database that remembers the hash cannot
silently bind old rows to a different definition just because a fresh Registry
happens to number its revisions from 1 again.

The comparison rule is fixed by the task and deliberately locale independent:

    NFD -> (full casefold if case_insensitive) -> drop Mn code points if not
    accent_sensitive -> NFC

Equality is string equality of that key; ordering is the Unicode code point
lexicographic order of the key (no natural-language collation).
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import unicodedata

__all__ = [
    'DefinitionError', 'UnknownVersion', 'InvalidDefinition', 'InvalidName',
    'key_for', 'definition_id', 'Handle', 'Registry',
]

_OPTIONS = ('case_insensitive', 'accent_sensitive')


class DefinitionError(ValueError):
    """Base class for catalogue definition / lookup failures."""


class InvalidDefinition(DefinitionError):
    pass


class InvalidName(DefinitionError):
    pass


class UnknownVersion(KeyError, DefinitionError):
    pass


def key_for(text, definition):
    """Return the comparison key for *text* under *definition*.

    *definition* must be an already validated mapping with the two boolean
    options.  The function is deterministic and side-effect free so independent
    import tools and the store compute identical keys.
    """
    if not isinstance(text, str):
        raise TypeError('text must be a Unicode string, not %r' % type(text).__name__)
    value = unicodedata.normalize('NFD', text)
    if definition['case_insensitive']:
        value = value.casefold()
    if not definition['accent_sensitive']:
        value = ''.join(ch for ch in value if unicodedata.category(ch) != 'Mn')
    return unicodedata.normalize('NFC', value)


def _validate_definition(definition):
    if not isinstance(definition, dict):
        raise InvalidDefinition('definition must be a dict of the two options')
    unknown = set(definition) - set(_OPTIONS)
    if unknown:
        raise InvalidDefinition('unknown definition options: %s' % sorted(unknown))
    clean = {}
    for option in _OPTIONS:
        if option not in definition:
            raise InvalidDefinition('missing option %r' % option)
        value = definition[option]
        # bool is a subclass of int; it is the *only* accepted truth value.
        if not isinstance(value, bool):
            raise InvalidDefinition(
                'option %r must be a real bool, got %r' % (option, type(value).__name__))
        clean[option] = value
    return clean


def definition_id(definition):
    """Stable content identity for a validated definition."""
    clean = _validate_definition(definition)
    canonical = json.dumps(clean, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


class _FrozenDict(dict):
    """Read-only dict: comparisons use it, callers cannot mutate a version."""

    def _readonly(self, *a, **k):
        raise TypeError('collation definition is immutable')

    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = _readonly


class Handle:
    """An immutable comparison object bound to one catalogue version.

    Import tools may hold a Handle indefinitely: neither defining a new version
    nor rebuilding the store changes ``key`` or ``compare``.
    """

    __slots__ = ('name', 'revision', 'definition', 'definition_id', '_frozen')

    def __init__(self, name, revision, definition):
        clean = _validate_definition(definition)
        object.__setattr__(self, 'name', name)
        object.__setattr__(self, 'revision', int(revision))
        object.__setattr__(self, '_frozen', _FrozenDict(clean))
        object.__setattr__(self, 'definition', self._frozen)
        object.__setattr__(self, 'definition_id', definition_id(clean))

    def __setattr__(self, key, value):
        raise AttributeError('Handle is immutable')

    def key(self, text):
        return key_for(text, self.definition)

    def compare(self, left, right):
        a, b = self.key(left), self.key(right)
        return (a > b) - (a < b)

    @property
    def version(self):
        """``(name, revision)`` tuple, the primary version identity."""
        return (self.name, self.revision)

    def to_dict(self):
        return {
            'name': self.name,
            'revision': self.revision,
            'definition': dict(self.definition),
            'definition_id': self.definition_id,
        }

    def __repr__(self):
        return ('Handle(name=%r, revision=%r, definition=%r, definition_id=%s)'
                % (self.name, self.revision, dict(self.definition), self.definition_id[:12]))

    def __eq__(self, other):
        return (isinstance(other, Handle) and self.name == other.name
                and self.revision == other.revision
                and self.definition_id == other.definition_id)

    def __hash__(self):
        return hash((self.name, self.revision, self.definition_id))


class Registry:
    """Public, append-only catalogue of versioned comparison rules.

    ``define`` always creates a *new* revision; ``snapshot(name, revision=...)``
    returns the current version or an explicit older one.  Definitions are
    validated before any state changes, so a rejected call leaves every
    previously registered version untouched.

    Optional ``store_path`` persists every version (and the current revision per
    name) in a small SQLite database that anyone can re-read and audit.
    """

    def __init__(self, store_path=None):
        self._by_version = {}      # (name, revision) -> record
        self._by_id = {}           # definition_id -> [records, oldest first]
        self._current = {}         # name -> revision
        self._lock = threading.RLock()
        self._path = None
        self._db = None
        if store_path:
            self._open(store_path)

    # -- persistence -----------------------------------------------------
    def _open(self, path):
        self._path = path
        self._db = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self._db.execute('PRAGMA journal_mode=WAL')
        self._db.execute(
            'CREATE TABLE IF NOT EXISTS definitions('
            ' definition_id TEXT NOT NULL,'
            ' name TEXT NOT NULL,'
            ' revision INTEGER NOT NULL,'
            ' case_insensitive INTEGER NOT NULL,'
            ' accent_sensitive INTEGER NOT NULL,'
            ' PRIMARY KEY(name, revision))')
        self._db.execute('CREATE INDEX IF NOT EXISTS definitions_by_id'
                         ' ON definitions(definition_id)')
        self._db.execute(
            'CREATE TABLE IF NOT EXISTS current_versions('
            ' name TEXT PRIMARY KEY, revision INTEGER NOT NULL)')
        for row in self._db.execute(
                'SELECT definition_id, name, revision, case_insensitive, accent_sensitive'
                ' FROM definitions ORDER BY name, revision'):
            self._install_row(*row)
        for name, rev in self._db.execute('SELECT name, revision FROM current_versions'):
            rec = self._by_version.get((name, rev))
            if rec is None:
                raise DefinitionError('current revision for %r is missing from catalogue' % name)
            self._current[name] = rev

    def _install_row(self, did, name, rev, ci, acc):
        definition = {'case_insensitive': bool(ci), 'accent_sensitive': bool(acc)}
        if definition_id(definition) != did:
            raise DefinitionError('persisted definition %r does not match its id' % did)
        rec = {'name': name, 'revision': int(rev), 'definition': definition,
               'definition_id': did}
        self._by_version[(name, int(rev))] = rec
        self._by_id.setdefault(did, []).append(rec)
        return rec

    def close(self):
        with self._lock:
            if self._db is not None:
                self._db.close()
                self._db = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -- public API ------------------------------------------------------
    def define(self, name, *, case_insensitive, accent_sensitive):
        if not isinstance(name, str) or not name:
            raise InvalidName('name must be a non-empty string')
        definition = _validate_definition(
            {'case_insensitive': case_insensitive, 'accent_sensitive': accent_sensitive})
        did = definition_id(definition)
        with self._lock:
            new_rev = self._current.get(name, 0)
            while True:
                new_rev += 1
                if (name, new_rev) not in self._by_version:
                    break
            if self._db is not None:
                self._db.execute('BEGIN IMMEDIATE')
                try:
                    self._db.execute(
                        'INSERT INTO definitions VALUES (?,?,?,?,?)',
                        (did, name, new_rev,
                         int(definition['case_insensitive']),
                         int(definition['accent_sensitive'])))
                    self._db.execute(
                        'INSERT INTO current_versions(name, revision) VALUES (?,?)'
                        ' ON CONFLICT(name) DO UPDATE SET revision=excluded.revision',
                        (name, new_rev))
                    self._db.execute('COMMIT')
                except BaseException:
                    self._db.execute('ROLLBACK')
                    raise
            rec = self._install_row(did, name, new_rev,
                                    int(definition['case_insensitive']),
                                    int(definition['accent_sensitive']))
            self._current[name] = new_rev
            return Handle(rec['name'], rec['revision'], rec['definition'])

    def snapshot(self, name, revision=None):
        """Return the current handle for *name*, or one explicit old *revision*.

        Unknown names / revisions are rejected rather than silently defaulting.
        """
        if not isinstance(name, str):
            raise InvalidName('name must be a string')
        with self._lock:
            if revision is None:
                try:
                    revision = self._current[name]
                except KeyError:
                    raise UnknownVersion('no catalogue version for name %r' % name) from None
            try:
                rec = self._by_version[(name, int(revision))]
            except (KeyError, ValueError, TypeError):
                raise UnknownVersion(
                    'unknown catalogue version %r revision %r' % (name, revision)) from None
            return Handle(rec['name'], rec['revision'], rec['definition'])

    def resolve(self, name, revision, expected_definition_id):
        """Resolve an exact version and verify its content identity.

        Raises ``UnknownVersion`` if the version is absent and ``DefinitionError``
        if the version exists but carries a different definition -- the guard
        that stops a restarted Registry from explaining old data with a new rule
        that merely reused a small revision number.
        """
        handle = self.snapshot(name, revision)
        if handle.definition_id != expected_definition_id:
            raise DefinitionError(
                'catalogue version %r/%r has definition %s, expected %s'
                % (name, revision, handle.definition_id, expected_definition_id))
        return handle

    def ensure_version(self, name, definition, revision=None):
        """Bind *definition* for *name*, reusing an identical version if present.

        Resolution order (used by callers that hold only a definition):
        1. the exact ``(name, revision)`` must, if present, carry the same
           definition -- otherwise a same-number/different-definition binding is
           rejected rather than silently reinterpreted;
        2. any version of *name* with the same content identity is reused;
        3. only then is the definition newly registered.
        """
        clean = _validate_definition(definition)
        did = definition_id(clean)
        with self._lock:
            if revision is not None:
                rec = self._by_version.get((name, int(revision)))
                if rec is not None:
                    if rec['definition_id'] != did:
                        raise DefinitionError(
                            'catalogue version %r/%r is %s but requested definition is %s'
                            % (name, revision, rec['definition_id'], did))
                    return Handle(rec['name'], rec['revision'], rec['definition'])
            for rec in self._by_id.get(did, ()):
                if rec['name'] == name:
                    return Handle(rec['name'], rec['revision'], rec['definition'])
            return self.define(name, **clean)

    def restore(self, name, revision, definition):
        """Restore one audited version at its exact ``(name, revision)``.

        Used when a store reopens with a fresh Registry: the database's own
        audit trail says which rule definition it was built with.  If that
        revision number is already taken by a *different* definition the restore
        is rejected -- the whole point of not trusting a counter that restarted
        from 1.  Restoring never lowers the current revision of a name.
        """
        if not isinstance(name, str) or not name:
            raise InvalidName('name must be a non-empty string')
        clean = _validate_definition(definition)
        did = definition_id(clean)
        revision = int(revision)
        with self._lock:
            rec = self._by_version.get((name, revision))
            if rec is not None:
                if rec['definition_id'] != did:
                    raise DefinitionError(
                        'catalogue %r revision %r is %s but persisted data uses %s'
                        % (name, revision, rec['definition_id'], did))
                return Handle(rec['name'], rec['revision'], rec['definition'])
            current = self._current.get(name)
            if self._db is not None:
                self._db.execute('BEGIN IMMEDIATE')
                try:
                    self._db.execute(
                        'INSERT INTO definitions VALUES (?,?,?,?,?)',
                        (did, name, revision, int(clean['case_insensitive']),
                         int(clean['accent_sensitive'])))
                    target_current = revision if current is None else max(current, revision)
                    self._db.execute(
                        'INSERT INTO current_versions(name, revision) VALUES (?,?)'
                        ' ON CONFLICT(name) DO UPDATE SET revision=excluded.revision',
                        (name, target_current))
                    self._db.execute('COMMIT')
                except BaseException:
                    self._db.execute('ROLLBACK')
                    raise
            rec = self._install_row(did, name, revision,
                                    int(clean['case_insensitive']),
                                    int(clean['accent_sensitive']))
            if current is None or revision > current:
                self._current[name] = revision
            return Handle(rec['name'], rec['revision'], rec['definition'])

    def find(self, name, definition_id_value):
        """Return a handle for the version of *name* with this content id, or None."""
        with self._lock:
            for rec in self._by_id.get(definition_id_value, ()):
                if rec['name'] == name:
                    return Handle(rec['name'], rec['revision'], rec['definition'])
            return None

    def versions(self, name=None):
        with self._lock:
            rows = []
            for (n, rev), rec in sorted(self._by_version.items()):
                if name is not None and n != name:
                    continue
                rows.append({'name': rec['name'], 'revision': rec['revision'],
                             'definition': dict(rec['definition']),
                             'definition_id': rec['definition_id']})
            return rows

    def __repr__(self):
        return 'Registry(%d versions, path=%r)' % (len(self._by_version), self._path)
