"""Comprehensive verification of the versioned collation catalogue and store.

Run from this directory:  python3 verify.py

Covers (see the task statement):
  * the two e/E/é equivalence classes and old-Handle stability;
  * independently computed keys on a short array for equality, range, duplicate
    keys and repeated rule switches;
  * a reader fixed before interleaved writes + migration, checking old vs new
    reads and original row identity;
  * both migration failure points, reopen-and-judge, and safe retry;
  * same catalogue number bound to a different definition (rejected);
  * 20000 rows, 10 registrations, two names, all legal option combinations;
  * index-usage evidence (query plan + entries examined) for a selective sample;
  * reclamation of an old reader's dedicated structure.
"""
import itertools
import json
import random
import shutil
import tempfile
import traceback
import unicodedata
from pathlib import Path

from collations import (
    DefinitionError,
    Handle,
    InvalidDefinition,
    Registry,
    UnknownVersion,
    definition_id,
    key_for,
)
from store import CrashPoint, Store

PASS = []
FAIL = []


def check(name, condition, detail=''):
    if condition:
        PASS.append(name)
        print('  ok   %s' % name)
    else:
        FAIL.append((name, detail))
        print('  FAIL %s %s' % (name, detail))
    return condition


def reference_key(text, definition):
    """Deliberately independent re-implementation of the rule."""
    v = unicodedata.normalize('NFD', text)
    if definition['case_insensitive']:
        v = v.casefold()
    if not definition['accent_sensitive']:
        v = ''.join(c for c in v if unicodedata.category(c) != 'Mn')
    return unicodedata.normalize('NFC', v)


# --------------------------------------------------------------------------
def test_equivalence_classes():
    print('\n[1] e / E / é equivalence classes and old-handle stability')
    reg = Registry()
    v1 = reg.define('name', case_insensitive=True, accent_sensitive=True)
    old = reg.snapshot('name')
    v2 = reg.define('name', case_insensitive=True, accent_sensitive=False)
    check('v1 e == E', old.compare('e', 'E') == 0)
    check('v1 e != é', old.compare('e', 'é') != 0)
    check('old handle unchanged after define', old.compare('e', 'é') != 0
          and old.compare('e', 'E') == 0)
    new = reg.snapshot('name')
    check('v2 e == é == E', new.compare('e', 'é') == 0 and new.compare('e', 'E') == 0)
    check('v1 key stable', old.key('é') == reference_key('é', v1.definition))
    check('snapshot(old revision)', reg.snapshot('name', v1.revision).definition_id
          == v1.definition_id)
    check('handle immutable', _raises(lambda: setattr(old, 'revision', 99)))
    check('handle definition immutable', _raises(lambda: old.definition.update({'x': 1})))
    # ß full casefold equivalence
    ci = reg.define('fold', case_insensitive=True, accent_sensitive=True)
    check('ß == ẞ under casefold', ci.key('ß') == ci.key('ẞ') == 'ss')
    check('straße == STRASSE', ci.key('straße') == ci.key('STRASSE'))


def _raises(fn):
    try:
        fn()
    except Exception:
        return True
    return False


# --------------------------------------------------------------------------
def test_registry_contract():
    print('\n[2] Registry validation, immutability and persistence')
    reg = Registry()
    v1 = reg.define('a', case_insensitive=True, accent_sensitive=False)
    check('define returns Handle', isinstance(v1, Handle))
    before = reg.versions('a')
    check('non-bool rejected', _raises(lambda: reg.define('a', case_insensitive=1,
                                                         accent_sensitive=True)))
    check('string bool rejected', _raises(lambda: reg.define('a', case_insensitive='true',
                                                            accent_sensitive=True)))
    check('rejection did not change versions', reg.versions('a') == before)
    check('unknown version rejected', _raises(lambda: reg.snapshot('a', 99)))
    check('unknown name rejected', _raises(lambda: reg.snapshot('nope')))
    check('same revision resolves old def', reg.snapshot('a', v1.revision).definition_id
          == v1.definition_id)

    with tempfile.TemporaryDirectory() as tmp:
        cat = str(Path(tmp) / 'cat.sqlite')
        r1 = Registry(cat)
        h = r1.define('name', case_insensitive=False, accent_sensitive=True)
        h2 = r1.define('name', case_insensitive=True, accent_sensitive=False)
        r1.close()
        r2 = Registry(cat)
        check('reopen finds version 1', r2.snapshot('name', 1).definition_id == h.definition_id)
        check('reopen finds version 2', r2.snapshot('name', 2).definition_id == h2.definition_id)
        check('reopen current is 2', r2.snapshot('name').revision == 2)
        r2.close()


# --------------------------------------------------------------------------
def test_independent_short_array():
    print('\n[3] Independent rule computation on a short array')
    words = ['e', 'E', 'é', 'é', 'f', 'F', 'ß', 'SS', 'ẞ', 'a', 'A',
             'É', 'g', 'İ']
    ids = list(range(len(words)))
    reg = Registry()
    combos = list(itertools.product([False, True], [False, True]))
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / 'w.sqlite')
        store = Store(path, reg, 'name')
        # bootstrap used default (True, True); migrate through each combo
        first = True
        for ci, acc in combos:
            h = reg.define('name', case_insensitive=ci, accent_sensitive=acc)
            if first:
                # add rows once under the bootstrap version, then migrate
                store.add(list(zip(ids, words)))
                first = False
            store.migrate(revision=h.revision)
            reader = store.open_reader()
            defn = {'case_insensitive': ci, 'accent_sensitive': acc}
            # expected grouping by independently computed key
            expected_groups = {}
            for i, w in zip(ids, words):
                expected_groups.setdefault(reference_key(w, defn), []).append(i)
            for probe in words:
                got = sorted(r.id for r in reader.equal(probe))
                want = sorted(expected_groups[reference_key(probe, defn)])
                if not check('equal probe %r combo %s' % (probe, (ci, acc)), got == want,
                             'got %r want %r' % (got, want)):
                    break
            # range: keys in [key('a'), key('f'))
            lo = reference_key('a', defn)
            hi = reference_key('f', defn)
            expected_range = [(reference_key(w, defn), i)
                              for i, w in zip(ids, words)
                              if lo <= reference_key(w, defn) < hi]
            expected_range.sort()
            got_range = [(reader.key(r.word), r.id) for r in reader.order('a', 'f')]
            check('range [a,f) combo %s' % ((ci, acc),), got_range == expected_range,
                  'got %r want %r' % (got_range, expected_range))
            # order is globally sorted by (key, id)
            got_order = [(reader.key(r.word), r.id) for r in reader.order()]
            check('global order combo %s' % ((ci, acc),), got_order == sorted(got_order))
            # every original word preserved
            check('originals preserved', sorted(r.word for r in reader.order())
                  == sorted(words))
            reader.close()
        store.close()


# --------------------------------------------------------------------------
def test_reader_snapshot_interleaving():
    print('\n[4] Fixed reader vs interleaved writes and migration')
    reg = Registry()
    v1 = reg.define('name', case_insensitive=True, accent_sensitive=True)
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / 'w.sqlite')
        store = Store(path, reg, 'name')     # bootstrap v1 (same def, maybe rev1 reused)
        store.add([(0, 'e'), (1, 'E'), (2, 'f'), (3, 'é')])
        old_reader = store.open_reader()
        old_vk = old_reader.version['definition_id']
        old_equal = sorted(r.id for r in old_reader.equal('e'))
        check('old reader initial e', old_equal == [0, 1])

        # updates + deletes + new inserts after the fixed reader
        store.update([(2, 'É')])            # f -> É (accent)
        store.delete([3])                   # remove é
        store.add([(4, 'e'), (5, 'x')])     # duplicate key e, new x
        v2 = reg.define('name', case_insensitive=True, accent_sensitive=False)
        store.migrate(revision=v2.revision)

        # old reader still exactly its snapshot, old version, no lost/dup rows
        now_old = old_reader.order()
        check('old reader snapshot rows', [(r.id, r.word) for r in now_old]
              == [(0, 'e'), (1, 'E'), (2, 'f'), (3, 'é')])
        check('old reader equal e unchanged', sorted(r.id for r in old_reader.equal('e'))
              == [0, 1])
        check('old reader version still v1', old_reader.version['definition_id'] == old_vk)

        # new reader sees new committed state under v2
        new_reader = store.open_reader()
        # v2 is accent-insensitive: e, E, updated 'É' (id 2) and new 'e' (id 4).
        check('new reader equal e', sorted(r.id for r in new_reader.equal('e'))
              == [0, 1, 2, 4])
        check('new reader equal É', sorted(r.id for r in new_reader.equal('é'))
              == [0, 1, 2, 4])
        check('new reader no deleted 3', all(r.id != 3 for r in new_reader.order()))
        check('new reader duplicate e ids', sorted(r.id for r in new_reader.equal('e'))
              == [0, 1, 2, 4])
        check('new reader original words', all(isinstance(r.word, str) for r in new_reader.order()))
        # row ids independent: two equal originals both present
        check('equal originals retain distinct ids',
              sorted(r.id for r in new_reader.equal('e')) == [0, 1, 2, 4])

        old_reader.close()
        new_reader.close()
        store.close()


# --------------------------------------------------------------------------
def test_failure_points_and_reopen():
    print('\n[5] Build/publish failure points, reopen and safe retry')
    reg = Registry()
    v1 = reg.define('name', case_insensitive=True, accent_sensitive=True)
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / 'w.sqlite')
        store = Store(path, reg, 'name')
        store.add([(i, w) for i, w in enumerate(['e', 'E', 'é', 'f'])])
        good = store.open_reader()
        good_rows = [(r.id, r.word) for r in good.order()]
        good_vk = good.version['definition_id']
        v2 = reg.define('name', case_insensitive=True, accent_sensitive=False)

        # --- crash after build, before publish
        try:
            store.migrate(revision=v2.revision, crash_after_build=True)
            check('crash_after_build raised', False)
        except CrashPoint:
            check('crash_after_build raised', True)
        # previous version still complete & readable
        check('post-build old reader intact', [(r.id, r.word) for r in good.order()] == good_rows)
        st = store.status()
        v2row = next(v for v in st['versions'] if v['revision'] == v2.revision)
        check('post-build v2 complete unpublished',
              v2row['state'] == 'complete' and v2row['catalog_state'] == 'unpublished')
        check('current still v1', st['current']['definition_id'] == good_vk)

        # reopen while old reader is "dead" (simulate process restart)
        store.close()
        reg2 = Registry()
        store = Store(path, reg2, 'name')
        st = store.status()
        # v2 not published, so current must still be v1, and rows intact
        check('reopen current still v1', st['current']['definition_id'] == good_vk)
        check('reopen rows intact', [r.id for r in store.order()] == [r[0] for r in good_rows])
        # safe retry publishes the complete structure, no rebuild needed
        v2_resolved = reg2.snapshot('name', v2.revision)
        res = store.migrate(revision=v2_resolved.revision)
        check('retry published', res.published_seq >= res.built_seq)
        st = store.status()
        v2row = next(v for v in st['versions'] if v['revision'] == v2.revision)
        check('retry v2 published', v2row['catalog_state'] == 'published')
        r = store.open_reader()
        check('retry v2 equal e includes é', sorted(x.id for x in r.equal('e')) == [0, 1, 2])
        r.close()

        # --- crash after publish, before return
        v3 = reg2.define('name', case_insensitive=False, accent_sensitive=True)
        before_vk = store.status()['current']['definition_id']
        try:
            store.migrate(revision=v3.revision, crash_after_publish=True)
            check('crash_after_publish raised', False)
        except CrashPoint:
            check('crash_after_publish raised', True)
        store.close()
        store = Store(path, Registry(), 'name')
        st = store.status()
        v3row = next(v for v in st['versions'] if v['revision'] == v3.revision)
        check('publish committed despite crash', v3row['catalog_state'] == 'published')
        check('reopen picks published v3', st['current']['revision'] == v3.revision)
        # retry is idempotent
        res2 = store.migrate(revision=v3.revision)
        check('post-publish retry idempotent', res2.already_published)
        r = store.open_reader()
        check('case-sensitive: E separate from e',
              sorted(x.id for x in r.equal('e')) == [0])
        r.close()
        store.close()


# --------------------------------------------------------------------------
def test_same_revision_different_definition():
    print('\n[6] Same revision number bound to a different definition is rejected')
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / 'w.sqlite')
        reg = Registry()
        v1 = reg.define('name', case_insensitive=True, accent_sensitive=True)
        store = Store(path, reg, 'name')
        store.add([(0, 'e'), (1, 'é')])
        store.close()

        # Fresh registry, no catalogue history: define a *different* first version
        # (revision 1) and reopen.  The store must reject, not reinterpret.
        reg2 = Registry()
        reg2.define('name', case_insensitive=False, accent_sensitive=False)
        try:
            Store(path, reg2, 'name')
            check('different def at same revision rejected', False)
        except DefinitionError:
            check('different def at same revision rejected', True)

        # A matching definition restored from the DB audit trail is fine.
        store = Store(path, Registry(), 'name')
        check('fresh registry restores audited rule',
              store.status()['current']['revision'] == v1.revision)
        store.close()


# --------------------------------------------------------------------------
def test_scale_20000():
    print('\n[7] 20000 rows, 10 registrations, 2 names, all option combos')
    rng = random.Random(1234)
    base = ['e', 'E', 'é', 'é', 'f', 'F', 'ß', 'SS', 'ẞ',
            'a', 'A', 'Á', 'à', 'g', 'İ', 'i', 'ı']
    words = []
    for i in range(20000):
        w = rng.choice(base) + (str(rng.randrange(30)) if rng.random() < 0.5 else '')
        words.append(w)
    ids = list(range(20000))
    reg = Registry()
    combos = list(itertools.product([False, True], [False, True]))
    names = ['name', 'alt']
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / 'big.sqlite')
        store = Store(path, reg, 'name')
        store.add(list(zip(ids, words)))
        regs = 0
        for k in range(10):
            ci, acc = combos[k % 4]
            nm = names[k % 2]
            h = reg.define(nm, case_insensitive=ci, accent_sensitive=acc)
            regs += 1
            store.migrate(name=nm, revision=h.revision)
            reader = store.open_reader()
            defn = {'case_insensitive': ci, 'accent_sensitive': acc}
            # sample equality probes computed independently
            ok = True
            for probe in ['e', 'É', 'ss', 'i', 'A']:
                pkey = reference_key(probe, defn)
                want = sorted(i for i, w in zip(ids, words)
                              if reference_key(w, defn) == pkey)
                got = sorted(r.id for r in reader.equal(probe))
                if got != want:
                    ok = False
                    break
            check('scale equal combo %s name %s' % ((ci, acc), nm), ok)
            # narrow range
            lo, hi = 'a', 'f'
            want_r = sorted((reference_key(w, defn), i) for i, w in zip(ids, words)
                            if reference_key(lo, defn) <= reference_key(w, defn)
                            < reference_key(hi, defn))
            got_r = [(reader.key(r.word), r.id) for r in reader.order(lo, hi)]
            check('scale range combo %s' % ((ci, acc),), got_r == want_r)
            reader.close()
        check('10 registrations', regs == 10)
        st = store.status()
        check('2 names represented', len({v['name'] for v in st['versions']}) == 2)
        store.close()


# --------------------------------------------------------------------------
def test_index_evidence():
    print('\n[8] Index usage evidence for a selective sample')
    reg = Registry()
    reg.define('name', case_insensitive=True, accent_sensitive=True)
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / 'w.sqlite')
        store = Store(path, reg, 'name')
        store.add([(i, w) for i, w in enumerate(['e', 'E', 'é', 'f', 'g', 'z'])])
        v2 = reg.define('name', case_insensitive=True, accent_sensitive=False)
        store.migrate(revision=v2.revision)
        reader = store.open_reader()
        eq = reader.explain_equal('e')
        rg = reader.explain_range('e', 'g')
        check('equal uses index', any('USING INDEX' in p for p in eq['plan']), eq['plan'])
        check('range uses index', any('USING INDEX' in p for p in rg['plan']), rg['plan'])
        check('equal examines few entries', eq['index_entries_examined'] <= 6)
        check('equal returns ids', sorted(r.id for r in reader.equal('e')) == [0, 1, 2])
        check('range returns ids', [r.id for r in reader.order('e', 'g')] == [0, 1, 2, 3])
        check('range end exclusive', all(r.id != 4 for r in reader.order('e', 'g')))
        print('   equal plan:', eq['plan'], 'examined', eq['index_entries_examined'])
        print('   range plan:', rg['plan'], 'examined', rg['index_entries_examined'])
        reader.close()
        store.close()


# --------------------------------------------------------------------------
def test_range_bounds_and_reclaim():
    print('\n[9] Half-open bounds, unbounded reads and old-structure reclaim')
    reg = Registry()
    v1 = reg.define('name', case_insensitive=True, accent_sensitive=True)
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / 'w.sqlite')
        store = Store(path, reg, 'name')
        store.add([(0, 'a'), (1, 'b'), (2, 'b'), (3, 'c'), (4, 'd')])
        r1 = store.open_reader()
        check('lower bound inclusive', [r.id for r in r1.order('b', 'd')] == [1, 2, 3])
        check('upper bound exclusive', [r.id for r in r1.order('b', 'c')] == [1, 2])
        check('no lower bound', [r.id for r in r1.order(None, 'c')] == [0, 1, 2])
        check('no upper bound', [r.id for r in r1.order('c', None)] == [3, 4])
        check('both unbounded', [r.id for r in r1.order()] == [0, 1, 2, 3, 4])
        check('duplicate keys keep all ids', [r.id for r in r1.equal('b')] == [1, 2])
        v2 = reg.define('name', case_insensitive=False, accent_sensitive=False)
        store.migrate(revision=v2.revision)
        st = store.status()
        v1row = next(v for v in st['versions'] if v['revision'] == v1.revision)
        check('old structure still present while reader active',
              v1row['total_key_rows'] == 5)
        r1.close()   # now reclaim
        st = store.status()
        v1row = next(v for v in st['versions'] if v['revision'] == v1.revision)
        check('old structure reclaimed after last reader',
              v1row['state'] == 'reclaimed' and v1row['total_key_rows'] == 0)
        store.close()


# --------------------------------------------------------------------------
def test_randomized_differential():
    print('\n[10] Randomized differential vs independent computation (20000 rows)')
    rng = random.Random(7)
    alpha = ['e', 'E', 'é', 'é', 'É', 'f', 'F', 'ß', 'SS', 'ẞ', 'a',
             'Á', 'à', 'g', 'İ', 'i', 'ı', 'z', 'Z', 'ø', 'Ø', 'ñ', 'n']
    words = []
    for i in range(20000):
        w = rng.choice(alpha)
        if rng.random() < 0.6:
            w += rng.choice(['', 'x', '1', '2']) + rng.choice(['', 'é', 'E'])
        words.append(w)
    ids = list(range(20000))
    probes = rng.sample(words, 60) + list(alpha)
    reg = Registry()
    errors = 0
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / 'w.sqlite')
        store = Store(path, reg, 'name')
        store.add(list(zip(ids, words)))
        for trial in range(12):
            ci = bool(rng.getrandbits(1))
            acc = bool(rng.getrandbits(1))
            h = reg.define('name', case_insensitive=ci, accent_sensitive=acc)
            store.migrate(revision=h.revision)
            reader = store.open_reader()
            defn = {'case_insensitive': ci, 'accent_sensitive': acc}
            for p in probes:
                want = sorted(i for i, w in zip(ids, words)
                              if reference_key(w, defn) == reference_key(p, defn))
                got = sorted(r.id for r in reader.equal(p))
                if got != want:
                    errors += 1
                    break
            for _ in range(5):
                lo, hi = rng.choice(probes), rng.choice(probes)
                a, b = rng.random() < 0.2, rng.random() < 0.2
                L = None if a else reference_key(lo, defn)
                H = None if b else reference_key(hi, defn)
                want = sorted((reference_key(w, defn), i) for i, w in zip(ids, words)
                              if (L is None or reference_key(w, defn) >= L)
                              and (H is None or reference_key(w, defn) < H))
                got = [(reader.key(r.word), r.id)
                       for r in reader.order(None if a else lo, None if b else hi)]
                if got != want:
                    errors += 1
                    break
            allr = [(reader.key(r.word), r.id) for r in reader.order()]
            if allr != sorted(allr):
                errors += 1
            reader.close()
        store.close()
    check('randomized differential zero errors', errors == 0, 'errors=%d' % errors)


# --------------------------------------------------------------------------
def test_multiprocess_judge_publish_state():
    print('\n[11] Reopen can judge publish state from the persistent audit trail')
    reg = Registry()
    reg.define('name', case_insensitive=True, accent_sensitive=True)
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / 'w.sqlite')
        store = Store(path, reg, 'name')
        store.add([(0, 'a'), (1, 'b')])
        v2 = reg.define('name', case_insensitive=False, accent_sensitive=False)
        try:
            store.migrate(revision=v2.revision, crash_after_build=True)
        except CrashPoint:
            pass
        # Inspect the durable state with a completely independent connection.
        import sqlite3
        raw = sqlite3.connect(path)
        rows = raw.execute(
            'SELECT revision, catalog_state, state FROM doc_versions ORDER BY revision'
        ).fetchall()
        raw.close()
        v2row = [r for r in rows if r[0] == v2.revision][0]
        check('durable incomplete build judged unpublished',
              v2row[1] == 'unpublished' and v2row[2] == 'complete', rows)
        store.close()
        # reopen and confirm it is safe to retry (documented contract)
        store = Store(path, Registry(), 'name')
        check('reopen does not silently adopt unpublished version',
              store.status()['current']['revision'] == 1)
        res = store.migrate(revision=v2.revision)
        check('safe retry after reopen publishes',
              not res.already_published and res.published_seq >= res.built_seq)
        store.close()


# --------------------------------------------------------------------------
def test_cross_instance_adoption():
    print('\n[12] Second Store instance adopts a newly published version')
    reg = Registry()
    reg.define('name', case_insensitive=True, accent_sensitive=True)
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / 'w.sqlite')
        a = Store(path, reg, 'name')
        a.add([(0, 'e')])
        old = a.open_reader()                  # bound to v1
        b = Store(path, reg, 'name')
        v2 = reg.define('name', case_insensitive=True, accent_sensitive=False)
        b.migrate(revision=v2.revision)
        b.close()
        # A's next write and next reader must use v2; the old reader stays v1.
        a.add([(1, 'é')])
        new = a.open_reader()
        check('A new reader adopts v2', new.version['revision'] == v2.revision)
        check('A new write keyed by v2', sorted(r.id for r in new.equal('e')) == [0, 1])
        check('A old reader still v1', old.version['revision'] == 1)
        check('A old reader unaffected', [r.id for r in old.equal('e')] == [0])
        old.close()
        new.close()
        a.close()


# --------------------------------------------------------------------------
def main():
    tests = [
        test_equivalence_classes,
        test_registry_contract,
        test_independent_short_array,
        test_reader_snapshot_interleaving,
        test_failure_points_and_reopen,
        test_same_revision_different_definition,
        test_index_evidence,
        test_range_bounds_and_reclaim,
        test_scale_20000,
        test_randomized_differential,
        test_multiprocess_judge_publish_state,
        test_cross_instance_adoption,
    ]
    for t in tests:
        try:
            t()
        except Exception:
            FAIL.append((t.__name__, traceback.format_exc()))
            print('  EXCEPTION in %s\n%s' % (t.__name__, traceback.format_exc()))
    print('\n================ SUMMARY ================')
    print('passed: %d   failed: %d' % (len(PASS), len(FAIL)))
    for name, detail in FAIL:
        print('FAILED:', name)
        if detail:
            print(detail)
    return 0 if not FAIL else 1


if __name__ == '__main__':
    raise SystemExit(main())
