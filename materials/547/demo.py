"""Smoke demo: equivalence classes, ordering, old handles and old readers.

Run ``python3 demo.py`` from this directory.  It writes real SQLite files in an
isolated temporary directory and prints:
  * e / E / é equivalence classes under both catalogue versions;
  * that an old Handle keeps its comparison after a new definition is defined;
  * that a reader opened before the migration keeps old semantics afterwards,
    while a new reader uses the rebuilt structure;
  * SQLite query plans proving the indexed equality / narrow range paths.
"""
import json
import tempfile
from pathlib import Path

from collations import Registry
from store import Store


def main():
    report = {}
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / 'words.sqlite')
        registry = Registry(str(Path(tmp) / 'catalogue.sqlite'))
        v1 = registry.define('name', case_insensitive=True, accent_sensitive=True)
        old_handle = registry.snapshot('name')      # independent import tool holds this

        store = Store(path, registry, 'name')
        store.add([(0, 'e'), (1, 'é'), (2, 'f'), (3, 'E'), (4, 'É')])
        reader1 = store.open_reader()               # bound to v1 + snapshot after adds

        report['v1_definition'] = dict(v1.definition)
        report['v1_equal_e_ids'] = [r.id for r in reader1.equal('e')]

        v2 = registry.define('name', case_insensitive=True, accent_sensitive=False)
        migration = store.migrate(revision=v2.revision)
        report['migrated'] = migration._asdict()
        report['old_handle_compare_e_é'] = old_handle.compare('e', 'é')

        # The old reader is still interpreted by v1, and does not lose rows.
        report['old_reader_equal_e_after_migration'] = [r.id for r in reader1.equal('e')]
        report['old_reader_version'] = reader1.version

        reader2 = store.open_reader()               # bound to v2
        report['new_reader_equal_e_ids'] = [r.id for r in reader2.equal('e')]
        report['new_reader_equal_e_words'] = [r.word for r in reader2.equal('e')]
        report['new_reader_order'] = [(r.id, r.word) for r in reader2.order()]
        report['new_reader_version'] = reader2.version

        # Indexed access evidence for the selective sample (v2 is current).
        report['explain_equal_e'] = reader2.explain_equal('e')
        report['explain_range_e_to_f'] = reader2.explain_range('e', 'f')
        report['status_before_reclaim'] = store.status()

        reader1.close()                             # v1 is no longer current -> reclaim
        report['status_after_old_reader_reclaim'] = [
            {k: v[k] for k in ('revision', 'state', 'catalog_state', 'total_key_rows')}
            for v in store.status()['versions']]
        reader2.close()
        store.close()

    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))


if __name__ == '__main__':
    main()
