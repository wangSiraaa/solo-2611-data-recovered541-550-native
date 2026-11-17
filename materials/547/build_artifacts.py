"""Build real, persistent SQLite files demonstrating the migration contract.

Usage:  python3 build_artifacts.py [output_dir]

Produces, in ``artifacts/`` by default:
  * ``catalogue.sqlite`` -- the public versioned catalogue (auditable definitions
    and current revision per name);
  * ``index.sqlite``     -- the document store with its versioned physical index
    structures, migration audit log and read-version metadata.

The script leaves BOTH the old and the new published structures on disk (no
automatic reclamation), so the files exhibit the full state: a superseded
version kept for an old read handle, and the current version rebuilt for the new
rule.  Re-running is deterministic: it starts from a clean pair of files.
"""
import json
import os
import sys
from pathlib import Path

from collations import Registry
from store import Store

SAMPLE = [
    (0, 'e'), (1, 'é'), (2, 'f'), (3, 'E'), (4, 'É'),
    (5, 'ß'), (6, 'SS'), (7, 'Straße'), (8, 'STRASSE'),
    (9, 'a'), (10, 'A'), (11, 'Á'), (12, 'g'), (13, 'İ'), (14, 'i'),
]


def build(out_dir):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for name in ('catalogue.sqlite', 'index.sqlite',
                 'index.sqlite-wal', 'index.sqlite-shm',
                 'catalogue.sqlite-wal', 'catalogue.sqlite-shm'):
        p = out / name
        if p.exists():
            p.unlink()

    registry = Registry(str(out / 'catalogue.sqlite'))
    v1 = registry.define('name', case_insensitive=True, accent_sensitive=True)
    store = Store(str(out / 'index.sqlite'), registry, 'name')
    store.add(SAMPLE)
    old_reader = store.open_reader()

    v2 = registry.define('name', case_insensitive=True, accent_sensitive=False)
    migration = store.migrate(revision=v2.revision)
    new_reader = store.open_reader()

    report = {
        'catalogue': str(out / 'catalogue.sqlite'),
        'index': str(out / 'index.sqlite'),
        'versions': registry.versions('name'),
        'migration': migration._asdict(),
        'old_reader': {'version': old_reader.version,
                       'equal_e_ids': [r.id for r in old_reader.equal('e')]},
        'new_reader': {'version': new_reader.version,
                       'equal_e_ids': [r.id for r in new_reader.equal('e')],
                       'order': [r.as_dict() for r in new_reader.order()]},
        'status': store.status(),
    }
    # Keep old_reader open so reclaim does not delete the superseded structure;
    # the built files therefore contain both complete structures.
    new_reader.close()
    store.close()
    registry.close()
    (out / 'report.json').write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    return report


if __name__ == '__main__':
    target = sys.argv[1] if len(sys.argv) > 1 else str(Path(__file__).parent / 'artifacts')
    rep = build(target)
    print(json.dumps({k: rep[k] for k in ('catalogue', 'index', 'migration')},
                     ensure_ascii=False, indent=2, default=str))
