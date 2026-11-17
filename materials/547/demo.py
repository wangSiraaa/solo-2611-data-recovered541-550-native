import json
import tempfile
from pathlib import Path
from collations import Registry
from store import Store

registry = Registry()
registry.define('name', case_insensitive=True, accent_sensitive=True)
old = registry.snapshot('name')
with tempfile.TemporaryDirectory() as tmp:
    store = Store(str(Path(tmp) / 'words.sqlite'), registry)
    store.add([(0, 'e'), (1, '\u00e9'), (2, 'f'), (3, 'E')])
    before = store.equal('e')
    registry.define('name', case_insensitive=True, accent_sensitive=False)
    store.activate()
    print(json.dumps({'before': before, 'old_handle_equal_after_definition': old.compare('e', '\u00e9') == 0,
                      'after_unreindexed': store.equal('e'), 'physical_order_after': store.order()}, ensure_ascii=False))
    store.close()
