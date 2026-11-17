"""A small current-metadata catalog; supports an initial single-set harvest."""
import sqlite3
import xml.etree.ElementTree as ET

DC='{http://purl.org/dc/elements/1.1/}'
MODS='{http://www.loc.gov/mods/v3}'


def title(xml, prefix):
    root=ET.fromstring(xml)
    tag=DC+'title' if prefix=='oai_dc' else MODS+'title'
    return [node.text or '' for node in root.iter(tag)]


class Catalog:
    def __init__(self,path):
        self.db=sqlite3.connect(path)
        self.db.execute('CREATE TABLE IF NOT EXISTS rows(source TEXT,prefix TEXT,identifier TEXT,title TEXT,PRIMARY KEY(source,prefix,identifier))')
        self.db.execute('CREATE TABLE IF NOT EXISTS checkpoints(source TEXT,prefix TEXT,set_spec TEXT,stamp INTEGER,PRIMARY KEY(source,prefix,set_spec))')

    def sync(self,provider,source,prefix,set_spec=None,page_size=10):
        key=(source,prefix,set_spec or '')
        prior=self.db.execute('SELECT stamp FROM checkpoints WHERE source=? AND prefix=? AND set_spec=?',key).fetchone()
        page=provider.start(prefix,set_spec,None if prior is None else prior[0]+1,None,page_size)
        records=[]
        while True:
            records.extend(page['records'])
            token=page['resumption_token']
            if token is None: break
            page=provider.continue_(token)
        with self.db:
            self.db.execute('DELETE FROM rows WHERE source=? AND prefix=?',(source,prefix))
            for record in records:
                h=record['header']
                if not h['deleted']:
                    import json
                    self.db.execute('INSERT OR REPLACE INTO rows VALUES(?,?,?,?)',(source,prefix,h['identifier'],json.dumps(title(record['xml'],prefix))))
            if records:
                self.db.execute('INSERT OR REPLACE INTO checkpoints VALUES(?,?,?,?)',(*key,max(r['header']['stamp'] for r in records)))
        return self.rows(source,prefix)

    def rows(self,source,prefix):
        import json
        return [{'identifier':identifier,'titles':json.loads(titles)} for identifier,titles in self.db.execute('SELECT identifier,title FROM rows WHERE source=? AND prefix=? ORDER BY identifier',(source,prefix))]

    def close(self):
        self.db.close()
