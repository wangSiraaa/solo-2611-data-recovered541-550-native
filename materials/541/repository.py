"""Runnable small metadata repository. Response dictionaries model a local XML endpoint."""
import copy
import json
import sqlite3


class Repository:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute('CREATE TABLE IF NOT EXISTS records(identifier TEXT,prefix TEXT,stamp INTEGER,sets TEXT,xml TEXT,deleted INTEGER,PRIMARY KEY(identifier,prefix))')

    def put(self, identifier, prefix, stamp, sets, xml):
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO records VALUES(?,?,?,?,?,0)', (identifier,prefix,stamp,json.dumps(sorted(set(sets))),xml))

    def delete(self, identifier, stamp):
        with self.db:
            self.db.execute('UPDATE records SET stamp=?,xml=NULL,deleted=1 WHERE identifier=?',(stamp,identifier))

    def _records(self, prefix, set_spec, from_stamp, until_stamp):
        result=[]
        for identifier,prefix_,stamp,sets,xml,deleted in self.db.execute('SELECT * FROM records WHERE prefix=? ORDER BY identifier',(prefix,)):
            sets=json.loads(sets)
            if set_spec is not None and set_spec not in sets: continue
            if from_stamp is not None and stamp < from_stamp: continue
            if until_stamp is not None and stamp > until_stamp: continue
            result.append({'header':{'identifier':identifier,'stamp':stamp,'sets':sets,'deleted':bool(deleted)},'xml':xml})
        return result

    def start(self, prefix, set_spec=None, from_stamp=None, until_stamp=None, page_size=10):
        return self._page({'prefix':prefix,'set_spec':set_spec,'from_stamp':from_stamp,'until_stamp':until_stamp,'page_size':page_size,'offset':0})

    def continue_(self, token):
        return self._page(json.loads(token))

    def _page(self, request):
        rows=self._records(request['prefix'],request['set_spec'],request['from_stamp'],request['until_stamp'])
        offset=request['offset']; stop=offset+request['page_size']
        following=copy.deepcopy(request); following['offset']=stop
        return {'records':rows[offset:stop],'resumption_token':json.dumps(following,sort_keys=True) if stop<len(rows) else None}

    def close(self):
        self.db.close()
