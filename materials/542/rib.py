"""Persistent route observations, not a forwarding or route-selection daemon."""
import ipaddress
import sqlite3
from vrps import validate


class Rib:
    def __init__(self, path):
        self.db=sqlite3.connect(path)
        self.db.execute('CREATE TABLE IF NOT EXISTS paths(peer TEXT,prefix TEXT,path_id INTEGER,origin INTEGER,PRIMARY KEY(peer,prefix))')
        self.db.commit()

    def announce(self, peer, prefix, path_id, origin):
        prefix=str(ipaddress.ip_network(prefix))
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO paths VALUES(?,?,?,?)',(peer,prefix,path_id,origin))

    def withdraw(self, peer, prefix, path_id):
        with self.db:
            self.db.execute('DELETE FROM paths WHERE peer=? AND prefix=?',(peer,str(ipaddress.ip_network(prefix))))

    def rows(self):
        return [{'peer':p,'prefix':n,'path_id':i,'origin':o} for p,n,i,o in self.db.execute('SELECT * FROM paths ORDER BY peer,prefix,path_id')]

    def facts(self, directory):
        snapshot=directory.snapshot()
        return [dict(row,validation=validate(row['prefix'],row['origin'],snapshot),vrp_cache=snapshot['cache'],vrp_serial=snapshot['serial']) for row in self.rows()]

    def close(self):
        self.db.close()
