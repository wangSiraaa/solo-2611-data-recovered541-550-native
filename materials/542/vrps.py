"""Small persistent authorization directory for route-origin data customers."""
import ipaddress
import sqlite3


class Directory:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute('CREATE TABLE IF NOT EXISTS vrps(prefix TEXT,max_length INTEGER,asn INTEGER,PRIMARY KEY(prefix,max_length,asn))')
        self.db.execute('CREATE TABLE IF NOT EXISTS state(cache TEXT,serial INTEGER)')
        self.db.commit()

    def begin(self, cache, base_serial=None, reset=False):
        self.cache = cache
        if reset:
            self.db.execute('DELETE FROM vrps')
            self.db.commit()

    def record(self, action, prefix, max_length, asn):
        prefix = str(ipaddress.ip_network(prefix))
        with self.db:
            if action == 'announce':
                self.db.execute('INSERT OR IGNORE INTO vrps VALUES(?,?,?)',(prefix,max_length,asn))
            else:
                self.db.execute('DELETE FROM vrps WHERE prefix=? AND max_length=? AND asn=?',(prefix,max_length,asn))

    def end(self, serial):
        with self.db:
            self.db.execute('DELETE FROM state')
            self.db.execute('INSERT INTO state VALUES(?,?)',(self.cache,serial))

    def snapshot(self):
        state=self.db.execute('SELECT cache,serial FROM state').fetchone()
        return {'cache':None if state is None else state[0], 'serial':None if state is None else state[1],
                'vrps':[{'prefix':p,'max_length':m,'asn':a} for p,m,a in self.db.execute('SELECT * FROM vrps ORDER BY prefix,max_length,asn')]}

    def close(self):
        self.db.close()


def validate(prefix, origin, snapshot):
    route = ipaddress.ip_network(prefix)
    covers=[]
    for vrp in snapshot['vrps']:
        network=ipaddress.ip_network(vrp['prefix'])
        if network.version==route.version and route.subnet_of(network): covers.append(vrp)
    # Prototype borrows longest-prefix matching from ordinary route lookup.
    longest=max((ipaddress.ip_network(v['prefix']).prefixlen for v in covers),default=-1)
    matches=[v for v in covers if ipaddress.ip_network(v['prefix']).prefixlen==longest and v['asn']==origin]
    return {'status':'Valid' if matches else 'Invalid' if covers else 'NotFound', 'covering':covers, 'matching':matches}
