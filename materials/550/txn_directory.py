"""Small transaction/write-number directory for independently read table files."""
import sqlite3

class Directory:
    def __init__(self, path):
        self.db=sqlite3.connect(path)
        self.db.execute('CREATE TABLE IF NOT EXISTS txns(txn_id INTEGER PRIMARY KEY,state TEXT)')
        self.db.execute('CREATE TABLE IF NOT EXISTS writes(table_name TEXT,write_id INTEGER,txn_id INTEGER,PRIMARY KEY(table_name,write_id),UNIQUE(table_name,txn_id))')
        self.db.commit()
    def begin(self):
        txn=self.db.execute('SELECT COALESCE(MAX(txn_id),0)+1 FROM txns').fetchone()[0]
        with self.db:self.db.execute('INSERT INTO txns VALUES(?,?)',(txn,'open'))
        return txn
    def allocate(self,txn,table):
        old=self.db.execute('SELECT write_id FROM writes WHERE table_name=? AND txn_id=?',(table,txn)).fetchone()
        if old:return old[0]
        wid=self.db.execute('SELECT COALESCE(MAX(write_id),0)+1 FROM writes WHERE table_name=?',(table,)).fetchone()[0]
        with self.db:self.db.execute('INSERT INTO writes VALUES(?,?,?)',(table,wid,txn))
        return wid
    def finish(self,txn,state):
        with self.db:self.db.execute('UPDATE txns SET state=? WHERE txn_id=?',(state,txn))
    def snapshot(self,table):
        rows=self.db.execute('SELECT w.write_id,w.txn_id,t.state FROM writes w JOIN txns t USING(txn_id) WHERE w.table_name=? ORDER BY w.write_id',(table,)).fetchall()
        high=max([r[0] for r in rows],default=0)
        # Prototype assumes global transaction numbers are table write numbers.
        return {'table':table,'txn_high_watermark':self.db.execute('SELECT COALESCE(MAX(txn_id),0) FROM txns').fetchone()[0],'high_write_id':high,'invalid_write_ids':[t for w,t,s in rows if s!='committed'],
                'open_write_ids':[t for w,t,s in rows if s=='open'], 'aborted_write_ids':[t for w,t,s in rows if s=='aborted']}
    def close(self):self.db.close()
