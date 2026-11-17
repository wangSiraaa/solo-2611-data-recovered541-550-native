"""Invoice import client of the public signature verifier."""
from lxml import etree
import sqlite3

INVOICE = "urn:offline:invoice:v1"


class Warehouse:
    def __init__(self, path, verifier):
        self.path = str(path)
        self.verifier = verifier
        self.db = sqlite3.connect(self.path)
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS invoices(number TEXT PRIMARY KEY, seller TEXT NOT NULL,
                                               total_cents INTEGER NOT NULL, signed_digest TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS lines(number TEXT NOT NULL, pos INTEGER NOT NULL,
                                            sku TEXT NOT NULL, qty INTEGER NOT NULL, unit_cents INTEGER NOT NULL,
                                            PRIMARY KEY(number,pos));
        """)
        self.db.commit()

    def _invoice(self, verified):
        # The archive verified one element; this client reselects a business element.
        return verified.root.find("{%s}Invoice" % INVOICE)

    def import_document(self, xml_bytes):
        verified = self.verifier.verify(xml_bytes)
        element = self._invoice(verified)
        number = element.get("number")
        total = int(element.findtext("{%s}Total" % INVOICE))
        self.db.execute("INSERT OR REPLACE INTO invoices VALUES (?,?,?,?)",
                        (number, element.get("seller"), total, verified.digest_sha256))
        self.db.commit()
        self.db.execute("DELETE FROM lines WHERE number=?", (number,))
        self.db.commit()
        for pos, line in enumerate(element.findall("{%s}Line" % INVOICE)):
            qty = int(line.get("qty"))
            price = int(line.get("unit_cents"))
            if qty <= 0 or price < 0:
                raise ValueError("invalid line")
            self.db.execute("INSERT INTO lines VALUES (?,?,?,?,?)",
                            (number, pos, line.get("sku"), qty, price))
            self.db.commit()
        return number

    def rows(self):
        return {"invoices": [list(r) for r in self.db.execute("SELECT number,seller,total_cents FROM invoices ORDER BY number")],
                "lines": [list(r) for r in self.db.execute("SELECT number,pos,sku,qty,unit_cents FROM lines ORDER BY number,pos")]}

    def close(self):
        self.db.close()
