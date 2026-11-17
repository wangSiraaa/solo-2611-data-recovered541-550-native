import json
from pathlib import Path
import tempfile
from signatures import Verifier
from warehouse import Warehouse


if __name__ == "__main__":
    fixtures = Path(__file__).resolve().parent / "fixtures"
    verifier = Verifier(json.loads((fixtures / "trust-anchor.json").read_text())["public_pem"].encode())
    document = (fixtures / "wrapping.xml").read_bytes()
    verified = verifier.verify(document)
    with tempfile.TemporaryDirectory() as root:
        warehouse = Warehouse(Path(root)/"warehouse.sqlite", verifier)
        warehouse.import_document(document)
        print(json.dumps({"verified_reference": verified.reference_uri,
                          "verified_total": verified.element[-1].text,
                          "imported": warehouse.rows()}, ensure_ascii=False, indent=2))
        warehouse.close()
