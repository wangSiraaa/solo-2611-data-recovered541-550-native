"""All keys and documents here are synthetic local test fixtures."""
import base64
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from lxml import etree
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from signatures import DS, C14N, RSA_SHA256, SHA256, canonical
from warehouse import INVOICE


def invoice(total=100, bad_line=False):
    node = etree.Element("{%s}Invoice" % INVOICE, Id="signed-invoice", number="N1", seller="S1")
    etree.SubElement(node, "{%s}Line" % INVOICE, sku="A", qty="1", unit_cents=str(total))
    if bad_line:
        etree.SubElement(node, "{%s}Line" % INVOICE, sku="B", qty="0", unit_cents="1")
    etree.SubElement(node, "{%s}Total" % INVOICE).text = str(total)
    return node


def document(key, total=100, bad_line=False):
    root = etree.Element("Submission", nsmap={"i": INVOICE, "ds": DS})
    target = invoice(total, bad_line)
    root.append(target)
    signature = etree.SubElement(root, "{%s}Signature" % DS)
    info = etree.SubElement(signature, "{%s}SignedInfo" % DS)
    etree.SubElement(info, "{%s}CanonicalizationMethod" % DS, Algorithm=C14N)
    etree.SubElement(info, "{%s}SignatureMethod" % DS, Algorithm=RSA_SHA256)
    reference = etree.SubElement(info, "{%s}Reference" % DS, URI="#signed-invoice")
    transforms = etree.SubElement(reference, "{%s}Transforms" % DS)
    etree.SubElement(transforms, "{%s}Transform" % DS, Algorithm=C14N)
    etree.SubElement(reference, "{%s}DigestMethod" % DS, Algorithm=SHA256)
    etree.SubElement(reference, "{%s}DigestValue" % DS).text = base64.b64encode(hashlib.sha256(canonical(target)).digest()).decode()
    etree.SubElement(signature, "{%s}SignatureValue" % DS).text = base64.b64encode(key.sign(canonical(info), padding.PKCS1v15(), hashes.SHA256())).decode()
    return root


def make(root):
    root.mkdir(parents=True, exist_ok=True)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public = key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode()
    (root / "trust-anchor.json").write_text(json.dumps({"kind":"public_rsa_test_anchor", "public_pem":public}, indent=2)+"\n")
    normal = document(key)
    after = document(key, total=200)
    bad = document(key, bad_line=True)
    wrapping = deepcopy(normal)
    signed = wrapping.find("{%s}Invoice" % INVOICE)
    wrapping.remove(signed)
    archive = etree.Element("{urn:offline:archive-extension}Archive")
    archive.append(signed)
    wrapping.insert(0, archive)
    unsigned = invoice(999)
    unsigned.set("Id", "unsigned-invoice")
    wrapping.insert(1, unsigned)
    duplicate = deepcopy(wrapping)
    duplicate.find("{%s}Invoice" % INVOICE).set("Id", "signed-invoice")
    for name, node in {"normal.xml":normal,"after.xml":after,"bad-line.xml":bad,
                       "wrapping.xml":wrapping,"duplicate-id.xml":duplicate}.items():
        (root / name).write_bytes(etree.tostring(node, xml_declaration=True, encoding="UTF-8"))


if __name__ == "__main__":
    make(Path(__file__).resolve().parent / "fixtures")
