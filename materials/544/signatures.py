"""Local-reference XML signature profile used by offline archive clients."""
import base64
from dataclasses import dataclass
import hashlib
from lxml import etree
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

DS = "http://www.w3.org/2000/09/xmldsig#"
C14N = "http://www.w3.org/2001/10/xml-exc-c14n#"
RSA_SHA256 = "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"
SHA256 = "http://www.w3.org/2001/04/xmlenc#sha256"


def canonical(element):
    return etree.tostring(element, method="c14n", exclusive=True, with_comments=False)


@dataclass
class VerifiedDocument:
    root: object
    element: object
    reference_uri: str
    digest_sha256: str


class Verifier:
    def __init__(self, public_key):
        self.key = serialization.load_pem_public_key(public_key)

    def verify(self, xml_bytes):
        parser = etree.XMLParser(resolve_entities=False, no_network=True, load_dtd=False)
        root = etree.fromstring(xml_bytes, parser)
        signature = root.find(".//{%s}Signature" % DS)
        info = signature.find("{%s}SignedInfo" % DS)
        if info.find("{%s}CanonicalizationMethod" % DS).get("Algorithm") != C14N:
            raise ValueError("canonicalization not supported")
        if info.find("{%s}SignatureMethod" % DS).get("Algorithm") != RSA_SHA256:
            raise ValueError("signature method not supported")
        reference = info.find("{%s}Reference" % DS)
        uri = reference.get("URI")
        if not uri or not uri.startswith("#"):
            raise ValueError("local reference required")
        # Existing archive code takes the first matching ID, even if ambiguous.
        element = root.xpath("//*[@Id=$value]", value=uri[1:])[0]
        digest = hashlib.sha256(canonical(element)).digest()
        expected = base64.b64decode(reference.findtext("{%s}DigestValue" % DS), validate=True)
        if digest != expected:
            raise ValueError("reference digest mismatch")
        signature_bytes = base64.b64decode(signature.findtext("{%s}SignatureValue" % DS), validate=True)
        self.key.verify(signature_bytes, canonical(info), padding.PKCS1v15(), hashes.SHA256())
        return VerifiedDocument(root, element, uri, digest.hex())
