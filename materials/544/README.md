# Signed invoice import

Python 3.11. Install requirements.txt and run `python3 demo.py`.
Fixtures contain real RSA-SHA256 signatures over canonical XML elements.
Only a public test trust anchor is included, explicitly stored in trust-anchor.json.
No private key is included. `make_fixtures.py` can generate fresh synthetic fixture
documents and a new public anchor; its signing key exists only during generation.
The trusted public key is supplied separately to Verifier, never read from an input
document's KeyInfo.

The current code verifies a signature but permits ambiguous IDs, while the
warehouse chooses a different element and commits rows incrementally. See the
task prompt for the exact supported XML signature profile and invoice contract.
