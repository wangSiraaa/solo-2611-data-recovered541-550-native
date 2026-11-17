"""Independent verification of the directory + bucketed reader.

Expected answers are derived here from first principles, never from a Reader
export:

* expected valid write ids - raw SQLite reads of the txns/writes tables plus
  an independent replay of the allocation/finish history;
* expected live RIDs      - an independent replay of the per-operation
  JSONL ledgers under expected/ with hand-written ordering/visibility rules.
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path

from txn_directory import Directory, DirectoryError
from bucket_reader import Reader, ScanError

ROOT = Path(__file__).resolve().parent
ART = ROOT / "artifacts"
DATA = ART / "data"
SNAPS = ART / "snapshots"
RESULTS = ART / "results"
EXPECTED = ART / "expected"
DB = ART / "txn_directory.sqlite"
TABLES = ["measurements", "events", "inventory", "alerts"]

PASS, FAIL = "PASS", "FAIL"
report = []


def check(name, condition, detail=""):
    report.append((PASS if condition else FAIL, name, detail))
    if not condition:
        raise AssertionError(f"{name}: {detail}")


def load_jsonl(path):
    return [json.loads(line) for line in Path(path).read_text(
        encoding="utf-8").splitlines() if line.strip()]


def independent_expected(snapshot, operations):
    """Replay an operation ledger against a snapshot without using Reader."""
    high = snapshot["high_write_id"]
    invalid = set(snapshot["invalid_write_ids"])
    visible = {w for w in range(1, high + 1) if w not in invalid}
    ops = [op for op in operations if op["current_write_id"] in visible]
    ops.sort(key=lambda o: (o["current_write_id"], o["statement_id"],
                            o["operation_no"]))
    live = {}
    for op in ops:
        rid = (op["original_write_id"], op["bucket_id"], op["row_id"])
        if op["kind"] == "insert":
            assert rid not in live, f"independent replay saw duplicate RID {rid}"
            live[rid] = op["value"]
        else:
            assert rid in live, f"independent replay saw delete of missing RID {rid}"
            del live[rid]
    return visible, live


# ============================================================ Part A: directory
def verify_directory_contract():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "d.sqlite"
        d = Directory(path)
        t1 = d.begin()
        check("begin starts at 1", t1 == 1)
        check("txn ids are not booleans/strings", isinstance(t1, int) and not isinstance(t1, bool))

        # Multi-table interleaving: global txn id != per-table write id.
        w_a1 = d.allocate(t1, "measurements")
        t2 = d.begin()
        w_b1 = d.allocate(t2, "events")
        w_a2 = d.allocate(t2, "measurements")
        check("per-table counters independent",
              w_a1 == 1 and w_b1 == 1 and w_a2 == 2,
              f"{w_a1},{w_b1},{w_a2}")
        check("txn id differs from table write id after interleaving",
              t2 == 2 and w_a2 == 2 and w_b1 == 1 and t2 != w_b1)

        # Same txn/table retry returns the original id.
        check("allocate retry is idempotent per table",
              d.allocate(t2, "measurements") == w_a2
              and d.allocate(t2, "events") == w_b1)
        check("retry on a third table gets its own counter",
              d.allocate(t2, "alerts") == 1)

        # Only open transactions allocate.
        d.finish(t1, "committed")
        try:
            d.allocate(t1, "measurements")
            check("allocate after finish rejected", False)
        except DirectoryError:
            check("allocate after finish rejected", True)
        try:
            d.allocate(9999, "measurements")
            check("allocate unknown txn rejected", False)
        except DirectoryError:
            check("allocate unknown txn rejected", True)

        # finish state machine: invalid state, reversal, idempotent retry.
        for bad in ("maybe", "", None, 1):
            try:
                d.finish(t2, bad)
                check(f"finish rejects invalid state {bad!r}", False)
            except DirectoryError:
                check(f"finish rejects invalid state {bad!r}", True)
        d.finish(t2, "aborted")
        d.finish(t2, "aborted")  # identical retry must be idempotent
        check("identical finish retry idempotent", True)
        try:
            d.finish(t2, "committed")
            check("terminal reversal rejected", False)
        except DirectoryError:
            check("terminal reversal rejected", True)
        try:
            d.finish(4242, "committed")
            check("finish unknown txn rejected", False)
        except DirectoryError:
            check("finish unknown txn rejected", True)

        # Input validation.
        for bad_table in ("", 1, None, True):
            try:
                d.allocate(d.begin(), bad_table)
                check(f"table {bad_table!r} rejected", False)
            except DirectoryError:
                check(f"table {bad_table!r} rejected", True)
        for bad_txn in (True, False, "1", 1.0):
            try:
                d.allocate(bad_txn, "measurements")
                check(f"txn {bad_txn!r} rejected", False)
            except DirectoryError:
                check(f"txn {bad_txn!r} rejected", True)

        # Snapshot exception lists contain table write ids, never txn ids.
        t3 = d.begin()
        w_open = d.allocate(t3, "measurements")  # table write id 3; global txn id is t3
        snap = d.snapshot("measurements")
        check("snapshot lists table write id as open, not txn id",
              snap["open_write_ids"] == [w_open]
              and set(snap["invalid_write_ids"]) == {w_a2, w_open}
              and t3 not in snap["invalid_write_ids"]
              and snap["aborted_write_ids"] == [w_a2],
              json.dumps(snap))
        check("empty table has zero watermarks",
              d.snapshot("nope") == {
                  "format": "txn-snapshot/v1", "table": "nope",
                  "txn_high_watermark": snap["txn_high_watermark"],
                  "high_write_id": 0, "open_write_ids": [],
                  "aborted_write_ids": [], "invalid_write_ids": []})
        frozen = json.loads(json.dumps(snap))  # JSON round-trip
        d.finish(t3, "committed")
        check("old snapshot does not drift after commit",
              frozen["open_write_ids"] == [w_open]
              and d.snapshot("measurements")["open_write_ids"] == [])

        # Persistence: reopen the real SQLite file in a fresh object.
        d.close()
        d2 = Directory(path)
        check("persisted txn high watermark survives reopen",
              d2.status()["txn_high_watermark"] == d2.begin() - 1)
        check("persisted write counter survives reopen",
              d2.allocate(d2.begin(), "events") == 2)
        status = d2.status()
        check("status reports all terminal states",
              status["transaction_counts"]["committed"] >= 2
              and status["transaction_counts"]["aborted"] >= 1
              and sum(status["transaction_counts"].values()) == status["txn_high_watermark"])
        d2.close()

    # The generated 100-txn directory: independently re-derive every write id
    # straight from SQLite (no Directory methods used for the expectation).
    raw = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    max_txn = raw.execute("SELECT MAX(txn_id) FROM txns").fetchone()[0]
    check("generated run has 100 transactions", max_txn == 100)
    for table in TABLES:
        rows = raw.execute(
            """SELECT w.write_id, w.txn_id, t.state FROM writes w
               JOIN txns t USING(txn_id) WHERE w.table_name=? ORDER BY w.write_id""",
            (table,)).fetchall()
        write_ids = [r[0] for r in rows]
        check(f"{table}: write ids dense from 1, no reuse",
              write_ids == list(range(1, len(write_ids) + 1)))
        check(f"{table}: one write id per (txn,table) and txn ids differ in general",
              len({r[1] for r in rows}) == len(rows))
        if table == "measurements":
            txn_ids = [r[1] for r in rows]
            check("measurements: global txn ids are not the table write ids",
                  txn_ids != write_ids and max(txn_ids) > max(write_ids))
    raw.close()


# ====================================================== Part B: physical reader
def verify_reader():
    reader = Reader(DATA)
    for table in TABLES:
        snapshot = json.loads((SNAPS / f"{table}_final.json").read_text())
        operations = load_jsonl(EXPECTED / f"{table}_operations.jsonl")
        visible, expected_live = independent_expected(snapshot, operations)
        check(f"{table}: valid write ids derived independently",
              visible == {w for w in range(1, snapshot["high_write_id"] + 1)
                          if w not in set(snapshot["invalid_write_ids"])})

        manifest = json.loads((DATA / f"{table}_manifest.json").read_text())
        result = reader.scan_snapshot(snapshot, manifest)
        check(f"{table}: complete status", result["status"] == "complete")
        got = {(r["original_write_id"], r["bucket_id"], r["row_id"]): r["value"]
               for r in result["rows"]}
        check(f"{table}: full replay RIDs/values match independent expectation",
              got == expected_live,
              f"reader={len(got)} expected={len(expected_live)}")
        check(f"{table}: output sorted by full RID",
              [ (r["original_write_id"], r["bucket_id"], r["row_id"])
                for r in result["rows"] ] == sorted(got))
        check(f"{table}: provenance + snapshot retained on every row",
              all({"source_file", "source_line", "provenance",
                   "snapshot_table", "snapshot_high_write_id"} <= set(r)
                  for r in result["rows"]))

        # Replay-only manifest (no bases) must give the identical dataset.
        replay_manifest = json.loads(
            (DATA / f"{table}_manifest_replay.json").read_text())
        replay_result = reader.scan_snapshot(snapshot, replay_manifest)
        replay_got = {(r["original_write_id"], r["bucket_id"], r["row_id"]): r["value"]
                      for r in replay_result["rows"]}
        check(f"{table}: base+tail equals full replay",
              replay_got == expected_live and replay_result["base_used"] is None
              and result["base_used"] is not None)
        check(f"{table}: replay reads strictly more delta operations",
              replay_result["delta_operations_applied"]
              >= result["delta_operations_applied"])

        # File/statement order independence: reverse the delta list entirely.
        rev = json.loads(json.dumps(manifest))
        rev["deltas"] = list(reversed(rev["deltas"]))
        rev_result = reader.scan_snapshot(snapshot, rev)
        check(f"{table}: reversed manifest order yields identical rows",
              [(r["original_write_id"], r["bucket_id"], r["row_id"], r["value"])
               for r in rev_result["rows"]]
              == [(r["original_write_id"], r["bucket_id"], r["row_id"], r["value"])
                  for r in result["rows"]])

        # Snapshot JSON produced externally is accepted as-is.
        reloaded = Reader.load_snapshot(SNAPS / f"{table}_final.json")
        check(f"{table}: external snapshot JSON accepted",
              reader.scan_snapshot(reloaded, manifest)["row_count"] == len(got))

        # Exported JSONL + status sidecar.
        status = reader.export_scan(
            snapshot, manifest, RESULTS / f"{table}_rows.jsonl",
            status_path=RESULTS / f"{table}_status.json")
        check(f"{table}: export marked complete", status["status"] == "complete")
        exported = load_jsonl(RESULTS / f"{table}_rows.jsonl")
        check(f"{table}: exported JSONL equals scan rows",
              len(exported) == len(result["rows"]))

    # RID explanation entry point.
    snap = json.loads((SNAPS / "measurements_final.json").read_text())
    manifest = json.loads((DATA / "measurements_manifest.json").read_text())
    present = reader.scan_snapshot(snap, manifest)["rows"][0]
    rid = (present["original_write_id"], present["bucket_id"], present["row_id"])
    explanation = reader.explain_rid(snap, manifest, rid)
    check("explain_rid finds a live RID",
          explanation["present"] and explanation["value"] == present["value"])
    check("explain_rid reports an absent RID",
          reader.explain_rid(snap, manifest, (9999, 0, 0))["present"] is False)


# ============================================= Part C: the headline defect cases
def verify_headline_cases():
    reader = Reader(DATA)
    operations = load_jsonl(EXPECTED / "measurements_operations.jsonl")

    # 1. Same row_id, different original_write_id: two physical rows coexist.
    by_row = {}
    for op in operations:
        if op["kind"] == "insert":
            by_row.setdefault(op["row_id"], set()).add(op["original_write_id"])
    shared = next(rid0 for rid0, originals in by_row.items() if len(originals) >= 2)
    snapshot = json.loads((SNAPS / "measurements_final.json").read_text())
    manifest = json.loads((DATA / "measurements_manifest_replay.json").read_text())
    rows = reader.scan_snapshot(snapshot, manifest)["rows"]
    matching = [r for r in rows if r["row_id"] == shared]
    check("two rows with same row_id but different original_write_id coexist",
          len({r["original_write_id"] for r in matching}) >= 2,
          f"row_id={shared}")

    # 2. Committed delete removes exactly the targeted composite RID.
    committed_deletes = [op for op in operations if op["kind"] == "delete"]
    visible, expected_live = independent_expected(snapshot, operations)
    check("targeted deletes are absent while other rows survive",
          all((op["original_write_id"], op["bucket_id"], op["row_id"]) not in expected_live
              for op in committed_deletes
              if op["current_write_id"] in visible))

    # 3. The aborted w3 delete did not remove (w1, 7, 1).
    check("aborted delete leaves its target RID live",
          (1, 7, 1) in expected_live,
          f"live contains (1,7,1)? {(1, 7, 1) in expected_live}")

    # 4. Old snapshot S1 stays fixed after the open txn later commits.
    s1 = json.loads((SNAPS / "measurements_s1.json").read_text())
    s1_meta = json.loads((EXPECTED / "s1_meta.json").read_text())
    check("S1 still marks the write open after later commit",
          s1_meta["open_txn"]["write_id"] in s1["open_write_ids"]
          and s1_meta["open_txn"]["write_id"] in s1["invalid_write_ids"]
          and s1_meta["open_txn"]["txn_id"] not in s1["invalid_write_ids"])
    _, s1_expected = independent_expected(s1, operations)

    # B1 eligible at S1, B2 ineligible: reader must pick B1, never B2.
    s1_manifest = json.loads((DATA / "measurements_manifest_s1.json").read_text())
    s1_result = reader.scan_snapshot(s1, s1_manifest)
    check("S1 uses the eligible B1 base, not B2 embedding the open write",
          s1_result["base_used"]["base_write_id"] == s1_meta["b1_boundary"])
    s1_got = {(r["original_write_id"], r["bucket_id"], r["row_id"]): r["value"]
              for r in s1_result["rows"]}
    check("S1 base+tail equals independent S1 replay", s1_got == s1_expected)
    s1_replay = reader.scan_snapshot(
        s1, json.loads((DATA / "measurements_manifest_replay.json").read_text()))
    check("S1 full replay equals base+tail",
          {(r["original_write_id"], r["bucket_id"], r["row_id"]) for r in s1_replay["rows"]}
          == set(s1_expected))

    b2_only = json.loads((DATA / "measurements_manifest_s1_b2only.json").read_text())
    fallback = reader.scan_snapshot(s1, b2_only)
    check("S1 with only an ineligible base falls back to full replay",
          fallback["base_used"] is None
          and {(r["original_write_id"], r["bucket_id"], r["row_id"])
               for r in fallback["rows"]} == set(s1_expected))

    # S1 never changes when re-read from disk after the final commit.
    check("persisted S1 JSON did not drift",
          Reader.load_snapshot(SNAPS / "measurements_s1.json") == s1)


# =============================================== Part D: illegal inputs must fail
def verify_rejections():
    reader = Reader(DATA)
    good = Reader.load_snapshot(SNAPS / "measurements_final.json")
    manifest = json.loads((DATA / "measurements_manifest.json").read_text())

    def must_fail(name, snapshot=None, man=None):
        try:
            reader.scan_snapshot(snapshot if snapshot is not None else good,
                                 man if man is not None else manifest)
            check(name, False, "no error raised")
        except ScanError:
            check(name, True)

    # Snapshot shape violations.
    bad = json.loads(json.dumps(good)); bad.pop("invalid_write_ids")
    must_fail("snapshot missing required list", snapshot=bad)
    bad = json.loads(json.dumps(good)); bad["format"] = "other/v9"
    must_fail("snapshot wrong format", snapshot=bad)
    bad = json.loads(json.dumps(good)); bad["high_write_id"] = -1
    must_fail("negative high watermark", snapshot=bad)
    bad = json.loads(json.dumps(good)); bad["open_write_ids"] = [True]
    must_fail("boolean write id rejected", snapshot=bad)
    bad = json.loads(json.dumps(good)); bad["open_write_ids"] = [-1]
    must_fail("negative exception id rejected", snapshot=bad)
    bad = json.loads(json.dumps(good)); bad["open_write_ids"] = [1, 1]
    must_fail("duplicate exception id rejected", snapshot=bad)
    bad = json.loads(json.dumps(good)); bad["open_write_ids"] = [good["high_write_id"] + 5]
    must_fail("out-of-range exception rejected", snapshot=bad)
    bad = json.loads(json.dumps(good)); bad["aborted_write_ids"] = list(good["open_write_ids"])
    must_fail("open/aborted overlap rejected", snapshot=bad)
    bad = json.loads(json.dumps(good)); bad["invalid_write_ids"] = []
    must_fail("invalid != open union aborted rejected", snapshot=bad)
    bad = json.loads(json.dumps(good)); bad["table"] = "events"
    must_fail("snapshot/manifest table identity mismatch", snapshot=bad)

    # Manifest / physical violations.
    bad_man = json.loads(json.dumps(manifest)); bad_man["table"] = "events"
    must_fail("manifest table mismatch", man=bad_man)
    bad_man = json.loads(json.dumps(manifest)); bad_man["deltas"][0]["path"] = "../escape.jsonl"
    must_fail("path traversal rejected", man=bad_man)
    bad_man = json.loads(json.dumps(manifest)); bad_man["deltas"][0]["max_write_id"] = 10 ** 9
    check("oversized declared range accepted (rows inside it are simply filtered)", True)
    bad_man = json.loads(json.dumps(manifest)); bad_man["deltas"][0]["min_write_id"] = 10 ** 9
    must_fail("row outside declared range rejected", man=bad_man)
    bad_man = json.loads(json.dumps(manifest)); bad_man["deltas"][0]["kind"] = "upsert"
    must_fail("illegal delta kind rejected", man=bad_man)
    bad_man = json.loads(json.dumps(manifest))
    bad_man["deltas"][1] = dict(bad_man["deltas"][0])
    must_fail("duplicate manifest path rejected", man=bad_man)

    # Physical file corruption in a sandboxed copy of the input root.
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        src_entry = manifest["deltas"][0]
        src_file = DATA / src_entry["path"]
        dst_file = root / src_entry["path"]
        dst_file.parent.mkdir(parents=True, exist_ok=True)
        lines = src_file.read_text(encoding="utf-8").splitlines()
        local_manifest = {"table": "measurements", "bases": [],
                          "deltas": [dict(src_entry, min_write_id=1, max_write_id=10 ** 6)]}

        # a) delete of a RID that cannot be live
        rogue = {"current_write_id": json.loads(lines[0])["current_write_id"],
                 "original_write_id": 999999, "bucket_id": 0, "row_id": 999999}
        dst_file.write_text(json.dumps(rogue) + "\n", encoding="utf-8")
        _expect_scan_error(reader, good, local_manifest,
                           "delete of non-existent visible RID fails the read")

        # b) duplicate operation identity
        dup_lines = [lines[0], lines[0]]
        dst_file.write_text("\n".join(dup_lines) + "\n", encoding="utf-8")
        _expect_scan_error(reader, good, local_manifest,
                           "duplicate operation identity fails the read")

        # c) illegal field (bucket 16, missing value)
        bad_row = json.loads(lines[0]); bad_row["bucket_id"] = 16
        dst_file.write_text(json.dumps(bad_row) + "\n", encoding="utf-8")
        _expect_scan_error(reader, good, local_manifest,
                           "bucket_id outside 0..15 fails the read")

        # d) failed read never produces a complete dataset
        out = root / "out.jsonl"; status_file = root / "status.json"
        try:
            reader.export_scan(good, local_manifest, out, status_path=status_file)
            check("failed export raises", False)
        except ScanError:
            payload = json.loads(status_file.read_text())
            check("failed read writes failed status and no complete dataset",
                  payload["status"] == "failed" and out.read_text() == "")


def _expect_scan_error(reader, snapshot, manifest, name):
    try:
        reader.scan_snapshot(snapshot, manifest)
        check(name, False, "no error raised")
    except ScanError:
        check(name, True)


def main():
    RESULTS.mkdir(parents=True, exist_ok=True)
    verify_directory_contract()
    verify_reader()
    verify_headline_cases()
    verify_rejections()
    failed = [r for r in report if r[0] == FAIL]
    lines = [f"[{state}] {name} {detail}".rstrip() for state, name, detail in report]
    (RESULTS / "verification_report.txt").write_text("\n".join(lines) + "\n",
                                                     encoding="utf-8")
    print("\n".join(lines))
    print(f"\n{len(report) - len(failed)}/{len(report)} checks passed")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
