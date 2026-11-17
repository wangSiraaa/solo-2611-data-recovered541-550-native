"""Generate the materials/550 artifact family (deterministic).

Scale envelope: 4 tables, 100 transactions, <=1000 physical operations per
table, 16 buckets, <=100 JSONL files.  Everything produced here is written
under artifacts/ and is reproducible by re-running this script.
"""
from __future__ import annotations

import json
import random
import shutil
from pathlib import Path

from txn_directory import Directory

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "artifacts"
DATA = OUT / "data"
SNAPS = OUT / "snapshots"
RESULTS = OUT / "results"
EXPECTED = OUT / "expected"
DB_PATH = OUT / "txn_directory.sqlite"

TABLES = ["measurements", "events", "inventory", "alerts"]
TXN_COUNT = 100
OPEN_TXN = 30          # left open at the S1 cut, committed later
ABORT_RATE = 0.25
EMPTY_TXN_RATE = 0.15


def write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, sort_keys=True) + "\n")


def main():
    if OUT.exists():
        shutil.rmtree(OUT)
    for d in (DATA, SNAPS, RESULTS, EXPECTED):
        d.mkdir(parents=True, exist_ok=True)

    rng = random.Random(550)
    directory = Directory(DB_PATH)

    # Per-table generator state.
    live = {t: {} for t in TABLES}                 # rid tuple -> value (committed view)
    ledger = {t: [] for t in TABLES}               # every physical op, for independent replay
    manifest_deltas = {t: [] for t in TABLES}      # delta entries in creation order
    statement_counter = {t: 0 for t in TABLES}
    file_seq = {t: 0 for t in TABLES}
    # Per (table, kind) buffered file: records plus the write-id interval it spans.
    buffers = {t: {} for t in TABLES}
    file_count = 0
    op_count = {t: 0 for t in TABLES}
    MAX_RECORDS_PER_FILE = 200
    open_txn_info = None
    s1_meta = None

    def _flush(table, kind):
        """Write one buffered physical file covering a closed write-id interval."""
        nonlocal file_count
        buf = buffers[table].pop(kind, None)
        if not buf or not buf["records"]:
            return
        statement_counter[table] += 1
        file_seq[table] += 1
        stmt = statement_counter[table]
        seq = file_seq[table]
        rel = f"{table}/f{seq:03d}_s{stmt:03d}_{kind}.jsonl"
        write_jsonl(DATA / rel, buf["records"])
        file_count += 1
        manifest_deltas[table].append({
            "path": rel, "kind": kind, "statement_id": stmt,
            "min_write_id": buf["min_wid"], "max_write_id": buf["max_wid"],
        })
        # Promote provisional statement ids / source locations on ledger entries.
        for ledger_rec, lineno in buf["ledger_meta"]:
            ledger_rec["statement_id"] = stmt
            ledger_rec["source_file"] = rel
            ledger_rec["source_line"] = lineno

    def _flush_all(table):
        for kind in sorted(buffers[table]):
            _flush(table, kind)

    def emit(table, txn_id, wid, groups):
        """Append groups to per-kind physical files, packing many write ids.

        A physical file may span several write ids (its declared [min, max]
        covers all of them); operation_no restarts per write id, while the
        (current_write_id, statement_id, operation_no) identity stays unique.
        """
        for kind, rows in groups:
            if not rows:
                continue
            buf = buffers[table].get(kind)
            if buf is None or \
                    len(buf["records"]) + len(rows) > MAX_RECORDS_PER_FILE:
                if buf is not None:
                    _flush(table, kind)
                buf = {"records": [], "min_wid": wid, "max_wid": wid,
                       "ledger_meta": []}
                buffers[table][kind] = buf
            physical = []
            for op_no, row in enumerate(rows):
                record = dict(row)
                record["operation_no"] = op_no
                physical.append(record)
                op_count[table] += 1
                lineno = len(buf["records"]) + op_no + 1
                ledger_rec = {
                    "current_write_id": record["current_write_id"],
                    "original_write_id": record["original_write_id"],
                    "bucket_id": record["bucket_id"],
                    "row_id": record["row_id"],
                    "operation_no": op_no,
                    # Provisional id; replaced with the real statement id at flush.
                    "statement_id": -1,
                    "kind": kind,
                    **({"value": record["value"]} if kind == "insert" else {}),
                    "source_file": None,
                    "source_line": lineno,
                }
                buf["ledger_meta"].append((ledger_rec, lineno))
                ledger[table].append(ledger_rec)
            buf["records"].extend(physical)
            buf["min_wid"] = min(buf["min_wid"], wid)
            buf["max_wid"] = max(buf["max_wid"], wid)

    def insert_rows(table, txn_id, wid, n):
        rows = []
        for row_id in range(n):
            rows.append({
                "current_write_id": wid, "original_write_id": wid,
                "bucket_id": rng.randrange(16), "row_id": row_id,
                "value": rng.randint(1, 1_000_000),
            })
        return rows

    # ----------------------------------------------------- forced txns 1..4
    # txn 1: measurements w1 - two inserts, including (w1, bucket 3, row 0).
    t1 = directory.begin()
    w1 = directory.allocate(t1, "measurements")
    forced_a = {"current_write_id": w1, "original_write_id": w1,
                "bucket_id": 3, "row_id": 0, "value": 100}
    forced_b = {"current_write_id": w1, "original_write_id": w1,
                "bucket_id": 7, "row_id": 1, "value": 101}
    emit("measurements", t1, w1, [("insert", [forced_a, forced_b])])
    for r in (forced_a, forced_b):
        live["measurements"][(r["original_write_id"], r["bucket_id"], r["row_id"])] = r["value"]
    directory.finish(t1, "committed")

    # txn 2: measurements w2 - business update on row_id 0 (delete old RID,
    # insert a brand-new RID sharing row_id 0) plus a parallel row_id 0 in
    # another bucket, so two live rows share row_id with different originals.
    t2 = directory.begin()
    w2 = directory.allocate(t2, "measurements")
    upd_new = {"current_write_id": w2, "original_write_id": w2,
               "bucket_id": 3, "row_id": 0, "value": 200}
    parallel = {"current_write_id": w2, "original_write_id": w2,
                "bucket_id": 9, "row_id": 0, "value": 201}
    emit("measurements", t2, w2, [
        ("delete", [{"current_write_id": w2, "original_write_id": w1,
                     "bucket_id": 3, "row_id": 0}]),
        ("insert", [upd_new, parallel]),
    ])
    del live["measurements"][(w1, 3, 0)]
    for r in (upd_new, parallel):
        live["measurements"][(r["original_write_id"], r["bucket_id"], r["row_id"])] = r["value"]
    directory.finish(t2, "committed")

    # txn 3: measurements w3 - ABORTED delete of a live RID; the row survives.
    t3 = directory.begin()
    w3 = directory.allocate(t3, "measurements")
    aborted_target = (w1, 7, 1)
    emit("measurements", t3, w3, [
        ("delete", [{"current_write_id": w3, "original_write_id": aborted_target[0],
                     "bucket_id": aborted_target[1], "row_id": aborted_target[2]}])
    ])
    directory.finish(t3, "aborted")
    assert aborted_target in live["measurements"]

    # txn 4: measurements w4 - committed delete removes exactly one RID.
    t4 = directory.begin()
    w4 = directory.allocate(t4, "measurements")
    emit("measurements", t4, w4, [
        ("delete", [{"current_write_id": w4, "original_write_id": w2,
                     "bucket_id": 9, "row_id": 0}])
    ])
    del live["measurements"][(w2, 9, 0)]
    directory.finish(t4, "committed")

    # A forced multi-table transaction (separate counters per table).
    t5 = directory.begin()
    wm = directory.allocate(t5, "measurements")
    we = directory.allocate(t5, "events")
    emit("measurements", t5, wm, [("insert", insert_rows("measurements", t5, wm, 2))])
    emit("events", t5, we, [("insert", insert_rows("events", t5, we, 3))])
    directory.finish(t5, "committed")

    def rebuild_committed_live(table, up_to_wid):
        """Rebuild the committed live view from scratch via ledger replay.

        This is the single source of truth for delete-target selection:
        aborted/open write ids are filtered using the directory's own
        snapshot, so rows inserted by a write that never commits can never
        appear as a valid delete target.
        """
        invalid = set(directory.snapshot(table)["invalid_write_ids"])
        view = {}
        ops = [
            (idx, op) for idx, op in enumerate(ledger[table])
            if op["current_write_id"] <= up_to_wid
            and op["current_write_id"] not in invalid
        ]
        ops.sort(key=lambda pair: (pair[1]["current_write_id"],
                                   pair[1]["statement_id"], pair[0]))
        for _, op in ops:
            rid = (op["original_write_id"], op["bucket_id"], op["row_id"])
            if op["kind"] == "insert":
                view[rid] = op["value"]
            else:
                view.pop(rid, None)
        live[table] = view

    def apply_committed(table, up_to_wid):
        """Committed-tx hook: rebuild the table's committed view."""
        rebuild_committed_live(table, up_to_wid)

    apply_committed("measurements", wm)
    apply_committed("events", we)

    # ----------------------------------------------------- generic txns 6..100
    for txn_id in range(6, TXN_COUNT + 1):
        txn = directory.begin()
        is_open_special = txn_id == OPEN_TXN
        if is_open_special:
            choices = ["measurements"]
        elif rng.random() < EMPTY_TXN_RATE:
            choices = []                      # a transaction may write no table
        else:
            k = 2 if txn_id in (12, 47, 73, 88) else 1
            choices = rng.sample(TABLES, k)
        aborted = (not is_open_special) and rng.random() < ABORT_RATE
        wid_by_table = {}
        for table in choices:
            wid = directory.allocate(txn, table)
            wid_by_table[table] = wid
            n_ins = rng.randint(8, 20)
            ins = insert_rows(table, txn, wid, n_ins)
            dels = []
            # Only writes that will be committed may delete committed rows;
            # an aborted/open delete physically present but filtered out by
            # every snapshot would target a row with no visible tombstone.
            if not aborted and not is_open_special and live[table] and rng.random() < 0.7:
                n_del = min(rng.randint(1, 6), len(live[table]))
                for target in rng.sample(sorted(live[table]), n_del):
                    dels.append({"current_write_id": wid, "original_write_id": target[0],
                                 "bucket_id": target[1], "row_id": target[2]})
            groups = []
            if dels:
                groups.append(("delete", dels))
            groups.append(("insert", ins))
            emit(table, txn, wid, groups)
            if is_open_special:
                open_txn_info = {"txn_id": txn, "table": table, "write_id": wid}
        if is_open_special:
            # S1 cut: the measurements write above stays OPEN. All earlier
            # physical groups must be flushed into closed files first.
            _flush_all("measurements")
            s1_snapshot = directory.snapshot("measurements")
            directory.export_snapshot("measurements", SNAPS / "measurements_s1.json")
            b1_boundary = open_txn_info["write_id"] - 1
            s1_meta = {"open_txn": open_txn_info, "b1_boundary": b1_boundary,
                       "s1_high_watermark": s1_snapshot["high_write_id"]}
            (EXPECTED / "s1_meta.json").write_text(
                json.dumps(s1_meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            # B1: independently replay the finalized committed ledger up to
            # the boundary; the open write is excluded by the snapshot itself.
            s1_invalid = set(s1_snapshot["invalid_write_ids"])
            b1_live = {}
            for op in sorted(
                (o for o in ledger["measurements"]
                 if o["current_write_id"] <= b1_boundary
                 and o["current_write_id"] not in s1_invalid),
                key=lambda o: (o["current_write_id"], o["statement_id"], o["operation_no"]),
            ):
                rid = (op["original_write_id"], op["bucket_id"], op["row_id"])
                if op["kind"] == "insert":
                    b1_live[rid] = op["value"]
                else:
                    b1_live.pop(rid, None)
            b1_path = DATA / "bases" / "measurements_b1.jsonl"
            write_jsonl(b1_path, [
                {"original_write_id": rid[0], "bucket_id": rid[1],
                 "row_id": rid[2], "value": value}
                for rid, value in sorted(b1_live.items())
            ])
            file_count += 1
            continue
        directory.finish(txn, "aborted" if aborted else "committed")
        if not aborted:
            for table in choices:
                apply_committed(table, wid_by_table[table])

    # The formerly-open transaction commits *after* the S1 cut.
    directory.finish(open_txn_info["txn_id"], "committed")
    open_wid = open_txn_info["write_id"]
    apply_committed("measurements", open_wid)

    # Every remaining buffered physical group must land before manifests exist.
    for table in TABLES:
        _flush_all(table)

    # Single source of truth: rebuild every committed view from the finalized
    # ledger + directory terminal states (independent of any Reader code).
    invalid_by_table = {
        table: set(directory.snapshot(table)["invalid_write_ids"])
        for table in TABLES
    }

    def replay_view(table, boundary):
        view = {}
        for op in sorted(
            (o for o in ledger[table]
             if o["current_write_id"] <= boundary
             and o["current_write_id"] not in invalid_by_table[table]),
            key=lambda o: (o["current_write_id"], o["statement_id"], o["operation_no"]),
        ):
            rid = (op["original_write_id"], op["bucket_id"], op["row_id"])
            if op["kind"] == "insert":
                view[rid] = op["value"]
            else:
                view.pop(rid, None)
        return view

    final_snapshots = {t: directory.snapshot(t) for t in TABLES}
    for table in TABLES:
        live[table] = replay_view(table, final_snapshots[table]["high_write_id"])

    # Midpoint bases; eligibility/visibility is derived from the directory,
    # so aborted/open write ids never enter a base despite physical rows.
    mid_bases = {}
    for table in TABLES:
        wids = sorted({r["current_write_id"] for r in ledger[table]
                       if r["current_write_id"] not in invalid_by_table[table]})
        if table == "measurements":
            mid_bases[table] = {"path": "bases/measurements_b1.jsonl",
                                "base_write_id": s1_meta["b1_boundary"]}
            continue
        if not wids:
            continue
        boundary = wids[len(wids) // 2]
        path = DATA / "bases" / f"{table}_mid.jsonl"
        sub_live = replay_view(table, boundary)
        write_jsonl(path, [
            {"original_write_id": rid[0], "bucket_id": rid[1],
             "row_id": rid[2], "value": value}
            for rid, value in sorted(sub_live.items())
        ])
        file_count += 1
        mid_bases[table] = {"path": f"bases/{table}_mid.jsonl", "base_write_id": boundary}

    # B2 for measurements: finalized-only base that embeds the formerly-open write.
    b2_boundary = final_snapshots["measurements"]["high_write_id"]
    b2_path = DATA / "bases" / "measurements_b2.jsonl"
    write_jsonl(b2_path, [
        {"original_write_id": rid[0], "bucket_id": rid[1],
         "row_id": rid[2], "value": value}
        for rid, value in sorted(live["measurements"].items())
    ])
    file_count += 1
    for table, snap in final_snapshots.items():
        directory.export_snapshot(table, SNAPS / f"{table}_final.json")

    # --------------------------------------------------------------- manifests
    shuffled_deltas = {}
    for table in TABLES:
        entries = [dict(e) for e in manifest_deltas[table]]
        rng.shuffle(entries)                 # prove file enumeration order is irrelevant
        shuffled_deltas[table] = entries

    for table in TABLES:
        bases = []
        if table in mid_bases and mid_bases[table]["base_write_id"] >= 1:
            bases.append(mid_bases[table])
        if table == "measurements":
            bases.append({"path": "bases/measurements_b2.jsonl",
                          "base_write_id": b2_boundary})
        (DATA / f"{table}_manifest.json").write_text(json.dumps(
            {"table": table, "bases": bases, "deltas": shuffled_deltas[table]},
            indent=2, sort_keys=True) + "\n", encoding="utf-8")
        # Replay-only manifest: no bases at all, full delta family.
        (DATA / f"{table}_manifest_replay.json").write_text(json.dumps(
            {"table": table, "bases": [], "deltas": shuffled_deltas[table]},
            indent=2, sort_keys=True) + "\n", encoding="utf-8")

    # S1 manifests for measurements: B1 eligible, B2 embeds the open write.
    s1_deltas = shuffled_deltas["measurements"]
    (DATA / "measurements_manifest_s1.json").write_text(json.dumps(
        {"table": "measurements",
         "bases": [mid_bases["measurements"],
                   {"path": "bases/measurements_b2.jsonl", "base_write_id": b2_boundary}],
         "deltas": s1_deltas}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (DATA / "measurements_manifest_s1_b2only.json").write_text(json.dumps(
        {"table": "measurements",
         "bases": [{"path": "bases/measurements_b2.jsonl", "base_write_id": b2_boundary}],
         "deltas": s1_deltas}, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    # Independent per-operation ledgers (not produced by the Reader).
    for table in TABLES:
        write_jsonl(EXPECTED / f"{table}_operations.jsonl", ledger[table])

    summary = {
        "db": str(DB_PATH.relative_to(ROOT)),
        "tables": TABLES,
        "txn_count": TXN_COUNT,
        "jsonl_files": file_count,
        "physical_ops_per_table": op_count,
        "final_live_rows_per_table": {t: len(live[t]) for t in TABLES},
        "s1": s1_meta,
        "b2_boundary": b2_boundary,
    }
    (OUT / "generation_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    directory.close()

    assert file_count <= 100, f"file budget exceeded: {file_count}"
    assert all(n <= 1000 for n in op_count.values()), op_count
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
