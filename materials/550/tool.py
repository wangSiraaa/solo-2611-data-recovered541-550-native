"""Command line entry points for the directory and the bucketed reader.

Examples
--------
python3 tool.py status  --db artifacts/txn_directory.sqlite
python3 tool.py snapshot --db artifacts/txn_directory.sqlite --table measurements --out /tmp/s.json
python3 tool.py scan --root artifacts/data --snapshot artifacts/snapshots/measurements_final.json \
                     --manifest artifacts/data/measurements_manifest.json \
                     --out artifacts/results/cli_rows.jsonl
python3 tool.py explain --root artifacts/data --snapshot ... --manifest ... --rid 1,7,1
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from txn_directory import Directory, DirectoryError
from bucket_reader import Reader, ScanError


def _load_manifest(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def main(argv=None):
    parser = argparse.ArgumentParser(description="txn directory / bucketed row reader CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    p_status = sub.add_parser("status", help="directory state check")
    p_status.add_argument("--db", required=True)

    p_snap = sub.add_parser("snapshot", help="take and export a table snapshot")
    p_snap.add_argument("--db", required=True)
    p_snap.add_argument("--table", required=True)
    p_snap.add_argument("--out", required=True)

    p_scan = sub.add_parser("scan", help="physical scan / export")
    p_scan.add_argument("--root", required=True)
    p_scan.add_argument("--snapshot", required=True,
                        help="snapshot JSON from any conformant directory")
    p_scan.add_argument("--manifest", required=True)
    p_scan.add_argument("--out", required=True)
    p_scan.add_argument("--status-out", default=None)

    p_explain = sub.add_parser("explain", help="explain one full RID")
    p_explain.add_argument("--root", required=True)
    p_explain.add_argument("--snapshot", required=True)
    p_explain.add_argument("--manifest", required=True)
    p_explain.add_argument("--rid", required=True,
                           help="original_write_id,bucket_id,row_id")

    args = parser.parse_args(argv)
    try:
        if args.command == "status":
            with Directory(args.db) as directory:
                print(json.dumps(directory.status(), indent=2, sort_keys=True))
            return 0

        if args.command == "snapshot":
            with Directory(args.db) as directory:
                path = directory.export_snapshot(args.table, args.out)
            print(json.dumps({"exported": str(path),
                              "snapshot": json.loads(path.read_text(encoding="utf-8"))},
                             indent=2, sort_keys=True))
            return 0

        reader = Reader(args.root)
        snapshot = reader.load_snapshot(args.snapshot)
        manifest = _load_manifest(args.manifest)

        if args.command == "scan":
            status = reader.export_scan(
                snapshot, manifest, args.out,
                status_path=args.status_out or (str(args.out) + ".status.json"))
            print(json.dumps(status, indent=2, sort_keys=True))
            return 0

        if args.command == "explain":
            try:
                rid = tuple(int(part) for part in args.rid.split(","))
                assert len(rid) == 3
            except (ValueError, AssertionError):
                parser.error("--rid must be original_write_id,bucket_id,row_id")
            print(json.dumps(reader.explain_rid(snapshot, manifest, rid),
                             indent=2, sort_keys=True))
            return 0
    except (DirectoryError, ScanError) as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}), file=sys.stderr)
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
