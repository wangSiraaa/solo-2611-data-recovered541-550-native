"""Read bucketed physical rows (JSONL) against a valid write-id snapshot.

Row identity is the full composite RID ``(original_write_id, bucket_id,
row_id)`` - never row_id alone or (bucket, row).  Visibility is decided by
the *current* write id of each operation against the snapshot; a delete is
never validated through its target's original write id.

A scan either returns a complete, sorted dataset or raises :class:`ScanError`;
a failed read never produces a ``complete`` dataset.
"""
from __future__ import annotations

import json
from pathlib import Path

SNAPSHOT_FORMAT = "txn-snapshot/v1"
BUCKET_MIN, BUCKET_MAX = 0, 15
VALID_KINDS = ("insert", "delete")


class ScanError(ValueError):
    """Raised when a snapshot, manifest or physical file makes the read invalid."""


class ScanResult(dict):
    """Plain mapping with attribute access (still JSON serialisable)."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as exc:  # pragma: no cover - trivial
            raise AttributeError(name) from exc


def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


# ---------------------------------------------------------------- validation
def _validate_snapshot(snapshot):
    if not isinstance(snapshot, dict):
        raise ScanError("snapshot must be a JSON object")
    if snapshot.get("format") != SNAPSHOT_FORMAT:
        raise ScanError(f"snapshot format must be {SNAPSHOT_FORMAT!r}")
    table = snapshot.get("table")
    if not isinstance(table, str) or table == "":
        raise ScanError("snapshot table must be a non-empty string")

    def ids(name, *, non_negative):
        value = snapshot.get(name)
        if not isinstance(value, list) or not all(_is_int(v) for v in value):
            raise ScanError(f"{name} must be a list of non-boolean integers")
        if non_negative and any(v < 0 for v in value):
            raise ScanError(f"{name} contains a negative write id")
        if len(set(value)) != len(value):
            raise ScanError(f"{name} contains duplicate write ids")
        return list(value)

    open_ids = ids("open_write_ids", non_negative=True)
    aborted_ids = ids("aborted_write_ids", non_negative=True)
    invalid_ids = ids("invalid_write_ids", non_negative=True)
    high = snapshot.get("high_write_id")
    txn_high = snapshot.get("txn_high_watermark")
    if not _is_int(high) or high < 0:
        raise ScanError("high_write_id must be a non-negative integer")
    if not _is_int(txn_high) or txn_high < 0:
        raise ScanError("txn_high_watermark must be a non-negative integer")
    if any(v > high for v in invalid_ids):
        raise ScanError("invalid_write_ids exceeds the table high watermark")
    if any(v > high for v in open_ids) or any(v > high for v in aborted_ids):
        raise ScanError("open/aborted write id exceeds the table high watermark")
    overlap = set(open_ids) & set(aborted_ids)
    if overlap:
        raise ScanError(f"open and aborted write ids overlap: {sorted(overlap)}")
    if set(invalid_ids) != set(open_ids) | set(aborted_ids):
        raise ScanError("invalid_write_ids must equal open union aborted")
    return table, high, set(invalid_ids), set(open_ids)


def _safe_path(root: Path, raw_path):
    """Only ordinary relative paths inside the input root are accepted."""
    if not isinstance(raw_path, str) or raw_path == "":
        raise ScanError("manifest path must be a non-empty string")
    p = Path(raw_path)
    if p.is_absolute() or ".." in p.parts or p.is_absolute():
        raise ScanError(f"path escapes the input root or is absolute: {raw_path!r}")
    full = (root / p).resolve()
    root_resolved = root.resolve()
    if full != root_resolved and root_resolved not in full.parents:
        raise ScanError(f"path escapes the input root: {raw_path!r}")
    if not full.is_file():
        raise ScanError(f"manifest path is not an existing regular file: {raw_path!r}")
    return full


def _validate_manifest(manifest, expected_table):
    if not isinstance(manifest, dict):
        raise ScanError("manifest must be a JSON object")
    if manifest.get("table") != expected_table:
        raise ScanError(
            f"manifest table {manifest.get('table')!r} does not match snapshot table {expected_table!r}"
        )
    bases = manifest.get("bases")
    deltas = manifest.get("deltas")
    if not isinstance(bases, list) or not isinstance(deltas, list):
        raise ScanError("manifest bases and deltas must be lists")
    seen_paths = set()
    for item in bases:
        if not isinstance(item, dict):
            raise ScanError("base entry must be an object")
        if not _is_int(item.get("base_write_id")) or item["base_write_id"] < 1:
            raise ScanError("base_write_id must be a positive integer")
        path = item.get("path")
        if path in seen_paths:
            raise ScanError(f"duplicate manifest path: {path!r}")
        seen_paths.add(path)
    for item in deltas:
        if not isinstance(item, dict):
            raise ScanError("delta entry must be an object")
        if item.get("kind") not in VALID_KINDS:
            raise ScanError("delta kind must be 'insert' or 'delete'")
        lo, hi = item.get("min_write_id"), item.get("max_write_id")
        if not _is_int(lo) or not _is_int(hi) or lo < 1 or hi < lo:
            raise ScanError("delta write range must be positive with min <= max")
        if not _is_int(item.get("statement_id")) or item["statement_id"] < 0:
            raise ScanError("statement_id must be a non-negative integer")
        path = item.get("path")
        if path in seen_paths:
            raise ScanError(f"duplicate manifest path: {path!r}")
        seen_paths.add(path)
    return bases, deltas


def _parse_line(line, lineno, source, kind):
    try:
        row = json.loads(line)
    except json.JSONDecodeError as exc:
        raise ScanError(f"{source}:{lineno}: invalid JSON ({exc})") from exc
    if not isinstance(row, dict):
        raise ScanError(f"{source}:{lineno}: physical line must be a JSON object")
    required = ("current_write_id", "original_write_id", "bucket_id", "row_id",
                "operation_no")
    for field in required:
        if not _is_int(row.get(field)):
            raise ScanError(f"{source}:{lineno}: field {field!r} must be a non-boolean integer")
    if row["current_write_id"] < 1 or row["original_write_id"] < 1:
        raise ScanError(f"{source}:{lineno}: write ids must be positive integers")
    if not (BUCKET_MIN <= row["bucket_id"] <= BUCKET_MAX):
        raise ScanError(f"{source}:{lineno}: bucket_id must be within 0..15")
    if row["row_id"] < 0 or row["operation_no"] < 0:
        raise ScanError(f"{source}:{lineno}: row_id/operation_no must be non-negative")
    if kind == "insert":
        if row["original_write_id"] != row["current_write_id"]:
            raise ScanError(
                f"{source}:{lineno}: insert original_write_id must equal current_write_id"
            )
        if not _is_int(row.get("value")):
            raise ScanError(f"{source}:{lineno}: insert row needs an integer value")
        row["operation"] = "insert"
    else:
        if "value" in row:
            raise ScanError(f"{source}:{lineno}: delete row must not carry a value")
        row["operation"] = "delete"
    row["_source_file"] = str(source)
    row["_source_line"] = lineno
    row["_statement_id"] = None  # filled by caller
    return row


class Reader:
    def __init__(self, root):
        self.root = Path(root)
        if not self.root.is_dir():
            raise ScanError(f"reader root is not a directory: {self.root}")

    # ------------------------------------------------------------------ scan
    def scan(self, directory, table, manifest):
        """Take a fresh snapshot from a Directory and scan against it."""
        return self.scan_snapshot(directory.snapshot(table), manifest)

    def scan_snapshot(self, snapshot, manifest):
        table, high_watermark, invalid, open_ids = _validate_snapshot(snapshot)
        bases, deltas = _validate_manifest(manifest, table)
        visible = {
            wid
            for wid in range(1, high_watermark + 1)
            if wid not in invalid
        }

        base_item, base_rows = self._load_base(
            bases, high_watermark, open_ids, manifest
        )
        operations = self._load_delta_operations(deltas, visible, manifest)
        rows, applied = self._apply(
            base_rows, operations, high_watermark, base_item, snapshot
        )
        return ScanResult(
            status="complete",
            table=table,
            snapshot=snapshot,
            base_used=base_item,
            delta_operations_applied=applied,
            row_count=len(rows),
            rows=rows,
        )

    # ------------------------------------------------------------- internals
    def _load_base(self, bases, high_watermark, open_ids, manifest):
        """Pick the highest *eligible* base; anything else is rejected.

        Eligible means base_write_id <= table high watermark and no open
        write id of this snapshot is <= the base boundary.  Bases that embed
        an open (at snapshot time) write are never used, even if that
        transaction was finalized later.
        """
        if not bases:
            return None, {}
        eligible = []
        for item in bases:
            boundary = item["base_write_id"]
            if boundary > high_watermark:
                continue
            if any(open_wid <= boundary for open_wid in open_ids):
                continue
            eligible.append(item)
        if not eligible:
            return None, {}
        chosen = max(eligible, key=lambda item: item["base_write_id"])
        full = _safe_path(self.root, chosen["path"])
        live = {}
        with full.open(encoding="utf-8") as stream:
            for lineno, line in enumerate(stream, 1):
                if not line.strip():
                    raise ScanError(f"{chosen['path']}:{lineno}: empty line in base")
                row = json.loads(line)
                if not isinstance(row, dict) or not _is_int(row.get("value")):
                    raise ScanError(
                        f"{chosen['path']}:{lineno}: base row must be an object with integer value"
                    )
                rid = (row.get("original_write_id"), row.get("bucket_id"),
                       row.get("row_id"))
                if not all(_is_int(part) and not isinstance(part, bool) for part in rid):
                    raise ScanError(f"{chosen['path']}:{lineno}: invalid full RID in base")
                if rid[0] < 1 or rid[1] < 0 or rid[2] < 0:
                    raise ScanError(f"{chosen['path']}:{lineno}: invalid full RID in base")
                if not (BUCKET_MIN <= rid[1] <= BUCKET_MAX):
                    raise ScanError(f"{chosen['path']}:{lineno}: bucket_id outside 0..15")
                if rid in live:
                    raise ScanError(f"{chosen['path']}:{lineno}: duplicate RID in base")
                live[rid] = {
                    "original_write_id": rid[0],
                    "bucket_id": rid[1],
                    "row_id": rid[2],
                    "value": row["value"],
                    "source_file": chosen["path"],
                    "source_line": lineno,
                    "provenance": "base",
                }
        return chosen, live

    def _load_delta_operations(self, deltas, visible_wids, manifest):
        operations = []
        seen_identities = set()
        seen_paths = set()
        for item in deltas:
            full = _safe_path(self.root, item["path"])
            if full in seen_paths:
                raise ScanError(f"duplicate manifest path: {item['path']!r}")
            seen_paths.add(full)
            kind, lo, hi, statement_id = (
                item["kind"], item["min_write_id"],
                item["max_write_id"], item["statement_id"],
            )
            with full.open(encoding="utf-8") as stream:
                for lineno, line in enumerate(stream, 1):
                    if not line.strip():
                        raise ScanError(f"{item['path']}:{lineno}: empty physical line")
                    row = _parse_line(line, lineno, item["path"], kind)
                    wid = row["current_write_id"]
                    if not (lo <= wid <= hi):
                        raise ScanError(
                            f"{item['path']}:{lineno}: current_write_id {wid} outside declared "
                            f"range [{lo}, {hi}]"
                        )
                    if wid not in visible_wids:
                        continue  # open/aborted/above-watermark: physically present but invisible
                    identity = (wid, statement_id, row["operation_no"])
                    if identity in seen_identities:
                        raise ScanError(
                            f"{item['path']}:{lineno}: duplicate operation identity {identity}"
                        )
                    seen_identities.add(identity)
                    row["_statement_id"] = statement_id
                    operations.append(row)
        return operations

    def _apply(self, live, operations, high_watermark, base_item, snapshot):
        # File enumeration order never decides the result: order by the
        # declared operation identity (current_write_id, statement_id,
        # operation_no) before applying anything.
        operations.sort(
            key=lambda r: (r["current_write_id"], r["_statement_id"], r["operation_no"])
        )
        applied = 0
        base_boundary = base_item["base_write_id"] if base_item else 0
        for row in operations:
            wid = row["current_write_id"]
            if base_item is not None and wid <= base_boundary:
                # Eligible base already contains every committed operation up
                # to its boundary; replaying one would double-apply a delete.
                continue
            applied += 1
            rid = (row["original_write_id"], row["bucket_id"], row["row_id"])
            if row["operation"] == "insert":
                # The live map is the sole uniqueness judge: a RID that was
                # inserted, deleted and never re-inserted cannot collide, and
                # a second visible insert of a currently live RID is an error.
                if rid in live:
                    raise ScanError(
                        f"duplicate visible insert of live RID {rid} at "
                        f"{row['_source_file']}:{row['_source_line']}"
                    )
                live[rid] = {
                    "current_write_id": wid,
                    "original_write_id": rid[0],
                    "bucket_id": rid[1],
                    "row_id": rid[2],
                    "operation_no": row["operation_no"],
                    "statement_id": row["_statement_id"],
                    "value": row["value"],
                    "source_file": row["_source_file"],
                    "source_line": row["_source_line"],
                    "provenance": "delta",
                }
            else:
                # The delete's validity is judged purely by whether the
                # targeted composite RID is currently live - never by the
                # original_write_id of the delete statement.
                if rid not in live:
                    raise ScanError(
                        f"delete of non-existent visible RID {rid} at "
                        f"{row['_source_file']}:{row['_source_line']}"
                    )
                del live[rid]

        rows = [
            {
                **live[rid],
                "snapshot_table": snapshot["table"],
                "snapshot_high_write_id": high_watermark,
            }
            for rid in sorted(live)
        ]
        return rows, applied

    # -------------------------------------------------------------- helpers
    def export_scan(self, snapshot, manifest, jsonl_path, *, status_path=None):
        """Run a scan and write the complete dataset plus a status sidecar."""
        jsonl_path = Path(jsonl_path)
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            result = self.scan_snapshot(snapshot, manifest)
        except ScanError as exc:
            if status_path is not None:
                status_path = Path(status_path)
                status_path.parent.mkdir(parents=True, exist_ok=True)
                status_path.write_text(
                    json.dumps({"status": "failed", "error": str(exc)},
                               indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
            jsonl_path.write_text("", encoding="utf-8")
            raise
        with jsonl_path.open("w", encoding="utf-8") as stream:
            for row in result["rows"]:
                stream.write(json.dumps(row, sort_keys=True) + "\n")
        status = {
            "status": "complete",
            "table": result["table"],
            "row_count": result["row_count"],
            "base_used": result["base_used"],
            "delta_operations_applied": result["delta_operations_applied"],
            "snapshot": result["snapshot"],
            "output": str(jsonl_path),
        }
        if status_path is not None:
            status_path = Path(status_path)
            status_path.parent.mkdir(parents=True, exist_ok=True)
            status_path.write_text(
                json.dumps(status, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        return status

    def explain_rid(self, snapshot, manifest, rid):
        """Explain one full RID against a snapshot without exporting rows."""
        if (not isinstance(rid, tuple) or len(rid) != 3
                or not all(_is_int(v) and not isinstance(v, bool) for v in rid)):
            raise ScanError("rid must be a (original_write_id, bucket_id, row_id) integer tuple")
        result = self.scan_snapshot(snapshot, manifest)
        for row in result["rows"]:
            candidate = (row["original_write_id"], row["bucket_id"], row["row_id"])
            if candidate == rid:
                return {
                    "rid": list(rid),
                    "present": True,
                    "value": row["value"],
                    "provenance": row["provenance"],
                    "source_file": row["source_file"],
                    "source_line": row["source_line"],
                }
        return {"rid": list(rid), "present": False}

    @staticmethod
    def load_snapshot(path):
        """Read a snapshot JSON produced by any conformant directory."""
        return json.loads(Path(path).read_text(encoding="utf-8"))
