#!/usr/bin/env python3
"""Refresh provenance metadata in an existing normalized warehouse.

This operation changes only the ``warehouse_metadata`` table. It is useful
when the immutable raw archives and normalized rows are unchanged but the
research-assumption/reference registries have been extended. The update is a
single SQLite transaction and validates the database before and after it.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any


UTC = dt.timezone.utc


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--knowledge-base", type=Path, required=True)
    parser.add_argument("--assumptions", type=Path, required=True)
    return parser.parse_args()


def refresh(
    warehouse_path: Path,
    manifest_path: Path,
    knowledge_base_path: Path,
    assumptions_path: Path,
) -> dict[str, Any]:
    warehouse_path = warehouse_path.expanduser().resolve()
    manifest_path = manifest_path.expanduser().resolve()
    knowledge_base_path = knowledge_base_path.expanduser().resolve()
    assumptions_path = assumptions_path.expanduser().resolve()
    manifest = load_json(manifest_path)
    knowledge_base = load_json(knowledge_base_path)
    assumptions = load_json(assumptions_path)
    if manifest.get("verification_status") != "complete" or manifest.get("error_count", 0):
        raise ValueError("source retrieval manifest is not complete")
    if not knowledge_base.get("references"):
        raise ValueError("knowledge-base registry is empty")
    if not assumptions.get("assumptions"):
        raise ValueError("research assumptions registry is empty")
    if not warehouse_path.is_file():
        raise FileNotFoundError(warehouse_path)

    knowledge_base_hash = sha256_file(knowledge_base_path)
    assumptions_hash = sha256_file(assumptions_path)
    manifest_hash = sha256_file(manifest_path)
    refreshed_at = dt.datetime.now(UTC).isoformat()

    connection = sqlite3.connect(warehouse_path)
    try:
        integrity_before = connection.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity_before != "ok":
            raise RuntimeError(f"warehouse integrity check failed before refresh: {integrity_before}")
        row_count = int(connection.execute("SELECT COUNT(*) FROM klines").fetchone()[0])
        archive_count = int(connection.execute("SELECT COUNT(*) FROM source_archives").fetchone()[0])
        if archive_count != int(manifest["job_count"]):
            raise ValueError(
                f"warehouse archive count {archive_count} does not match manifest {manifest['job_count']}"
            )

        values = {
            "provenance_refresh_version": "warehouse-metadata-v1",
            "provenance_refreshed_at": refreshed_at,
            "source_manifest_path": str(manifest_path),
            "source_manifest_sha256": manifest_hash,
            "knowledge_base_registry": str(knowledge_base_path),
            "knowledge_base_references": knowledge_base["references"],
            "knowledge_base_sha256": knowledge_base_hash,
            "research_assumptions_path": str(assumptions_path),
            "research_assumptions": assumptions["assumptions"],
            "research_assumptions_sha256": assumptions_hash,
            "raw_data_modified": False,
        }
        connection.execute("BEGIN IMMEDIATE")
        connection.executemany(
            "INSERT INTO warehouse_metadata (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [
                (
                    key,
                    json.dumps(value, sort_keys=True) if not isinstance(value, str) else value,
                )
                for key, value in values.items()
            ],
        )
        connection.commit()
        integrity_after = connection.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity_after != "ok":
            raise RuntimeError(f"warehouse integrity check failed after refresh: {integrity_after}")
        return {
            "warehouse": str(warehouse_path),
            "row_count": row_count,
            "archive_count": archive_count,
            "manifest_sha256": manifest_hash,
            "knowledge_base_sha256": knowledge_base_hash,
            "assumptions_sha256": assumptions_hash,
            "integrity": integrity_after,
            "refreshed_at": refreshed_at,
        }
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def main() -> int:
    args = parse_args()
    try:
        result = refresh(args.warehouse, args.manifest, args.knowledge_base, args.assumptions)
    except Exception as exc:
        print(f"warehouse metadata refresh failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
