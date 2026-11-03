#!/usr/bin/env python3
"""Build a canonical, UTC-normalized SQLite warehouse from Binance klines.

Raw ZIP archives are never modified.  The retrieval manifest is the source of
truth for which archives belong to the canonical dataset; valid archives that
exist outside that manifest are intentionally not ingested.  Every normalized
row retains archive-level provenance and a quality classification so research
code can choose how conservative it needs to be.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import math
import os
import sqlite3
import sys
import uuid
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

import download_binance_klines as downloader


KLINE_FIELDS = (
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
)

INTERVAL_US = {
    "1s": 1_000_000,
    "1m": 60_000_000,
    "3m": 180_000_000,
    "5m": 300_000_000,
    "15m": 900_000_000,
    "30m": 1_800_000_000,
    "1h": 3_600_000_000,
    "2h": 7_200_000_000,
    "4h": 14_400_000_000,
    "6h": 21_600_000_000,
    "8h": 28_800_000_000,
    "12h": 43_200_000_000,
    "1d": 86_400_000_000,
    "3d": 259_200_000_000,
    "1w": 604_800_000_000,
}

NUMERIC_FIELDS = {
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_asset_volume",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        required=True,
        help="Canonical Binance retrieval manifest JSON",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Output SQLite database path",
    )
    parser.add_argument("--force", action="store_true", help="Replace an existing output database")
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-archive progress output",
    )
    return parser.parse_args()


def timestamp_to_us(value: int, unit: str) -> int:
    if unit == "microseconds":
        return value
    if unit == "milliseconds":
        return value * 1_000
    raise ValueError(f"unsupported timestamp unit: {unit}")


def timestamp_unit(value: int) -> str:
    return "microseconds" if value >= 100_000_000_000_000 else "milliseconds"


def us_to_iso(value: int | None) -> str | None:
    if value is None or value < 0:
        return None
    seconds, micros = divmod(value, 1_000_000)
    instant = dt.datetime.fromtimestamp(seconds, tz=dt.timezone.utc).replace(microsecond=micros)
    return instant.isoformat()


def parse_period_start(job: dict[str, Any]) -> dt.date:
    period = job["period"]
    if job["period_type"] == "monthly":
        return dt.date.fromisoformat(f"{period}-01")
    return dt.date.fromisoformat(period)


def sort_jobs(jobs: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        jobs,
        key=lambda job: (
            job["interval"],
            parse_period_start(job),
            0 if job["period_type"] == "monthly" else 1,
        ),
    )


def numeric(value: str, field: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"non-numeric {field}: {value!r}") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"non-finite {field}: {value!r}")
    return parsed


def create_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE warehouse_metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE source_archives (
            archive_id INTEGER PRIMARY KEY,
            archive_name TEXT NOT NULL UNIQUE,
            path TEXT NOT NULL,
            source_url TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            checksum_verified INTEGER NOT NULL CHECK (checksum_verified IN (0, 1)),
            exchange TEXT NOT NULL,
            market TEXT NOT NULL,
            symbol TEXT NOT NULL,
            base_asset TEXT,
            quote_currency TEXT,
            interval TEXT NOT NULL,
            period_type TEXT NOT NULL,
            period TEXT NOT NULL,
            manifest_status TEXT NOT NULL,
            row_count INTEGER NOT NULL DEFAULT 0,
            inserted_row_count INTEGER NOT NULL DEFAULT 0,
            invalid_row_count INTEGER NOT NULL DEFAULT 0,
            source_anomaly_row_count INTEGER NOT NULL DEFAULT 0,
            duplicate_row_count INTEGER NOT NULL DEFAULT 0,
            malformed_row_count INTEGER NOT NULL DEFAULT 0,
            first_open_time_us INTEGER,
            last_open_time_us INTEGER
        );

        CREATE TABLE klines (
            row_id INTEGER PRIMARY KEY,
            archive_id INTEGER NOT NULL REFERENCES source_archives (archive_id),
            exchange TEXT NOT NULL,
            market TEXT NOT NULL,
            symbol TEXT NOT NULL,
            base_asset TEXT,
            quote_currency TEXT,
            timestamp_us INTEGER NOT NULL,
            close_time_us INTEGER NOT NULL,
            timestamp_utc TEXT NOT NULL,
            close_time_utc TEXT,
            timestamp_unit TEXT NOT NULL,
            data_type TEXT NOT NULL,
            source TEXT NOT NULL,
            interval TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            quote_asset_volume REAL NOT NULL,
            number_of_trades INTEGER NOT NULL,
            taker_buy_base_asset_volume REAL NOT NULL,
            taker_buy_quote_asset_volume REAL NOT NULL,
            ignore_value REAL NOT NULL,
            quality_status TEXT NOT NULL CHECK (quality_status IN ('ok', 'source_anomaly', 'invalid')),
            quality_flags TEXT NOT NULL,
            UNIQUE (exchange, market, symbol, interval, timestamp_us)
        );

        CREATE INDEX klines_interval_time_idx
            ON klines (symbol, interval, timestamp_us);
        CREATE INDEX klines_quality_idx
            ON klines (quality_status, symbol, interval, timestamp_us);
        CREATE INDEX source_archives_interval_period_idx
            ON source_archives (symbol, interval, period_type, period);

        CREATE VIEW valid_klines AS
            SELECT * FROM klines WHERE quality_status <> 'invalid';
        """
    )


def write_metadata(connection: sqlite3.Connection, metadata: dict[str, Any]) -> None:
    connection.executemany(
        "INSERT INTO warehouse_metadata (key, value) VALUES (?, ?)",
        [(key, json.dumps(value, sort_keys=True) if not isinstance(value, str) else value) for key, value in metadata.items()],
    )


def archive_rows(
    archive: dict[str, Any],
    previous_open_by_interval: dict[str, int],
) -> tuple[list[tuple[Any, ...]], dict[str, Any]]:
    path = Path(archive["path"])
    interval = archive["interval"]
    interval_us = INTERVAL_US.get(interval)
    if interval_us is None:
        raise ValueError(f"unsupported interval in manifest: {interval}")

    rows_to_insert: list[tuple[Any, ...]] = []
    row_count = 0
    malformed_count = 0
    invalid_count = 0
    source_anomaly_count = 0
    first_open: int | None = None
    last_open: int | None = None
    previous_open = previous_open_by_interval.get(interval)

    with zipfile.ZipFile(path) as zipped:
        csv_members = [name for name in zipped.namelist() if name.lower().endswith(".csv")]
        if len(csv_members) != 1:
            raise ValueError(f"{path}: expected one CSV member, found {len(csv_members)}")
        with zipped.open(csv_members[0], "r") as binary_stream:
            text_stream = io.TextIOWrapper(binary_stream, encoding="utf-8", newline="")
            reader = csv.reader(text_stream)
            for line_number, raw_row in enumerate(reader, start=1):
                if not raw_row or all(not cell.strip() for cell in raw_row):
                    continue
                if line_number == 1 and raw_row[0].strip().lower() in {"open time", "open_time", "opentime"}:
                    continue
                row_count += 1
                if len(raw_row) != len(KLINE_FIELDS):
                    malformed_count += 1
                    continue

                flags: list[str] = []
                try:
                    raw_open_time = int(raw_row[0])
                    raw_close_time = int(raw_row[6])
                    trades = int(raw_row[8])
                    values = {field: numeric(raw_row[index], field) for index, field in enumerate(KLINE_FIELDS) if field in NUMERIC_FIELDS}
                except (TypeError, ValueError) as exc:
                    malformed_count += 1
                    if not archive.get("malformed_examples"):
                        archive["malformed_examples"] = []
                    if len(archive["malformed_examples"]) < 10:
                        archive["malformed_examples"].append(f"line {line_number}: {exc}")
                    continue

                unit = timestamp_unit(raw_open_time)
                open_time_us = timestamp_to_us(raw_open_time, unit)
                close_time_us = timestamp_to_us(raw_close_time, unit)
                # Compare UTC dates without relying on the raw unit or a
                # floating-point conversion of the source timestamp.
                observed_dt = dt.datetime.fromtimestamp(open_time_us / 1_000_000, tz=dt.timezone.utc)
                expected_unit = "microseconds" if observed_dt >= dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc) else "milliseconds"
                if unit != expected_unit and archive["market"] == "spot":
                    flags.append("timestamp_unit_transition_mismatch")

                required_price_fields = (values["open"], values["high"], values["low"], values["close"])
                if any(price <= 0 for price in required_price_fields):
                    flags.append("nonpositive_price")
                if values["high"] < values["low"]:
                    flags.append("high_below_low")
                if not (values["low"] <= values["open"] <= values["high"]):
                    flags.append("open_outside_high_low")
                if not (values["low"] <= values["close"] <= values["high"]):
                    flags.append("close_outside_high_low")
                if values["volume"] < 0 or values["quote_asset_volume"] < 0:
                    flags.append("negative_volume")
                if trades < 0:
                    flags.append("negative_trade_count")
                if values["taker_buy_base_asset_volume"] > values["volume"]:
                    flags.append("taker_base_volume_exceeds_total")
                if values["taker_buy_quote_asset_volume"] > values["quote_asset_volume"]:
                    flags.append("taker_quote_volume_exceeds_total")
                if open_time_us <= 0 or close_time_us < 0:
                    flags.append("invalid_timestamp")
                # Validate close times in the source unit first.  Pre-2025
                # spot archives have millisecond precision, so comparing a
                # converted close time to a microsecond boundary would mark
                # every otherwise-valid candle as shortened.
                source_interval = interval_us if unit == "microseconds" else interval_us // 1_000
                if raw_close_time < raw_open_time:
                    flags.append("close_time_before_open_time")
                elif raw_close_time > raw_open_time + source_interval - 1:
                    flags.append("close_time_after_interval")
                elif raw_close_time != raw_open_time + source_interval - 1:
                    flags.append("shortened_source_close_time")
                if previous_open is not None and open_time_us - previous_open != interval_us:
                    flags.append("gap_from_previous_candle")

                fatal = {"invalid_timestamp", "close_time_before_open_time", "close_time_after_interval"}
                if any(flag in fatal for flag in flags):
                    status = "invalid"
                    invalid_count += 1
                elif flags:
                    status = "source_anomaly"
                    source_anomaly_count += 1
                else:
                    status = "ok"

                row_values = (
                    archive["archive_id"],
                    archive["exchange"],
                    archive["market"],
                    archive["symbol"],
                    archive.get("base_asset"),
                    archive.get("quote_currency"),
                    open_time_us,
                    close_time_us,
                    us_to_iso(open_time_us),
                    us_to_iso(close_time_us),
                    unit,
                    "klines",
                    "binance-public-data",
                    interval,
                    values["open"],
                    values["high"],
                    values["low"],
                    values["close"],
                    values["volume"],
                    values["quote_asset_volume"],
                    trades,
                    values["taker_buy_base_asset_volume"],
                    values["taker_buy_quote_asset_volume"],
                    values["ignore"],
                    status,
                    json.dumps(sorted(set(flags)), separators=(",", ":")),
                )
                rows_to_insert.append(row_values)
                previous_open = open_time_us
                if first_open is None:
                    first_open = open_time_us
                last_open = open_time_us

    if previous_open is not None:
        previous_open_by_interval[interval] = previous_open
    return rows_to_insert, {
        "row_count": row_count,
        "inserted_row_count": len(rows_to_insert),
        "invalid_row_count": invalid_count,
        "source_anomaly_row_count": source_anomaly_count,
        "duplicate_row_count": 0,
        "malformed_row_count": malformed_count,
        "first_open_time_us": first_open,
        "last_open_time_us": last_open,
        "malformed_examples": archive.get("malformed_examples", []),
    }


def load_manifest(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("verification_status") != "complete":
        raise ValueError("retrieval manifest is not complete")
    if payload.get("error_count", 0):
        raise ValueError("retrieval manifest contains errors")
    jobs = payload.get("jobs")
    if not isinstance(jobs, list) or not jobs:
        raise ValueError("retrieval manifest contains no jobs")
    return payload


def load_knowledge_base_references(path: Path | None = None) -> dict[str, Any]:
    reference_path = path or Path(__file__).with_name("knowledge_base_references.json")
    if not reference_path.is_file():
        raise FileNotFoundError(f"knowledge-base reference registry is missing: {reference_path}")
    payload = json.loads(reference_path.read_text(encoding="utf-8"))
    if not isinstance(payload.get("references"), list) or not payload["references"]:
        raise ValueError("knowledge-base reference registry contains no references")
    return payload


def load_research_assumptions(path: Path | None = None) -> dict[str, Any]:
    assumptions_path = path or Path(__file__).with_name("research_assumptions.json")
    if not assumptions_path.is_file():
        raise FileNotFoundError(f"research assumptions registry is missing: {assumptions_path}")
    payload = json.loads(assumptions_path.read_text(encoding="utf-8"))
    if not isinstance(payload.get("assumptions"), list) or not payload["assumptions"]:
        raise ValueError("research assumptions registry contains no assumptions")
    return payload


def normalize(manifest_path: Path, output_path: Path, force: bool = False, quiet: bool = False) -> dict[str, Any]:
    manifest = load_manifest(manifest_path)
    knowledge_base = load_knowledge_base_references()
    assumptions_path = Path(__file__).with_name("research_assumptions.json")
    assumptions = load_research_assumptions(assumptions_path)
    output_path = output_path.expanduser().resolve()
    if output_path.exists() and not force:
        raise FileExistsError(f"output exists; pass --force to replace it: {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_name(f".{output_path.name}.{uuid.uuid4().hex}.part")
    jobs = sort_jobs(manifest["jobs"])
    previous_open_by_interval: dict[str, int] = {}
    total_rows = 0
    total_inserted = 0
    total_invalid = 0
    total_anomalies = 0
    total_duplicates = 0
    total_malformed = 0
    archive_errors: list[dict[str, Any]] = []
    started_at = dt.datetime.now(dt.timezone.utc).isoformat()

    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(temporary_path)
        connection.execute("PRAGMA journal_mode = OFF")
        connection.execute("PRAGMA synchronous = OFF")
        create_schema(connection)
        insert_sql = """
            INSERT OR IGNORE INTO klines (
                archive_id, exchange, market, symbol, base_asset, quote_currency,
                timestamp_us, close_time_us, timestamp_utc, close_time_utc,
                timestamp_unit, data_type, source, interval,
                open, high, low, close, volume, quote_asset_volume,
                number_of_trades, taker_buy_base_asset_volume,
                taker_buy_quote_asset_volume, ignore_value, quality_status, quality_flags
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        for index, manifest_job in enumerate(jobs, start=1):
            archive_path = Path(manifest_job["path"])
            checksum_path = archive_path.with_name(f"{archive_path.name}.CHECKSUM")
            verified, reason = downloader.verify_archive(archive_path, checksum_path)
            if not verified:
                archive_errors.append({"archive": manifest_job.get("archive"), "error": reason})
                continue

            archive_record = {
                **manifest_job,
                "archive_id": None,
                "exchange": "binance",
                "market": manifest.get("market", "spot"),
                "symbol": manifest.get("symbol", "BTCUSDT"),
                "base_asset": manifest.get("base_asset", "BTC"),
                "quote_currency": manifest.get("quote_currency", "USDT"),
                "checksum_verified": 1,
                "malformed_examples": [],
            }
            cursor = connection.execute(
                """
                INSERT INTO source_archives (
                    archive_name, path, source_url, sha256, checksum_verified,
                    exchange, market, symbol, base_asset, quote_currency, interval,
                    period_type, period, manifest_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    manifest_job["archive"],
                    str(archive_path),
                    manifest_job["source_url"],
                    manifest_job["sha256"],
                    1,
                    "binance",
                    manifest.get("market", "spot"),
                    manifest.get("symbol", "BTCUSDT"),
                    archive_record["base_asset"],
                    archive_record["quote_currency"],
                    manifest_job["interval"],
                    manifest_job["period_type"],
                    manifest_job["period"],
                    manifest_job["status"],
                ),
            )
            archive_record["archive_id"] = int(cursor.lastrowid)
            try:
                rows, counts = archive_rows(archive_record, previous_open_by_interval)
            except Exception as exc:
                archive_errors.append({"archive": manifest_job.get("archive"), "error": str(exc)})
                continue

            for offset in range(0, len(rows), 5_000):
                batch = rows[offset : offset + 5_000]
                connection.executemany(insert_sql, batch)
            # ``total_changes`` is cumulative; count duplicates with a direct row-count query.
            inserted_count = connection.execute(
                "SELECT COUNT(*) FROM klines WHERE archive_id = ?", (archive_record["archive_id"],)
            ).fetchone()[0]
            duplicate_count = len(rows) - inserted_count
            counts["inserted_row_count"] = inserted_count
            counts["duplicate_row_count"] = duplicate_count
            connection.execute(
                """
                UPDATE source_archives SET
                    row_count = ?, inserted_row_count = ?, invalid_row_count = ?,
                    source_anomaly_row_count = ?, duplicate_row_count = ?,
                    malformed_row_count = ?, first_open_time_us = ?, last_open_time_us = ?
                WHERE archive_id = ?
                """,
                (
                    counts["row_count"],
                    counts["inserted_row_count"],
                    counts["invalid_row_count"],
                    counts["source_anomaly_row_count"],
                    counts["duplicate_row_count"],
                    counts["malformed_row_count"],
                    counts["first_open_time_us"],
                    counts["last_open_time_us"],
                    archive_record["archive_id"],
                ),
            )
            connection.commit()
            total_rows += counts["row_count"]
            total_inserted += counts["inserted_row_count"]
            total_invalid += counts["invalid_row_count"]
            total_anomalies += counts["source_anomaly_row_count"]
            total_duplicates += counts["duplicate_row_count"]
            total_malformed += counts["malformed_row_count"]
            if not quiet:
                print(
                    f"[{index}/{len(jobs)}] {manifest_job['interval']} "
                    f"{manifest_job['period_type']} {manifest_job['period']} "
                    f"rows={counts['inserted_row_count']} status=verified",
                    flush=True,
                )

        metadata = {
            "schema_version": "normalized-kline-v1",
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "started_at": started_at,
            "exchange": manifest.get("exchange", "binance"),
            "source": manifest.get("source", "binance-public-data"),
            "source_repository": manifest.get("source_repository", downloader.SOURCE_REPOSITORY),
            "source_base_url": manifest.get("source_base_url", downloader.BASE_URL),
            "symbol": manifest.get("symbol", "BTCUSDT"),
            "market": manifest.get("market", "spot"),
            "base_asset": manifest.get("base_asset", "BTC"),
            "quote_currency": manifest.get("quote_currency", "USDT"),
            "timezone": "UTC",
            "timestamp_storage": "integer microseconds since Unix epoch",
            "numeric_storage": "SQLite REAL (raw decimal strings remain in source archives)",
            "data_type": "klines",
            "intervals": manifest.get("intervals", []),
            "manifest_path": str(manifest_path.expanduser().resolve()),
            "knowledge_base_registry": str(Path(__file__).with_name("knowledge_base_references.json").resolve()),
            "knowledge_base_references": knowledge_base["references"],
            "knowledge_base_sha256": downloader.sha256_file(
                Path(__file__).with_name("knowledge_base_references.json")
            ),
            "research_assumptions_path": str(assumptions_path.resolve()),
            "research_assumptions": assumptions["assumptions"],
            "research_assumptions_sha256": downloader.sha256_file(assumptions_path),
            "archive_count_expected": len(jobs),
            "archive_count_ingested": len(jobs) - len(archive_errors),
            "archive_error_count": len(archive_errors),
            "row_count_source": total_rows,
            "row_count_inserted": total_inserted,
            "invalid_row_count": total_invalid,
            "source_anomaly_row_count": total_anomalies,
            "duplicate_row_count": total_duplicates,
            "malformed_row_count": total_malformed,
            "quality_policy": "Rows with parseable prices and timestamps are retained; quality_status marks source anomalies. Invalid timestamp rows remain auditable but are excluded by valid_klines.",
            "raw_data_modified": False,
            "archive_errors": archive_errors,
        }
        write_metadata(connection, metadata)
        connection.commit()
        connection.close()
        connection = None
        if archive_errors:
            raise RuntimeError(f"{len(archive_errors)} archive(s) failed normalization")
        os.replace(temporary_path, output_path)
        return metadata
    finally:
        if connection is not None:
            connection.close()
        temporary_path.unlink(missing_ok=True)


def main() -> int:
    args = parse_args()
    try:
        metadata = normalize(args.manifest, args.output, args.force, args.quiet)
    except Exception as exc:
        print(f"normalization failed: {exc}", file=sys.stderr)
        return 1
    print(
        "normalized "
        f"{metadata['archive_count_ingested']}/{metadata['archive_count_expected']} archives, "
        f"{metadata['row_count_inserted']} rows, "
        f"{metadata['source_anomaly_row_count']} source-anomaly rows, "
        f"{metadata['invalid_row_count']} invalid rows",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
