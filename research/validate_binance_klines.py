#!/usr/bin/env python3
"""Validate Binance public kline archives without modifying raw data.

The validator intentionally works on the archive format published by the
binance-public-data repository.  It checks checksums, ZIP integrity, the
12-column kline schema, timestamp units, candle invariants, ordering, and
coverage.  It uses only the Python standard library so it can run before the
research environment is fully provisioned.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import sys
import zipfile
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable


KLINE_COLUMNS = [
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
]

INTERVAL_MS = {
    "1s": 1_000,
    "1m": 60_000,
    "3m": 3 * 60_000,
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "1h": 60 * 60_000,
    "2h": 2 * 60 * 60_000,
    "4h": 4 * 60 * 60_000,
    "6h": 6 * 60 * 60_000,
    "8h": 8 * 60 * 60_000,
    "12h": 12 * 60 * 60_000,
    "1d": 24 * 60 * 60_000,
    "3d": 3 * 24 * 60 * 60_000,
    "1w": 7 * 24 * 60 * 60_000,
}

ARCHIVE_NAME = re.compile(
    r"^(?P<symbol>[^-]+)-(?P<interval>[^-]+)-(?P<period>\d{4}-\d{2}(?:-\d{2})?)\.zip$"
)
CHECKSUM_LINE = re.compile(r"^\s*(?P<digest>[0-9a-fA-F]{64})\s+\*?(?P<name>\S+)\s*$")
HEADER_FIRST_CELL = {"open time", "open_time", "opentime"}
UTC_2025 = datetime(2025, 1, 1, tzinfo=timezone.utc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        required=True,
        help="Directory containing Binance data/spot (or another archive root)",
    )
    parser.add_argument("--output", type=Path, help="Write the JSON manifest to this path")
    parser.add_argument("--start-date", default="2019-01-01", help="Inclusive UTC date")
    parser.add_argument("--end-date", help="Inclusive UTC date; defaults to today")
    parser.add_argument("--symbol", default="BTCUSDT", help="Filter symbol")
    parser.add_argument("--market", default="spot", help="Binance market family")
    parser.add_argument(
        "--require-checksum",
        action="store_true",
        help="Treat a missing or malformed .CHECKSUM file as an error",
    )
    parser.add_argument(
        "--fail-on-error",
        action="store_true",
        help="Exit with status 1 when any archive has an error",
    )
    return parser.parse_args()


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO date: {value}") from exc


def parse_period(value: str) -> tuple[str, date]:
    if len(value) == 7:
        return "monthly", date.fromisoformat(f"{value}-01")
    return "daily", date.fromisoformat(value)


def period_string(period_type: str, period_start: date) -> str:
    if period_type == "monthly":
        return period_start.strftime("%Y-%m")
    return period_start.isoformat()


def iter_dates(start: date, end: date, step: str) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        if step == "daily":
            current += timedelta(days=1)
        else:
            if current.month == 12:
                current = date(current.year + 1, 1, 1)
            else:
                current = date(current.year, current.month + 1, 1)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def archive_metadata(path: Path) -> dict[str, Any] | None:
    match = ARCHIVE_NAME.match(path.name)
    if not match:
        return None
    period_type, period_start = parse_period(match.group("period"))
    return {
        "path": path,
        "symbol": match.group("symbol").upper(),
        "interval": match.group("interval"),
        "period_type": period_type,
        "period_start": period_start,
    }


def checksum_result(path: Path) -> dict[str, Any]:
    checksum_path = path.with_name(f"{path.name}.CHECKSUM")
    result: dict[str, Any] = {
        "present": checksum_path.is_file(),
        "path": str(checksum_path),
    }
    if not checksum_path.is_file():
        return result

    text = checksum_path.read_text(encoding="utf-8", errors="replace").strip()
    match = CHECKSUM_LINE.match(text)
    result["raw"] = text
    if not match:
        result["valid_format"] = False
        return result

    actual = sha256_file(path)
    expected = match.group("digest").lower()
    result.update(
        {
            "valid_format": True,
            "expected_sha256": expected,
            "actual_sha256": actual,
            "filename": match.group("name"),
            "filename_matches": match.group("name") == path.name,
            "matches": actual == expected,
        }
    )
    return result


def timestamp_unit(timestamp: int) -> str:
    # Binance spot data changed from milliseconds to microseconds in 2025.
    return "microseconds" if timestamp >= 100_000_000_000_000 else "milliseconds"


def timestamp_to_datetime(timestamp: int, unit: str) -> datetime:
    seconds = timestamp / (1_000_000 if unit == "microseconds" else 1_000)
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def native_interval(interval: str, unit: str) -> int | None:
    interval_ms = INTERVAL_MS.get(interval)
    if interval_ms is None:
        return None
    return interval_ms * (1_000 if unit == "microseconds" else 1)


def decimal_value(value: str, field: str, errors: list[str]) -> Decimal | None:
    try:
        number = Decimal(value)
    except (InvalidOperation, ValueError):
        errors.append(f"non-numeric {field}: {value!r}")
        return None
    if not number.is_finite():
        errors.append(f"non-finite {field}: {value!r}")
        return None
    return number


def append_limited(items: list[str], value: str, limit: int = 10) -> None:
    if len(items) < limit:
        items.append(value)


def validate_rows(
    stream: io.TextIOBase,
    interval: str,
    expected_period: date,
    period_type: str,
    market: str,
) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    row_count = 0
    malformed_rows = 0
    duplicate_timestamps = 0
    gap_count = 0
    gap_examples: list[str] = []
    close_time_anomaly_count = 0
    close_time_anomaly_examples: list[str] = []
    first_open: int | None = None
    last_open: int | None = None
    first_close: int | None = None
    last_close: int | None = None
    previous_open: int | None = None
    unit: str | None = None
    header_seen = False
    expected_step: int | None = None

    reader = csv.reader(stream)
    for line_number, row in enumerate(reader, start=1):
        if not row or all(not cell.strip() for cell in row):
            continue
        if line_number == 1 and row and row[0].strip().lower() in HEADER_FIRST_CELL:
            header_seen = True
            continue
        if len(row) != len(KLINE_COLUMNS):
            malformed_rows += 1
            append_limited(errors, f"line {line_number}: expected 12 columns, got {len(row)}")
            continue

        try:
            open_time = int(row[0])
            close_time = int(row[6])
            trade_count = int(row[8])
        except ValueError:
            malformed_rows += 1
            append_limited(errors, f"line {line_number}: invalid integer timestamp/trade count")
            continue

        if open_time < 0 or close_time < 0:
            append_limited(errors, f"line {line_number}: negative timestamp")
            continue
        row_unit = timestamp_unit(open_time)
        if unit is None:
            unit = row_unit
            expected_step = native_interval(interval, unit)
        elif row_unit != unit:
            append_limited(errors, f"line {line_number}: timestamp unit changes from {unit} to {row_unit}")

        open_price = decimal_value(row[1], "open", errors)
        high = decimal_value(row[2], "high", errors)
        low = decimal_value(row[3], "low", errors)
        close = decimal_value(row[4], "close", errors)
        volume = decimal_value(row[5], "volume", errors)
        quote_volume = decimal_value(row[7], "quote_asset_volume", errors)
        taker_base = decimal_value(row[9], "taker_buy_base_asset_volume", errors)
        taker_quote = decimal_value(row[10], "taker_buy_quote_asset_volume", errors)

        if open_price is not None and open_price <= 0:
            append_limited(errors, f"line {line_number}: non-positive open price")
        if high is not None and high <= 0:
            append_limited(errors, f"line {line_number}: non-positive high price")
        if low is not None and low <= 0:
            append_limited(errors, f"line {line_number}: non-positive low price")
        if close is not None and close <= 0:
            append_limited(errors, f"line {line_number}: non-positive close price")
        if high is not None and low is not None and high < low:
            append_limited(errors, f"line {line_number}: high below low")
        if open_price is not None and high is not None and open_price > high:
            append_limited(errors, f"line {line_number}: open above high")
        if open_price is not None and low is not None and open_price < low:
            append_limited(errors, f"line {line_number}: open below low")
        if close is not None and high is not None and close > high:
            append_limited(errors, f"line {line_number}: close above high")
        if close is not None and low is not None and close < low:
            append_limited(errors, f"line {line_number}: close below low")
        if volume is not None and volume < 0:
            append_limited(errors, f"line {line_number}: negative volume")
        if quote_volume is not None and quote_volume < 0:
            append_limited(errors, f"line {line_number}: negative quote volume")
        if trade_count < 0:
            append_limited(errors, f"line {line_number}: negative trade count")
        if taker_base is not None and volume is not None and taker_base > volume:
            append_limited(errors, f"line {line_number}: taker base volume exceeds volume")
        if taker_quote is not None and quote_volume is not None and taker_quote > quote_volume:
            append_limited(errors, f"line {line_number}: taker quote volume exceeds quote volume")

        if expected_step is not None:
            expected_close_time = open_time + expected_step - 1
            if close_time < open_time or close_time > expected_close_time:
                append_limited(errors, f"line {line_number}: close time is outside interval bounds")
            elif close_time != expected_close_time:
                # Some archived Binance candles contain an early close time at
                # a source-side gap. Keep the raw record and surface it as a
                # quality warning instead of silently normalizing it.
                close_time_anomaly_count += 1
                append_limited(
                    close_time_anomaly_examples,
                    f"line {line_number}: close {close_time}, expected {expected_close_time}",
                )
            if open_time % expected_step != 0:
                append_limited(errors, f"line {line_number}: open time is not interval-aligned")

        if unit is not None:
            try:
                observed_date = timestamp_to_datetime(open_time, unit)
                if market == "spot":
                    expected_unit = "microseconds" if observed_date >= UTC_2025 else "milliseconds"
                    if unit != expected_unit:
                        append_limited(
                            errors,
                            f"line {line_number}: expected {expected_unit} for {observed_date.date()}, got {unit}",
                        )
            except (OverflowError, OSError, ValueError):
                append_limited(errors, f"line {line_number}: timestamp outside valid UTC range")

        if previous_open is not None and expected_step is not None:
            delta = open_time - previous_open
            if delta == 0:
                duplicate_timestamps += 1
                append_limited(errors, f"line {line_number}: duplicate open timestamp")
            elif delta < 0:
                append_limited(errors, f"line {line_number}: timestamps are not increasing")
            elif delta != expected_step:
                gap_count += 1
                append_limited(
                    gap_examples,
                    f"line {line_number}: delta {delta} (expected {expected_step})",
                )

        if first_open is None:
            first_open = open_time
            first_close = close_time
        last_open = open_time
        last_close = close_time
        previous_open = open_time
        row_count += 1

    if row_count == 0:
        errors.append("archive contains no data rows")

    if unit is None:
        warnings.append("timestamp unit unavailable")
    if period_type == "monthly" and expected_period.day != 1:
        warnings.append("monthly archive period is not the first day of a month")
    if close_time_anomaly_count:
        warnings.append(
            f"{close_time_anomaly_count} candle(s) have an early source close time; raw values preserved"
        )
    if gap_count:
        warnings.append(f"{gap_count} gap(s) detected between consecutive candle open times")

    return {
        "row_count": row_count,
        "header_seen": header_seen,
        "timestamp_unit": unit,
        "first_open_time": first_open,
        "last_open_time": last_open,
        "first_close_time": first_close,
        "last_close_time": last_close,
        "first_open_utc": timestamp_to_datetime(first_open, unit).isoformat() if first_open and unit else None,
        "last_open_utc": timestamp_to_datetime(last_open, unit).isoformat() if last_open and unit else None,
        "duplicate_timestamps": duplicate_timestamps,
        "gap_count": gap_count,
        "gap_examples": gap_examples,
        "close_time_anomaly_count": close_time_anomaly_count,
        "close_time_anomaly_examples": close_time_anomaly_examples,
        "malformed_rows": malformed_rows,
        "errors": errors,
        "warnings": warnings,
    }


def validate_archive(
    path: Path,
    metadata: dict[str, Any],
    market: str,
    require_checksum: bool,
) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    checksum = checksum_result(path)
    if require_checksum and not checksum.get("present"):
        errors.append("checksum file is missing")
    if checksum.get("present") and not checksum.get("valid_format"):
        errors.append("checksum file has an invalid format")
    if checksum.get("present") and checksum.get("valid_format"):
        if not checksum.get("filename_matches"):
            errors.append("checksum filename does not match archive filename")
        if not checksum.get("matches"):
            errors.append("SHA-256 checksum mismatch")

    row_result: dict[str, Any] = {}
    member_names: list[str] = []
    zip_ok = False
    try:
        with zipfile.ZipFile(path) as archive:
            bad_member = archive.testzip()
            if bad_member is not None:
                errors.append(f"ZIP CRC failure in {bad_member}")
            else:
                zip_ok = True
            member_names = archive.namelist()
            csv_members = [name for name in member_names if name.lower().endswith(".csv")]
            if len(csv_members) != 1:
                errors.append(f"expected exactly one CSV member, found {len(csv_members)}")
            elif zip_ok:
                with archive.open(csv_members[0], "r") as binary_stream:
                    with io.TextIOWrapper(binary_stream, encoding="utf-8", newline="") as text_stream:
                        row_result = validate_rows(
                            text_stream,
                            metadata["interval"],
                            metadata["period_start"],
                            metadata["period_type"],
                            market,
                        )
                        errors.extend(row_result.pop("errors", []))
                        warnings.extend(row_result.pop("warnings", []))
    except (OSError, zipfile.BadZipFile, zipfile.LargeZipFile) as exc:
        errors.append(f"cannot read ZIP: {exc}")

    return {
        "archive": path.name,
        "path": str(path),
        "source": "binance-public-data",
        "source_url": (
            f"https://data.binance.vision/data/{market}/{metadata['period_type']}/klines/"
            f"{metadata['symbol']}/{metadata['interval']}/{path.name}"
        ),
        "market": market,
        "symbol": metadata["symbol"],
        "interval": metadata["interval"],
        "period_type": metadata["period_type"],
        "period": period_string(metadata["period_type"], metadata["period_start"]),
        "size_bytes": path.stat().st_size,
        "sha256": checksum.get("actual_sha256") or sha256_file(path),
        "checksum": checksum,
        "zip_ok": zip_ok,
        "zip_members": member_names,
        **row_result,
        "errors": errors,
        "warnings": warnings,
        "status": "error" if errors else "ok",
    }


def coverage_summary(
    records: list[dict[str, Any]],
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        groups[(record["symbol"], record["interval"], record["period_type"])].append(record)

    summaries: list[dict[str, Any]] = []
    for (symbol, interval, period_type), group in sorted(groups.items()):
        expected_start = start_date
        expected_end = end_date
        if period_type == "monthly":
            expected_start = date(start_date.year, start_date.month, 1)
            expected_end = date(end_date.year, end_date.month, 1)
        expected = list(iter_dates(expected_start, expected_end, period_type))
        observed = sorted({date.fromisoformat(record["period"]) if period_type == "daily" else date.fromisoformat(f"{record['period']}-01") for record in group})
        observed_set = set(observed)
        missing = [period_string(period_type, item) for item in expected if item not in observed_set]
        summaries.append(
            {
                "symbol": symbol,
                "interval": interval,
                "period_type": period_type,
                "expected_period_count": len(expected),
                "observed_period_count": len(observed),
                "missing_period_count": len(missing),
                "missing_periods": missing,
                "first_period": period_string(period_type, observed[0]) if observed else None,
                "last_period": period_string(period_type, observed[-1]) if observed else None,
            }
        )
    return summaries


def main() -> int:
    args = parse_args()
    start_date = parse_iso_date(args.start_date)
    end_date = parse_iso_date(args.end_date) if args.end_date else datetime.now(timezone.utc).date()
    if end_date < start_date:
        print("end date must not precede start date", file=sys.stderr)
        return 2

    root = args.root.expanduser().resolve()
    if not root.is_dir():
        print(f"root directory does not exist: {root}", file=sys.stderr)
        return 2

    records: list[dict[str, Any]] = []
    skipped: list[str] = []
    for path in sorted(root.rglob("*.zip")):
        metadata = archive_metadata(path)
        if metadata is None:
            skipped.append(str(path))
            continue
        if metadata["symbol"] != args.symbol.upper():
            continue
        if not (start_date <= metadata["period_start"] <= end_date):
            continue
        records.append(validate_archive(path, metadata, args.market, args.require_checksum))

    error_count = sum(1 for record in records if record["status"] == "error")
    warning_count = sum(len(record.get("warnings", [])) for record in records)
    manifest: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "root": str(root),
        "exchange": "binance",
        "source": "binance-public-data",
        "source_repository": "https://github.com/binance/binance-public-data",
        "data_type": "klines",
        "timezone": "UTC",
        "schema": KLINE_COLUMNS,
        "data_license": "Not stated separately in the archive README; verify current Binance terms before reuse or redistribution.",
        "transformation_version": "validation-v1",
        "raw_data_modified": False,
        "market": args.market,
        "symbol": args.symbol.upper(),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "archive_count": len(records),
        "error_archive_count": error_count,
        "warning_count": warning_count,
        "coverage": coverage_summary(records, start_date, end_date),
        "archives": records,
        "skipped_unrecognized_archives": skipped,
    }

    encoded = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    if args.output:
        output = args.output.expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(encoded, encoding="utf-8")
    else:
        print(encoded, end="")

    print(
        f"validated {len(records)} archives: {error_count} errors, {warning_count} warnings",
        file=sys.stderr,
    )
    if args.fail_on_error and error_count:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
