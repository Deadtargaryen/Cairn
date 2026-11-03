#!/usr/bin/env python3
"""Resumably download Binance public spot kline archives.

The URL layout and archive names follow the ``binance-public-data``
repository instructions.  Downloads are written atomically, retried with
curl, and accepted only after ZIP and SHA-256 verification.  Existing
archives anywhere below the requested output tree are reused when their
checksum verifies, which makes interrupted runs safe to resume.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import threading
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any


BASE_URL = "https://data.binance.vision/data"
SOURCE_REPOSITORY = "https://github.com/binance/binance-public-data"
DEFAULT_INTERVALS = ("1m", "5m", "15m", "1h", "4h", "1d")
CHECKSUM_RE = re.compile(r"^\s*([0-9a-fA-F]{64})\s+\*?(\S+)\s*$")
KLINE_SCHEMA = (
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
KNOWN_QUOTES = (
    "USDT",
    "FDUSD",
    "USDC",
    "TUSD",
    "BUSD",
    "BTC",
    "ETH",
    "BNB",
    "EUR",
    "GBP",
    "TRY",
    "BRL",
    "AUD",
    "NGN",
)


@dataclass(frozen=True)
class Job:
    period_type: str
    period: date
    interval: str
    symbol: str
    market: str
    output_root: Path

    @property
    def archive_name(self) -> str:
        if self.period_type == "monthly":
            period = self.period.strftime("%Y-%m")
        else:
            period = self.period.isoformat()
        return f"{self.symbol}-{self.interval}-{period}.zip"

    @property
    def relative_directory(self) -> Path:
        return Path("data") / self.market / self.period_type / "klines" / self.symbol / self.interval

    @property
    def destination(self) -> Path:
        return self.output_root / self.relative_directory / self.archive_name

    @property
    def source_url(self) -> str:
        return f"{BASE_URL}/{self.market}/{self.period_type}/klines/{self.symbol}/{self.interval}/{self.archive_name}"


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO date: {value}") from exc


def first_of_month(value: date) -> date:
    return value.replace(day=1)


def next_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def previous_month(value: date) -> date:
    if value.month == 1:
        return date(value.year - 1, 12, 1)
    return date(value.year, value.month - 1, 1)


def month_periods(start: date, end: date) -> list[date]:
    current = first_of_month(start)
    final = first_of_month(end)
    periods: list[date] = []
    while current <= final:
        periods.append(current)
        current = next_month(current)
    return periods


def day_periods(start: date, end: date) -> list[date]:
    periods: list[date] = []
    current = start
    while current <= end:
        periods.append(current)
        current += timedelta(days=1)
    return periods


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def parse_checksum(path: Path) -> tuple[str, str] | None:
    try:
        text = path.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return None
    match = CHECKSUM_RE.match(text)
    if not match:
        return None
    return match.group(1).lower(), match.group(2)


def zip_is_valid(path: Path) -> tuple[bool, str | None]:
    try:
        with zipfile.ZipFile(path) as archive:
            bad_member = archive.testzip()
            if bad_member is not None:
                return False, f"ZIP CRC failure in {bad_member}"
            csv_members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if len(csv_members) != 1:
                return False, f"expected one CSV member, found {len(csv_members)}"
    except (OSError, zipfile.BadZipFile, zipfile.LargeZipFile) as exc:
        return False, f"invalid ZIP: {exc}"
    return True, None


def verify_archive(path: Path, checksum_path: Path) -> tuple[bool, str]:
    if not path.is_file():
        return False, "archive is missing"
    valid_zip, zip_error = zip_is_valid(path)
    if not valid_zip:
        return False, zip_error or "invalid ZIP"
    if not checksum_path.is_file():
        return False, "checksum is missing"
    parsed = parse_checksum(checksum_path)
    if parsed is None:
        return False, "checksum has invalid format"
    expected, filename = parsed
    if filename != path.name:
        return False, f"checksum names {filename}, expected {path.name}"
    actual = sha256_file(path)
    if actual != expected:
        return False, "SHA-256 mismatch"
    return True, "verified"


def existing_candidates(root: Path, job: Job) -> list[Path]:
    canonical = job.destination
    candidates: list[Path] = []
    if canonical.is_file():
        candidates.append(canonical)
    search_root = root / "data" / job.market / job.period_type / "klines" / job.symbol / job.interval
    if search_root.is_dir():
        candidates.extend(
            path
            for path in search_root.rglob(job.archive_name)
            if path.is_file() and path not in candidates
        )
    return candidates


def run_curl(url: str, destination: Path, retries: int) -> None:
    command = [
        "curl",
        "--fail",
        "--location",
        "--silent",
        "--show-error",
        "--retry",
        str(retries),
        "--retry-all-errors",
        "--retry-delay",
        "2",
        "--retry-max-time",
        "600",
        "--connect-timeout",
        "30",
        "--output",
        str(destination),
        url,
    ]
    result = subprocess.run(command, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        detail = result.stderr.strip() or f"curl exited with {result.returncode}"
        raise RuntimeError(detail)


def atomic_download(url: str, destination: Path, retries: int) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.part")
    try:
        run_curl(url, temporary, retries)
        if not temporary.is_file() or temporary.stat().st_size == 0:
            raise RuntimeError("download produced an empty file")
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def find_verified_existing(root: Path, job: Job) -> tuple[Path, str] | None:
    for candidate in existing_candidates(root, job):
        checksum_path = candidate.with_name(f"{candidate.name}.CHECKSUM")
        verified, reason = verify_archive(candidate, checksum_path)
        if verified:
            return candidate, reason
    return None


def download_job(job: Job, retries: int) -> dict[str, Any]:
    destination = job.destination
    checksum_destination = destination.with_name(f"{destination.name}.CHECKSUM")
    started = datetime.now(timezone.utc).isoformat()

    existing = find_verified_existing(job.output_root, job)
    if existing is not None:
        path, reason = existing
        return {
            "status": "existing",
            "period_type": job.period_type,
            "period": job.period.isoformat() if job.period_type == "daily" else job.period.strftime("%Y-%m"),
            "interval": job.interval,
            "symbol": job.symbol,
            "archive": job.archive_name,
            "path": str(path),
            "source_url": job.source_url,
            "sha256": sha256_file(path),
            "size_bytes": path.stat().st_size,
            "verification": reason,
            "started_at": started,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }

    try:
        atomic_download(job.source_url, destination, retries)
        atomic_download(f"{job.source_url}.CHECKSUM", checksum_destination, retries)
        verified, reason = verify_archive(destination, checksum_destination)
        if not verified:
            raise RuntimeError(reason)
        return {
            "status": "downloaded",
            "period_type": job.period_type,
            "period": job.period.isoformat() if job.period_type == "daily" else job.period.strftime("%Y-%m"),
            "interval": job.interval,
            "symbol": job.symbol,
            "archive": job.archive_name,
            "path": str(destination),
            "source_url": job.source_url,
            "sha256": sha256_file(destination),
            "size_bytes": destination.stat().st_size,
            "verification": reason,
            "started_at": started,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as exc:  # Keep the batch resumable when one URL fails.
        return {
            "status": "error",
            "period_type": job.period_type,
            "period": job.period.isoformat() if job.period_type == "daily" else job.period.strftime("%Y-%m"),
            "interval": job.interval,
            "symbol": job.symbol,
            "archive": job.archive_name,
            "path": str(destination),
            "source_url": job.source_url,
            "error": str(exc),
            "started_at": started,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, required=True, help="Raw data root, e.g. research/raw/binance")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--base-asset", help="Base asset for provenance; inferred for common symbols")
    parser.add_argument("--quote-asset", help="Quote asset for provenance; inferred for common symbols")
    parser.add_argument("--market", default="spot", choices=("spot",))
    parser.add_argument("--start-date", type=parse_date, default=date(2019, 1, 1))
    parser.add_argument("--end-date", type=parse_date, default=datetime.now(timezone.utc).date())
    parser.add_argument(
        "--monthly-end",
        type=parse_date,
        help="Last month to retrieve as monthly data; defaults to the month before --end-date",
    )
    parser.add_argument(
        "--daily-start",
        type=parse_date,
        help="First date to retrieve as daily data; defaults to the first day of --end-date's month",
    )
    parser.add_argument("--intervals", nargs="+", default=list(DEFAULT_INTERVALS))
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--retries", type=int, default=6)
    parser.add_argument("--manifest", type=Path, help="Output JSON manifest path")
    return parser


def build_jobs(args: argparse.Namespace) -> list[Job]:
    symbol = args.symbol.upper()
    output_root = args.output_root.expanduser().resolve()
    end_date: date = args.end_date
    if end_date < args.start_date:
        raise ValueError("--end-date must not precede --start-date")
    monthly_end = args.monthly_end or previous_month(first_of_month(end_date))
    daily_start = args.daily_start or first_of_month(end_date)
    if monthly_end < args.start_date:
        monthly_period_list: list[date] = []
    else:
        monthly_period_list = month_periods(args.start_date, monthly_end)
    daily_period_list = day_periods(max(daily_start, args.start_date), end_date)
    jobs: list[Job] = []
    for interval in args.intervals:
        for period in monthly_period_list:
            jobs.append(Job("monthly", period, interval, symbol, args.market, output_root))
        for period in daily_period_list:
            jobs.append(Job("daily", period, interval, symbol, args.market, output_root))
    return jobs


def asset_metadata(symbol: str, base_asset: str | None, quote_asset: str | None) -> tuple[str | None, str | None]:
    if base_asset and quote_asset:
        return base_asset.upper(), quote_asset.upper()
    upper_symbol = symbol.upper()
    inferred_quote = next((quote for quote in KNOWN_QUOTES if upper_symbol.endswith(quote)), None)
    if inferred_quote is None:
        return base_asset.upper() if base_asset else None, quote_asset.upper() if quote_asset else None
    inferred_base = upper_symbol[: -len(inferred_quote)]
    return (
        base_asset.upper() if base_asset else inferred_base,
        quote_asset.upper() if quote_asset else inferred_quote,
    )


def write_manifest(path: Path, args: argparse.Namespace, jobs: list[Job], records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    base_asset, quote_asset = asset_metadata(args.symbol, args.base_asset, args.quote_asset)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "exchange": "binance",
        "source": "binance-public-data",
        "source_base_url": BASE_URL,
        "source_repository": SOURCE_REPOSITORY,
        "retrieval_method": "official Binance public-data ZIP archives with published SHA-256 sidecars",
        "data_type": "klines",
        "base_asset": base_asset,
        "quote_currency": quote_asset,
        "timezone": "UTC",
        "timestamp_units": {
            "before_2025-01-01": "milliseconds",
            "from_2025-01-01": "microseconds",
        },
        "schema": list(KLINE_SCHEMA),
        "data_license": "Not stated separately in the archive README; verify current Binance terms before reuse or redistribution.",
        "transformation_version": "raw-v1",
        "raw_data_modified": False,
        "archive_policy": "Completed months use monthly archives; the incomplete current month uses daily archives.",
        "checksum_policy": "Every archive must have a matching published SHA-256 sidecar.",
        "verification_policy": "ZIP CRC, exactly one CSV member, and sidecar SHA-256 are checked before acceptance.",
        "legacy_archive_policy": "Valid archives outside the requested 600-job set are retained as raw data and are not included in canonical coverage counts.",
        "market": args.market,
        "symbol": args.symbol.upper(),
        "start_date": args.start_date.isoformat(),
        "end_date": args.end_date.isoformat(),
        "intervals": list(args.intervals),
        "job_count": len(jobs),
        "verified_count": len(records),
        "verification_status": "complete" if len(records) == len(jobs) and not any(record["status"] == "error" for record in records) else "incomplete",
        "downloaded_count": sum(record["status"] == "downloaded" for record in records),
        "existing_count": sum(record["status"] == "existing" for record in records),
        "error_count": sum(record["status"] == "error" for record in records),
        "jobs": sorted(records, key=lambda record: (record["period_type"], record["period"], record["interval"])),
    }
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.part")
    try:
        temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    args = build_parser().parse_args()
    if args.workers < 1 or args.retries < 0:
        print("--workers must be positive and --retries must be non-negative", file=sys.stderr)
        return 2
    try:
        jobs = build_jobs(args)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if not jobs:
        print("no download jobs requested", file=sys.stderr)
        return 2

    manifest = args.manifest or args.output_root / "binance-kline-retrieval-manifest.json"
    args.output_root.expanduser().resolve().mkdir(parents=True, exist_ok=True)
    print(f"processing {len(jobs)} archives with {args.workers} workers", flush=True)
    records: list[dict[str, Any]] = []
    lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(download_job, job, args.retries) for job in jobs]
        for index, future in enumerate(as_completed(futures), start=1):
            record = future.result()
            records.append(record)
            with lock:
                print(
                    f"[{index}/{len(jobs)}] {record['status']}: "
                    f"{record['interval']} {record['period_type']} {record['period']}",
                    flush=True,
                )

    write_manifest(manifest.expanduser().resolve(), args, jobs, records)
    errors = [record for record in records if record["status"] == "error"]
    print(f"completed: {len(records) - len(errors)} verified, {len(errors)} errors", flush=True)
    if errors:
        for record in errors[:20]:
            print(f"ERROR {record['archive']}: {record['error']}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
