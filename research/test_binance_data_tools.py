from __future__ import annotations

import csv
import hashlib
import io
import tempfile
import unittest
import zipfile
from argparse import Namespace
from datetime import date
from pathlib import Path

import download_binance_klines as downloader
import validate_binance_klines as validator


def kline_row(open_time: int, interval: int, close_time: int | None = None) -> list[str]:
    return [
        str(open_time),
        "100.0",
        "101.0",
        "99.0",
        "100.5",
        "2.0",
        str(close_time if close_time is not None else open_time + interval - 1),
        "200.0",
        "10",
        "1.0",
        "100.0",
        "0",
    ]


class DownloaderTests(unittest.TestCase):
    def test_builds_monthly_history_and_current_daily_jobs(self) -> None:
        args = Namespace(
            symbol="BTCUSDT",
            market="spot",
            output_root=Path("raw"),
            start_date=date(2019, 1, 1),
            end_date=date(2026, 9, 8),
            monthly_end=None,
            daily_start=None,
            intervals=["1m", "5m", "15m", "1h", "4h", "1d"],
        )

        jobs = downloader.build_jobs(args)

        self.assertEqual(len(jobs), 600)
        self.assertEqual(sum(job.period_type == "monthly" for job in jobs), 552)
        self.assertEqual(sum(job.period_type == "daily" for job in jobs), 48)
        self.assertEqual(jobs[0].archive_name, "BTCUSDT-1m-2019-01.zip")
        self.assertEqual(jobs[-1].archive_name, "BTCUSDT-1d-2026-09-08.zip")

    def test_infers_common_base_and_quote_assets(self) -> None:
        self.assertEqual(downloader.asset_metadata("BTCUSDT", None, None), ("BTC", "USDT"))
        self.assertEqual(downloader.asset_metadata("ETHNGN", None, None), ("ETH", "NGN"))

    def test_verifies_archive_against_sidecar_checksum(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            archive_path = Path(directory) / "BTCUSDT-1m-2019-01.zip"
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("BTCUSDT-1m-2019-01.csv", "1,2,3\n")
            digest = hashlib.sha256(archive_path.read_bytes()).hexdigest()
            checksum_path = archive_path.with_name(f"{archive_path.name}.CHECKSUM")
            checksum_path.write_text(f"{digest}  {archive_path.name}\n", encoding="utf-8")

            valid, reason = downloader.verify_archive(archive_path, checksum_path)

            self.assertTrue(valid)
            self.assertEqual(reason, "verified")


class ValidatorTests(unittest.TestCase):
    def validate(self, rows: list[list[str]], interval: str = "1m") -> dict[str, object]:
        stream = io.StringIO()
        csv.writer(stream, lineterminator="\n").writerows(rows)
        stream.seek(0)
        return validator.validate_rows(stream, interval, date(2019, 1, 1), "monthly", "spot")

    def test_accepts_valid_kline_rows(self) -> None:
        interval = 60_000
        start = 1_546_300_800_000

        result = self.validate([kline_row(start, interval), kline_row(start + interval, interval)])

        self.assertEqual(result["errors"], [])
        self.assertEqual(result["warnings"], [])
        self.assertEqual(result["row_count"], 2)
        self.assertEqual(result["gap_count"], 0)

    def test_records_early_close_and_gap_as_quality_warnings(self) -> None:
        interval = 60_000
        start = 1_546_300_800_000
        rows = [
            kline_row(start, interval, close_time=start + 10_000),
            kline_row(start + 2 * interval, interval),
        ]

        result = self.validate(rows)

        self.assertEqual(result["errors"], [])
        self.assertEqual(result["close_time_anomaly_count"], 1)
        self.assertEqual(result["gap_count"], 1)
        self.assertEqual(len(result["warnings"]), 2)

    def test_rejects_close_time_before_open_time(self) -> None:
        interval = 60_000
        start = 1_546_300_800_000

        result = self.validate([kline_row(start, interval, close_time=start - 1)])

        self.assertIn("line 1: close time is outside interval bounds", result["errors"])

    def test_accepts_microsecond_timestamps_from_2025(self) -> None:
        interval = 60_000_000
        start = 1_735_689_600_000_000
        stream = io.StringIO()
        csv.writer(stream, lineterminator="\n").writerow(kline_row(start, interval))
        stream.seek(0)

        result = validator.validate_rows(stream, "1m", date(2025, 1, 1), "monthly", "spot")

        self.assertEqual(result["errors"], [])
        self.assertEqual(result["timestamp_unit"], "microseconds")


if __name__ == "__main__":
    unittest.main()
