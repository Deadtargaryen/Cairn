from __future__ import annotations

import csv
import hashlib
import io
import json
import sqlite3
import tempfile
import unittest
import zipfile
from pathlib import Path

import normalize_binance_klines as normalizer


def make_row(open_time: int, close_time: int, close: str = "100.5") -> list[str]:
    return [
        str(open_time),
        "100.0",
        "101.0",
        "99.0",
        close,
        "2.0",
        str(close_time),
        "200.0",
        "10",
        "1.0",
        "100.0",
        "0",
    ]


class NormalizerTests(unittest.TestCase):
    def write_archive(self, directory: Path, name: str, rows: list[list[str]]) -> tuple[Path, str]:
        archive = directory / name
        with zipfile.ZipFile(archive, "w") as zipped:
            stream = io.StringIO()
            csv.writer(stream, lineterminator="\n").writerows(rows)
            zipped.writestr(f"{name[:-4]}.csv", stream.getvalue())
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        archive.with_name(f"{name}.CHECKSUM").write_text(f"{digest}  {name}\n", encoding="utf-8")
        return archive, digest

    def test_timestamp_conversion_and_iso_format(self) -> None:
        self.assertEqual(normalizer.timestamp_to_us(1_546_300_800_000, "milliseconds"), 1_546_300_800_000_000)
        self.assertEqual(normalizer.us_to_iso(1_546_300_800_000_123), "2019-01-01T00:00:00.000123+00:00")

    def test_normalizes_rows_and_preserves_quality_flags(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive, digest = self.write_archive(
                root,
                "BTCUSDT-1m-2019-01.zip",
                [
                    make_row(1_546_300_800_000, 1_546_300_859_999),
                    # A source-side gap and a shortened close time are retained as flags.
                    make_row(1_546_300_920_000, 1_546_300_920_500),
                ],
            )
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps(
                    {
                        "verification_status": "complete",
                        "error_count": 0,
                        "exchange": "binance",
                        "source": "binance-public-data",
                        "source_repository": "https://github.com/binance/binance-public-data",
                        "source_base_url": "https://data.binance.vision/data",
                        "symbol": "BTCUSDT",
                        "market": "spot",
                        "base_asset": "BTC",
                        "quote_currency": "USDT",
                        "intervals": ["1m"],
                        "jobs": [
                            {
                                "archive": archive.name,
                                "path": str(archive),
                                "source_url": "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2019-01.zip",
                                "sha256": digest,
                                "interval": "1m",
                                "period_type": "monthly",
                                "period": "2019-01",
                                "status": "existing",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            output = root / "warehouse.sqlite"

            metadata = normalizer.normalize(manifest, output)

            self.assertEqual(metadata["archive_count_ingested"], 1)
            self.assertEqual(metadata["row_count_inserted"], 2)
            with sqlite3.connect(output) as connection:
                rows = connection.execute(
                    "SELECT timestamp_us, timestamp_utc, quality_status, quality_flags FROM klines ORDER BY timestamp_us"
                ).fetchall()
            self.assertEqual(rows[0][0], 1_546_300_800_000_000)
            self.assertEqual(rows[0][1], "2019-01-01T00:00:00+00:00")
            self.assertEqual(rows[0][2], "ok")
            self.assertEqual(rows[1][2], "source_anomaly")
            self.assertIn("gap_from_previous_candle", rows[1][3])
            self.assertIn("shortened_source_close_time", rows[1][3])

    def test_invalid_close_time_is_auditable_but_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive, digest = self.write_archive(
                root,
                "BTCUSDT-1m-2019-01.zip",
                [make_row(1_546_300_800_000, 1_546_300_799_999)],
            )
            rows, counts = normalizer.archive_rows(
                {
                    "archive_id": 1,
                    "path": str(archive),
                    "interval": "1m",
                    "market": "spot",
                    "symbol": "BTCUSDT",
                    "exchange": "binance",
                    "base_asset": "BTC",
                    "quote_currency": "USDT",
                },
                {},
            )
            self.assertEqual(len(rows), 1)
            self.assertEqual(counts["invalid_row_count"], 1)
            self.assertEqual(rows[0][-2], "invalid")
            self.assertIn("close_time_before_open_time", rows[0][-1])


if __name__ == "__main__":
    unittest.main()
