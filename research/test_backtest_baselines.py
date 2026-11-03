from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

import backtest_baselines as backtester


UTC = timezone.utc


def fixture_frame() -> pd.DataFrame:
    start = 1_600_000_000_000_000
    step = backtester.INTERVAL_US["1m"]
    closes = np.array([100.0, 100.2, 100.5, 100.8, 100.4, 100.1, 100.0, 100.3], dtype=float)
    return pd.DataFrame(
        {
            "timestamp_us": start + np.arange(len(closes), dtype=np.int64) * step,
            "open": closes,
            "high": closes + 0.1,
            "low": closes - 0.1,
            "close": closes,
            "volume": np.full(len(closes), 10.0),
            "quote_asset_volume": np.full(len(closes), 1_000.0),
            "quality_status": ["ok"] * len(closes),
            "gap_before": [False] * len(closes),
            "segment_id": [0] * len(closes),
            "same_as_previous": [False] + [True] * (len(closes) - 1),
        }
    )


class BacktesterTests(unittest.TestCase):
    def test_cost_model_increases_buy_adverse_price_and_charges_fee(self) -> None:
        costs = backtester.CostModel(
            fee_rate=0.001,
            half_spread_rate=0.001,
            slippage_rate=0.002,
            impact_floor_rate=0.001,
            max_impact_rate=0.01,
        )

        position, details = backtester.execute_buy(100.0, 100.0, 1_000.0, 1.0, 0.0, costs, 1, 1, 2)

        self.assertIsNotNone(position)
        self.assertGreater(position.entry_price, 100.0)
        self.assertGreater(details["fee"], 0.0)
        self.assertLess(details["cash_after"], 100.0)

    def test_open_fill_uses_only_prior_bar_volume(self) -> None:
        frame = fixture_frame()
        frame.loc[3, "quote_asset_volume"] = 999_999_999.0

        self.assertEqual(backtester.known_quote_volume(frame, 3), frame.loc[2, "quote_asset_volume"])
        self.assertEqual(
            backtester.known_quote_volume(frame, 3, at_close=True),
            frame.loc[3, "quote_asset_volume"],
        )

    def test_regular_exit_uses_next_bar_open(self) -> None:
        frame = fixture_frame()
        position, _ = backtester.execute_buy(
            100.0,
            float(frame.loc[1, "open"]),
            float(frame.loc[0, "quote_asset_volume"]),
            1.0,
            0.0,
            backtester.CostModel(
                fee_rate=0.0,
                half_spread_rate=0.0,
                slippage_rate=0.0,
                impact_floor_rate=0.0,
            ),
            1,
            int(frame.loc[1, "timestamp_us"]),
            2,
        )
        self.assertIsNotNone(position)
        split = backtester.Split(
            "test",
            datetime.fromtimestamp(frame.timestamp_us.iloc[0] / 1_000_000, tz=UTC),
            datetime.fromtimestamp(frame.timestamp_us.iloc[-1] / 1_000_000 + 60, tz=UTC),
            "test",
        )

        trade, _ = backtester.close_trade(
            position,
            frame,
            2,
            "signal_reversal",
            split,
            "1m",
            "test",
            backtester.CostModel(
                fee_rate=0.0,
                half_spread_rate=0.0,
                slippage_rate=0.0,
                impact_floor_rate=0.0,
            ),
        )

        self.assertEqual(trade["exit_raw_price"], float(frame.loc[2, "open"]))
        self.assertEqual(trade["execution_price_basis"], "next_bar_open")

    def test_signal_does_not_use_next_bar_price(self) -> None:
        frame = fixture_frame()
        config = backtester.StrategyConfig("simple_momentum", lookback_bars=2, holding_bars=2)
        original_entry, _, _ = backtester.signal_arrays(frame, config, 7)
        changed = frame.copy()
        changed.loc[5:, "close"] = changed.loc[5:, "close"] * 100.0
        changed_entry, _, _ = backtester.signal_arrays(changed, config, 7)

        # A later-bar mutation cannot alter decisions strictly before it.
        np.testing.assert_array_equal(original_entry[:5], changed_entry[:5])

    def test_gap_forces_exit_and_does_not_carry_position(self) -> None:
        frame = fixture_frame()
        frame.loc[4:, "timestamp_us"] += backtester.INTERVAL_US["1m"]
        frame["gap_before"] = frame["timestamp_us"].diff().fillna(backtester.INTERVAL_US["1m"]) != backtester.INTERVAL_US["1m"]
        frame["segment_id"] = frame["gap_before"].cumsum()
        frame["same_as_previous"] = ~frame["gap_before"]
        split = backtester.Split(
            "test",
            datetime.fromtimestamp(frame.timestamp_us.iloc[0] / 1_000_000, tz=UTC),
            datetime.fromtimestamp(frame.timestamp_us.iloc[-1] / 1_000_000 + 60, tz=UTC),
            "test",
        )
        config = backtester.StrategyConfig("buy_and_hold", 0, 0)
        result = backtester.simulate(
            frame,
            "1m",
            split,
            config,
            backtester.CostModel(),
            100.0,
            1.0,
            0.0,
            1,
            {"source_rows": len(frame), "invalid_rows": 0, "source_anomaly_rows": 0, "tradable_rows": len(frame), "gap_boundaries": 1, "gap_rows": 1},
        )

        self.assertGreaterEqual(result.summary["gap_exit_count"], 1)
        self.assertGreaterEqual(result.summary["gap_reentry_count"], 1)
        self.assertGreaterEqual(len(result.trades), 1)

    def test_buy_and_hold_does_not_double_enter_when_split_starts_after_gap(self) -> None:
        frame = fixture_frame()
        frame.loc[0, "gap_before"] = True
        frame["segment_id"] = frame["gap_before"].cumsum()
        frame["same_as_previous"] = ~frame["gap_before"]
        split = backtester.Split(
            "post_gap",
            datetime.fromtimestamp(frame.timestamp_us.iloc[0] / 1_000_000, tz=UTC),
            datetime.fromtimestamp(frame.timestamp_us.iloc[-1] / 1_000_000 + 60, tz=UTC),
            "test",
        )

        result = backtester.simulate(
            frame,
            "1m",
            split,
            backtester.StrategyConfig("buy_and_hold", 0, 0),
            backtester.CostModel(),
            100.0,
            1.0,
            0.0,
            1,
            {"source_rows": len(frame), "invalid_rows": 0, "source_anomaly_rows": 0, "tradable_rows": len(frame), "gap_boundaries": 1, "gap_rows": 1},
        )

        self.assertEqual(result.summary["gap_reentry_count"], 0)
        self.assertEqual(result.summary["trade_count"], 1)

    def test_chronological_splits_do_not_overlap(self) -> None:
        splits = backtester.default_splits()
        for previous, current in zip(splits, splits[1:]):
            self.assertEqual(previous.end, current.start)
            self.assertLess(previous.start, previous.end)

    def test_load_bars_excludes_invalid_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "warehouse.sqlite"
            with sqlite3.connect(database) as connection:
                connection.execute(
                    """
                    CREATE TABLE klines (
                        timestamp_us INTEGER,
                        open REAL,
                        high REAL,
                        low REAL,
                        close REAL,
                        volume REAL,
                        quote_asset_volume REAL,
                        quality_status TEXT,
                        symbol TEXT,
                        market TEXT,
                        interval TEXT
                    )
                    """
                )
                base = 1_600_000_000_000_000
                connection.executemany(
                    "INSERT INTO klines VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (base, 100, 101, 99, 100, 1, 100, "ok", "BTCUSDT", "spot", "1m"),
                        (base + backtester.INTERVAL_US["1m"], 100, 101, 99, 100, 1, 100, "invalid", "BTCUSDT", "spot", "1m"),
                        (base + 2 * backtester.INTERVAL_US["1m"], 100, 101, 99, 100, 1, 100, "ok", "BTCUSDT", "spot", "1m"),
                    ],
                )
                connection.commit()
                frame, counts = backtester.load_bars(connection, "1m")

            self.assertEqual(len(frame), 2)
            self.assertEqual(counts["invalid_rows"], 1)
            self.assertEqual(counts["gap_boundaries"], 1)

    def test_report_trade_count_is_independent_of_journal_writing(self) -> None:
        frame = fixture_frame()
        split = backtester.Split(
            "test",
            datetime.fromtimestamp(frame.timestamp_us.iloc[0] / 1_000_000, tz=UTC),
            datetime.fromtimestamp(frame.timestamp_us.iloc[-1] / 1_000_000 + 60, tz=UTC),
            "test",
        )
        config = backtester.StrategyConfig("buy_and_hold", 0, 0)
        result = backtester.simulate(
            frame,
            "1m",
            split,
            config,
            backtester.CostModel(),
            100.0,
            1.0,
            0.0,
            1,
            {"source_rows": len(frame), "invalid_rows": 0, "source_anomaly_rows": 0, "tradable_rows": len(frame), "gap_boundaries": 0, "gap_rows": 0},
        )

        self.assertEqual(result.summary["trade_count"], len(result.trades))


if __name__ == "__main__":
    unittest.main()
