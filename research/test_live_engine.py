from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import live_engine as engine
import binance_live as binance


def market_features(symbol: str = "BTCUSDT") -> engine.MarketFeatures:
    return engine.MarketFeatures(
        timestamp_ms=1_700_000_000_000,
        symbol=symbol,
        last_price=100.0,
        best_bid=99.9,
        best_ask=100.1,
        mid_price=100.0,
        spread_bps=20.0,
        bid_depth=10.0,
        ask_depth=10.0,
        depth_imbalance=0.0,
        trade_count=10,
        buy_volume=6.0,
        sell_volume=4.0,
        trade_imbalance=0.2,
        return_1s=0.001,
        return_5s=0.002,
        realized_vol=0.001,
        volume_rate=2.0,
        regime="SIDEWAYS",
        data_quality="ok",
        data_age_ms=10,
    )


class FixedReasoner:
    def __init__(self, decision: engine.Decision):
        self.decision = decision

    async def decide(self, market, opportunity):
        return self.decision


class FakeExecutor:
    def __init__(self):
        self.entered = 0
        self.exited = 0

    async def enter(self, decision, market, state=None):
        self.entered += 1
        return engine.OpenPosition("p1", "BUY", 0.01, 100.0, market.timestamp_ms, market.timestamp_ms + 1000, 99.0, 101.0, 0.01, "PAPER")

    async def exit(self, position, market, reason, state=None):
        self.exited += 1
        return {"status": "closed", "net_pnl": 0.05, "gross_pnl": 0.06, "fees": 0.01, "reason": reason, "mode": "PAPER"}


class LiveEngineTests(unittest.TestCase):
    def test_paper_executor_respects_initial_capital(self):
        config = engine.EngineConfig(
            mode="PAPER",
            paper_initial_quote=3.0,
            max_position_quote=2.0,
            max_total_exposure_quote=2.0,
            journal_path=Path("/tmp/live-engine-test-journal.jsonl"),
            state_path=Path("/tmp/live-engine-test-state.json"),
        )
        executor = engine.PaperExecutor(config)
        decision = engine.Decision("BUY", "BTCUSDT", 0.9, 100.0, 99.0, 101.0, 2.0, 0.05, 0.02, "test", "test", 5, "test")
        position = asyncio.run(executor.enter(decision, market_features()))
        self.assertIsNotNone(position)
        self.assertLessEqual(executor.quote_balance, 3.0)
        self.assertGreater(executor.base_balance, 0.0)

    def test_live_engine_rejects_sell_decision_for_spot(self):
        config = engine.EngineConfig(
            mode="PAPER",
            journal_path=Path("/tmp/live-engine-test-journal.jsonl"),
            state_path=Path("/tmp/live-engine-test-state.json"),
        )
        decision = engine.Decision("SELL", "BTCUSDT", 0.9, 100.0, 101.0, 99.0, 1.0, 0.1, 0.02, "test", "test", 5, "test")
        with self.assertRaises(ValueError):
            decision.validate_for_spot("BTCUSDT")

    def test_risk_manager_blocks_stale_data_and_large_position(self):
        config = engine.EngineConfig(
            mode="PAPER",
            max_position_quote=2.0,
            max_total_exposure_quote=2.0,
            journal_path=Path("/tmp/live-engine-test-journal.jsonl"),
            state_path=Path("/tmp/live-engine-test-state.json"),
        )
        risk = engine.RiskManager(config)
        decision = engine.Decision("BUY", "BTCUSDT", 0.9, 100.0, 99.0, 101.0, 3.0, 0.1, 0.02, "test", "test", 5, "test")
        valid, reason = risk.validate(decision, market_features(), live=False)
        self.assertFalse(valid)
        self.assertIn("position size", reason)
        stale = market_features()
        stale = engine.MarketFeatures(**{**stale.as_dict(), "data_quality": "stale_or_unsynchronized"})
        decision = engine.Decision("BUY", "BTCUSDT", 0.9, 100.0, 99.0, 101.0, 1.0, 0.1, 0.02, "test", "test", 5, "test")
        valid, reason = risk.validate(decision, stale, live=False)
        self.assertFalse(valid)
        self.assertIn("stale", reason)

    def test_engine_processes_decision_and_exit_through_executor(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = engine.EngineConfig(
                mode="PAPER",
                decision_cooldown_ms=0,
                minimum_trade_interval_ms=0,
                max_position_quote=2.0,
                max_total_exposure_quote=2.0,
                journal_path=root / "journal.jsonl",
                state_path=root / "state.json",
            )
            executor = FakeExecutor()
            decision = engine.Decision("BUY", "BTCUSDT", 0.9, 100.0, 99.0, 101.0, 1.0, 0.1, 0.02, "test", "test", 5, "test")
            live_engine = engine.LiveTradingEngine(config, reasoner=FixedReasoner(decision), executor=executor)
            state = binance.MarketState("BTCUSDT", binance.LocalOrderBook("BTCUSDT"))
            state.order_book.load_snapshot({"lastUpdateId": 1, "bids": [["99.9", "1"]], "asks": [["100.1", "1"]]})
            state.order_book.apply_event({"U": 2, "u": 2, "E": 1_700_000_000_000, "b": [], "a": []})
            # Seed an event and use a direct feature-compatible path.
            live_engine.features.last_features = market_features()
            result = asyncio.run(live_engine._open_position(decision, market_features(), state))
            self.assertIsNone(result)
            self.assertEqual(executor.entered, 1)
            self.assertIsNotNone(live_engine.position)
            asyncio.run(live_engine._close_position(market_features(), state, "test"))
            self.assertEqual(executor.exited, 1)
            self.assertEqual(live_engine.win_count, 1)
            self.assertTrue((root / "journal.jsonl").exists())


if __name__ == "__main__":
    unittest.main()
