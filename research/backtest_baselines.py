#!/usr/bin/env python3
"""Run reproducible, cost-aware baseline backtests on the kline warehouse.

This module deliberately models only what the normalized Binance kline data
can support.  Signals are formed from information available through a bar's
close and are filled at the next bar's open.  Since the warehouse has no
historical bid/ask, queue, or order-book state, spread, slippage, and impact
are explicit conservative proxies rather than reconstructed executions.

No live exchange or order-submission code exists in this module.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import sqlite3
import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd


INTERVAL_US: dict[str, int] = {
    "1m": 60_000_000,
    "5m": 300_000_000,
    "15m": 900_000_000,
    "1h": 3_600_000_000,
    "4h": 14_400_000_000,
    "1d": 86_400_000_000,
}

DEFAULT_INTERVALS = tuple(INTERVAL_US)
DEFAULT_SEED = 20260909
UTC = dt.timezone.utc
YEAR_US = 365.25 * 24 * 60 * 60 * 1_000_000
REQUIRED_ASSUMPTION_IDS = {
    "binance_kline_schema",
    "binance_timestamp_units",
    "archive_integrity",
    "event_time_not_processing_time",
    "derived_data_and_reprocessing",
    "integrity_and_atomic_outputs",
    "explicit_transaction_costs",
    "volume_is_not_orderbook_liquidity",
    "fixed_diagnostic_hypotheses",
}


@dataclass(frozen=True)
class Split:
    name: str
    start: dt.datetime
    end: dt.datetime
    purpose: str

    @property
    def start_us(self) -> int:
        return int(self.start.timestamp() * 1_000_000)

    @property
    def end_us(self) -> int:
        return int(self.end.timestamp() * 1_000_000)


@dataclass(frozen=True)
class StrategyConfig:
    name: str
    lookback_bars: int
    holding_bars: int
    threshold: float = 0.0
    window_bars: int = 0
    z_entry: float = 2.0
    z_exit: float = 0.0
    random_probability: float = 0.01


@dataclass(frozen=True)
class CostModel:
    fee_rate: float = 0.001
    half_spread_rate: float = 0.0005
    slippage_rate: float = 0.0005
    impact_floor_rate: float = 0.0001
    impact_participation_multiplier: float = 0.5
    max_impact_rate: float = 0.005


@dataclass
class Position:
    quantity: float
    entry_index: int
    decision_time_us: int | None
    entry_time_us: int
    entry_raw_price: float
    entry_price: float
    entry_notional: float
    entry_fee: float
    entry_spread_cost: float
    entry_slippage_cost: float
    entry_impact_cost: float
    exit_due_index: int
    cash_before_entry: float


@dataclass
class SimulationResult:
    summary: dict[str, Any]
    trades: list[dict[str, Any]] = field(default_factory=list)


def utc_datetime(value: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso_date_end(value: str) -> dt.datetime:
    return dt.datetime.combine(dt.date.fromisoformat(value), dt.time.min, tzinfo=UTC)


def parse_intervals(value: str) -> list[str]:
    intervals = [item.strip() for item in value.split(",") if item.strip()]
    unknown = sorted(set(intervals) - set(INTERVAL_US))
    if unknown:
        raise argparse.ArgumentTypeError(f"unsupported interval(s): {', '.join(unknown)}")
    if not intervals:
        raise argparse.ArgumentTypeError("at least one interval is required")
    return intervals


def default_splits() -> list[Split]:
    def split(name: str, start: str, end: str, purpose: str) -> Split:
        return Split(name, utc_datetime(start), utc_datetime(end), purpose)

    return [
        split("research_train", "2019-01-01T00:00:00+00:00", "2022-01-01T00:00:00+00:00", "research/training"),
        split("validation_2022", "2022-01-01T00:00:00+00:00", "2023-01-01T00:00:00+00:00", "validation"),
        split("test_2023", "2023-01-01T00:00:00+00:00", "2024-01-01T00:00:00+00:00", "out_of_sample_test"),
        split("test_2024", "2024-01-01T00:00:00+00:00", "2025-01-01T00:00:00+00:00", "additional_out_of_sample_test"),
        split("current_2025_present", "2025-01-01T00:00:00+00:00", "2026-09-09T00:00:00+00:00", "additional_out_of_sample_validation"),
    ]


def duration_bars(interval: str, minutes: int, minimum: int = 1) -> int:
    interval_minutes = INTERVAL_US[interval] / 60_000_000
    return max(minimum, int(round(minutes / interval_minutes)))


def strategy_configs(interval: str, random_probability: float = 0.01) -> list[StrategyConfig]:
    momentum_lookback = duration_bars(interval, 60, minimum=2)
    momentum_hold = duration_bars(interval, 30, minimum=1)
    mean_window = duration_bars(interval, 240, minimum=5)
    mean_hold = duration_bars(interval, 30, minimum=1)
    breakout_window = duration_bars(interval, 240, minimum=5)
    breakout_hold = duration_bars(interval, 60, minimum=1)
    return [
        StrategyConfig("buy_and_hold", 0, 0),
        StrategyConfig("no_trade", 0, 0),
        StrategyConfig("simple_momentum", momentum_lookback, momentum_hold, threshold=0.0),
        StrategyConfig(
            "simple_mean_reversion",
            mean_window,
            mean_hold,
            window_bars=mean_window,
            z_entry=2.0,
            z_exit=0.0,
        ),
        StrategyConfig(
            "volatility_breakout",
            breakout_window,
            breakout_hold,
            window_bars=breakout_window,
            threshold=0.0,
        ),
        StrategyConfig(
            "random_entry",
            0,
            momentum_hold,
            random_probability=random_probability,
        ),
    ]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def stable_seed(*parts: object) -> int:
    encoded = "|".join(str(part) for part in parts).encode("utf-8")
    return int.from_bytes(hashlib.sha256(encoded).digest()[:8], "big") % (2**32)


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_knowledge_base(knowledge_base: dict[str, Any], project_root: Path) -> None:
    references = knowledge_base.get("references")
    if not isinstance(references, list) or not references:
        raise ValueError("knowledge-base registry has no references")
    for reference in references:
        source_path = reference.get("path")
        if not source_path:
            raise ValueError("knowledge-base reference has no path")
        if not (project_root / source_path).exists():
            raise FileNotFoundError(f"knowledge-base source is unavailable: {source_path}")


def validate_assumptions(assumptions: dict[str, Any]) -> None:
    entries = assumptions.get("assumptions")
    if not isinstance(entries, list):
        raise ValueError("research assumptions registry has no assumptions")
    identifiers = {entry.get("id") for entry in entries}
    missing = sorted(REQUIRED_ASSUMPTION_IDS - identifiers)
    if missing:
        raise ValueError(f"research assumptions are missing required IDs: {', '.join(missing)}")


def load_bars(connection: sqlite3.Connection, interval: str) -> tuple[pd.DataFrame, dict[str, int]]:
    query = """
        SELECT timestamp_us, open, high, low, close, volume, quote_asset_volume, quality_status
        FROM klines
        WHERE symbol = 'BTCUSDT' AND market = 'spot' AND interval = ?
        ORDER BY timestamp_us
    """
    frame = pd.read_sql_query(query, connection, params=(interval,))
    if frame.empty:
        raise ValueError(f"no BTCUSDT spot bars found for interval {interval}")
    counts = {
        "source_rows": int(len(frame)),
        "invalid_rows": int((frame["quality_status"] == "invalid").sum()),
        "source_anomaly_rows": int((frame["quality_status"] == "source_anomaly").sum()),
    }
    frame = frame.loc[frame["quality_status"] == "ok"].copy()
    frame.reset_index(drop=True, inplace=True)
    if frame.empty:
        raise ValueError(f"no quality_status=ok bars found for interval {interval}")
    step = INTERVAL_US[interval]
    frame["gap_before"] = frame["timestamp_us"].diff().fillna(step).astype("int64") != step
    frame["segment_id"] = frame["gap_before"].cumsum().astype("int64")
    counts["tradable_rows"] = int(len(frame))
    counts["gap_boundaries"] = int(frame["gap_before"].sum())
    counts["gap_rows"] = int(frame["gap_before"].sum())
    return frame, counts


def grouped_shift(values: pd.Series, groups: pd.Series, periods: int) -> pd.Series:
    return values.groupby(groups, sort=False).shift(periods)


def grouped_rolling(values: pd.Series, groups: pd.Series, window: int, operation: str) -> pd.Series:
    grouped = values.groupby(groups, sort=False)
    if operation == "mean":
        return grouped.transform(lambda series: series.rolling(window, min_periods=window).mean())
    if operation == "std":
        return grouped.transform(lambda series: series.rolling(window, min_periods=window).std(ddof=0))
    if operation == "max":
        return grouped.transform(lambda series: series.rolling(window, min_periods=window).max())
    if operation == "median":
        return grouped.transform(lambda series: series.rolling(window, min_periods=window).median())
    raise ValueError(f"unsupported rolling operation: {operation}")


def add_features(frame: pd.DataFrame, interval: str, configs: Sequence[StrategyConfig]) -> pd.DataFrame:
    result = frame.copy()
    groups = result["segment_id"]
    momentum_lookback = max((config.lookback_bars for config in configs if config.name == "simple_momentum"), default=2)
    result["momentum_return"] = result["close"] / grouped_shift(result["close"], groups, momentum_lookback) - 1.0

    mean_config = next((config for config in configs if config.name == "simple_mean_reversion"), None)
    if mean_config is not None:
        result["mean"] = grouped_rolling(result["close"], groups, mean_config.window_bars, "mean")
        result["std"] = grouped_rolling(result["close"], groups, mean_config.window_bars, "std")
        result["zscore"] = (result["close"] - result["mean"]) / result["std"].replace(0.0, np.nan)

    breakout_config = next((config for config in configs if config.name == "volatility_breakout"), None)
    if breakout_config is not None:
        previous_high = grouped_shift(result["high"], groups, 1)
        result["previous_high"] = grouped_rolling(previous_high, groups, breakout_config.window_bars, "max")
        result["range"] = result["high"] - result["low"]
        result["range_mean"] = grouped_rolling(result["range"], groups, breakout_config.window_bars, "mean")
        result["breakout_strength"] = (result["close"] - result["previous_high"]) / result["range_mean"].replace(0.0, np.nan)

    # Signals may only be generated on a bar whose predecessor is in the same
    # contiguous segment.  This prevents a rolling feature from crossing a
    # missing or invalid source interval.
    result["same_as_previous"] = ~result["gap_before"]
    return result


def signal_arrays(
    frame: pd.DataFrame,
    config: StrategyConfig,
    seed: int,
) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    n = len(frame)
    entry = np.zeros(n, dtype=bool)
    exit_signal = np.zeros(n, dtype=bool)
    eligible = frame["same_as_previous"].to_numpy(dtype=bool)
    groups = frame["segment_id"].to_numpy()

    if config.name == "simple_momentum":
        values = frame["close"].to_numpy(dtype=float)
        lookback = config.lookback_bars
        momentum = np.full(n, np.nan)
        for start, end in segment_ranges(groups):
            if end - start > lookback:
                momentum[start + lookback : end] = values[start + lookback : end] / values[start : end - lookback] - 1.0
        condition = np.isfinite(momentum) & (momentum > config.threshold) & eligible
        entry[1:] = condition[1:] & ~condition[:-1] & (groups[1:] == groups[:-1])
        exit_signal[1:] = (~condition[1:]) & condition[:-1] & (groups[1:] == groups[:-1])
        details = {"lookback_bars": lookback, "threshold": config.threshold}
    elif config.name == "simple_mean_reversion":
        zscore = frame["zscore"].to_numpy(dtype=float)
        condition = np.isfinite(zscore) & (zscore < -config.z_entry) & eligible
        entry[1:] = condition[1:] & ~condition[:-1] & (groups[1:] == groups[:-1])
        exit_signal[1:] = (np.isfinite(zscore[1:]) & (zscore[1:] >= config.z_exit)) & condition[:-1] & (groups[1:] == groups[:-1])
        details = {"window_bars": config.window_bars, "z_entry": config.z_entry, "z_exit": config.z_exit}
    elif config.name == "volatility_breakout":
        strength = frame["breakout_strength"].to_numpy(dtype=float)
        condition = np.isfinite(strength) & (strength > config.threshold) & eligible
        entry[1:] = condition[1:] & ~condition[:-1] & (groups[1:] == groups[:-1])
        exit_signal[1:] = (~condition[1:]) & condition[:-1] & (groups[1:] == groups[:-1])
        details = {"window_bars": config.window_bars, "threshold": config.threshold}
    elif config.name == "random_entry":
        # A local seeded generator makes this baseline reproducible without
        # using prices, labels, or any information from a future split.
        rng = np.random.default_rng(seed)
        draws = rng.random(n)
        candidate = (draws < config.random_probability) & eligible
        entry[1:] = candidate[1:] & (groups[1:] == groups[:-1])
        details = {"random_probability": config.random_probability, "seed": seed}
    elif config.name in {"buy_and_hold", "no_trade"}:
        details = {}
    else:
        raise ValueError(f"unsupported strategy: {config.name}")

    return entry, exit_signal, details


def segment_ranges(groups: np.ndarray) -> Iterable[tuple[int, int]]:
    if len(groups) == 0:
        return
    starts = np.flatnonzero(np.r_[True, groups[1:] != groups[:-1]])
    ends = np.r_[starts[1:], len(groups)]
    return zip(starts.tolist(), ends.tolist())


def impact_rate(notional: float, quote_volume: float, costs: CostModel) -> float:
    if not math.isfinite(quote_volume) or quote_volume <= 0 or notional <= 0:
        return costs.max_impact_rate
    participation = notional / quote_volume
    return min(
        costs.max_impact_rate,
        max(costs.impact_floor_rate, costs.impact_participation_multiplier * participation),
    )


def known_quote_volume(frame: pd.DataFrame, execution_index: int, at_close: bool = False) -> float:
    """Return volume available when an execution price becomes observable.

    A bar's aggregate volume is only known after that bar closes. For an
    open-priced fill, use the immediately preceding retained bar. For an
    explicit end-of-split liquidation at the current close, the current bar
    has completed and its volume is available. This remains an OHLCV proxy,
    not historical order-book depth.
    """
    if at_close:
        return float(frame.iloc[execution_index]["quote_asset_volume"])
    if execution_index <= 0:
        return float("nan")
    return float(frame.iloc[execution_index - 1]["quote_asset_volume"])


def execute_buy(
    cash: float,
    raw_price: float,
    quote_volume: float,
    allocation: float,
    min_notional: float,
    costs: CostModel,
    index: int,
    timestamp_us: int,
    holding_bars: int,
    decision_time_us: int | None = None,
) -> tuple[Position | None, dict[str, Any]]:
    requested_notional = cash * allocation
    if requested_notional < min_notional or requested_notional <= 0:
        return None, {"skipped": True, "reason": "minimum_notional"}
    impact = impact_rate(requested_notional, quote_volume, costs)
    adverse_rate = costs.half_spread_rate + costs.slippage_rate + impact
    if adverse_rate >= 1.0:
        return None, {"skipped": True, "reason": "invalid_adverse_cost_rate"}
    entry_price = raw_price * (1.0 + adverse_rate)
    quantity = requested_notional / (entry_price * (1.0 + costs.fee_rate))
    gross = quantity * entry_price
    fee = gross * costs.fee_rate
    total_spend = gross + fee
    if total_spend > cash + 1e-12:
        return None, {"skipped": True, "reason": "insufficient_cash_after_costs"}
    position = Position(
        quantity=quantity,
        entry_index=index,
        decision_time_us=decision_time_us,
        entry_time_us=timestamp_us,
        entry_raw_price=raw_price,
        entry_price=entry_price,
        entry_notional=quantity * raw_price,
        entry_fee=fee,
        entry_spread_cost=quantity * raw_price * costs.half_spread_rate,
        entry_slippage_cost=quantity * raw_price * costs.slippage_rate,
        entry_impact_cost=quantity * raw_price * impact,
        exit_due_index=index + max(1, holding_bars),
        cash_before_entry=cash,
    )
    return position, {
        "skipped": False,
        "cash_after": cash - total_spend,
        "gross": gross,
        "fee": fee,
        "spread_cost": position.entry_spread_cost,
        "slippage_cost": position.entry_slippage_cost,
        "impact_cost": position.entry_impact_cost,
    }


def execute_sell(
    position: Position,
    raw_price: float,
    quote_volume: float,
    costs: CostModel,
) -> dict[str, Any]:
    raw_notional = position.quantity * raw_price
    impact = impact_rate(raw_notional, quote_volume, costs)
    adverse_rate = costs.half_spread_rate + costs.slippage_rate + impact
    if adverse_rate >= 1.0:
        raise ValueError("adverse sell cost rate must be less than 100%")
    exit_price = raw_price * (1.0 - adverse_rate)
    gross = position.quantity * exit_price
    fee = gross * costs.fee_rate
    net_proceeds = gross - fee
    return {
        "exit_price": exit_price,
        "gross": gross,
        "fee": fee,
        "net_proceeds": net_proceeds,
        "spread_cost": raw_notional * costs.half_spread_rate,
        "slippage_cost": raw_notional * costs.slippage_rate,
        "impact_cost": raw_notional * impact,
    }


def close_trade(
    position: Position,
    frame: pd.DataFrame,
    exit_index: int,
    reason: str,
    split: Split,
    interval: str,
    strategy: str,
    costs: CostModel,
    at_close: bool = False,
) -> tuple[dict[str, Any], float]:
    row = frame.iloc[exit_index]
    raw_exit_price = float(row["close"] if at_close else row["open"])
    sell = execute_sell(
        position,
        raw_exit_price,
        known_quote_volume(frame, exit_index, at_close=at_close),
        costs,
    )
    pnl = sell["net_proceeds"] - (
        position.quantity * position.entry_price + position.entry_fee
    )
    total_fees = position.entry_fee + sell["fee"]
    trade = {
        "trade_id": hashlib.sha256(
            (
                f"{strategy}|{interval}|{split.name}|{position.entry_time_us}|"
                f"{int(row['timestamp_us'])}|{reason}"
            ).encode("utf-8")
        ).hexdigest()[:24],
        "timestamp": us_to_iso(int(position.entry_time_us)),
        "decision_timestamp_utc": us_to_iso(int(position.decision_time_us)) if position.decision_time_us is not None else None,
        "exchange": "binance",
        "market": "spot",
        "symbol": "BTCUSDT",
        "interval": interval,
        "split": split.name,
        "strategy": strategy,
        "side": "LONG",
        "entry_timestamp_utc": us_to_iso(int(position.entry_time_us)),
        "exit_timestamp_utc": us_to_iso(int(row["timestamp_us"])),
        "entry_raw_price": position.entry_raw_price,
        "entry_price": position.entry_price,
        "exit_raw_price": raw_exit_price,
        "exit_price": sell["exit_price"],
        "quantity": position.quantity,
        "entry_notional": position.entry_notional,
        "exit_notional": position.quantity * raw_exit_price,
        "entry_fee": position.entry_fee,
        "exit_fee": sell["fee"],
        "fees": total_fees,
        "spread": position.entry_spread_cost + sell["spread_cost"],
        "slippage": position.entry_slippage_cost + sell["slippage_cost"],
        "market_impact": position.entry_impact_cost + sell["impact_cost"],
        "total_costs": total_fees + position.entry_spread_cost + sell["spread_cost"] + position.entry_slippage_cost + sell["slippage_cost"] + position.entry_impact_cost + sell["impact_cost"],
        "gross_pnl_before_costs": position.quantity * (raw_exit_price - position.entry_raw_price),
        "net_pnl": pnl,
        "return_on_allocated_capital": pnl / max(position.quantity * position.entry_price + position.entry_fee, 1e-12),
        "holding_bars": max(0, exit_index - position.entry_index),
        "exit_reason": reason,
        "execution_price_basis": "bar_close" if at_close else "next_bar_open",
        "data_quality": "quality_status=ok bars; gap boundaries force exit",
        "invalidation": reason if reason in {"gap_exit", "risk_limit"} else None,
        "model_version": "baseline-v1",
    }
    return trade, sell["net_proceeds"]


def us_to_iso(value: int) -> str:
    seconds, micros = divmod(int(value), 1_000_000)
    return dt.datetime.fromtimestamp(seconds, tz=UTC).replace(microsecond=micros).isoformat()


def empty_summary(
    interval: str,
    split: Split,
    strategy: str,
    frame: pd.DataFrame,
    start_index: int,
    end_index: int,
    initial_equity: float,
    details: dict[str, Any],
    quality_counts: dict[str, int],
) -> dict[str, Any]:
    bars = max(0, end_index - start_index)
    return {
        "model_id": f"baseline-v1-{strategy}-{interval}-{split.name}",
        "strategy": strategy,
        "interval": interval,
        "symbol": "BTCUSDT",
        "exchange": "binance",
        "market": "spot",
        "split": split.name,
        "split_purpose": split.purpose,
        "start": split.start.isoformat(),
        "end": split.end.isoformat(),
        "bars": bars,
        "initial_equity": initial_equity,
        "final_equity": initial_equity,
        "net_return": 0.0,
        "cagr": 0.0,
        "max_drawdown": 0.0,
        "sharpe": 0.0,
        "sortino": 0.0,
        "trade_count": 0,
        "win_rate": 0.0,
        "average_win": 0.0,
        "average_loss": 0.0,
        "expectancy": 0.0,
        "profit_factor": None,
        "turnover": 0.0,
        "total_fees": 0.0,
        "total_spread": 0.0,
        "total_slippage": 0.0,
        "total_market_impact": 0.0,
        "gross_pnl_before_costs": 0.0,
        "net_pnl": 0.0,
        "total_modeled_costs": 0.0,
        "cost_to_gross_ratio": None,
        "exposure_fraction": 0.0,
        "signal_count": 0,
        "skipped_min_notional": 0,
        "gap_exit_count": 0,
        "gap_reentry_count": 0,
        "quality_counts": quality_counts,
        "strategy_parameters": details,
        "hypothesis": hypothesis_for_strategy(strategy),
        "dataset": "Binance BTCUSDT spot klines; exchange event-time UTC; quality_status=ok rows",
        "features": feature_names_for_strategy(strategy),
        "target": "net portfolio return after modeled fees, spread proxy, slippage proxy, and impact proxy",
        "train_period": "2019-01-01/2022-01-01",
        "validation_period": "2022-01-01/2023-01-01",
        "test_period": "2023-01-01/2025-01-01",
        "out_of_sample_result": "not applicable for this empty report",
        "walk_forward_result": "not run in baseline-v1",
        "regime_dependency": "not classified in baseline-v1",
        "failure_modes": ["OHLCV-only execution proxy", "source gaps", "fixed non-fitted rules"],
        "overfitting_risk": "baseline fixed parameters; no parameter search performed",
        "position_policy": "Each chronological split starts flat and liquidates at split end; no position or cash carries between split reports.",
        "deployment_status": "research_only",
    }


def hypothesis_for_strategy(strategy: str) -> str:
    return {
        "buy_and_hold": "Passive BTC exposure is a benchmark, not a discovered trading signal.",
        "no_trade": "Abstention is the zero-exposure benchmark and is always admissible.",
        "simple_momentum": "Positive trailing return may conditionally precede positive next-bar-to-holding-horizon return.",
        "simple_mean_reversion": "A sufficiently negative close-to-rolling-mean z-score may precede a reversion toward the rolling mean.",
        "volatility_breakout": "A close above the prior rolling high, scaled by recent range, may precede continuation.",
        "random_entry": "Random timing estimates a non-predictive participation baseline under the same execution model.",
    }.get(strategy, "Unspecified baseline hypothesis.")


def feature_names_for_strategy(strategy: str) -> list[str]:
    return {
        "buy_and_hold": ["open", "close"],
        "no_trade": [],
        "simple_momentum": ["close", "trailing_return"],
        "simple_mean_reversion": ["close", "rolling_mean", "rolling_std", "zscore"],
        "volatility_breakout": ["high", "close", "rolling_prior_high", "rolling_range_mean"],
        "random_entry": ["exchange_event_time", "seeded_rng"],
    }.get(strategy, [])


def metrics_from_equity(
    equity: np.ndarray,
    timestamps: np.ndarray,
    interval: str,
    initial_equity: float,
) -> dict[str, float]:
    if len(equity) == 0:
        return {"final_equity": initial_equity, "net_return": 0.0, "cagr": 0.0, "max_drawdown": 0.0, "sharpe": 0.0, "sortino": 0.0}
    final_equity = float(equity[-1])
    net_return = final_equity / initial_equity - 1.0 if initial_equity else 0.0
    elapsed_years = max((int(timestamps[-1]) - int(timestamps[0])) / YEAR_US, 1.0 / 365.25)
    cagr = (final_equity / initial_equity) ** (1.0 / elapsed_years) - 1.0 if final_equity > 0 and initial_equity > 0 else -1.0
    running_max = np.maximum.accumulate(equity)
    drawdown = equity / np.maximum(running_max, 1e-12) - 1.0
    returns = equity[1:] / np.maximum(equity[:-1], 1e-12) - 1.0
    # A missing source interval is not an observed regular-period return. Do
    # not let gap jumps contaminate volatility or annualization estimates.
    timestamp_deltas = np.diff(timestamps)
    regular = timestamp_deltas == INTERVAL_US[interval]
    regular_returns = returns[regular]
    periods_per_year = YEAR_US / INTERVAL_US[interval]
    std = float(np.std(regular_returns, ddof=1)) if len(regular_returns) > 1 else 0.0
    negative = regular_returns[regular_returns < 0]
    downside = float(np.std(negative, ddof=1)) if len(negative) > 1 else 0.0
    mean_return = float(np.mean(regular_returns)) if len(regular_returns) else 0.0
    sharpe = float(mean_return / std * math.sqrt(periods_per_year)) if std > 0 else 0.0
    sortino = float(mean_return / downside * math.sqrt(periods_per_year)) if downside > 0 else 0.0
    return {
        "final_equity": final_equity,
        "net_return": float(net_return),
        "cagr": float(cagr),
        "max_drawdown": float(np.min(drawdown)) if len(drawdown) else 0.0,
        "sharpe": sharpe,
        "sortino": sortino,
    }


def simulate(
    frame: pd.DataFrame,
    interval: str,
    split: Split,
    config: StrategyConfig,
    costs: CostModel,
    initial_equity: float,
    allocation: float,
    min_notional: float,
    seed: int,
    quality_counts: dict[str, int],
) -> SimulationResult:
    timestamps = frame["timestamp_us"].to_numpy(dtype=np.int64)
    opens = frame["open"].to_numpy(dtype=float)
    closes = frame["close"].to_numpy(dtype=float)
    quote_volumes = frame["quote_asset_volume"].to_numpy(dtype=float)
    gap_before = frame["gap_before"].to_numpy(dtype=bool)
    start_index = int(np.searchsorted(timestamps, split.start_us, side="left"))
    end_index = int(np.searchsorted(timestamps, split.end_us, side="left"))
    if start_index >= end_index:
        return SimulationResult(empty_summary(interval, split, config.name, frame, start_index, end_index, initial_equity, {}, quality_counts))

    entry_signal, exit_signal, signal_details = signal_arrays(frame, config, seed)
    signal_count = int(entry_signal[start_index:end_index].sum())
    if config.name == "no_trade":
        return SimulationResult(empty_summary(interval, split, config.name, frame, start_index, end_index, initial_equity, signal_details, quality_counts))

    cash = float(initial_equity)
    position: Position | None = None
    trades: list[dict[str, Any]] = []
    equity_values: list[float] = []
    equity_times: list[int] = []
    exposure_bars = 0
    skipped_min_notional = 0
    gap_exit_count = 0
    gap_reentry_count = 0
    turnover = 0.0

    def open_position(index: int, raw_price: float, quote_volume: float) -> None:
        nonlocal position, cash, skipped_min_notional, turnover
        position_candidate, result = execute_buy(
            cash,
            raw_price,
            quote_volume,
            allocation,
            min_notional,
            costs,
            index,
            int(timestamps[index]),
            config.holding_bars,
            decision_time_us=(int(timestamps[index - 1]) if index > start_index else None),
        )
        if result.get("skipped"):
            skipped_min_notional += 1
            return
        position = position_candidate
        cash = float(result["cash_after"])
        turnover += float(position.entry_notional)

    def close_position(index: int, reason: str, at_close: bool = False) -> None:
        nonlocal position, cash, turnover, gap_exit_count
        if position is None:
            return
        trade, proceeds = close_trade(
            position,
            frame,
            index,
            reason,
            split,
            interval,
            config.name,
            costs,
            at_close=at_close,
        )
        trades.append(trade)
        cash += proceeds
        turnover += float(trade["exit_notional"])
        if reason == "gap_exit":
            gap_exit_count += 1
        position = None

    if config.name == "buy_and_hold":
        open_position(
            start_index,
            float(opens[start_index]),
            float(quote_volumes[start_index - 1]) if start_index > 0 else float("nan"),
        )

    for index in range(start_index, end_index):
        if gap_before[index]:
            if position is not None and index > start_index:
                close_position(index, "gap_exit")
            # A bar following a gap cannot consume the preceding decision.
            if config.name != "buy_and_hold":
                equity_values.append(cash)
                equity_times.append(int(timestamps[index]))
                continue
            # The passive benchmark resumes from the first observable price
            # after the gap. The close/re-entry pair charges both sides of
            # the modeled execution cost and never invents an interim fill.
            if position is None:
                open_position(
                    index,
                    float(opens[index]),
                    float(quote_volumes[index - 1]) if index > 0 else float("nan"),
                )
                if position is not None:
                    gap_reentry_count += 1

        if position is not None and config.name != "buy_and_hold":
            due = index >= position.exit_due_index
            requested_exit = index > start_index and bool(exit_signal[index - 1]) and not gap_before[index]
            if due or requested_exit:
                close_position(index, "time_stop" if due else "signal_reversal")

        if position is None and config.name not in {"buy_and_hold", "no_trade"} and index > start_index:
            prior_contiguous = not gap_before[index]
            if prior_contiguous and bool(entry_signal[index - 1]):
                open_position(index, float(opens[index]), float(quote_volumes[index - 1]))

        if position is not None:
            exposure_bars += 1
            marked_equity = cash + position.quantity * float(closes[index])
        else:
            marked_equity = cash
        equity_values.append(float(marked_equity))
        equity_times.append(int(timestamps[index]))

    if position is not None:
        close_position(end_index - 1, "split_end", at_close=True)
        if equity_values:
            equity_values[-1] = float(cash)

    equity = np.asarray(equity_values, dtype=float)
    equity_times_array = np.asarray(equity_times, dtype=np.int64)
    metrics = metrics_from_equity(equity, equity_times_array, interval, initial_equity)
    pnl_values = np.asarray([trade["net_pnl"] for trade in trades], dtype=float)
    wins = pnl_values[pnl_values > 0]
    losses = pnl_values[pnl_values < 0]
    gross_wins = float(wins.sum()) if len(wins) else 0.0
    gross_losses = float(-losses.sum()) if len(losses) else 0.0
    total_fees = float(sum(trade["fees"] for trade in trades))
    total_spread = float(sum(trade["spread"] for trade in trades))
    total_slippage = float(sum(trade["slippage"] for trade in trades))
    total_impact = float(sum(trade["market_impact"] for trade in trades))
    summary = {
        "model_id": f"baseline-v1-{config.name}-{interval}-{split.name}",
        "strategy": config.name,
        "interval": interval,
        "symbol": "BTCUSDT",
        "exchange": "binance",
        "market": "spot",
        "split": split.name,
        "split_purpose": split.purpose,
        "start": split.start.isoformat(),
        "end": split.end.isoformat(),
        "bars": int(end_index - start_index),
        "initial_equity": initial_equity,
        **metrics,
        "trade_count": len(trades),
        "win_rate": float(len(wins) / len(trades)) if trades else 0.0,
        "average_win": float(wins.mean()) if len(wins) else 0.0,
        "average_loss": float(losses.mean()) if len(losses) else 0.0,
        "expectancy": float(pnl_values.mean()) if len(pnl_values) else 0.0,
        "gross_pnl_before_costs": float(sum(trade["gross_pnl_before_costs"] for trade in trades)),
        "net_pnl": float(sum(trade["net_pnl"] for trade in trades)),
        "total_modeled_costs": float(total_fees + total_spread + total_slippage + total_impact),
        "cost_to_gross_ratio": (
            float((total_fees + total_spread + total_slippage + total_impact) / abs(sum(trade["gross_pnl_before_costs"] for trade in trades)))
            if abs(sum(trade["gross_pnl_before_costs"] for trade in trades)) > 1e-12
            else None
        ),
        "profit_factor": float(gross_wins / gross_losses) if gross_losses > 0 else None,
        "turnover": float(turnover / initial_equity) if initial_equity else 0.0,
        "total_fees": total_fees,
        "total_spread": total_spread,
        "total_slippage": total_slippage,
        "total_market_impact": total_impact,
        "exposure_fraction": float(exposure_bars / max(end_index - start_index, 1)),
        "signal_count": signal_count,
        "skipped_min_notional": skipped_min_notional,
        "gap_exit_count": gap_exit_count,
        "gap_reentry_count": gap_reentry_count,
        "quality_counts": quality_counts,
        "strategy_parameters": {**signal_details, "holding_bars": config.holding_bars},
        "hypothesis": hypothesis_for_strategy(config.name),
        "dataset": "Binance BTCUSDT spot klines; exchange event-time UTC; quality_status=ok rows",
        "features": feature_names_for_strategy(config.name),
        "target": "net portfolio return after modeled fees, spread proxy, slippage proxy, and impact proxy",
        "train_period": "2019-01-01/2022-01-01",
        "validation_period": "2022-01-01/2023-01-01",
        "test_period": "2023-01-01/2025-01-01",
        "out_of_sample_result": "reported by chronological split fields in this record",
        "walk_forward_result": "not run in baseline-v1",
        "regime_dependency": "not classified in baseline-v1",
        "failure_modes": [
            "OHLCV-only execution proxy",
            "fixed fractional fills",
            "source gaps and excluded source-anomaly rows",
            "no order-book, queue, latency-distribution, or cross-exchange state",
        ],
        "overfitting_risk": "low relative to searched models because parameters are fixed a priori; economic validity remains unproven",
        "execution_model": {
            "signal_timing": "bar_close",
            "fill_timing": "next_bar_open",
            "latency_proxy": "one decision-bar to next-open interval; sub-bar latency unavailable",
            "fill_type": "fully_filled_fractional_position; no queue reconstruction",
            "intrabar_path_used_for_entries": False,
            "minimum_notional": min_notional,
            "allocation": allocation,
            "fee_rate": costs.fee_rate,
            "half_spread_rate_proxy": costs.half_spread_rate,
            "slippage_rate_proxy": costs.slippage_rate,
            "impact_floor_rate_proxy": costs.impact_floor_rate,
            "max_impact_rate_proxy": costs.max_impact_rate,
        },
        "position_policy": "Each chronological split starts flat and liquidates at split end; active strategies close across gaps, while BUY_AND_HOLD re-enters at the first post-gap observable open with modeled costs.",
        "deployment_status": "research_only",
    }
    return SimulationResult(summary, trades)


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.part")
    try:
        temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def write_trades(path: Path, trades: Iterable[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.part")
    count = 0
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            for trade in trades:
                stream.write(json.dumps(trade, sort_keys=True) + "\n")
                count += 1
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return count


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--intervals", type=parse_intervals, default=list(DEFAULT_INTERVALS))
    parser.add_argument("--initial-equity", type=float, default=10_000.0)
    parser.add_argument("--allocation", type=float, default=1.0)
    parser.add_argument("--min-notional", type=float, default=0.0)
    parser.add_argument("--fee-rate", type=float, default=0.001)
    parser.add_argument("--half-spread-rate", type=float, default=0.0005)
    parser.add_argument("--slippage-rate", type=float, default=0.0005)
    parser.add_argument("--impact-floor-rate", type=float, default=0.0001)
    parser.add_argument("--impact-participation-multiplier", type=float, default=0.5)
    parser.add_argument("--max-impact-rate", type=float, default=0.005)
    parser.add_argument("--random-probability", type=float, default=0.01)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--write-trades", action="store_true")
    parser.add_argument("--assumptions", type=Path, default=Path(__file__).with_name("research_assumptions.json"))
    parser.add_argument("--knowledge-base", type=Path, default=Path(__file__).with_name("knowledge_base_references.json"))
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.initial_equity <= 0:
        raise ValueError("--initial-equity must be positive")
    if not 0 < args.allocation <= 1:
        raise ValueError("--allocation must be in (0, 1]")
    for name in (
        "fee_rate",
        "half_spread_rate",
        "slippage_rate",
        "impact_floor_rate",
        "impact_participation_multiplier",
        "max_impact_rate",
        "random_probability",
    ):
        if getattr(args, name) < 0:
            raise ValueError(f"--{name.replace('_', '-')} must be non-negative")
    if args.random_probability > 1:
        raise ValueError("--random-probability must not exceed 1")
    if args.min_notional < 0:
        raise ValueError("--min-notional must be non-negative")


def run(args: argparse.Namespace) -> dict[str, Any]:
    validate_args(args)
    warehouse = args.warehouse.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    assumptions_path = args.assumptions.expanduser().resolve()
    knowledge_base_path = args.knowledge_base.expanduser().resolve()
    assumptions = load_json(assumptions_path)
    knowledge_base = load_json(knowledge_base_path)
    project_root = Path(__file__).resolve().parents[1]
    validate_assumptions(assumptions)
    validate_knowledge_base(knowledge_base, project_root)
    splits = default_splits()
    costs = CostModel(
        fee_rate=args.fee_rate,
        half_spread_rate=args.half_spread_rate,
        slippage_rate=args.slippage_rate,
        impact_floor_rate=args.impact_floor_rate,
        impact_participation_multiplier=args.impact_participation_multiplier,
        max_impact_rate=args.max_impact_rate,
    )

    reports: list[dict[str, Any]] = []
    trade_count = 0
    trade_temp: Path | None = None
    trade_stream = None
    if args.write_trades:
        output_dir.mkdir(parents=True, exist_ok=True)
        trade_temp = output_dir / f".trade_journal.jsonl.{uuid.uuid4().hex}.part"
        trade_stream = trade_temp.open("w", encoding="utf-8")
    generated_at = dt.datetime.now(UTC).isoformat()
    try:
        with sqlite3.connect(warehouse) as connection:
            for interval in args.intervals:
                print(f"loading {interval}", flush=True)
                frame, quality_counts = load_bars(connection, interval)
                configs = strategy_configs(interval, args.random_probability)
                frame = add_features(frame, interval, configs)
                for split_index, split in enumerate(splits):
                    for config_index, config in enumerate(configs):
                        seed = stable_seed(args.seed, interval, split.name, config.name, split_index, config_index)
                        result = simulate(
                            frame,
                            interval,
                            split,
                            config,
                            costs,
                            args.initial_equity,
                            args.allocation,
                            args.min_notional,
                            seed,
                            quality_counts,
                        )
                        reports.append(result.summary)
                        trade_count += len(result.trades)
                        if trade_stream is not None:
                            for trade in result.trades:
                                trade_stream.write(json.dumps(trade, sort_keys=True) + "\n")
                        print(
                            f"  {split.name} {config.name}: trades={result.summary['trade_count']} "
                            f"return={result.summary['net_return']:.6f}",
                            flush=True,
                        )

        experiment_spec = {
            "model_version": "baseline-v1",
            "code_sha256": sha256_file(Path(__file__).resolve()),
            "warehouse_sha256": sha256_file(warehouse),
            "intervals": args.intervals,
            "initial_equity": args.initial_equity,
            "allocation": args.allocation,
            "minimum_notional": args.min_notional,
            "cost_model": {
                "fee_rate": costs.fee_rate,
                "half_spread_rate_proxy": costs.half_spread_rate,
                "slippage_rate_proxy": costs.slippage_rate,
                "impact_floor_rate_proxy": costs.impact_floor_rate,
                "impact_participation_multiplier": costs.impact_participation_multiplier,
                "max_impact_rate_proxy": costs.max_impact_rate,
            },
            "random_seed": args.seed,
            "assumptions_sha256": sha256_file(assumptions_path),
            "knowledge_base_sha256": sha256_file(knowledge_base_path),
        }
        experiment_id = hashlib.sha256(
            json.dumps(experiment_spec, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()[:24]
        report = {
        "experiment_id": f"baseline-v1-{experiment_id}",
        "generated_at": generated_at,
        "experiment_spec": experiment_spec,
        "dataset": {
            "warehouse": str(warehouse),
            "warehouse_sha256": sha256_file(warehouse),
            "symbol": "BTCUSDT",
            "exchange": "binance",
            "market": "spot",
            "data_type": "klines",
            "timestamp_basis": "exchange event timestamp, normalized to UTC microseconds",
            "quality_policy": "only quality_status=ok rows are tradable; invalid/source-anomaly rows are excluded and gaps are explicit",
        },
        "features": {
            "signal_information_cutoff": "current bar close",
            "execution_information_cutoff": "next bar open",
            "future_data_used_for_signal": False,
            "warmup_policy": "Features are computed on the full chronological source frame; split evaluation may use only pre-split history for fixed, non-fitted indicators. No labels or future observations are used.",
            "gap_policy": "Invalid rows and source-anomaly rows are non-tradable; removing them creates explicit gap boundaries, and open positions are closed at the first post-gap executable open.",
            "strategies": [config.name for config in strategy_configs("1m", args.random_probability)],
        },
        "chronological_splits": [
            {"name": split.name, "start": split.start.isoformat(), "end": split.end.isoformat(), "purpose": split.purpose}
            for split in splits
        ],
        "cost_model": {
            "fee_rate": costs.fee_rate,
            "half_spread_rate_proxy": costs.half_spread_rate,
            "slippage_rate_proxy": costs.slippage_rate,
            "impact_floor_rate_proxy": costs.impact_floor_rate,
            "impact_participation_multiplier": costs.impact_participation_multiplier,
            "max_impact_rate_proxy": costs.max_impact_rate,
            "basis": "OHLCV-only conservative proxies; historical bid/ask, queue, and order-book state are unavailable",
        },
        "account_model": {
            "initial_equity": args.initial_equity,
            "allocation": args.allocation,
            "minimum_notional": args.min_notional,
            "leverage": 1.0,
            "withdrawals": 0.0,
            "live_execution": False,
        },
        "assumptions_path": str(assumptions_path),
        "assumptions_sha256": sha256_file(assumptions_path),
        "assumptions": assumptions,
        "knowledge_base_path": str(knowledge_base_path),
        "knowledge_base_sha256": sha256_file(knowledge_base_path),
        "knowledge_base_references": knowledge_base.get("references", []),
        "random_seed": args.seed,
        "reports": reports,
        "trade_count": trade_count,
        "deployment_status": "research_only",
        "decision": "NO_LIVE_TRADING",
        }
        atomic_write_json(output_dir / "baseline_report.json", report)
        if trade_stream is not None and trade_temp is not None:
            trade_stream.close()
            os.replace(trade_temp, output_dir / "trade_journal.jsonl")
            trade_stream = None
        return report
    finally:
        if trade_stream is not None:
            trade_stream.close()
        if trade_temp is not None:
            trade_temp.unlink(missing_ok=True)


def main() -> int:
    args = build_parser().parse_args()
    try:
        report = run(args)
    except Exception as exc:
        print(f"baseline backtest failed: {exc}", file=sys.stderr)
        return 1
    print(
        f"completed {len(report['reports'])} reports; "
        f"trades={report['trade_count']}; output={args.output_dir / 'baseline_report.json'}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
