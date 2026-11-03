#!/usr/bin/env python3
"""Evaluate predeclared volume/price hypotheses with causal diagnostics.

This is a research diagnostic, not an execution engine. Features are known at
the signal bar close; outcomes use the next retained bar's open and a future
close only for evaluation. Rows crossing a source gap are excluded. Volume
and quote volume are activity proxies, never order-book liquidity.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import sqlite3
import uuid
from collections import Counter, defaultdict
from collections import deque
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
from scipy import stats

from backtest_baselines import (
    INTERVAL_US,
    CostModel,
    default_splits,
    duration_bars,
    load_bars,
    load_json,
    sha256_file,
    stable_seed,
    validate_assumptions,
    validate_knowledge_base,
)


UTC = dt.timezone.utc
MIN_SAMPLE = 30
ALPHA = 0.05
HYPOTHESIS_NAMES = (
    "volume_confirmed_momentum",
    "volume_confirmed_reversal",
    "volume_breakout",
    "volume_dryup_reversal",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--intervals", default="1m,5m,15m,1h,4h,1d")
    parser.add_argument("--initial-equity", type=float, default=10_000.0)
    parser.add_argument("--fee-rate", type=float, default=0.001)
    parser.add_argument("--half-spread-rate", type=float, default=0.0005)
    parser.add_argument("--slippage-rate", type=float, default=0.0005)
    parser.add_argument("--impact-floor-rate", type=float, default=0.0001)
    parser.add_argument("--seed", type=int, default=20260909)
    parser.add_argument("--assumptions", type=Path, default=Path(__file__).with_name("research_assumptions.json"))
    parser.add_argument("--knowledge-base", type=Path, default=Path(__file__).with_name("knowledge_base_references.json"))
    return parser.parse_args()


def parse_intervals(value: str) -> list[str]:
    intervals = [part.strip() for part in value.split(",") if part.strip()]
    unknown = sorted(set(intervals) - set(INTERVAL_US))
    if unknown:
        raise ValueError(f"unsupported interval(s): {', '.join(unknown)}")
    if not intervals:
        raise ValueError("at least one interval is required")
    return intervals


def interval_windows(interval: str) -> dict[str, int]:
    minutes = INTERVAL_US[interval] / 60_000_000
    return {
        "trend": max(2, int(round(240 / minutes))),
        "volatility": max(5, int(round(240 / minutes))),
        "volume": max(20, int(round(60 / minutes))),
        "short_return": max(2, int(round(30 / minutes))),
    }


def horizons(interval: str) -> list[int]:
    minutes = INTERVAL_US[interval] / 60_000_000
    values = [1, max(2, int(round(30 / minutes))), max(3, int(round(240 / minutes)))]
    return list(dict.fromkeys(values))


def grouped_shift(values: pd.Series, groups: pd.Series, periods: int) -> pd.Series:
    return values.groupby(groups, sort=False).shift(periods)


def grouped_rolling(values: pd.Series, groups: pd.Series, window: int, operation: str) -> pd.Series:
    grouped = values.groupby(groups, sort=False)
    if operation == "mean":
        return grouped.transform(lambda series: series.rolling(window, min_periods=window).mean())
    if operation == "std":
        return grouped.transform(lambda series: series.rolling(window, min_periods=window).std(ddof=0))
    if operation == "median":
        return grouped.transform(lambda series: series.rolling(window, min_periods=window).median())
    if operation == "max":
        return grouped.transform(lambda series: series.rolling(window, min_periods=window).max())
    raise ValueError(f"unsupported rolling operation: {operation}")


def add_causal_features(frame: pd.DataFrame, interval: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    result = frame.copy()
    groups = result["segment_id"]
    windows = interval_windows(interval)
    close = result["close"]
    result["log_return"] = np.log(close / grouped_shift(close, groups, 1))
    result["trend_return"] = close / grouped_shift(close, groups, windows["trend"]) - 1.0
    result["short_return"] = close / grouped_shift(close, groups, windows["short_return"]) - 1.0
    result["realized_vol"] = grouped_rolling(result["log_return"], groups, windows["volatility"], "std")
    prior_vol_median = grouped_rolling(
        result["realized_vol"].groupby(groups, sort=False).shift(1),
        groups,
        windows["volatility"],
        "median",
    )
    result["volatility_ratio"] = result["realized_vol"] / prior_vol_median.replace(0.0, np.nan)
    prior_volume_median = grouped_rolling(
        result["quote_asset_volume"].groupby(groups, sort=False).shift(1),
        groups,
        windows["volume"],
        "median",
    )
    result["volume_ratio"] = result["quote_asset_volume"] / prior_volume_median.replace(0.0, np.nan)
    prior_high = grouped_shift(result["high"], groups, 1)
    result["prior_high"] = grouped_rolling(prior_high, groups, windows["trend"], "max")
    result["range_mean"] = grouped_rolling(result["high"] - result["low"], groups, windows["volatility"], "mean")
    result["breakout_strength"] = (close - result["prior_high"]) / result["range_mean"].replace(0.0, np.nan)
    result["same_as_previous"] = ~result["gap_before"]
    metadata = {"windows_bars": windows}
    return result, metadata


def assign_regimes(frame: pd.DataFrame) -> pd.Series:
    regime = np.full(len(frame), "UNKNOWN", dtype=object)
    valid = (
        frame["same_as_previous"]
        & frame["trend_return"].notna()
        & frame["short_return"].notna()
        & frame["realized_vol"].notna()
        & frame["volume_ratio"].notna()
    ).to_numpy()
    trend = frame["trend_return"].to_numpy(dtype=float)
    short = frame["short_return"].to_numpy(dtype=float)
    vol_ratio = frame["volatility_ratio"].to_numpy(dtype=float)
    volume_ratio = frame["volume_ratio"].to_numpy(dtype=float)
    # Fixed, causal precedence. These are descriptive labels, not latent
    # truth and not a substitute for an independently validated regime model.
    panic = valid & (short <= -0.03)
    euphoria = valid & (short >= 0.03)
    high_vol = valid & ~panic & ~euphoria & np.isfinite(vol_ratio) & (vol_ratio >= 2.0)
    low_vol = valid & ~panic & ~euphoria & ~high_vol & (np.isfinite(vol_ratio) & (vol_ratio <= 0.5))
    trending_up = valid & ~panic & ~euphoria & ~high_vol & ~low_vol & (trend >= 0.05)
    trending_down = valid & ~panic & ~euphoria & ~high_vol & ~low_vol & (trend <= -0.05)
    order = (
        (panic, "PANIC"),
        (euphoria, "EUPHORIA"),
        (high_vol, "HIGH_VOLATILITY"),
        (low_vol, "LOW_VOLATILITY"),
        (trending_up, "TRENDING_UP"),
        (trending_down, "TRENDING_DOWN"),
    )
    for mask, name in order:
        regime[mask] = name
    remaining = valid & (regime == "UNKNOWN")
    regime[remaining] = "SIDEWAYS"
    # Very high activity is retained as a separate descriptive flag only when
    # no stronger price/volatility state has already taken precedence.
    activity_spike = valid & (regime == "SIDEWAYS") & (volume_ratio >= 2.0)
    regime[activity_spike] = "ACTIVITY_SPIKE"
    return pd.Series(regime, index=frame.index, name="regime")


def hypothesis_masks(frame: pd.DataFrame) -> dict[str, tuple[np.ndarray, np.ndarray]]:
    valid = frame["same_as_previous"].to_numpy(dtype=bool)
    trend = frame["trend_return"].to_numpy(dtype=float)
    volume = frame["volume_ratio"].to_numpy(dtype=float)
    breakout = frame["breakout_strength"].to_numpy(dtype=float)
    conditions = {
        "volume_confirmed_momentum": valid & np.isfinite(trend) & np.isfinite(volume) & (trend >= 0.002) & (volume >= 1.5),
        "volume_confirmed_reversal": valid & np.isfinite(trend) & np.isfinite(volume) & (trend <= -0.002) & (volume >= 1.5),
        "volume_breakout": valid & np.isfinite(breakout) & np.isfinite(volume) & (breakout > 0.0) & (volume >= 1.5),
        "volume_dryup_reversal": valid & np.isfinite(trend) & np.isfinite(volume) & (trend <= -0.002) & (volume <= 0.5),
    }
    scores = {
        "volume_confirmed_momentum": np.abs(trend) * volume,
        "volume_confirmed_reversal": np.abs(trend) * volume,
        "volume_breakout": np.maximum(breakout, 0.0) * volume,
        "volume_dryup_reversal": np.abs(trend) * np.maximum(1.0 - volume, 0.0),
    }
    output: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    for name, condition in conditions.items():
        previous = np.r_[False, condition[:-1]]
        same_segment = frame["segment_id"].to_numpy()[1:] == frame["segment_id"].to_numpy()[:-1]
        edge = condition.copy()
        edge[1:] &= ~previous[1:] & same_segment
        edge[0] = False
        output[name] = (edge, scores[name])
    return output


def split_for_timestamp(timestamp_us: int, splits: list[Any]) -> str | None:
    for split in splits:
        if split.start_us <= timestamp_us < split.end_us:
            return split.name
    return None


def _forward_extreme(values: np.ndarray, groups: np.ndarray, horizon: int, find_min: bool) -> np.ndarray:
    """Compute [i+1, i+horizon] extrema in O(n) per segment."""
    result = np.full(len(values), np.nan, dtype=float)
    if horizon < 1:
        return result
    for start, end in _segment_ranges(groups):
        if end - start <= horizon:
            continue
        candidates: deque[int] = deque()

        def better(left: int, right: int) -> bool:
            return values[left] <= values[right] if find_min else values[left] >= values[right]

        for index in range(start + 1, min(end, start + horizon + 1)):
            while candidates and better(index, candidates[-1]):
                candidates.pop()
            candidates.append(index)
        for index in range(start, end - horizon):
            while candidates and candidates[0] <= index:
                candidates.popleft()
            if candidates:
                result[index] = values[candidates[0]]
            incoming = index + horizon + 1
            if incoming < end:
                while candidates and better(incoming, candidates[-1]):
                    candidates.pop()
                candidates.append(incoming)
    return result


def _segment_ranges(groups: np.ndarray) -> Iterable[tuple[int, int]]:
    if len(groups) == 0:
        return
    starts = np.flatnonzero(np.r_[True, groups[1:] != groups[:-1]])
    ends = np.r_[starts[1:], len(groups)]
    return zip(starts.tolist(), ends.tolist())


def future_extrema(frame: pd.DataFrame, horizon: int) -> tuple[np.ndarray, np.ndarray]:
    lows = frame["low"].to_numpy(dtype=float)
    highs = frame["high"].to_numpy(dtype=float)
    groups = frame["segment_id"].to_numpy()
    return _forward_extreme(lows, groups, horizon, True), _forward_extreme(highs, groups, horizon, False)


def one_sided_pvalue(values: np.ndarray) -> float | None:
    finite = values[np.isfinite(values)]
    if len(finite) < MIN_SAMPLE:
        return None
    mean = float(np.mean(finite))
    if np.allclose(finite, finite[0]):
        return 0.0 if mean > 0 else 1.0
    result = stats.ttest_1samp(finite, 0.0, alternative="greater")
    return float(result.pvalue)


def sign_test_pvalue(values: np.ndarray) -> float | None:
    finite = values[np.isfinite(values)]
    if len(finite) < MIN_SAMPLE:
        return None
    positives = int(np.sum(finite > 0.0))
    return float(stats.binomtest(positives, len(finite), 0.5, alternative="greater").pvalue)


def benjamini_hochberg(
    pvalues: list[float | None],
    family_size: int | None = None,
) -> list[float | None]:
    indexed = [(index, value) for index, value in enumerate(pvalues) if value is not None and math.isfinite(value)]
    result: list[float | None] = [None] * len(pvalues)
    if not indexed:
        return result
    indexed.sort(key=lambda item: item[1])
    family_count = family_size if family_size is not None else len(indexed)
    if family_count < len(indexed):
        raise ValueError("multiple-testing family size cannot be smaller than the valid p-value count")
    valid_count = len(indexed)
    running = 1.0
    for rank in range(valid_count, 0, -1):
        index, value = indexed[rank - 1]
        running = min(running, value * family_count / rank)
        result[index] = float(min(1.0, running))
    return result


def fit_calibration(train_scores: np.ndarray, train_labels: np.ndarray, bins: int = 10) -> dict[str, Any] | None:
    valid = np.isfinite(train_scores) & np.isfinite(train_labels)
    scores = train_scores[valid]
    labels = train_labels[valid]
    if len(scores) < MIN_SAMPLE:
        return None
    edges = np.unique(np.quantile(scores, np.linspace(0.0, 1.0, bins + 1)))
    if len(edges) < 2:
        probability = float((labels.sum() + 1.0) / (len(labels) + 2.0))
        return {"edges": [], "probabilities": [probability], "bin_count": 1}
    ids = np.clip(np.searchsorted(edges[1:-1], scores, side="right"), 0, len(edges) - 2)
    probabilities = []
    for bin_id in range(len(edges) - 1):
        selected = labels[ids == bin_id]
        probabilities.append(float((selected.sum() + 1.0) / (len(selected) + 2.0)) if len(selected) else float(labels.mean()))
    return {"edges": edges.tolist(), "probabilities": probabilities, "bin_count": len(probabilities)}


def calibrated_metrics(scores: np.ndarray, labels: np.ndarray, calibration: dict[str, Any] | None) -> dict[str, Any]:
    valid = np.isfinite(scores) & np.isfinite(labels)
    if calibration is None or not np.any(valid):
        return {"status": "insufficient_sample"}
    values = scores[valid]
    observed = labels[valid]
    if calibration["bin_count"] == 1:
        predicted = np.full(len(values), calibration["probabilities"][0], dtype=float)
    else:
        edges = np.asarray(calibration["edges"], dtype=float)
        ids = np.clip(np.searchsorted(edges[1:-1], values, side="right"), 0, calibration["bin_count"] - 1)
        predicted = np.asarray(calibration["probabilities"], dtype=float)[ids]
    clipped = np.clip(predicted, 1e-6, 1.0 - 1e-6)
    brier = float(np.mean((predicted - observed) ** 2))
    log_loss = float(-np.mean(observed * np.log(clipped) + (1.0 - observed) * np.log(1.0 - clipped)))
    ece = 0.0
    bins: list[dict[str, Any]] = []
    for lower, upper in zip(np.linspace(0.0, 1.0, 11)[:-1], np.linspace(0.0, 1.0, 11)[1:]):
        selected = (predicted >= lower) & ((predicted < upper) if upper < 1.0 else (predicted <= upper))
        if not np.any(selected):
            continue
        observed_rate = float(np.mean(observed[selected]))
        predicted_rate = float(np.mean(predicted[selected]))
        weight = float(np.mean(selected))
        ece += weight * abs(observed_rate - predicted_rate)
        bins.append({"lower": lower, "upper": upper, "count": int(np.sum(selected)), "predicted": predicted_rate, "observed": observed_rate})
    return {"status": "ok", "sample_size": int(len(observed)), "brier_score": brier, "log_loss": log_loss, "expected_calibration_error": float(ece), "bins": bins}


def summarize_events(
    frame: pd.DataFrame,
    event_indices: np.ndarray,
    scores: np.ndarray,
    hypothesis: str,
    interval: str,
    splits: list[Any],
    cost_hurdle: float,
    future_cache: dict[int, tuple[np.ndarray, np.ndarray]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    timestamps = frame["timestamp_us"].to_numpy(dtype=np.int64)
    opens = frame["open"].to_numpy(dtype=float)
    closes = frame["close"].to_numpy(dtype=float)
    regimes = frame["regime"].to_numpy(dtype=object)
    horizons_for_interval = horizons(interval)
    event_split = np.asarray([split_for_timestamp(int(timestamps[i]), splits) for i in event_indices], dtype=object)
    rows: list[dict[str, Any]] = []
    calibration_reports: list[dict[str, Any]] = []
    for horizon in horizons_for_interval:
        future_low, future_high = future_cache[horizon]
        entry_indices = event_indices + 1
        exit_indices = event_indices + horizon
        in_bounds = (entry_indices < len(frame)) & (exit_indices < len(frame))
        safe_entry_indices = np.minimum(entry_indices, len(frame) - 1)
        safe_exit_indices = np.minimum(exit_indices, len(frame) - 1)
        target_split = np.asarray(
            [
                split_for_timestamp(int(timestamps[index]), splits) if is_valid else None
                for index, is_valid in zip(exit_indices, in_bounds)
            ],
            dtype=object,
        )
        event_split_valid = np.asarray(
            [(value is not None) for value in event_split],
            dtype=bool,
        )
        valid = (
            in_bounds
            & event_split_valid
            & (target_split == event_split)
            & np.isfinite(future_low[event_indices])
            & np.isfinite(future_high[event_indices])
            & np.isfinite(opens[safe_entry_indices])
            & (opens[safe_entry_indices] > 0)
            & np.isfinite(closes[safe_exit_indices])
        )
        valid_indices = event_indices[valid]
        if len(valid_indices) == 0:
            continue
        entry = opens[valid_indices + 1]
        exit_price = closes[valid_indices + horizon]
        gross_array = exit_price / entry - 1.0
        net_array = gross_array - cost_hurdle
        mae_array = future_low[valid_indices] / entry - 1.0
        mfe_array = future_high[valid_indices] / entry - 1.0
        scores_valid = scores[valid]
        regimes_valid = regimes[valid_indices].astype(object)
        splits_valid = event_split[valid]
        labels = (net_array > 0.0).astype(float)
        for split_name in ["research_train", "validation_2022", "test_2023", "test_2024", "current_2025_present"]:
            split_mask = np.asarray(splits_valid, dtype=object) == split_name
            if not np.any(split_mask):
                continue
            for regime in sorted(set(np.asarray(regimes_valid, dtype=object)[split_mask])):
                regime_mask = split_mask & (np.asarray(regimes_valid, dtype=object) == regime)
                values = net_array[regime_mask]
                gross_values = gross_array[regime_mask]
                pvalues = one_sided_pvalue(values)
                sign_p = sign_test_pvalue(values)
                rows.append(
                    {
                        "interval": interval,
                        "hypothesis": hypothesis,
                        "horizon_bars": horizon,
                        "split": split_name,
                        "regime": regime,
                        "sample_size": int(len(values)),
                        "positive_count": int(np.sum(values > 0.0)),
                        "win_rate": float(np.mean(values > 0.0)),
                        "average_gross_return": float(np.mean(gross_values)),
                        "median_gross_return": float(np.median(gross_values)),
                        "average_net_return": float(np.mean(values)),
                        "median_net_return": float(np.median(values)),
                        "p_value_mean_positive": pvalues,
                        "p_value_win_rate_gt_50": sign_p,
                        "mean_adverse_excursion": float(np.mean(mae_array[regime_mask])),
                        "worst_adverse_excursion": float(np.min(mae_array[regime_mask])),
                        "mean_favorable_excursion": float(np.mean(mfe_array[regime_mask])),
                        "best_favorable_excursion": float(np.max(mfe_array[regime_mask])),
                        "cost_hurdle": cost_hurdle,
                        "status": "estimable" if len(values) >= MIN_SAMPLE else "insufficient_sample",
                    }
                )
            # Calibration is fit on training events only, then evaluated on
            # every split for this hypothesis/horizon.
            train_mask = np.asarray(splits_valid, dtype=object) == "research_train"
            calibration = fit_calibration(scores_valid[train_mask], labels[train_mask])
            split_scores = scores_valid[split_mask]
            split_labels = labels[split_mask]
            calibration_metrics = calibrated_metrics(split_scores, split_labels, calibration)
            calibration_reports.append(
                {
                    "interval": interval,
                    "hypothesis": hypothesis,
                    "horizon_bars": horizon,
                    "split": split_name,
                    "calibration_fit_split": "research_train",
                    "calibration": calibration,
                    **calibration_metrics,
                }
            )
    return rows, {"calibration": calibration_reports}


def correct_pvalues(rows: list[dict[str, Any]]) -> None:
    # Correct the mean-return p-values over the complete declared family for
    # each interval/split. This family is fixed before reading results.
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[(row["interval"], row["split"])].append(row)
    for key, group in groups.items():
        valid_rows = [row for row in group if row["p_value_mean_positive"] is not None]
        pvalues = [row["p_value_mean_positive"] for row in valid_rows]
        qvalues = benjamini_hochberg(pvalues, family_size=len(group))
        family_size = len(group)
        for row, qvalue in zip(valid_rows, qvalues):
            pvalue = float(row["p_value_mean_positive"])
            row["multiple_testing_family"] = f"{key[0]}:{key[1]}:all_hypotheses_regimes_horizons"
            row["multiple_testing_family_size"] = family_size
            row["bonferroni_p_value"] = min(1.0, pvalue * family_size)
            row["benjamini_hochberg_q_value"] = qvalue
            row["significant_bonferroni_alpha_005"] = row["bonferroni_p_value"] < ALPHA
            row["significant_bh_alpha_005"] = qvalue is not None and qvalue < ALPHA
        for row in group:
            if row["p_value_mean_positive"] is None:
                row["multiple_testing_family"] = f"{key[0]}:{key[1]}:all_hypotheses_regimes_horizons"
                row["multiple_testing_family_size"] = family_size
                row["bonferroni_p_value"] = None
                row["benjamini_hochberg_q_value"] = None
                row["significant_bonferroni_alpha_005"] = False
                row["significant_bh_alpha_005"] = False


def run(args: argparse.Namespace) -> dict[str, Any]:
    intervals = parse_intervals(args.intervals)
    assumptions = load_json(args.assumptions.expanduser().resolve())
    knowledge_base = load_json(args.knowledge_base.expanduser().resolve())
    project_root = Path(__file__).resolve().parents[1]
    validate_assumptions(assumptions)
    validate_knowledge_base(knowledge_base, project_root)
    splits = default_splits()
    cost_hurdle = 2.0 * (args.fee_rate + args.half_spread_rate + args.slippage_rate + args.impact_floor_rate)
    all_rows: list[dict[str, Any]] = []
    all_calibration: list[dict[str, Any]] = []
    regime_counts: dict[str, dict[str, int]] = {}
    feature_metadata: dict[str, Any] = {}
    with sqlite3.connect(args.warehouse.expanduser().resolve()) as connection:
        for interval in intervals:
            frame, quality_counts = load_bars(connection, interval)
            frame, metadata = add_causal_features(frame, interval)
            frame["regime"] = assign_regimes(frame)
            feature_metadata[interval] = {**metadata, "quality_counts": quality_counts}
            regime_counts[interval] = dict(Counter(frame["regime"].tolist()))
            masks = hypothesis_masks(frame)
            future_cache = {horizon: future_extrema(frame, horizon) for horizon in horizons(interval)}
            for hypothesis, (mask, scores) in masks.items():
                indices = np.flatnonzero(mask)
                rows, extra = summarize_events(frame, indices, scores[indices], hypothesis, interval, splits, cost_hurdle, future_cache)
                all_rows.extend(rows)
                all_calibration.extend(extra["calibration"])
    correct_pvalues(all_rows)
    return {
        "diagnostic_version": "volume-price-regime-v1",
        "generated_at": dt.datetime.now(UTC).isoformat(),
        "code_sha256": sha256_file(Path(__file__).resolve()),
        "dataset": {
            "warehouse": str(args.warehouse.expanduser().resolve()),
            "warehouse_sha256": sha256_file(args.warehouse.expanduser().resolve()),
            "symbol": "BTCUSDT",
            "exchange": "binance",
            "market": "spot",
            "data_type": "klines",
            "event_time_basis": "exchange timestamp normalized to UTC microseconds",
            "quality_policy": "quality_status=ok only; outcomes crossing segment gaps are excluded",
        },
        "knowledge_base_path": str(args.knowledge_base.expanduser().resolve()),
        "knowledge_base_sha256": sha256_file(args.knowledge_base.expanduser().resolve()),
        "knowledge_base_references": knowledge_base["references"],
        "assumptions_path": str(args.assumptions.expanduser().resolve()),
        "assumptions_sha256": sha256_file(args.assumptions.expanduser().resolve()),
        "assumptions": assumptions,
        "hypotheses": {
            "volume_confirmed_momentum": "Trailing return >= 0.2% and prior-volume-median ratio >= 1.5; test long forward returns.",
            "volume_confirmed_reversal": "Trailing return <= -0.2% and prior-volume-median ratio >= 1.5; test long reversal returns.",
                "volume_breakout": "Close exceeds prior rolling high and prior-volume-median ratio >= 1.5; test long continuation returns.",
            "volume_dryup_reversal": "Trailing return <= -0.2% and prior-volume-median ratio <= 0.5; test long reversal returns.",
        },
        "regime_definition": {
            "precedence": ["PANIC", "EUPHORIA", "HIGH_VOLATILITY", "LOW_VOLATILITY", "TRENDING_UP", "TRENDING_DOWN", "ACTIVITY_SPIKE", "SIDEWAYS", "UNKNOWN"],
            "thresholds": {
                "panic_short_return": "<= -3%",
                "euphoria_short_return": ">= 3%",
                "high_volatility_ratio": ">= 2x prior rolling median",
                "low_volatility_ratio": "<= 0.5x prior rolling median",
                "trending_return": ">= 5% or <= -5% over causal trend window",
                "activity_spike_volume_ratio": ">= 2x prior rolling median",
            },
            "interpretation": "descriptive OHLCV regime proxy, not a latent-state claim",
        },
        "cost_hurdle": {
            "round_trip_rate_proxy": cost_hurdle,
            "fee_rate": args.fee_rate,
            "half_spread_rate_proxy": args.half_spread_rate,
            "slippage_rate_proxy": args.slippage_rate,
            "impact_floor_rate_proxy": args.impact_floor_rate,
            "note": "No historical bid/ask, depth, queue, or market-impact observations are available.",
        },
        "multiple_testing": {
            "family": "all hypothesis x regime x horizon tests within each interval and split",
            "corrections": ["Bonferroni", "Benjamini-Hochberg"],
            "alpha": ALPHA,
            "selection_data": "No strategy is selected or deployed from this artifact.",
        },
        "feature_metadata": feature_metadata,
        "regime_counts": regime_counts,
        "conditional_results": all_rows,
        "calibration_results": all_calibration,
        "decision": "NO_DEPLOYMENT_CANDIDATE",
        "deployment_status": "research_only",
        "caveats": [
            "Volume and quote volume are activity proxies, not order-book liquidity.",
            "Forward returns are conditional descriptive outcomes, not guaranteed predictions.",
            "P-values use naive per-event tests; serial dependence and regime shifts remain risks.",
            "Multiple-testing corrections reduce false-discovery risk but do not create economic significance.",
            "Calibration is fit on research_train only; validation/test are not used to fit probabilities.",
        ],
    }


def main() -> int:
    args = parse_args()
    try:
        payload = run(args)
        output = args.output.expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_name(f".{output.name}.{uuid.uuid4().hex}.part")
        try:
            temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            os.replace(temporary, output)
        finally:
            temporary.unlink(missing_ok=True)
        print(f"wrote {output}")
        return 0
    except Exception as exc:
        print(f"signal diagnostics failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
