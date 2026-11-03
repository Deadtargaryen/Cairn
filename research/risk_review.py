#!/usr/bin/env python3
"""Estimate empirical drawdown and ruin diagnostics from a trade journal.

This is a diagnostic, not a guarantee. It bootstraps chronological out-of-
sample trade returns in short blocks, which preserves a limited amount of
serial dependence but cannot reproduce changing regimes, liquidity, or future
execution conditions. Results are withheld when the observed sample is too
small.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np


OOS_SPLITS = {"validation_2022", "test_2023", "test_2024", "current_2025_present"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--simulations", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=20260909)
    parser.add_argument("--min-trades", type=int, default=30)
    parser.add_argument("--horizon", type=int, default=1000)
    return parser.parse_args()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def drawdown(path: np.ndarray) -> float:
    peaks = np.maximum.accumulate(path)
    return float(np.min(path / np.maximum(peaks, 1e-12) - 1.0)) if len(path) else 0.0


def bootstrap(returns: np.ndarray, simulations: int, horizon: int, seed: int) -> dict[str, Any]:
    if len(returns) == 0:
        return {}
    block_length = max(1, min(20, len(returns) // 10))
    target_horizon = max(1, min(horizon, max(len(returns), block_length)))
    rng = np.random.default_rng(seed)
    max_drawdowns: list[float] = []
    terminal: list[float] = []
    ruin_levels = {"50pct": 0, "75pct": 0, "90pct": 0, "100pct": 0}
    for _ in range(simulations):
        sampled: list[float] = []
        while len(sampled) < target_horizon:
            start = int(rng.integers(0, max(1, len(returns) - block_length + 1)))
            sampled.extend(returns[start : start + block_length].tolist())
        sequence = np.asarray(sampled[:target_horizon], dtype=float)
        equity = np.cumprod(np.maximum(0.0, 1.0 + sequence))
        path = np.r_[1.0, equity]
        max_drawdowns.append(drawdown(path))
        terminal_value = float(path[-1])
        terminal.append(terminal_value)
        if np.any(path <= 0.5):
            ruin_levels["50pct"] += 1
        if np.any(path <= 0.25):
            ruin_levels["75pct"] += 1
        if np.any(path <= 0.10):
            ruin_levels["90pct"] += 1
        if np.any(path <= 1e-6):
            ruin_levels["100pct"] += 1
    return {
        "simulations": simulations,
        "horizon_trades": target_horizon,
        "block_length": block_length,
        "probability_drawdown_at_least_50pct": ruin_levels["50pct"] / simulations,
        "probability_drawdown_at_least_75pct": ruin_levels["75pct"] / simulations,
        "probability_drawdown_at_least_90pct": ruin_levels["90pct"] / simulations,
        "probability_practical_total_loss": ruin_levels["100pct"] / simulations,
        "median_max_drawdown": float(np.median(max_drawdowns)),
        "worst_simulated_drawdown": float(np.min(max_drawdowns)),
        "median_terminal_multiple": float(np.median(terminal)),
        "terminal_multiple_p05": float(np.quantile(terminal, 0.05)),
        "terminal_multiple_p95": float(np.quantile(terminal, 0.95)),
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    if args.simulations < 1 or args.min_trades < 1 or args.horizon < 1:
        raise ValueError("simulations, min-trades, and horizon must be positive")
    journal = args.journal.expanduser().resolve()
    baseline_path = args.baseline.expanduser().resolve()
    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    returns: dict[tuple[str, str], list[float]] = defaultdict(list)
    with journal.open(encoding="utf-8") as stream:
        for line in stream:
            if not line.strip():
                continue
            trade = json.loads(line)
            if trade.get("split") not in OOS_SPLITS:
                continue
            if trade.get("strategy") in {"buy_and_hold", "no_trade", "random_entry"}:
                continue
            value = float(trade.get("return_on_allocated_capital", float("nan")))
            if math.isfinite(value):
                returns[(trade["interval"], trade["strategy"])].append(max(-0.999999, value))

    reports: list[dict[str, Any]] = []
    for index, ((interval, strategy), values) in enumerate(sorted(returns.items())):
        array = np.asarray(values, dtype=float)
        report: dict[str, Any] = {
            "interval": interval,
            "strategy": strategy,
            "observed_oos_trades": len(values),
            "risk_estimate_status": "estimated" if len(values) >= args.min_trades else "insufficient_sample",
            "deployment_status": "research_only",
        }
        if len(values) >= args.min_trades:
            report.update(bootstrap(array, args.simulations, args.horizon, args.seed + index))
        else:
            report["reason"] = f"fewer than {args.min_trades} out-of-sample trades"
        reports.append(report)

    return {
        "report_version": "risk-review-v1",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "journal": str(journal),
        "journal_sha256": sha256(journal),
        "baseline": str(baseline_path),
        "baseline_sha256": sha256(baseline_path),
        "oos_splits": sorted(OOS_SPLITS),
        "excluded_strategies": ["buy_and_hold", "no_trade", "random_entry"],
        "bootstrap_assumptions": {
            "method": "chronological block bootstrap of per-trade net returns",
            "regime_change_modeled": False,
            "liquidity_change_modeled": False,
            "execution_uncertainty_modeled": False,
            "practical_total_loss_threshold": "equity <= 1e-6 of starting equity",
            "interpretation": "diagnostic only; not a probability guarantee",
        },
        "reports": reports,
        "candidate_count": 0,
        "decision": "NO_DEPLOYMENT_CANDIDATE",
        "deployment_status": "research_only",
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
        print(f"risk review failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
