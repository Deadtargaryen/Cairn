#!/usr/bin/env python3
"""Run a fixed-rule walk-forward selection diagnostic.

This evaluator uses chronological train/validation/test folds. The baseline
rules are not fitted, and no parameters are searched. A small predeclared
candidate set is selected using validation net return subject to a minimum
validation trade count; the selected rule is then evaluated once on the next
unseen test window. ``NO_TRADE`` is the deterministic fallback.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

from backtest_baselines import (
    CostModel,
    Split,
    add_features,
    default_splits,
    load_bars,
    load_json,
    sha256_file,
    simulate,
    stable_seed,
    strategy_configs,
    utc_datetime,
    validate_assumptions,
    validate_knowledge_base,
)


UTC = dt.timezone.utc
SELECTABLE = ("no_trade", "simple_momentum", "simple_mean_reversion", "volatility_breakout")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--intervals", default="1m,5m,15m,1h,4h,1d")
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
    parser.add_argument("--seed", type=int, default=20260909)
    parser.add_argument("--min-validation-trades", type=int, default=10)
    parser.add_argument("--assumptions", type=Path, default=Path(__file__).with_name("research_assumptions.json"))
    parser.add_argument("--knowledge-base", type=Path, default=Path(__file__).with_name("knowledge_base_references.json"))
    return parser.parse_args()


def parse_intervals(value: str) -> list[str]:
    intervals = [part.strip() for part in value.split(",") if part.strip()]
    if not intervals:
        raise ValueError("at least one interval is required")
    return intervals


def walk_forward_folds() -> list[dict[str, Split]]:
    def make(name: str, start: str, end: str, purpose: str) -> Split:
        return Split(name, utc_datetime(start), utc_datetime(end), purpose)

    return [
        {
            "name": "wf_2023",
            "train": make("wf_2023_train", "2019-01-01T00:00:00+00:00", "2022-01-01T00:00:00+00:00", "walk_forward_train"),
            "validation": make("wf_2023_validation", "2022-01-01T00:00:00+00:00", "2023-01-01T00:00:00+00:00", "walk_forward_validation"),
            "test": make("wf_2023_test", "2023-01-01T00:00:00+00:00", "2024-01-01T00:00:00+00:00", "walk_forward_unseen_test"),
        },
        {
            "name": "wf_2024",
            "train": make("wf_2024_train", "2020-01-01T00:00:00+00:00", "2023-01-01T00:00:00+00:00", "walk_forward_train"),
            "validation": make("wf_2024_validation", "2023-01-01T00:00:00+00:00", "2024-01-01T00:00:00+00:00", "walk_forward_validation"),
            "test": make("wf_2024_test", "2024-01-01T00:00:00+00:00", "2025-01-01T00:00:00+00:00", "walk_forward_unseen_test"),
        },
        {
            "name": "wf_2025_present",
            "train": make("wf_2025_train", "2021-01-01T00:00:00+00:00", "2024-01-01T00:00:00+00:00", "walk_forward_train"),
            "validation": make("wf_2025_validation", "2024-01-01T00:00:00+00:00", "2025-01-01T00:00:00+00:00", "walk_forward_validation"),
            "test": make("wf_2025_test", "2025-01-01T00:00:00+00:00", "2026-09-09T00:00:00+00:00", "walk_forward_unseen_test"),
        },
    ]


def select_validation_candidate(
    candidates: list[dict[str, Any]],
    minimum_trades: int,
) -> tuple[str, str]:
    eligible = [candidate for candidate in candidates if candidate["trade_count"] >= minimum_trades]
    if not eligible:
        return "no_trade", "no candidate met minimum validation trade count"
    # Stable tie-breaking is deliberate: abstention wins ties, then fixed
    # lexical order. No test-period values enter this selection.
    eligible.sort(key=lambda item: (-float(item["net_return"]), item["strategy"]))
    best = eligible[0]
    if best["net_return"] <= 0:
        return "no_trade", "best eligible validation return was non-positive"
    return str(best["strategy"]), "highest positive validation net return"


def atomic_write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.part")
    try:
        temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def run(args: argparse.Namespace) -> dict[str, Any]:
    warehouse = args.warehouse.expanduser().resolve()
    assumptions_path = args.assumptions.expanduser().resolve()
    knowledge_base_path = args.knowledge_base.expanduser().resolve()
    assumptions = load_json(assumptions_path)
    knowledge_base = load_json(knowledge_base_path)
    project_root = Path(__file__).resolve().parents[1]
    validate_assumptions(assumptions)
    validate_knowledge_base(knowledge_base, project_root)
    intervals = parse_intervals(args.intervals)
    costs = CostModel(
        fee_rate=args.fee_rate,
        half_spread_rate=args.half_spread_rate,
        slippage_rate=args.slippage_rate,
        impact_floor_rate=args.impact_floor_rate,
        impact_participation_multiplier=args.impact_participation_multiplier,
        max_impact_rate=args.max_impact_rate,
    )
    folds = walk_forward_folds()
    fold_reports: list[dict[str, Any]] = []

    with sqlite3.connect(warehouse) as connection:
        for interval in intervals:
            frame, quality_counts = load_bars(connection, interval)
            configs = {config.name: config for config in strategy_configs(interval, args.random_probability)}
            frame = add_features(frame, interval, list(configs.values()))
            for fold_index, fold in enumerate(folds):
                validation_reports: list[dict[str, Any]] = []
                for candidate_index, strategy_name in enumerate(SELECTABLE):
                    config = configs[strategy_name]
                    result = simulate(
                        frame,
                        interval,
                        fold["validation"],
                        config,
                        costs,
                        args.initial_equity,
                        args.allocation,
                        args.min_notional,
                        stable_seed(args.seed, interval, fold["name"], "validation", strategy_name, candidate_index),
                        quality_counts,
                    )
                    validation_reports.append(result.summary)
                selected, selection_reason = select_validation_candidate(validation_reports, args.min_validation_trades)
                selected_config = configs[selected]
                train_result = simulate(
                    frame,
                    interval,
                    fold["train"],
                    selected_config,
                    costs,
                    args.initial_equity,
                    args.allocation,
                    args.min_notional,
                    stable_seed(args.seed, interval, fold["name"], "train", selected),
                    quality_counts,
                )
                test_result = simulate(
                    frame,
                    interval,
                    fold["test"],
                    selected_config,
                    costs,
                    args.initial_equity,
                    args.allocation,
                    args.min_notional,
                    stable_seed(args.seed, interval, fold["name"], "test", selected),
                    quality_counts,
                )
                fold_reports.append(
                    {
                        "fold": fold["name"],
                        "interval": interval,
                        "train": train_result.summary,
                        "validation_candidates": validation_reports,
                        "selected_strategy": selected,
                        "selection_reason": selection_reason,
                        "test": test_result.summary,
                        "selection_uses_test_data": False,
                        "parameters_fitted": False,
                        "deployment_status": "research_only",
                    }
                )
                print(
                    f"{interval} {fold['name']}: selected={selected} "
                    f"test_return={test_result.summary['net_return']:.6f}",
                    flush=True,
                )

    spec = {
        "model_version": "walk-forward-baseline-v1",
        "warehouse_sha256": sha256_file(warehouse),
        "code_sha256": sha256_file(Path(__file__).resolve()),
        "intervals": intervals,
        "cost_model": {
            "fee_rate": costs.fee_rate,
            "half_spread_rate_proxy": costs.half_spread_rate,
            "slippage_rate_proxy": costs.slippage_rate,
            "impact_floor_rate_proxy": costs.impact_floor_rate,
            "impact_participation_multiplier": costs.impact_participation_multiplier,
            "max_impact_rate_proxy": costs.max_impact_rate,
        },
        "minimum_validation_trades": args.min_validation_trades,
        "seed": args.seed,
    }
    experiment_id = hashlib.sha256(json.dumps(spec, sort_keys=True).encode("utf-8")).hexdigest()[:24]
    return {
        "experiment_id": f"walk-forward-baseline-v1-{experiment_id}",
        "generated_at": dt.datetime.now(UTC).isoformat(),
        "experiment_spec": spec,
        "dataset": {
            "warehouse": str(warehouse),
            "symbol": "BTCUSDT",
            "exchange": "binance",
            "market": "spot",
            "timestamp_basis": "exchange event timestamp, normalized UTC microseconds",
            "quality_policy": "quality_status=ok only; gaps force explicit position handling",
        },
        "knowledge_base_path": str(knowledge_base_path),
        "knowledge_base_sha256": sha256_file(knowledge_base_path),
        "knowledge_base_references": knowledge_base["references"],
        "assumptions_path": str(assumptions_path),
        "assumptions_sha256": sha256_file(assumptions_path),
        "assumptions": assumptions,
        "folds": [
            {
                "name": fold["name"],
                "train": {"start": fold["train"].start.isoformat(), "end": fold["train"].end.isoformat()},
                "validation": {"start": fold["validation"].start.isoformat(), "end": fold["validation"].end.isoformat()},
                "test": {"start": fold["test"].start.isoformat(), "end": fold["test"].end.isoformat()},
            }
            for fold in folds
        ],
        "reports": fold_reports,
        "selection_policy": "Select only from a fixed candidate set using validation net return and a minimum validation trade count; fall back to NO_TRADE for non-positive or insufficient validation evidence.",
        "walk_forward_policy": "Train windows provide historical context; baseline parameters are fixed and no model fitting occurs. Test windows are evaluated once after validation selection.",
        "decision": "NO_LIVE_TRADING",
        "deployment_status": "research_only",
    }


def main() -> int:
    args = parse_args()
    try:
        payload = run(args)
        atomic_write(args.output.expanduser().resolve(), payload)
        print(f"wrote {args.output}")
        return 0
    except Exception as exc:
        print(f"walk-forward evaluation failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
