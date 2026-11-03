#!/usr/bin/env python3
"""Analyze baseline backtest reports without treating them as proof of edge.

The analyzer is intentionally report-only: it does not search parameters,
select a live strategy, or modify the underlying warehouse. It summarizes
chronological out-of-sample stability, comparison with NO_TRADE and
BUY_AND_HOLD, modeled-cost burden, and explicit deployment gates.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import statistics
import uuid
from pathlib import Path
from typing import Any


OOS_SPLITS = (
    "validation_2022",
    "test_2023",
    "test_2024",
    "current_2025_present",
)
MIN_OOS_TRADES_PER_SPLIT = 10
STRATEGY_ORDER = (
    "buy_and_hold",
    "simple_momentum",
    "simple_mean_reversion",
    "volatility_breakout",
    "random_entry",
    "no_trade",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports", type=Path, nargs="+", required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def mean_or_zero(values: list[float]) -> float:
    return float(statistics.fmean(values)) if values else 0.0


def median_or_zero(values: list[float]) -> float:
    return float(statistics.median(values)) if values else 0.0


def report_label(path: Path, index: int) -> str:
    return path.parent.name or f"report_{index}"


def analyze_one(payload: dict[str, Any], path: Path, index: int) -> dict[str, Any]:
    reports = payload.get("reports", [])
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for report in reports:
        groups.setdefault((report["interval"], report["strategy"]), []).append(report)

    summaries: list[dict[str, Any]] = []
    for (interval, strategy), rows in sorted(groups.items()):
        by_split = {row["split"]: row for row in rows}
        oos = [by_split[name] for name in OOS_SPLITS if name in by_split]
        no_trade = {row["split"]: row for row in groups.get((interval, "no_trade"), [])}
        buy_hold = {row["split"]: row for row in groups.get((interval, "buy_and_hold"), [])}
        oos_returns = [float(row["net_return"]) for row in oos]
        oos_drawdowns = [float(row["max_drawdown"]) for row in oos]
        oos_sharpes = [float(row["sharpe"]) for row in oos]
        oos_positive = sum(value > 0.0 for value in oos_returns)
        sufficient_oos_trades = all(
            int(row["trade_count"]) >= MIN_OOS_TRADES_PER_SPLIT for row in oos
        ) if oos else False
        beats_no_trade = [
            float(row["net_return"]) > float(no_trade[name]["net_return"])
            for name, row in ((name, by_split[name]) for name in OOS_SPLITS if name in by_split and name in no_trade)
        ]
        beats_buy_hold = [
            float(by_split[name]["net_return"]) > float(buy_hold[name]["net_return"])
            for name in OOS_SPLITS
            if name in by_split and name in buy_hold
        ]
        # This is deliberately a strict descriptive gate, not a claim of
        # statistical significance. A candidate must survive every OOS slice,
        # beat abstention, and avoid a near-total loss in every slice.
        passes_descriptive_gate = bool(oos) and (
            oos_positive == len(oos)
            and all(beats_no_trade)
            and min(oos_returns) > -0.5
            and sufficient_oos_trades
        )
        gross_pnl = sum(float(row.get("gross_pnl_before_costs", 0.0)) for row in oos)
        modeled_costs = sum(
            float(row["total_fees"])
            + float(row["total_spread"])
            + float(row["total_slippage"])
            + float(row["total_market_impact"])
            for row in oos
        )
        summaries.append(
            {
                "interval": interval,
                "strategy": strategy,
                "oos_splits": [row["split"] for row in oos],
                "oos_count": len(oos),
                "oos_positive_count": oos_positive,
                "oos_positive_fraction": oos_positive / len(oos) if oos else 0.0,
                "oos_mean_return": mean_or_zero(oos_returns),
                "oos_median_return": median_or_zero(oos_returns),
                "oos_worst_return": min(oos_returns) if oos_returns else 0.0,
                "oos_mean_sharpe": mean_or_zero(oos_sharpes),
                "oos_worst_drawdown": min(oos_drawdowns) if oos_drawdowns else 0.0,
                "oos_trade_count": sum(int(row["trade_count"]) for row in oos),
                "minimum_oos_trades_per_split": MIN_OOS_TRADES_PER_SPLIT,
                "sufficient_oos_trades": sufficient_oos_trades,
                "oos_total_fees": sum(float(row["total_fees"]) for row in oos),
                "oos_total_modeled_costs": modeled_costs,
                "oos_gross_pnl_before_costs": gross_pnl,
                "oos_cost_to_gross_ratio": (modeled_costs / abs(gross_pnl)) if abs(gross_pnl) > 1e-12 else None,
                "beats_no_trade_all_oos": bool(beats_no_trade) and all(beats_no_trade),
                "beats_buy_and_hold_count": sum(beats_buy_hold),
                "beats_buy_and_hold_fraction": (sum(beats_buy_hold) / len(beats_buy_hold)) if beats_buy_hold else 0.0,
                "passes_descriptive_gate": passes_descriptive_gate,
                "deployment_status": "research_only",
            }
        )

    passing = [
        row
        for row in summaries
        if row["passes_descriptive_gate"]
        and row["strategy"] not in {"buy_and_hold", "no_trade", "random_entry"}
    ]
    return {
        "label": report_label(path, index),
        "path": str(path.resolve()),
        "sha256": sha256_file(path),
        "experiment_id": payload.get("experiment_id"),
        "warehouse_sha256": payload.get("dataset", {}).get("warehouse_sha256"),
        "cost_model": payload.get("cost_model"),
        "summaries": summaries,
        "descriptive_gate_pass_count": len(passing),
        "descriptive_gate_passes": passing,
        "decision": "NO_DEPLOYMENT_CANDIDATE" if not passing else "RESEARCH_CANDIDATE_ONLY",
    }


def compare(reports: list[dict[str, Any]]) -> dict[str, Any]:
    by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    for report in reports:
        for summary in report["summaries"]:
            by_key[(report["label"], summary["interval"], summary["strategy"])] = summary
    sensitivity: list[dict[str, Any]] = []
    keys = sorted({(summary["interval"], summary["strategy"]) for report in reports for summary in report["summaries"]})
    for interval, strategy in keys:
        entries = []
        for report in reports:
            summary = by_key.get((report["label"], interval, strategy))
            if summary is not None:
                entries.append(
                    {
                        "label": report["label"],
                        "oos_median_return": summary["oos_median_return"],
                        "oos_worst_return": summary["oos_worst_return"],
                        "oos_trade_count": summary["oos_trade_count"],
                        "passes_descriptive_gate": summary["passes_descriptive_gate"],
                    }
                )
        sensitivity.append({"interval": interval, "strategy": strategy, "scenarios": entries})
    return {"cost_sensitivity": sensitivity}


def main() -> int:
    args = parse_args()
    reports: list[dict[str, Any]] = []
    for index, path_arg in enumerate(args.reports):
        path = path_arg.expanduser().resolve()
        payload = json.loads(path.read_text(encoding="utf-8"))
        reports.append(analyze_one(payload, path, index))
    payload = {
        "analysis_version": "baseline-analysis-v1",
        "generated_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "input_reports": reports,
        **compare(reports),
        "global_decision": "NO_DEPLOYMENT",
        "caveats": [
            "This is descriptive baseline analysis, not proof of statistical significance.",
            "OHLCV-only spread, slippage, and impact values are proxies, not observed order-book execution.",
            "No parameter search, probability calibration, regime model, or walk-forward retraining was run.",
            "Tick/trade/order-book data is required before making microstructure claims.",
            "RANDOM_ENTRY is retained only as a non-predictive baseline and is never an eligible deployment candidate.",
            f"Descriptive OOS gates require at least {MIN_OOS_TRADES_PER_SPLIT} trades in every OOS split; this is not a significance test.",
        ],
    }
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


if __name__ == "__main__":
    raise SystemExit(main())
