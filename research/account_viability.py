#!/usr/bin/env python3
"""Calculate conservative viability gates for very small accounts.

This is a sizing/cost analysis, not a profitability forecast. It uses no
future price information and does not submit orders. Exchange minimums and
fees must be supplied from current official venue documentation before live
use; defaults are intentionally labeled as scenarios.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import uuid
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--equity", type=float, default=3.0)
    parser.add_argument("--minimum-notional", type=float, default=10.0)
    parser.add_argument("--maker-fee-rate", type=float, default=0.001)
    parser.add_argument("--taker-fee-rate", type=float, default=0.001)
    parser.add_argument("--half-spread-rate", type=float, default=0.0005)
    parser.add_argument("--slippage-rate", type=float, default=0.0005)
    parser.add_argument("--impact-rate", type=float, default=0.005)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def run(args: argparse.Namespace) -> dict[str, Any]:
    values = (
        args.equity,
        args.minimum_notional,
        args.maker_fee_rate,
        args.taker_fee_rate,
        args.half_spread_rate,
        args.slippage_rate,
        args.impact_rate,
    )
    if any(not math.isfinite(value) or value < 0 for value in values):
        raise ValueError("all viability inputs must be finite and non-negative")
    if args.equity <= 0:
        raise ValueError("equity must be positive")

    round_trip_cost_rate = (
        args.taker_fee_rate * 2
        + args.half_spread_rate * 2
        + args.slippage_rate * 2
        + args.impact_rate * 2
    )
    minimum_round_trip_cost = args.minimum_notional * round_trip_cost_rate
    equity_sized_round_trip_cost = args.equity * round_trip_cost_rate
    feasible_at_minimum = args.equity >= args.minimum_notional
    # A positive gross move must exceed all modeled costs; this is a hurdle,
    # not a predicted return.
    minimum_gross_edge = round_trip_cost_rate
    decision = "PAPER_TRADING_RESEARCH_ONLY"
    if not feasible_at_minimum or equity_sized_round_trip_cost <= 0:
        decision = "DO_NOT_TRADE_LIVE"

    return {
        "analysis_version": "account-viability-v1",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "inputs": {
            "equity": args.equity,
            "minimum_notional_scenario": args.minimum_notional,
            "maker_fee_rate": args.maker_fee_rate,
            "taker_fee_rate": args.taker_fee_rate,
            "half_spread_rate_proxy": args.half_spread_rate,
            "slippage_rate_proxy": args.slippage_rate,
            "impact_rate_proxy": args.impact_rate,
        },
        "derived": {
            "round_trip_cost_rate": round_trip_cost_rate,
            "minimum_round_trip_cost_at_minimum_notional": minimum_round_trip_cost,
            "round_trip_cost_at_account_equity": equity_sized_round_trip_cost,
            "minimum_gross_edge_hurdle": minimum_gross_edge,
            "minimum_notional_feasible": feasible_at_minimum,
            "risk_of_ruin_if_costs_exceed_edge": "unacceptable",
        },
        "assumptions": [
            "Minimum notional is a scenario and must be verified per venue, symbol, and account tier.",
            "Fee rates are scenarios and must be replaced by current official exchange schedules.",
            "Spread, slippage, and impact are conservative proxies; no historical order-book data is used.",
            "No return, fill probability, or profit is assumed.",
        ],
        "decision": decision,
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
        print(f"account viability failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
