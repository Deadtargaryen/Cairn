from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import analyze_baselines as analyzer


def report(strategy: str, interval: str, split: str, value: float) -> dict[str, object]:
    return {
        "interval": interval,
        "strategy": strategy,
        "split": split,
        "net_return": value,
        "max_drawdown": value,
        "sharpe": value,
        "trade_count": 1,
        "total_fees": 1.0,
        "total_spread": 1.0,
        "total_slippage": 1.0,
        "total_market_impact": 1.0,
    }


class AnalyzerTests(unittest.TestCase):
    def test_strict_gate_requires_positive_all_oos_slices(self) -> None:
        rows = []
        splits = ["research_train", *analyzer.OOS_SPLITS]
        for split in splits:
            rows.append(report("no_trade", "1d", split, 0.0))
            rows.append(report("buy_and_hold", "1d", split, 0.1))
            momentum = report("simple_momentum", "1d", split, 0.1)
            momentum["trade_count"] = 20
            rows.append(momentum)
        payload = {"reports": rows, "experiment_id": "x", "dataset": {}, "cost_model": {}}
        with tempfile.TemporaryDirectory() as directory:
            report_path = Path(directory) / "test-report.json"
            report_path.write_text("{}", encoding="utf-8")
            result = analyzer.analyze_one(payload, report_path, 0)

        momentum = next(row for row in result["summaries"] if row["strategy"] == "simple_momentum")
        self.assertTrue(momentum["passes_descriptive_gate"])

    def test_cost_comparison_preserves_scenario_labels(self) -> None:
        first = {"label": "low", "summaries": [{"interval": "1d", "strategy": "no_trade", "oos_median_return": 0, "oos_worst_return": 0, "oos_trade_count": 0, "passes_descriptive_gate": False}]}
        second = {"label": "high", "summaries": [{"interval": "1d", "strategy": "no_trade", "oos_median_return": 0, "oos_worst_return": 0, "oos_trade_count": 0, "passes_descriptive_gate": False}]}

        result = analyzer.compare([first, second])

        labels = [item["label"] for item in result["cost_sensitivity"][0]["scenarios"]]
        self.assertEqual(labels, ["low", "high"])

    def test_random_entry_is_not_a_deployment_candidate(self) -> None:
        rows = []
        for split in ["research_train", *analyzer.OOS_SPLITS]:
            rows.append(report("no_trade", "1d", split, 0.0))
            rows.append(report("buy_and_hold", "1d", split, 0.1))
            random_row = report("random_entry", "1d", split, 0.1)
            random_row["trade_count"] = 20
            rows.append(random_row)
        with tempfile.TemporaryDirectory() as directory:
            report_path = Path(directory) / "test-report.json"
            report_path.write_text("{}", encoding="utf-8")
            result = analyzer.analyze_one(
                {"reports": rows, "experiment_id": "x", "dataset": {}, "cost_model": {}},
                report_path,
                0,
            )
        self.assertEqual(result["descriptive_gate_pass_count"], 0)


if __name__ == "__main__":
    unittest.main()
