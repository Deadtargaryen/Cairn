from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import risk_review


class RiskReviewTests(unittest.TestCase):
    def test_bootstrap_returns_bounded_probabilities(self) -> None:
        result = risk_review.bootstrap(__import__("numpy").array([-0.1, 0.05, -0.02]), 20, 30, 7)
        self.assertGreaterEqual(result["probability_drawdown_at_least_50pct"], 0.0)
        self.assertLessEqual(result["probability_drawdown_at_least_50pct"], 1.0)
        self.assertGreaterEqual(result["probability_practical_total_loss"], 0.0)
        self.assertLessEqual(result["probability_practical_total_loss"], 1.0)

    def test_insufficient_sample_is_not_estimated(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            journal = root / "journal.jsonl"
            journal.write_text(json.dumps({"split": "test_2023", "interval": "1d", "strategy": "simple_momentum", "return_on_allocated_capital": 0.1}) + "\n")
            baseline = root / "baseline.json"
            baseline.write_text("{}")
            result = risk_review.run(
                type("Args", (), {"journal": journal, "baseline": baseline, "simulations": 10, "seed": 1, "min_trades": 30, "horizon": 10})()
            )
            self.assertEqual(result["reports"][0]["risk_estimate_status"], "insufficient_sample")


if __name__ == "__main__":
    unittest.main()
