from __future__ import annotations

import unittest

import walk_forward_baselines as walk_forward


class WalkForwardTests(unittest.TestCase):
    def test_folds_are_chronological_and_non_overlapping(self) -> None:
        folds = walk_forward.walk_forward_folds()
        for fold in folds:
            self.assertEqual(fold["train"].end, fold["validation"].start)
            self.assertEqual(fold["validation"].end, fold["test"].start)
            self.assertLess(fold["test"].start, fold["test"].end)

    def test_selection_falls_back_to_no_trade(self) -> None:
        selected, reason = walk_forward.select_validation_candidate(
            [
                {"strategy": "simple_momentum", "trade_count": 100, "net_return": -0.1},
                {"strategy": "no_trade", "trade_count": 0, "net_return": 0.0},
            ],
            minimum_trades=10,
        )
        self.assertEqual(selected, "no_trade")
        self.assertIn("non-positive", reason)

    def test_selection_uses_positive_candidate_with_enough_trades(self) -> None:
        selected, reason = walk_forward.select_validation_candidate(
            [
                {"strategy": "simple_momentum", "trade_count": 20, "net_return": 0.05},
                {"strategy": "volatility_breakout", "trade_count": 20, "net_return": 0.02},
            ],
            minimum_trades=10,
        )
        self.assertEqual(selected, "simple_momentum")
        self.assertIn("highest", reason)


if __name__ == "__main__":
    unittest.main()
