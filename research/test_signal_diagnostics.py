from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

import signal_diagnostics as diagnostics


class SignalDiagnosticsTests(unittest.TestCase):
    def test_benjamini_hochberg_is_monotone_in_sorted_order(self) -> None:
        result = diagnostics.benjamini_hochberg([0.01, 0.04, 0.2, None], family_size=4)
        self.assertEqual(len(result), 4)
        self.assertLessEqual(result[0], result[1])
        self.assertLessEqual(result[1], result[2])

    def test_multiple_testing_family_includes_insufficient_rows(self) -> None:
        rows = [
            {"interval": "1d", "split": "test_2023", "p_value_mean_positive": 0.01},
            {"interval": "1d", "split": "test_2023", "p_value_mean_positive": None},
        ]

        diagnostics.correct_pvalues(rows)

        self.assertEqual(rows[0]["multiple_testing_family_size"], 2)
        self.assertFalse(rows[1]["significant_bh_alpha_005"])

    def test_calibration_uses_train_only_and_returns_bounded_metrics(self) -> None:
        scores = np.asarray([0.1, 0.2, 0.3, 0.4] * 10, dtype=float)
        labels = np.asarray([0, 0, 1, 1] * 10, dtype=float)
        calibration = diagnostics.fit_calibration(scores, labels, bins=4)
        result = diagnostics.calibrated_metrics(scores, labels, calibration)
        self.assertEqual(result["status"], "ok")
        self.assertGreaterEqual(result["brier_score"], 0.0)
        self.assertLessEqual(result["brier_score"], 1.0)
        self.assertGreaterEqual(result["expected_calibration_error"], 0.0)
        self.assertLessEqual(result["expected_calibration_error"], 1.0)

    def test_regime_labels_unknown_insufficient_history(self) -> None:
        frame = pd.DataFrame(
            {
                "same_as_previous": [False, True],
                "trend_return": [np.nan, np.nan],
                "short_return": [np.nan, np.nan],
                "realized_vol": [np.nan, np.nan],
                "volume_ratio": [np.nan, np.nan],
                "volatility_ratio": [np.nan, np.nan],
            }
        )
        result = diagnostics.assign_regimes(frame)
        self.assertEqual(result.tolist(), ["UNKNOWN", "UNKNOWN"])

    def test_hypothesis_masks_are_edge_triggered(self) -> None:
        frame = pd.DataFrame(
            {
                "same_as_previous": [False, True, True, True],
                "segment_id": [0, 0, 0, 0],
                "trend_return": [np.nan, 0.01, 0.02, 0.03],
                "volume_ratio": [np.nan, 2.0, 2.0, 2.0],
                "breakout_strength": [np.nan, 0.1, 0.2, 0.3],
            }
        )
        masks = diagnostics.hypothesis_masks(frame)
        self.assertEqual(np.flatnonzero(masks["volume_confirmed_momentum"][0]).tolist(), [1])
        self.assertEqual(np.flatnonzero(masks["volume_breakout"][0]).tolist(), [1])

    def test_forward_extrema_excludes_current_bar_and_respects_segments(self) -> None:
        frame = pd.DataFrame(
            {
                "low": [10.0, 8.0, 9.0, 1.0, 2.0],
                "high": [11.0, 12.0, 13.0, 20.0, 21.0],
                "segment_id": [0, 0, 0, 1, 1],
            }
        )

        lows, highs = diagnostics.future_extrema(frame, 2)

        self.assertEqual(lows[0], 8.0)
        self.assertEqual(highs[0], 13.0)
        self.assertTrue(np.isnan(lows[1]))
        self.assertTrue(np.isnan(lows[2]))
        self.assertTrue(np.isnan(lows[3]))


if __name__ == "__main__":
    unittest.main()
