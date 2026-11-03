from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import deployment_gate


class DeploymentGateTests(unittest.TestCase):
    def test_gate_blocks_when_walk_forward_and_viability_fail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            warehouse = root / "warehouse.sqlite"
            warehouse.write_bytes(b"warehouse")
            baseline = root / "baseline.json"
            baseline.write_text(json.dumps({"reports": [], "trade_count": 0, "features": {"future_data_used_for_signal": False}}))
            analysis = root / "analysis.json"
            analysis.write_text(json.dumps({"input_reports": [{"descriptive_gate_passes": []}]}))
            walk_forward = root / "walk.json"
            walk_forward.write_text(json.dumps({"reports": [{"selected_strategy": "no_trade", "test": {"net_return": 0}}]}))
            viability = root / "viability.json"
            viability.write_text(json.dumps({"decision": "DO_NOT_TRADE_LIVE", "derived": {"minimum_notional_feasible": False}}))
            risk_review = root / "risk.json"
            risk_review.write_text(json.dumps({"decision": "NO_DEPLOYMENT_CANDIDATE", "candidate_count": 0, "reports": []}))
            signal_diagnostics = root / "signals.json"
            signal_diagnostics.write_text(json.dumps({"decision": "NO_DEPLOYMENT_CANDIDATE", "conditional_results": [], "calibration_results": []}))
            output = root / "gate.json"

            result = deployment_gate.run(
                type("Args", (), {
                    "warehouse": warehouse,
                    "baseline": baseline,
                    "analysis": analysis,
                    "walk_forward": walk_forward,
                    "viability": viability,
                    "risk_review": risk_review,
                    "signal_diagnostics": signal_diagnostics,
                })()
            )

            self.assertEqual(result["decision"], "ABSTAIN_NO_LIVE_TRADING")
            self.assertEqual(result["deployment_status"], "blocked")
            self.assertTrue(result["blockers"])


if __name__ == "__main__":
    unittest.main()
