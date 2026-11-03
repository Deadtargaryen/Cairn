from __future__ import annotations

import argparse
import unittest

import account_viability as viability


class AccountViabilityTests(unittest.TestCase):
    def test_three_dollars_below_ten_dollar_minimum_is_live_blocked(self) -> None:
        args = argparse.Namespace(
            equity=3.0,
            minimum_notional=10.0,
            maker_fee_rate=0.001,
            taker_fee_rate=0.001,
            half_spread_rate=0.0005,
            slippage_rate=0.0005,
            impact_rate=0.005,
        )
        result = viability.run(args)
        self.assertFalse(result["derived"]["minimum_notional_feasible"])
        self.assertEqual(result["decision"], "DO_NOT_TRADE_LIVE")

    def test_round_trip_cost_hurdle_is_positive(self) -> None:
        args = argparse.Namespace(
            equity=100.0,
            minimum_notional=10.0,
            maker_fee_rate=0.001,
            taker_fee_rate=0.001,
            half_spread_rate=0.0005,
            slippage_rate=0.0005,
            impact_rate=0.005,
        )
        result = viability.run(args)
        self.assertGreater(result["derived"]["round_trip_cost_rate"], 0.0)
        self.assertGreater(result["derived"]["minimum_gross_edge_hurdle"], 0.0)


if __name__ == "__main__":
    unittest.main()
