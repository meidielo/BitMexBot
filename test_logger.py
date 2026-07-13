"""Unit tests for current XBTUSDT linear-contract PnL accounting."""

import unittest

from logger import compute_pnl_usdt, log_trade, update_trade_exit


class ComputePnLUSDT(unittest.TestCase):
    def test_long_profit_uses_current_contract_size(self):
        self.assertAlmostEqual(
            compute_pnl_usdt("LONG", 70_000, 71_000, 200),
            0.2,
        )

    def test_long_loss(self):
        self.assertAlmostEqual(
            compute_pnl_usdt("LONG", 70_000, 69_000, 200),
            -0.2,
        )

    def test_short_profit_and_loss(self):
        self.assertAlmostEqual(
            compute_pnl_usdt("SHORT", 70_000, 69_000, 200),
            0.2,
        )
        self.assertAlmostEqual(
            compute_pnl_usdt("SHORT", 70_000, 71_000, 200),
            -0.2,
        )

    def test_custom_verified_contract_size(self):
        self.assertAlmostEqual(
            compute_pnl_usdt("LONG", 100, 110, 5, contract_size_btc=0.01),
            0.5,
        )

    def test_invalid_inputs_fail_closed(self):
        with self.assertRaises(ValueError):
            compute_pnl_usdt("BUY", 100, 110, 5)
        with self.assertRaises(ValueError):
            compute_pnl_usdt("LONG", 100, 110, -1)

    def test_legacy_writes_are_disabled(self):
        with self.assertRaisesRegex(RuntimeError, "read-only"):
            log_trade({})
        with self.assertRaisesRegex(RuntimeError, "read-only"):
            update_trade_exit("id", 100, "TP")


if __name__ == "__main__":
    unittest.main(verbosity=2)
