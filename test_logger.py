"""
Unit tests for logger.compute_pnl_usdt — realised PnL for XBTUSDT linear.
"""
import math
import unittest

from logger import compute_pnl_usdt


class ComputePnLUSDT(unittest.TestCase):
    """
    XBTUSDT linear perpetual: 1 contract = 1 USDT notional.
    A position of N contracts represents N/entry BTC of exposure;
    closing at exit realises contracts * (exit − entry) / entry USDT.
    """

    def assertClose(self, a, b, tol=1e-9):
        self.assertTrue(math.isclose(a, b, rel_tol=tol, abs_tol=tol),
                        f"{a} !≈ {b}")

    # ─── LONG winners ──────────────────────────────────────────────────
    def test_long_profit_round_numbers(self):
        # 200 contracts × ($71k − $70k) / $70k  =  2.857142… USDT
        pnl = compute_pnl_usdt("LONG", 70_000, 71_000, 200)
        self.assertClose(pnl, 200 * 1000 / 70_000)
        self.assertGreater(pnl, 0)

    def test_long_profit_realistic_trade(self):
        # Reproduces trade #1 from trades.db: 188.22 contracts,
        # 67386 → 67640. Expected PnL ≈ 0.7094 USDT.
        pnl = compute_pnl_usdt("LONG", 67_386, 67_640, 188.22)
        self.assertAlmostEqual(pnl, 0.7094, places=3)

    # ─── LONG losers ───────────────────────────────────────────────────
    def test_long_loss(self):
        pnl = compute_pnl_usdt("LONG", 70_000, 69_000, 200)
        self.assertClose(pnl, -200 * 1000 / 70_000)
        self.assertLess(pnl, 0)

    # ─── SHORT winners ─────────────────────────────────────────────────
    def test_short_profit(self):
        pnl = compute_pnl_usdt("SHORT", 70_000, 69_000, 200)
        self.assertClose(pnl, 200 * 1000 / 70_000)
        self.assertGreater(pnl, 0)

    # ─── SHORT losers ──────────────────────────────────────────────────
    def test_short_loss(self):
        pnl = compute_pnl_usdt("SHORT", 70_000, 71_000, 200)
        self.assertClose(pnl, -200 * 1000 / 70_000)
        self.assertLess(pnl, 0)

    # ─── No-move guard ─────────────────────────────────────────────────
    def test_zero_move(self):
        self.assertEqual(compute_pnl_usdt("LONG", 70_000, 70_000, 500), 0.0)
        self.assertEqual(compute_pnl_usdt("SHORT", 70_000, 70_000, 500), 0.0)

    # ─── Sanity: formula produces USDT, not contract-units ────────────
    def test_magnitude_is_small_for_small_move(self):
        # 1% move on 200 contracts should be ~2 USDT, not ~200.
        pnl = compute_pnl_usdt("LONG", 70_000, 70_700, 200)
        self.assertLess(abs(pnl), 3.0)
        self.assertGreater(pnl, 1.5)


if __name__ == "__main__":
    unittest.main()
