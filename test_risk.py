"""
test_risk.py — Unit tests for risk.py (Phase 4)
Run with:  python -m pytest test_risk.py -v
      or:  python -m unittest discover
Every hardcoded rule has at least one passing and one failing test case.
"""

import json
import os
import unittest
from unittest.mock import patch
from datetime import date

from risk import (
    validate_signal,
    _calc_liq_price,
    _calc_position_size,
    LEVERAGE,
    MAX_POSITION_BTC,
    MAX_CONTRACTS,
    RISK_PER_TRADE_PCT,
    MAX_DAILY_LOSS_USD,
    LIQ_BUFFER,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

def _long_signal(entry=70000.0):
    """Minimal valid LONG signal at a given entry price."""
    return {
        "signal":      "LONG",
        "reason":      "test long signal",
        "entry_price": entry,
        "sl_price":    round(entry * 0.98, 2),
        "tp_price":    round(entry * 1.03, 2),
    }


def _short_signal(entry=70000.0):
    """Minimal valid SHORT signal at a given entry price."""
    return {
        "signal":      "SHORT",
        "reason":      "test short signal",
        "entry_price": entry,
        "sl_price":    round(entry * 1.02, 2),
        "tp_price":    round(entry * 0.97, 2),
    }


NO_OPEN   = []            # no open positions
ONE_OPEN  = [{"id": "abc123"}]   # one existing position
BALANCE   = 1000.0        # $1,000 test balance


# ---------------------------------------------------------------------------
# Helper: patch _load_daily_loss to return a fixed value without touching disk
# ---------------------------------------------------------------------------
def _patch_loss(loss_usd: float):
    return patch("risk._load_daily_loss", return_value=loss_usd)


# ---------------------------------------------------------------------------
# Rule 1 — Signal must be LONG or SHORT (not NO_TRADE or missing)
# ---------------------------------------------------------------------------

class TestRule1SignalType(unittest.TestCase):

    def test_no_trade_is_vetoed(self):
        sig = {"signal": "NO_TRADE", "reason": "no setup",
               "entry_price": None, "sl_price": None, "tp_price": None}
        with _patch_loss(0):
            result = validate_signal(sig, BALANCE, NO_OPEN)
        self.assertFalse(result["approved"])
        self.assertIn("Rule 1", result["reason"])

    def test_missing_signal_key_is_vetoed(self):
        with _patch_loss(0):
            result = validate_signal({}, BALANCE, NO_OPEN)
        self.assertFalse(result["approved"])
        self.assertIn("Rule 1", result["reason"])

    def test_none_entry_on_directional_signal_is_vetoed(self):
        sig = {"signal": "LONG", "reason": "test",
               "entry_price": None, "sl_price": None, "tp_price": None}
        with _patch_loss(0):
            result = validate_signal(sig, BALANCE, NO_OPEN)
        self.assertFalse(result["approved"])
        self.assertIn("Rule 1", result["reason"])

    def test_long_signal_passes_rule1(self):
        with _patch_loss(0):
            result = validate_signal(_long_signal(), BALANCE, NO_OPEN)
        # Rule 1 should not be the veto reason
        self.assertNotIn("Rule 1", result.get("reason", ""))

    def test_short_signal_passes_rule1(self):
        with _patch_loss(0):
            result = validate_signal(_short_signal(), BALANCE, NO_OPEN)
        self.assertNotIn("Rule 1", result.get("reason", ""))


# ---------------------------------------------------------------------------
# Rule 2 — One trade at a time
# ---------------------------------------------------------------------------

class TestRule2OpenPositions(unittest.TestCase):

    def test_one_open_position_vetoes_long(self):
        with _patch_loss(0):
            result = validate_signal(_long_signal(), BALANCE, ONE_OPEN)
        self.assertFalse(result["approved"])
        self.assertIn("Rule 2", result["reason"])

    def test_one_open_position_vetoes_short(self):
        with _patch_loss(0):
            result = validate_signal(_short_signal(), BALANCE, ONE_OPEN)
        self.assertFalse(result["approved"])
        self.assertIn("Rule 2", result["reason"])

    def test_multiple_open_positions_vetoed(self):
        many_open = [{"id": "x"}, {"id": "y"}, {"id": "z"}]
        with _patch_loss(0):
            result = validate_signal(_long_signal(), BALANCE, many_open)
        self.assertFalse(result["approved"])
        self.assertIn("Rule 2", result["reason"])

    def test_no_open_positions_passes_rule2(self):
        with _patch_loss(0):
            result = validate_signal(_long_signal(), BALANCE, NO_OPEN)
        self.assertNotIn("Rule 2", result.get("reason", ""))


# ---------------------------------------------------------------------------
# Rule 3 — Daily loss limit ($50)
# ---------------------------------------------------------------------------

class TestRule3DailyLoss(unittest.TestCase):

    def test_loss_at_limit_is_vetoed(self):
        with _patch_loss(MAX_DAILY_LOSS_USD):
            result = validate_signal(_long_signal(), BALANCE, NO_OPEN)
        self.assertFalse(result["approved"])
        self.assertIn("Rule 3", result["reason"])

    def test_loss_above_limit_is_vetoed(self):
        with _patch_loss(MAX_DAILY_LOSS_USD + 1):
            result = validate_signal(_long_signal(), BALANCE, NO_OPEN)
        self.assertFalse(result["approved"])
        self.assertIn("Rule 3", result["reason"])

    def test_zero_loss_passes(self):
        with _patch_loss(0.0):
            result = validate_signal(_long_signal(), BALANCE, NO_OPEN)
        self.assertNotIn("Rule 3", result.get("reason", ""))

    def test_loss_just_under_limit_passes(self):
        with _patch_loss(MAX_DAILY_LOSS_USD - 0.01):
            result = validate_signal(_long_signal(), BALANCE, NO_OPEN)
        self.assertNotIn("Rule 3", result.get("reason", ""))


# ---------------------------------------------------------------------------
# Rule 4 — SL must fire before liquidation price
# ---------------------------------------------------------------------------

class TestRule4LiquidationCheck(unittest.TestCase):

    # --- helper calculations ---
    def _long_liq(self, entry):
        return entry * (1 - (1 / LEVERAGE) * LIQ_BUFFER)

    def _short_liq(self, entry):
        return entry * (1 + (1 / LEVERAGE) * LIQ_BUFFER)

    # LONG tests
    def test_long_sl_above_liq_passes(self):
        """Standard LONG SL (entry*0.98) should be above liq (entry*0.94)."""
        with _patch_loss(0):
            result = validate_signal(_long_signal(70000), BALANCE, NO_OPEN)
        self.assertNotIn("Rule 4", result.get("reason", ""))

    def test_long_sl_below_liq_is_vetoed(self):
        """Force SL below liq to trigger Rule 4."""
        entry = 70000.0
        liq   = self._long_liq(entry)
        # Put SL below liquidation price
        bad_sl = round(liq - 100, 2)
        sig = {**_long_signal(entry), "sl_price": bad_sl}
        with _patch_loss(0):
            result = validate_signal(sig, BALANCE, NO_OPEN)
        self.assertFalse(result["approved"])
        self.assertIn("Rule 4", result["reason"])

    def test_long_sl_equal_to_liq_is_vetoed(self):
        """SL exactly at liq price is also not safe."""
        entry = 70000.0
        liq   = self._long_liq(entry)
        sig = {**_long_signal(entry), "sl_price": round(liq, 2)}
        with _patch_loss(0):
            result = validate_signal(sig, BALANCE, NO_OPEN)
        self.assertFalse(result["approved"])
        self.assertIn("Rule 4", result["reason"])

    # SHORT tests
    def test_short_sl_below_liq_passes(self):
        """Standard SHORT SL (entry*1.02) should be below liq (entry*1.06)."""
        with _patch_loss(0):
            result = validate_signal(_short_signal(70000), BALANCE, NO_OPEN)
        self.assertNotIn("Rule 4", result.get("reason", ""))

    def test_short_sl_above_liq_is_vetoed(self):
        """Force SL above liq to trigger Rule 4."""
        entry = 70000.0
        liq   = self._short_liq(entry)
        bad_sl = round(liq + 100, 2)
        sig = {**_short_signal(entry), "sl_price": bad_sl}
        with _patch_loss(0):
            result = validate_signal(sig, BALANCE, NO_OPEN)
        self.assertFalse(result["approved"])
        self.assertIn("Rule 4", result["reason"])

    def test_short_sl_equal_to_liq_is_vetoed(self):
        entry = 70000.0
        liq   = self._short_liq(entry)
        sig = {**_short_signal(entry), "sl_price": round(liq, 2)}
        with _patch_loss(0):
            result = validate_signal(sig, BALANCE, NO_OPEN)
        self.assertFalse(result["approved"])
        self.assertIn("Rule 4", result["reason"])


# ---------------------------------------------------------------------------
# Rule 5 — Position sizing (2 % risk, 0.10 BTC cap)
# ---------------------------------------------------------------------------

class TestRule5PositionSizing(unittest.TestCase):

    def test_position_size_is_2pct_of_balance(self):
        # Contracts = int(balance * 2% * 15x) = int(1000 * 0.02 * 15) = 300
        balance  = 1000.0
        expected = int(balance * RISK_PER_TRADE_PCT * LEVERAGE)  # 300 contracts
        with _patch_loss(0):
            result = validate_signal(_long_signal(), balance, NO_OPEN)
        self.assertTrue(result["approved"])
        self.assertEqual(result["position_size_btc"], expected)

    def test_position_size_capped_at_max(self):
        """Very high balance → raw contracts exceed MAX_CONTRACTS → must be capped."""
        # $100,000 * 2% * 15 = 30,000 contracts → capped at MAX_CONTRACTS (1500)
        balance = 100_000.0
        with _patch_loss(0):
            result = validate_signal(_long_signal(), balance, NO_OPEN)
        self.assertTrue(result["approved"])
        self.assertEqual(result["position_size_btc"], MAX_CONTRACTS)

    def test_leverage_is_always_15(self):
        with _patch_loss(0):
            result = validate_signal(_long_signal(), BALANCE, NO_OPEN)
        self.assertTrue(result["approved"])
        self.assertEqual(result["leverage"], LEVERAGE)
        self.assertEqual(result["leverage"], 15)

    def test_position_size_none_when_vetoed(self):
        sig = {"signal": "NO_TRADE", "reason": "x",
               "entry_price": None, "sl_price": None, "tp_price": None}
        with _patch_loss(0):
            result = validate_signal(sig, BALANCE, NO_OPEN)
        self.assertIsNone(result["position_size_btc"])
        self.assertIsNone(result["leverage"])


# ---------------------------------------------------------------------------
# Internal helpers — unit-tested independently
# ---------------------------------------------------------------------------

class TestLiquidationPriceCalc(unittest.TestCase):

    def test_long_liq_is_below_entry(self):
        entry = 70000.0
        liq   = _calc_liq_price(entry, "LONG")
        self.assertLess(liq, entry)

    def test_short_liq_is_above_entry(self):
        entry = 70000.0
        liq   = _calc_liq_price(entry, "SHORT")
        self.assertGreater(liq, entry)

    def test_long_liq_formula(self):
        entry    = 70000.0
        expected = entry * (1 - (1 / LEVERAGE) * LIQ_BUFFER)
        self.assertAlmostEqual(_calc_liq_price(entry, "LONG"), expected, places=4)

    def test_short_liq_formula(self):
        entry    = 70000.0
        expected = entry * (1 + (1 / LEVERAGE) * LIQ_BUFFER)
        self.assertAlmostEqual(_calc_liq_price(entry, "SHORT"), expected, places=4)


class TestPositionSizeCalc(unittest.TestCase):

    def test_standard_calculation(self):
        # Contracts = int(1000 * 0.02 * 15) = 300
        result   = _calc_position_size(1000.0)
        expected = int(1000.0 * RISK_PER_TRADE_PCT * LEVERAGE)
        self.assertEqual(result, expected)

    def test_cap_enforced(self):
        # Very large balance → capped at MAX_CONTRACTS
        result = _calc_position_size(1_000_000.0)
        self.assertEqual(result, MAX_CONTRACTS)

    def test_exactly_at_cap_boundary(self):
        # Balance where raw = MAX_CONTRACTS exactly: balance = MAX_CONTRACTS / (0.02 * 15) = 5000
        balance = MAX_CONTRACTS / (RISK_PER_TRADE_PCT * LEVERAGE)
        result  = _calc_position_size(balance)
        self.assertEqual(result, MAX_CONTRACTS)


# ---------------------------------------------------------------------------
# Happy path — end-to-end approval for both directions
# ---------------------------------------------------------------------------

class TestHappyPath(unittest.TestCase):

    def test_valid_long_is_approved(self):
        with _patch_loss(0):
            result = validate_signal(_long_signal(70000), BALANCE, NO_OPEN)
        self.assertTrue(result["approved"])
        self.assertEqual(result["leverage"], 15)
        self.assertIsNotNone(result["position_size_btc"])
        self.assertGreater(result["position_size_btc"], 0)

    def test_valid_short_is_approved(self):
        with _patch_loss(0):
            result = validate_signal(_short_signal(70000), BALANCE, NO_OPEN)
        self.assertTrue(result["approved"])
        self.assertEqual(result["leverage"], 15)
        self.assertIsNotNone(result["position_size_btc"])
        self.assertGreater(result["position_size_btc"], 0)

    def test_approved_result_has_all_keys(self):
        with _patch_loss(0):
            result = validate_signal(_long_signal(), BALANCE, NO_OPEN)
        for key in ("approved", "reason", "position_size_btc", "leverage"):
            self.assertIn(key, result)

    def test_vetoed_result_has_all_keys(self):
        sig = {"signal": "NO_TRADE", "reason": "x",
               "entry_price": None, "sl_price": None, "tp_price": None}
        with _patch_loss(0):
            result = validate_signal(sig, BALANCE, NO_OPEN)
        for key in ("approved", "reason", "position_size_btc", "leverage"):
            self.assertIn(key, result)


if __name__ == "__main__":
    unittest.main(verbosity=2)
