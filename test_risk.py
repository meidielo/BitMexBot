import json
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from instrument import XBTUSDTInstrument
from risk import (
    CANARY_REVIEW_LIMITS,
    LEVERAGE,
    RiskLimits,
    TESTNET_LIMITS,
    _calc_liq_price,
    _calc_position_size,
    _estimated_loss_per_contract,
    _load_daily_loss,
    validate_signal,
)


def instrument() -> XBTUSDTInstrument:
    return XBTUSDTInstrument.from_ccxt_market(
        {
            "id": "XBTUSDT",
            "symbol": "BTC/USDT:USDT",
            "type": "swap",
            "contract": True,
            "linear": True,
            "inverse": False,
            "settle": "USDT",
            "active": True,
            "contractSize": 0.000001,
            "maker": 0.0005,
            "taker": 0.0005,
            "precision": {"amount": 100.0, "price": 0.1},
            "info": {
                "lotSize": "100",
                "tickSize": "0.1",
                "makerFee": "0.0005",
                "takerFee": "0.0005",
                "initMargin": "0.01",
                "maintMargin": "0.005",
            },
        }
    )


def long_signal(entry=62_500.0):
    return {
        "signal": "LONG",
        "entry_price": entry,
        "sl_price": entry * 0.98,
        "tp_price": entry * 1.03,
        "reason": "test",
    }


def short_signal(entry=62_500.0):
    return {
        "signal": "SHORT",
        "entry_price": entry,
        "sl_price": entry * 1.02,
        "tp_price": entry * 0.97,
        "reason": "test",
    }


class PositionSizingTests(unittest.TestCase):
    def setUp(self):
        self.instrument = instrument()

    def test_sizes_from_stop_loss_budget_and_lot(self):
        contracts = _calc_position_size(
            1_000, 62_500, 61_250, self.instrument, TESTNET_LIMITS
        )
        self.assertEqual(contracts, 13_300)
        self.assertEqual(contracts % 100, 0)
        expected_loss = Decimal(contracts) * _estimated_loss_per_contract(
            Decimal("62500"),
            Decimal("61250"),
            self.instrument,
            TESTNET_LIMITS,
        )
        self.assertEqual(expected_loss, Decimal("19.9084375000"))
        self.assertLessEqual(expected_loss, Decimal("20"))

    def test_applies_btc_exposure_cap(self):
        contracts = _calc_position_size(
            100_000, 62_500, 61_250, self.instrument, TESTNET_LIMITS
        )
        self.assertEqual(contracts, 100_000)
        self.assertEqual(
            self.instrument.contracts_to_btc(contracts), Decimal("0.100000")
        )

    def test_applies_optional_notional_cap(self):
        limits = RiskLimits(max_notional_usdt=Decimal("25"))
        contracts = _calc_position_size(
            1_000, 62_500, 61_250, self.instrument, limits
        )
        self.assertEqual(contracts, 400)
        self.assertEqual(
            self.instrument.notional_usdt(contracts, 62_500), Decimal("25.000000")
        )

    def test_below_minimum_lot_returns_zero(self):
        self.assertEqual(
            _calc_position_size(0.10, 62_500, 61_250, self.instrument), 0
        )


class DailyLossStateTests(unittest.TestCase):
    def test_reads_current_non_negative_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "loss.json"
            path.write_text(
                json.dumps(
                    {
                        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        "loss_usd": 3.25,
                        "source": "trades_v2.db",
                    }
                ),
                encoding="utf-8",
            )
            self.assertEqual(_load_daily_loss(str(path)), 3.25)

    def test_missing_stale_corrupt_or_negative_state_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "loss.json"
            self.assertIsNone(_load_daily_loss(str(path)))
            path.write_text("not-json", encoding="utf-8")
            self.assertIsNone(_load_daily_loss(str(path)))
            path.write_text(
                json.dumps({"date": "2000-01-01", "loss_usd": 0}),
                encoding="utf-8",
            )
            self.assertIsNone(_load_daily_loss(str(path)))
            path.write_text(
                json.dumps(
                    {
                        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        "loss_usd": 0,
                        "source": "legacy-or-manual",
                    }
                ),
                encoding="utf-8",
            )
            self.assertIsNone(_load_daily_loss(str(path)))
            path.write_text(
                json.dumps(
                    {
                        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        "loss_usd": -1,
                        "source": "trades_v2.db",
                    }
                ),
                encoding="utf-8",
            )
            self.assertIsNone(_load_daily_loss(str(path)))


class SignalValidationTests(unittest.TestCase):
    def setUp(self):
        self.instrument = instrument()

    def validate(self, signal, **kwargs):
        return validate_signal(
            signal,
            account_balance=1_000,
            open_positions=[],
            instrument=self.instrument,
            daily_loss_usdt=0,
            **kwargs,
        )

    def test_valid_long_has_explicit_units(self):
        result = self.validate(long_signal())
        self.assertTrue(result["approved"])
        self.assertEqual(result["position_size_contracts"], 13_300)
        self.assertAlmostEqual(result["position_size_btc"], 0.0133)
        self.assertAlmostEqual(result["notional_usdt"], 831.25)
        self.assertAlmostEqual(result["raw_stop_loss_usdt"], 16.625)
        self.assertAlmostEqual(result["estimated_costs_usdt"], 3.2834375)
        self.assertAlmostEqual(result["expected_max_loss_usdt"], 19.9084375)
        self.assertEqual(result["entry_slippage_bps"], 10)
        self.assertEqual(result["stop_slippage_bps"], 20)
        self.assertEqual(result["taker_fee_rate"], 0.0005)
        self.assertEqual(result["leverage"], LEVERAGE)

    def test_valid_short_is_approved(self):
        self.assertTrue(self.validate(short_signal())["approved"])

    def test_non_directional_and_malformed_signals_are_vetoed(self):
        cases = [
            {},
            {"signal": "NO_TRADE"},
            {"signal": "LONG", "entry_price": None, "sl_price": 1, "tp_price": 2},
            {"signal": "LONG", "entry_price": 100, "sl_price": 101, "tp_price": 102},
            {"signal": "SHORT", "entry_price": 100, "sl_price": 99, "tp_price": 98},
        ]
        for signal in cases:
            with self.subTest(signal=signal):
                self.assertFalse(self.validate(signal)["approved"])

    def test_open_position_is_vetoed(self):
        result = validate_signal(
            long_signal(),
            1_000,
            [{"contracts": 100}],
            instrument=self.instrument,
            daily_loss_usdt=0,
        )
        self.assertFalse(result["approved"])
        self.assertIn("Rule 2", result["reason"])

    def test_missing_or_reached_daily_loss_is_vetoed(self):
        for loss in (None, 50, 51):
            with self.subTest(loss=loss):
                result = validate_signal(
                    long_signal(),
                    1_000,
                    [],
                    instrument=self.instrument,
                    daily_loss_usdt=loss,
                )
                self.assertFalse(result["approved"])
                self.assertIn("Rule 3", result["reason"])

    def test_buffered_expected_loss_must_fit_remaining_daily_budget(self):
        result = validate_signal(
            long_signal(),
            1_000,
            [],
            instrument=self.instrument,
            daily_loss_usdt=31.5,
        )

        self.assertFalse(result["approved"])
        self.assertIn("Rule 3", result["reason"])
        self.assertIn("remaining daily loss budget", result["reason"])

    def test_trade_below_combined_daily_loss_limit_is_approved(self):
        result = validate_signal(
            long_signal(),
            1_000,
            [],
            instrument=self.instrument,
            daily_loss_usdt=30,
        )

        self.assertTrue(result["approved"])
        self.assertLessEqual(
            Decimal("30") + Decimal(str(result["expected_max_loss_usdt"])),
            TESTNET_LIMITS.max_daily_loss_usdt,
        )

    def test_stop_beyond_estimated_liquidation_is_vetoed(self):
        entry = 62_500
        unsafe_long = long_signal(entry)
        unsafe_long["sl_price"] = _calc_liq_price(entry, "LONG") - 1
        unsafe_short = short_signal(entry)
        unsafe_short["sl_price"] = _calc_liq_price(entry, "SHORT") + 1
        for signal in (unsafe_long, unsafe_short):
            with self.subTest(signal=signal["signal"]):
                result = self.validate(signal)
                self.assertFalse(result["approved"])
                self.assertIn("Rule 4", result["reason"])

    def test_insufficient_free_margin_is_vetoed(self):
        result = self.validate(long_signal(), free_balance_usdt=1)
        self.assertFalse(result["approved"])
        self.assertIn("Rule 6", result["reason"])

    def test_canary_notional_limit_is_enforced(self):
        result = self.validate(long_signal(), limits=CANARY_REVIEW_LIMITS)
        self.assertTrue(result["approved"])
        self.assertLessEqual(result["notional_usdt"], 25)
        self.assertEqual(result["leverage"], 1)
        self.assertGreaterEqual(
            1 - (result["notional_usdt"] / 1_000),
            float(CANARY_REVIEW_LIMITS.minimum_free_margin_fraction),
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
