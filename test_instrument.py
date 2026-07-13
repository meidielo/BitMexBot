import copy
import unittest
from decimal import Decimal

from instrument import InstrumentMetadataError, XBTUSDTInstrument


def valid_market() -> dict:
    return {
        "id": "XBTUSDT",
        "symbol": "BTC/USDT:USDT",
        "type": "swap",
        "contract": True,
        "linear": True,
        "inverse": False,
        "active": True,
        "settle": "USDT",
        "contractSize": 1e-6,
        "maker": 0.0002,
        "taker": 0.00075,
        "precision": {"amount": 100.0, "price": 0.1},
        "info": {
            "lotSize": "100",
            "tickSize": "0.1",
            "makerFee": "0.0002",
            "takerFee": "0.00075",
            "initMargin": "0.01",
            "maintMargin": "0.005",
        },
    }


class XBTUSDTInstrumentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.instrument = XBTUSDTInstrument.from_ccxt_market(valid_market())

    def test_builds_from_verified_ccxt_metadata(self):
        self.assertEqual(self.instrument.symbol, "BTC/USDT:USDT")
        self.assertEqual(self.instrument.market_id, "XBTUSDT")
        self.assertEqual(self.instrument.settlement_currency, "USDT")
        self.assertEqual(self.instrument.contract_size_btc, Decimal("0.000001"))
        self.assertEqual(self.instrument.lot_size_contracts, 100)
        self.assertEqual(self.instrument.tick_size_usdt, Decimal("0.1"))
        self.assertEqual(self.instrument.maker_fee_rate, Decimal("0.0002"))
        self.assertEqual(self.instrument.taker_fee_rate, Decimal("0.00075"))
        self.assertEqual(self.instrument.initial_margin_rate, Decimal("0.01"))
        self.assertEqual(self.instrument.maintenance_margin_rate, Decimal("0.005"))

    def test_contracts_convert_to_btc_and_notional(self):
        self.assertEqual(self.instrument.contracts_to_btc(100), Decimal("0.000100"))
        self.assertEqual(
            self.instrument.notional_usdt(100, 70_000), Decimal("7.000000")
        )

    def test_linear_gross_pnl_for_long_and_short(self):
        self.assertEqual(
            self.instrument.gross_pnl_usdt("LONG", 100, 70_000, 71_000),
            Decimal("0.100000"),
        )
        self.assertEqual(
            self.instrument.gross_pnl_usdt("SHORT", 100, 70_000, 71_000),
            Decimal("-0.100000"),
        )

    def test_fee_estimate_uses_position_notional(self):
        self.assertEqual(
            self.instrument.estimate_fee_usdt(100, 70_000, Decimal("0.00075")),
            Decimal("0.00525000000"),
        )

    def test_rounds_contracts_down_to_exchange_lot(self):
        cases = {0: 0, 99: 0, 100: 100, 173: 100, 199: 100, 200: 200}
        for proposed, expected in cases.items():
            with self.subTest(proposed=proposed):
                self.assertEqual(
                    self.instrument.round_down_contracts(proposed), expected
                )

    def test_rejects_invalid_calculation_inputs(self):
        with self.assertRaises(InstrumentMetadataError):
            self.instrument.contracts_to_btc(-1)
        with self.assertRaises(InstrumentMetadataError):
            self.instrument.notional_usdt(100, 0)
        with self.assertRaises(InstrumentMetadataError):
            self.instrument.estimate_fee_usdt(100, 70_000, -0.001)
        with self.assertRaises(ValueError):
            self.instrument.gross_pnl_usdt("BUY", 100, 70_000, 71_000)

    def test_fails_closed_on_wrong_instrument_shape(self):
        mutations = {
            "symbol": ("symbol", "BTC/USD:BTC"),
            "market id": ("id", "XBTUSD"),
            "type": ("type", "future"),
            "settlement": ("settle", "BTC"),
            "not contract": ("contract", False),
            "not linear": ("linear", False),
            "inverse": ("inverse", True),
            "inactive": ("active", False),
        }
        for label, (field, value) in mutations.items():
            market = valid_market()
            market[field] = value
            with self.subTest(label=label), self.assertRaises(InstrumentMetadataError):
                XBTUSDTInstrument.from_ccxt_market(market)

    def test_fails_closed_on_missing_or_non_positive_sizes(self):
        mutations = []
        for field, value in (("contractSize", None), ("contractSize", 0)):
            market = valid_market()
            market[field] = value
            mutations.append(market)

        for precision_field, value in (
            ("amount", None),
            ("amount", 0),
            ("price", None),
            ("price", -0.1),
        ):
            market = valid_market()
            market["precision"][precision_field] = value
            mutations.append(market)

        for market in mutations:
            with self.subTest(market=market), self.assertRaises(InstrumentMetadataError):
                XBTUSDTInstrument.from_ccxt_market(market)

    def test_fails_closed_when_normalized_and_raw_sizes_disagree(self):
        lot_mismatch = copy.deepcopy(valid_market())
        lot_mismatch["info"]["lotSize"] = "200"
        tick_mismatch = copy.deepcopy(valid_market())
        tick_mismatch["info"]["tickSize"] = "0.5"

        for market in (lot_mismatch, tick_mismatch):
            with self.subTest(market=market), self.assertRaises(InstrumentMetadataError):
                XBTUSDTInstrument.from_ccxt_market(market)

    def test_accepts_normalized_margin_rates_when_raw_rates_are_absent(self):
        market = valid_market()
        market["initialMargin"] = Decimal("0.01")
        market["maintenanceMargin"] = Decimal("0.005")
        del market["info"]["initMargin"]
        del market["info"]["maintMargin"]

        instrument = XBTUSDTInstrument.from_ccxt_market(market)

        self.assertEqual(instrument.initial_margin_rate, Decimal("0.01"))
        self.assertEqual(instrument.maintenance_margin_rate, Decimal("0.005"))

    def test_fails_closed_on_missing_or_invalid_fee_rates(self):
        markets = []

        missing_maker = valid_market()
        del missing_maker["maker"]
        del missing_maker["info"]["makerFee"]
        markets.append(missing_maker)

        negative_taker = valid_market()
        negative_taker["taker"] = -0.001
        negative_taker["info"]["takerFee"] = "-0.001"
        markets.append(negative_taker)

        mismatched_maker = valid_market()
        mismatched_maker["info"]["makerFee"] = "0.0003"
        markets.append(mismatched_maker)

        for market in markets:
            with self.subTest(market=market), self.assertRaises(InstrumentMetadataError):
                XBTUSDTInstrument.from_ccxt_market(market)

    def test_fails_closed_on_missing_or_invalid_margin_rates(self):
        markets = []

        missing_initial = valid_market()
        del missing_initial["info"]["initMargin"]
        markets.append(missing_initial)

        zero_maintenance = valid_market()
        zero_maintenance["info"]["maintMargin"] = "0"
        markets.append(zero_maintenance)

        equal_margins = valid_market()
        equal_margins["info"]["maintMargin"] = "0.01"
        markets.append(equal_margins)

        inverted_margins = valid_market()
        inverted_margins["info"]["maintMargin"] = "0.02"
        markets.append(inverted_margins)

        for market in markets:
            with self.subTest(market=market), self.assertRaises(InstrumentMetadataError):
                XBTUSDTInstrument.from_ccxt_market(market)


if __name__ == "__main__":
    unittest.main(verbosity=2)
