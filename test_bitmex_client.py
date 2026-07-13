import os
import unittest
from unittest.mock import patch

from instrument import InstrumentMetadataError, XBTUSDTInstrument

import bitmex_client


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


class FakeExchange:
    def __init__(self, markets=None):
        self.id = "bitmex"
        self.calls = []
        self._markets = markets or {"BTC/USDT:USDT": valid_market()}
        self.urls = {
            "api": {
                "public": "https://www.bitmex.com",
                "private": "https://www.bitmex.com",
            },
            "test": {
                "public": bitmex_client.TESTNET_API_ORIGIN,
                "private": bitmex_client.TESTNET_API_ORIGIN,
            },
        }

    def set_sandbox_mode(self, enabled):
        self.calls.append(("set_sandbox_mode", enabled))
        if enabled:
            self.urls["api"] = dict(self.urls["test"])

    def load_markets(self):
        self.calls.append(("load_markets",))
        return self._markets


class BitmexClientTests(unittest.TestCase):
    def test_rejects_every_non_exact_testnet_value_before_construction(self):
        for value in (None, "", "false", "TRUE", " true", "true "):
            env = {} if value is None else {"BITMEX_TESTNET": value}
            with self.subTest(value=value), patch.dict(os.environ, env, clear=True):
                with patch.object(bitmex_client.ccxt, "bitmex") as constructor:
                    with self.assertRaises(EnvironmentError):
                        bitmex_client.get_client()
                    constructor.assert_not_called()

    def test_rejects_missing_credentials_before_construction(self):
        with patch.dict(os.environ, {"BITMEX_TESTNET": "true"}, clear=True):
            with patch.object(bitmex_client.ccxt, "bitmex") as constructor:
                with self.assertRaises(EnvironmentError):
                    bitmex_client.get_client()
                constructor.assert_not_called()

    def test_uses_dedicated_credentials_and_sandbox_first(self):
        fake = FakeExchange()
        env = {
            "BITMEX_TESTNET": "true",
            "BITMEX_TESTNET_API_KEY": "dedicated-key",
            "BITMEX_TESTNET_API_SECRET": "dedicated-secret",
            "BITMEX_API_KEY": "legacy-key",
            "BITMEX_API_SECRET": "legacy-secret",
        }
        with patch.dict(os.environ, env, clear=True):
            with patch.object(
                bitmex_client.ccxt, "bitmex", return_value=fake
            ) as constructor:
                result = bitmex_client.get_client()

        config = constructor.call_args.args[0]
        self.assertEqual(config["apiKey"], "dedicated-key")
        self.assertEqual(config["secret"], "dedicated-secret")
        self.assertTrue(config["enableRateLimit"])
        self.assertEqual(config["timeout"], 10_000)
        self.assertNotIn("urls", config)
        self.assertEqual(
            fake.calls,
            [("set_sandbox_mode", True), ("load_markets",)],
        )
        self.assertIs(result, fake)
        self.assertIsInstance(result.xbtusdt_instrument, XBTUSDTInstrument)

    def test_rejects_generic_or_production_credential_names(self):
        fake = FakeExchange()
        env = {
            "BITMEX_TESTNET": "true",
            "BITMEX_API_KEY": "legacy-key",
            "BITMEX_API_SECRET": "legacy-secret",
        }
        with patch.dict(os.environ, env, clear=True):
            with patch.object(
                bitmex_client.ccxt, "bitmex", return_value=fake
            ) as constructor:
                with self.assertRaises(EnvironmentError):
                    bitmex_client.get_client()

        constructor.assert_not_called()

    def test_rejects_unverified_market_metadata(self):
        invalid = valid_market()
        invalid["id"] = "XBTUSD"
        fake = FakeExchange({"BTC/USDT:USDT": invalid})
        env = {
            "BITMEX_TESTNET": "true",
            "BITMEX_TESTNET_API_KEY": "key",
            "BITMEX_TESTNET_API_SECRET": "secret",
        }
        with patch.dict(os.environ, env, clear=True):
            with patch.object(bitmex_client.ccxt, "bitmex", return_value=fake):
                with self.assertRaises(InstrumentMetadataError):
                    bitmex_client.get_client()

        self.assertEqual(
            fake.calls,
            [("set_sandbox_mode", True), ("load_markets",)],
        )
        self.assertFalse(hasattr(fake, "xbtusdt_instrument"))

    def test_attestation_rejects_mainnet_and_non_bitmex_clients(self):
        fake = FakeExchange()
        with patch.dict(os.environ, {"BITMEX_TESTNET": "true"}, clear=True):
            with self.assertRaises(bitmex_client.ExchangeAttestationError):
                bitmex_client.attest_testnet_exchange(fake)
            fake.set_sandbox_mode(True)
            bitmex_client.attest_testnet_exchange(fake)
            fake.id = "other"
            with self.assertRaises(bitmex_client.ExchangeAttestationError):
                bitmex_client.attest_testnet_exchange(fake)

    def test_position_snapshot_rejects_missing_or_conflicting_native_state(self):
        class PositionExchange(FakeExchange):
            def __init__(self, response):
                super().__init__()
                self.set_sandbox_mode(True)
                self.response = response

            def fetch_positions(self, symbols):
                return self.response

        valid = {
            "symbol": "BTC/USDT:USDT",
            "contracts": 100,
            "side": "long",
            "info": {
                "symbol": "XBTUSDT",
                "currentQty": 100,
                "isOpen": True,
                "strategy": "OneWay",
            },
        }
        invalid = (
            None,
            [],
            [{}],
            [{**valid, "contracts": None}],
            [{**valid, "contracts": 0}],
            [{**valid, "info": {**valid["info"], "currentQty": -100}}],
        )
        with patch.dict(os.environ, {"BITMEX_TESTNET": "true"}, clear=True):
            snapshot = bitmex_client.fetch_xbtusdt_position(
                PositionExchange([valid])
            )
            self.assertEqual(snapshot.contracts, 100)
            self.assertEqual(snapshot.side, "long")
            for response in invalid:
                with self.subTest(response=response):
                    with self.assertRaises(bitmex_client.ExchangeAttestationError):
                        bitmex_client.fetch_xbtusdt_position(
                            PositionExchange(response)
                        )

    def test_public_data_client_is_unauthenticated_and_rate_limited(self):
        fake = FakeExchange()
        with patch.object(
            bitmex_client.ccxt, "bitmex", return_value=fake
        ) as constructor:
            result = bitmex_client.get_data_client()

        config = constructor.call_args.args[0]
        self.assertNotIn("apiKey", config)
        self.assertNotIn("secret", config)
        self.assertTrue(config["enableRateLimit"])
        self.assertEqual(config["timeout"], 10_000)
        self.assertEqual(fake.calls, [("load_markets",)])
        self.assertIs(result, fake)


if __name__ == "__main__":
    unittest.main(verbosity=2)
