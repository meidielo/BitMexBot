import os
import unittest
from unittest.mock import patch

import pandas as pd

import main
from main import (
    RunnerSafetyError,
    _decision_key,
    _get_open_positions,
    _get_usdt_balances,
    _settled_funding_for_candle,
    run_once,
)


class TestnetExchange:
    id = "bitmex"
    urls = {
        "api": {
            "public": "https://testnet.bitmex.com",
            "private": "https://testnet.bitmex.com",
        }
    }


class ExchangeStateTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {"BITMEX_TESTNET": "true"})
        self.env.start()

    def tearDown(self):
        self.env.stop()

    def test_reads_usdt_total_and_free_without_btc_conversion(self):
        class Exchange(TestnetExchange):
            def fetch_balance(self):
                return {
                    "USDT": {"total": 100.5, "free": 90.25},
                    "BTC": {"total": 99, "free": 99},
                }

        self.assertEqual(_get_usdt_balances(Exchange()), (100.5, 90.25))

    def test_rejects_missing_or_inconsistent_usdt_balance(self):
        cases = [
            {},
            {"USDT": {"total": 100}},
            {"USDT": {"total": 100, "free": 101}},
            {"USDT": {"total": float("nan"), "free": 0}},
        ]
        for value in cases:
            with self.subTest(value=value):
                class Exchange(TestnetExchange):
                    def fetch_balance(self):
                        return value

                with self.assertRaises(RunnerSafetyError):
                    _get_usdt_balances(Exchange())

    def test_position_query_failure_does_not_look_flat(self):
        class Exchange(TestnetExchange):
            def fetch_positions(self, symbols):
                return None

        with self.assertRaises(RunnerSafetyError):
            _get_open_positions(Exchange())


class CausalDecisionTests(unittest.TestCase):
    def frame(self):
        index = pd.DatetimeIndex([pd.Timestamp("2026-07-14T00:00:00Z")])
        return pd.DataFrame(
            [{"open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 1}],
            index=index,
        )

    def test_decision_key_uses_candle_close_not_open(self):
        frame = self.frame()
        close_ms = int(pd.Timestamp("2026-07-14T00:15:00Z").timestamp() * 1000)

        key = _decision_key(frame, "LONG", close_ms + 1)

        self.assertEqual(key, f"BTC/USDT:USDT|15m|{close_ms}|LONG")

    def test_funding_excludes_observations_after_candle_close(self):
        history = pd.DataFrame(
            {
                "timestamp": [
                    "2026-07-13T08:00:00Z",
                    "2026-07-13T16:00:00Z",
                    "2026-07-14T00:00:00Z",
                    "2026-07-14T08:00:00Z",
                ],
                "rate": [0.0001, 0.0002, 0.0003, 0.5],
            }
        )

        result = _settled_funding_for_candle(
            history,
            pd.Timestamp("2026-07-14T00:15:00Z"),
        )

        self.assertAlmostEqual(result["rate"], 0.0003)
        self.assertAlmostEqual(result["funding_24h"], 0.0006)

    def test_missing_settled_funding_fails_closed(self):
        self.assertIsNone(
            _settled_funding_for_candle(
                pd.DataFrame(columns=["timestamp", "rate"]),
                pd.Timestamp("2026-07-14T00:15:00Z"),
            )
        )


class RunOnceKillSwitchTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {"BITMEX_TESTNET": "true"})
        self.env.start()

    def tearDown(self):
        self.env.stop()

    def frame(self):
        index = pd.DatetimeIndex([pd.Timestamp("2026-07-14T00:00:00Z")])
        return pd.DataFrame(
            [{"open": 60000, "high": 60100, "low": 59900, "close": 60000, "volume": 10}],
            index=index,
        )

    def exchange(self):
        class Exchange(TestnetExchange):
            xbtusdt_instrument = object()

            def fetch_balance(self):
                return {"USDT": {"total": 1000.0, "free": 1000.0}}

            def fetch_positions(self, symbols):
                return [
                    {
                        "symbol": symbols[0],
                        "contracts": 0,
                        "side": None,
                        "info": {
                            "symbol": "XBTUSDT",
                            "currentQty": 0,
                            "isOpen": False,
                            "strategy": "OneWay",
                        },
                    }
                ]

        return Exchange()

    def test_fresh_daily_loss_at_limit_vetoes_before_execution(self):
        signal = {
            "signal": "LONG",
            "entry_price": 60000.0,
            "sl_price": 59400.0,
            "tp_price": 60900.0,
        }
        with (
            patch.object(main, "_reconcile_unresolved_intent", return_value=None),
            patch.object(
                main,
                "refresh_daily_loss_from_ledger",
                return_value={
                    "date": "2026-07-14",
                    "loss_usd": 50.0,
                    "source": "trades_v2.db",
                },
            ),
            patch.object(main, "fetch_ohlcv", return_value=self.frame()),
            patch.object(
                main,
                "fetch_recent_funding",
                return_value=pd.DataFrame(columns=["timestamp", "rate"]),
            ),
            patch.object(main, "get_signal", return_value=signal),
            patch.object(main, "_log_conditions"),
            patch.object(main, "execute_signal") as execute,
        ):
            result = run_once(self.exchange(), object(), now_ms=1)

        self.assertEqual(result["status"], "vetoed")
        self.assertIn("daily loss", result["risk"]["reason"])
        execute.assert_not_called()

    def test_daily_loss_refresh_failure_aborts_before_market_data_or_order(self):
        with (
            patch.object(main, "_reconcile_unresolved_intent", return_value=None),
            patch.object(
                main,
                "refresh_daily_loss_from_ledger",
                side_effect=RuntimeError("ledger unavailable"),
            ),
            patch.object(main, "fetch_ohlcv") as candles,
            patch.object(main, "execute_signal") as execute,
        ):
            result = run_once(self.exchange(), object(), now_ms=1)

        self.assertEqual(result["status"], "paused")
        self.assertIn("daily loss refresh failed", result["reason"])
        candles.assert_not_called()
        execute.assert_not_called()


class RunnerLoopTests(unittest.TestCase):
    def test_reconciling_state_rechecks_before_any_bar_sleep(self):
        outcomes = [
            {"status": "reconciling", "reason": "entry not visible"},
            {"status": "manual_halt", "reason": "late fill closed"},
        ]
        with (
            patch.object(main, "get_client", return_value=object()),
            patch.object(main, "get_data_client", return_value=object()),
            patch.object(main, "run_once", side_effect=outcomes) as run,
            patch.object(main, "_sleep_to_safety_check") as safety_sleep,
            patch.object(main, "_sleep_to_next_bar") as bar_sleep,
        ):
            with self.assertRaises(SystemExit):
                main.main()

        self.assertEqual(run.call_count, 2)
        safety_sleep.assert_called_once()
        bar_sleep.assert_not_called()

    def test_failed_execution_halts_without_sleep(self):
        with (
            patch.object(main, "get_client", return_value=object()),
            patch.object(main, "get_data_client", return_value=object()),
            patch.object(
                main,
                "run_once",
                return_value={"status": "failed", "reason": "unsafe state"},
            ),
            patch.object(main, "_sleep_to_safety_check") as safety_sleep,
            patch.object(main, "_sleep_to_next_bar") as bar_sleep,
        ):
            with self.assertRaises(SystemExit):
                main.main()

        safety_sleep.assert_not_called()
        bar_sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
