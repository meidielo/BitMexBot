import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from daily_loss_state import DailyLossStateError, refresh_daily_loss_from_ledger
from trade_ledger import register_intent, transition_intent


def intent(key="decision-1"):
    return {
        "decision_key": key,
        "strategy_version": "test-v1",
        "environment": "testnet",
        "symbol": "BTC/USDT:USDT",
        "side": "LONG",
        "decision_time_utc": "2026-07-13T00:00:00+00:00",
        "signal_entry_price_usdt": 100,
        "signal_stop_price_usdt": 90,
        "signal_target_price_usdt": 120,
        "requested_contracts": 1000,
        "contract_size_btc": 0.000001,
        "requested_base_btc": 0.001,
        "requested_notional_usdt": 0.1,
        "expected_max_loss_usdt": 0.01,
        "unit_model": "xbtusdt-linear-metadata-v1",
        "entry_client_order_id": f"entry-{key}",
        "stop_client_order_id": f"stop-{key}",
        "target_client_order_id": f"target-{key}",
    }


class DailyLossStateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.db = str(root / "trades_v2.db")
        self.output = str(root / "daily_loss.json")

    def tearDown(self):
        self.temp.cleanup()

    def close_losing_trade(self):
        register_intent(intent(), self.db)
        transition_intent(
            "decision-1", "ENTRY_PENDING", expected_statuses={"REGISTERED"}, db_path=self.db
        )
        transition_intent(
            "decision-1",
            "ENTRY_FILLED",
            expected_statuses={"ENTRY_PENDING"},
            updates={
                "entry_order_id": "entry-order",
                "filled_contracts": 1000,
                "actual_entry_price_usdt": 100,
            },
            db_path=self.db,
        )
        transition_intent(
            "decision-1",
            "PROTECTED_NO_TP",
            expected_statuses={"ENTRY_FILLED"},
            updates={"stop_order_id": "stop-order"},
            db_path=self.db,
        )
        transition_intent(
            "decision-1",
            "PROTECTED",
            expected_statuses={"PROTECTED_NO_TP"},
            updates={"target_order_id": "target-order"},
            db_path=self.db,
        )
        transition_intent(
            "decision-1",
            "CLOSED",
            expected_statuses={"PROTECTED"},
            updates={
                "closed_contracts": 1000,
                "actual_exit_price_usdt": 90,
                "gross_pnl_usdt": -0.01,
                "fees_usdt": 0.001,
                "funding_usdt": 0,
                "net_pnl_usdt": -0.011,
                "exit_reason": "SL",
            },
            db_path=self.db,
        )

    def test_publishes_gross_loss_from_completed_v2_rows(self):
        self.close_losing_trade()

        payload = refresh_daily_loss_from_ledger(
            db_path=self.db,
            output_path=self.output,
        )

        self.assertAlmostEqual(payload["loss_usd"], 0.011)
        stored = json.loads(Path(self.output).read_text(encoding="utf-8"))
        self.assertEqual(stored["source"], "trades_v2.db")
        self.assertAlmostEqual(stored["loss_usd"], 0.011)

    def test_unresolved_intent_blocks_publication(self):
        register_intent(intent(), self.db)

        with self.assertRaisesRegex(DailyLossStateError, "unresolved"):
            refresh_daily_loss_from_ledger(
                db_path=self.db,
                output_path=self.output,
            )
        self.assertFalse(Path(self.output).exists())

    def test_missing_ledger_blocks_publication(self):
        with self.assertRaisesRegex(DailyLossStateError, "does not exist"):
            refresh_daily_loss_from_ledger(
                db_path=self.db,
                output_path=self.output,
            )

    def test_unknown_status_blocks_publication(self):
        register_intent(intent(), self.db)
        with closing(sqlite3.connect(self.db)) as connection:
            connection.execute(
                "UPDATE execution_intents SET status = 'TAMPERED'"
            )
            connection.commit()

        with self.assertRaisesRegex(DailyLossStateError, "invalid ledger"):
            refresh_daily_loss_from_ledger(
                db_path=self.db,
                output_path=self.output,
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
