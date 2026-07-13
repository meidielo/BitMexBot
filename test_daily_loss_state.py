import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from daily_loss_state import DailyLossStateError, refresh_daily_loss_from_ledger
from trade_ledger import close_intent_from_events, register_intent, transition_intent


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

    def close_losing_trade(self, *, exit_time_utc: str | None = None):
        exit_time_utc = exit_time_utc or datetime.now(timezone.utc).isoformat()
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
        close_intent_from_events(
            "decision-1",
            expected_statuses={"PROTECTED"},
            events=(
                {
                    "exec_id": "entry-exec",
                    "account_id": "account-1",
                    "native_symbol": "XBTUSDT",
                    "event_type": "TRADE",
                    "event_role": "ENTRY",
                    "order_id": "entry-order",
                    "client_order_id": "entry-decision-1",
                    "link_id": None,
                    "side": "BUY",
                    "last_qty": 1000,
                    "last_price_usdt": 100,
                    "commission_usdt": 0.0005,
                    "funding_usdt": 0,
                    "realised_pnl_usdt": -0.0005,
                    "transact_time_utc": "2026-07-13T00:01:00+00:00",
                    "source_hash": "a" * 64,
                },
                {
                    "exec_id": "exit-exec",
                    "account_id": "account-1",
                    "native_symbol": "XBTUSDT",
                    "event_type": "TRADE",
                    "event_role": "EXIT",
                    "order_id": "target-order",
                    "client_order_id": "target-decision-1",
                    "link_id": None,
                    "side": "SELL",
                    "last_qty": 1000,
                    "last_price_usdt": 90,
                    "commission_usdt": 0.0005,
                    "funding_usdt": 0,
                    "realised_pnl_usdt": -0.0105,
                    "transact_time_utc": exit_time_utc,
                    "source_hash": "b" * 64,
                },
            ),
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

    def test_loss_uses_exchange_exit_date_not_reconciliation_date(self):
        self.close_losing_trade(exit_time_utc="2026-07-13T13:00:00+00:00")

        payload = refresh_daily_loss_from_ledger(
            db_path=self.db,
            output_path=self.output,
            now=datetime(2026, 7, 14, 0, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(payload["date"], "2026-07-14")
        self.assertEqual(payload["loss_usd"], 0)

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

    def test_tampered_terminal_evidence_blocks_publication(self):
        self.close_losing_trade()
        with closing(sqlite3.connect(self.db)) as connection:
            connection.execute(
                "UPDATE execution_intents SET evidence_source_hash = ?",
                ("0" * 64,),
            )
            connection.commit()

        with self.assertRaisesRegex(DailyLossStateError, "evidence hash"):
            refresh_daily_loss_from_ledger(
                db_path=self.db,
                output_path=self.output,
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
