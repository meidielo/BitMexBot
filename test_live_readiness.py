import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from live_readiness import evaluate_live_readiness
from trade_ledger import (
    UNIT_MODEL,
    close_intent_from_events,
    register_intent,
    transition_intent,
)


def promotion_evidence() -> dict:
    return {
        "research": {
            "independent_costed_oos_clusters": 200,
            "untouched_oos_clusters": 100,
            "oos_folds": 3,
            "acceptable_oos_folds": 3,
            "latest_oos_fold_positive": True,
            "bootstrap_expectancy_lower_bound": 0.01,
            "profit_factor_lower_bound": 1.01,
            "deflated_sharpe_probability": 0.95,
            "probability_of_backtest_overfitting": 0.20,
            "max_single_cluster_pnl_share": 0.20,
            "top_five_cluster_pnl_share": 0.50,
            "two_x_cost_net_expectancy": 0.01,
        },
        "shadow": {
            "consecutive_days": 90,
            "matured_clusters": 50,
            "order_authority_disabled": True,
        },
        "testnet_engineering": {
            "passed_lifecycle_drills": 100,
            "required_scenarios_passed": True,
            "zero_unreconciled_incidents": True,
        },
        "mainnet_dry_run": {
            "consecutive_days": 30,
            "reconciliation_differences": 0,
            "incomplete_candle_decisions": 0,
            "order_authority_disabled": True,
            "alert_drills_passed": True,
            "credential_rotation_drill_passed": True,
            "operator_response_drills_passed": True,
        },
    }


def valid_intent(decision_key: str = "v2|XBTUSDT|2026-07-13T12:45:00Z|LONG") -> dict:
    return {
        "decision_key": decision_key,
        "strategy_version": "v2-funding-locked",
        "environment": "testnet",
        "symbol": "BTC/USDT:USDT",
        "side": "LONG",
        "decision_time_utc": "2026-07-13T12:45:00+00:00",
        "signal_entry_price_usdt": 62_500,
        "signal_stop_price_usdt": 61_250,
        "signal_target_price_usdt": 64_375,
        "requested_contracts": 1_000,
        "contract_size_btc": 0.000001,
        "requested_base_btc": 0.001,
        "requested_notional_usdt": 62.5,
        "expected_max_loss_usdt": 1.25,
        "unit_model": UNIT_MODEL,
        "entry_client_order_id": "bmb-entry-readiness-001",
        "stop_client_order_id": "bmb-stop-readiness-001",
        "target_client_order_id": "bmb-target-readiness-001",
    }


def add_closed_intent(
    path: str,
    *,
    entry_commission: float = 0.03,
    exit_commission: float = 0.03,
) -> None:
    intent = valid_intent()
    register_intent(intent, path)
    transition_intent(
        intent["decision_key"],
        "ENTRY_PENDING",
        expected_statuses={"REGISTERED"},
        updates={"entry_order_id": "entry-1"},
        db_path=path,
    )
    transition_intent(
        intent["decision_key"],
        "PROTECTED",
        expected_statuses={"ENTRY_PENDING"},
        updates={
            "filled_contracts": 1_000,
            "actual_entry_price_usdt": 62_500,
            "stop_order_id": "stop-1",
            "target_order_id": "target-1",
        },
        db_path=path,
    )
    close_intent_from_events(
        intent["decision_key"],
        expected_statuses={"PROTECTED"},
        events=(
            {
                "exec_id": "exec-entry-readiness",
                "account_id": "account-1",
                "native_symbol": "XBTUSDT",
                "event_type": "TRADE",
                "event_role": "ENTRY",
                "order_id": "entry-1",
                "client_order_id": intent["entry_client_order_id"],
                "link_id": None,
                "side": "BUY",
                "last_qty": 1_000,
                "last_price_usdt": 62_500,
                "commission_usdt": entry_commission,
                "funding_usdt": 0.0,
                "realised_pnl_usdt": -entry_commission,
                "transact_time_utc": "2026-07-13T12:46:00+00:00",
                "source_hash": "a" * 64,
            },
            {
                "exec_id": "exec-exit-readiness",
                "account_id": "account-1",
                "native_symbol": "XBTUSDT",
                "event_type": "TRADE",
                "event_role": "EXIT",
                "order_id": "target-1",
                "client_order_id": intent["target_client_order_id"],
                "link_id": None,
                "side": "SELL",
                "last_qty": 1_000,
                "last_price_usdt": 63_000,
                "commission_usdt": exit_commission,
                "funding_usdt": 0.0,
                "realised_pnl_usdt": 0.5 - exit_commission,
                "transact_time_utc": "2026-07-13T13:00:00+00:00",
                "source_hash": "b" * 64,
            },
        ),
        db_path=path,
    )


class LiveReadinessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.ledger = str(self.base / "trades_v2.db")
        self.now = datetime(2026, 7, 13, 12, 0, tzinfo=timezone.utc)

    def tearDown(self):
        self.tmp.cleanup()

    def evaluate(self, **overrides):
        values = {
            "trade_db_path": self.ledger,
            "env": {"BITMEX_TESTNET": "true"},
            "now": self.now,
            "promotion_evidence": promotion_evidence(),
            "legacy_trade_db_path": str(self.base / "trades.db"),
        }
        values.update(overrides)
        return evaluate_live_readiness(**values)

    def test_legacy_trade_database_is_never_evidence(self):
        legacy = self.base / "trades.db"
        with closing(sqlite3.connect(legacy)) as connection:
            connection.execute("CREATE TABLE trades (pnl_usd REAL)")
            connection.execute("INSERT INTO trades VALUES (999999)")
            connection.commit()

        result = self.evaluate()

        self.assertEqual(result["verdict"], "NOT_READY")
        self.assertTrue(result["metrics"]["legacy_trades"]["excluded_from_evidence"])
        self.assertEqual(result["metrics"]["ledger_v2"]["total_intents"], 0)
        self.assertIn("Versioned execution ledger readable", result["history_data_blockers"])

    def test_complete_v2_history_reaches_canary_review_only(self):
        add_closed_intent(self.ledger)

        result = self.evaluate()

        self.assertEqual(result["verdict"], "READY_FOR_CANARY_REVIEW")
        self.assertFalse(result["production_enabled"])
        self.assertFalse(result["promotion"]["production_enabled"])
        self.assertEqual(result["failed_gates"], 0)

    def test_missing_promotion_evidence_fails_closed(self):
        add_closed_intent(self.ledger)

        result = self.evaluate(
            promotion_evidence=None,
            promotion_evidence_path=str(self.base / "missing.json"),
        )

        self.assertEqual(result["verdict"], "NOT_READY")
        self.assertEqual(result["promotion"]["stage"], "RESEARCH")
        self.assertIn("Promotion evidence loaded", result["history_data_blockers"])

    def test_unprotected_intent_rejects_promotion(self):
        register_intent(valid_intent(), self.ledger)

        result = self.evaluate()

        self.assertEqual(result["verdict"], "REJECT_DO_NOT_PROMOTE")
        self.assertEqual(result["metrics"]["ledger_v2"]["unprotected_intents"], 1)
        self.assertTrue(
            any(
                rule["name"] == "Safety-critical ledger breach"
                and rule["status"] == "TRIGGERED"
                for rule in result["no_go_rules"]
            )
        )

    def test_manual_halt_rejects_promotion(self):
        intent = valid_intent()
        register_intent(intent, self.ledger)
        transition_intent(
            intent["decision_key"],
            "HALTED_MANUAL",
            expected_statuses={"REGISTERED"},
            updates={"halt_reason": "protection state unknown"},
            db_path=self.ledger,
        )

        result = self.evaluate()

        self.assertEqual(result["verdict"], "REJECT_DO_NOT_PROMOTE")
        self.assertEqual(result["metrics"]["ledger_v2"]["halted_manual_intents"], 1)

    def test_tampered_accounting_rejects_promotion(self):
        add_closed_intent(self.ledger)
        with closing(sqlite3.connect(self.ledger)) as connection:
            connection.execute(
                "UPDATE execution_intents SET net_pnl_usdt = 999 WHERE status = 'CLOSED'"
            )
            connection.commit()

        result = self.evaluate()

        self.assertEqual(result["verdict"], "REJECT_DO_NOT_PROMOTE")
        self.assertEqual(result["metrics"]["ledger_v2"]["invalid_accounting_rows"], 1)

    def test_signed_maker_rebate_is_valid_accounting(self):
        add_closed_intent(
            self.ledger,
            entry_commission=-0.01,
            exit_commission=-0.01,
        )

        result = self.evaluate()

        self.assertEqual(result["metrics"]["ledger_v2"]["invalid_accounting_rows"], 0)
        self.assertEqual(result["metrics"]["ledger_v2"]["invalid_evidence_rows"], 0)

    def test_tampered_execution_evidence_hash_rejects_promotion(self):
        add_closed_intent(self.ledger)
        with closing(sqlite3.connect(self.ledger)) as connection:
            connection.execute(
                "UPDATE execution_intents SET evidence_source_hash = ? "
                "WHERE status = 'CLOSED'",
                ("0" * 64,),
            )
            connection.commit()

        result = self.evaluate()

        self.assertEqual(result["verdict"], "REJECT_DO_NOT_PROMOTE")
        self.assertEqual(result["metrics"]["ledger_v2"]["invalid_evidence_rows"], 1)

    def test_unmatched_appended_event_rejects_promotion(self):
        add_closed_intent(self.ledger)
        with closing(sqlite3.connect(self.ledger)) as connection:
            intent_id = connection.execute(
                "SELECT id FROM execution_intents WHERE status = 'CLOSED'"
            ).fetchone()[0]
            connection.execute(
                "INSERT INTO execution_events ("
                "intent_id, exec_id, account_id, native_symbol, event_type, event_role, "
                "order_id, client_order_id, link_id, side, last_qty, last_price_usdt, "
                "commission_usdt, funding_usdt, realised_pnl_usdt, transact_time_utc, "
                "source_hash, created_at_utc) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    intent_id,
                    "unmatched-funding",
                    "account-1",
                    "XBTUSDT",
                    "FUNDING",
                    "FUNDING",
                    None,
                    None,
                    None,
                    None,
                    1_000,
                    None,
                    0.0,
                    999.0,
                    999.0,
                    "2026-07-13T12:55:00+00:00",
                    "c" * 64,
                    "2026-07-13T13:01:00+00:00",
                ),
            )
            connection.commit()

        result = self.evaluate()

        self.assertEqual(result["verdict"], "REJECT_DO_NOT_PROMOTE")
        self.assertEqual(result["metrics"]["ledger_v2"]["invalid_evidence_rows"], 1)

    def test_old_schema_version_fails_closed(self):
        add_closed_intent(self.ledger)
        with closing(sqlite3.connect(self.ledger)) as connection:
            connection.execute("PRAGMA user_version=3")
            connection.commit()

        result = self.evaluate()

        self.assertFalse(result["metrics"]["ledger_v2"]["schema_valid"])
        self.assertEqual(
            result["metrics"]["ledger_v2"]["error"],
            "ledger schema version is not current",
        )
        self.assertEqual(result["verdict"], "NOT_READY")

    def test_unknown_ledger_status_rejects_promotion(self):
        register_intent(valid_intent(), self.ledger)
        with closing(sqlite3.connect(self.ledger)) as connection:
            connection.execute("UPDATE execution_intents SET status = 'UNKNOWN'")
            connection.commit()

        result = self.evaluate()

        self.assertEqual(result["verdict"], "REJECT_DO_NOT_PROMOTE")
        self.assertEqual(result["metrics"]["ledger_v2"]["invalid_status_rows"], 1)

    def test_missing_testnet_environment_blocks_review(self):
        add_closed_intent(self.ledger)

        result = self.evaluate(env={})

        self.assertEqual(result["verdict"], "NOT_READY")
        self.assertEqual(result["gates"][0]["status"], "FAIL")


if __name__ == "__main__":
    unittest.main(verbosity=2)
