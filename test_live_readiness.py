import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from live_readiness import evaluate_live_readiness
from trade_ledger import UNIT_MODEL, register_intent, transition_intent


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


def add_closed_intent(path: str) -> None:
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
    transition_intent(
        intent["decision_key"],
        "CLOSED",
        expected_statuses={"PROTECTED"},
        updates={
            "closed_contracts": 1_000,
            "actual_exit_price_usdt": 63_000,
            "gross_pnl_usdt": 0.5,
            "fees_usdt": 0.06,
            "funding_usdt": 0.0,
            "net_pnl_usdt": 0.44,
            "exit_reason": "TARGET",
        },
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
