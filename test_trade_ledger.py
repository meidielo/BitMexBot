import os
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path

from trade_ledger import (
    DB_PATH,
    LedgerError,
    UNIT_MODEL,
    close_intent_from_events,
    get_intent,
    get_intent_by_client_order_id,
    get_intent_by_external_order_id,
    initialize_ledger,
    list_active_intents,
    list_execution_events,
    list_open_intents,
    record_execution_evidence,
    register_intent,
    transition_intent,
)


def valid_intent() -> dict:
    return {
        "decision_key": "v2|XBTUSDT|2026-07-13T12:45:00Z|LONG",
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
        "entry_client_order_id": "bmb-entry-0123456789",
        "stop_client_order_id": "bmb-stop-0123456789",
        "target_client_order_id": "bmb-target-0123456789",
    }


def distinct_intent(sequence: int) -> dict:
    values = valid_intent()
    suffix = f"{sequence:010d}"
    values.update(
        {
            "decision_key": f"v2|XBTUSDT|2026-07-13T12:45:00Z|LONG|{sequence}",
            "decision_time_utc": f"2026-07-13T12:4{sequence}:00+00:00",
            "entry_client_order_id": f"bmb-entry-{suffix}",
            "stop_client_order_id": f"bmb-stop-{suffix}",
            "target_client_order_id": f"bmb-target-{suffix}",
        }
    )
    return values


def execution_event(
    exec_id: str,
    role: str,
    order_id: str,
    side: str,
    quantity: int,
    price: float,
    timestamp: str,
    source_hash: str,
    commission: float,
    realised_pnl: float,
) -> dict:
    return {
        "exec_id": exec_id,
        "account_id": "42",
        "native_symbol": "XBTUSDT",
        "event_type": "TRADE",
        "event_role": role,
        "order_id": order_id,
        "client_order_id": f"{role.lower()}-client",
        "link_id": None,
        "side": side,
        "last_qty": quantity,
        "last_price_usdt": price,
        "commission_usdt": commission,
        "funding_usdt": 0,
        "realised_pnl_usdt": realised_pnl,
        "transact_time_utc": timestamp,
        "source_hash": source_hash,
    }


class TradeLedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "trades_v2.db")

    def tearDown(self):
        self.tmp.cleanup()

    def protect_intent(self) -> str:
        key = valid_intent()["decision_key"]
        register_intent(valid_intent(), self.db_path)
        transition_intent(
            key,
            "ENTRY_PENDING",
            expected_statuses={"REGISTERED"},
            updates={"entry_order_id": "exchange-entry-1"},
            db_path=self.db_path,
        )
        transition_intent(
            key,
            "PROTECTED",
            expected_statuses={"ENTRY_PENDING"},
            updates={
                "filled_contracts": 1_000,
                "actual_entry_price_usdt": 62_510,
                "stop_order_id": "exchange-stop-1",
                "target_order_id": "exchange-target-1",
            },
            db_path=self.db_path,
        )
        return key

    def test_explicit_initialization_allows_fail_closed_reads(self):
        initialized = initialize_ledger(self.db_path)

        self.assertEqual(initialized, str(Path(self.db_path).resolve()))
        self.assertEqual(list_open_intents(self.db_path), [])

    def test_registers_intent_before_submission(self):
        row = register_intent(valid_intent(), self.db_path)
        self.assertEqual(row["status"], "REGISTERED")
        self.assertEqual(row["requested_contracts"], 1_000)
        self.assertEqual(len(list_active_intents(self.db_path)), 1)

    def test_exact_registration_is_idempotent(self):
        first = register_intent(valid_intent(), self.db_path)
        second = register_intent(valid_intent(), self.db_path)
        self.assertEqual(first["id"], second["id"])

    def test_distinct_concurrent_registrations_cannot_both_be_unresolved(self):
        barrier = threading.Barrier(2)

        def attempt(values: dict) -> tuple[str, str]:
            barrier.wait(timeout=5)
            try:
                row = register_intent(values, self.db_path)
            except LedgerError as exc:  # agent-quality: allow: expected race loser is asserted below
                return "error", str(exc)
            return "ok", str(row["decision_key"])

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(
                executor.map(attempt, (distinct_intent(1), distinct_intent(2)))
            )

        successes = [value for outcome, value in results if outcome == "ok"]
        errors = [value for outcome, value in results if outcome == "error"]
        self.assertEqual(len(successes), 1)
        self.assertEqual(len(errors), 1)
        self.assertIn("unresolved intent", errors[0])
        self.assertEqual(
            [row["decision_key"] for row in list_open_intents(self.db_path)],
            successes,
        )

    def test_database_has_unique_single_unresolved_invariant(self):
        first = register_intent(valid_intent(), self.db_path)
        transition_intent(
            first["decision_key"],
            "FAILED_FLAT",
            expected_statuses={"REGISTERED"},
            db_path=self.db_path,
        )
        register_intent(distinct_intent(3), self.db_path)
        with closing(sqlite3.connect(self.db_path)) as connection:
            indexes = {
                str(row[1]): bool(row[2])
                for row in connection.execute(
                    "PRAGMA index_list(execution_intents)"
                ).fetchall()
            }
            index_sql = connection.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
                ("ux_execution_intents_single_unresolved",),
            ).fetchone()[0]
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE execution_intents SET status = 'REGISTERED' "
                    "WHERE decision_key = ?",
                    (first["decision_key"],),
                )
            connection.rollback()
        self.assertTrue(indexes["ux_execution_intents_single_unresolved"])
        self.assertIn("WHERE status IN", index_sql)

    def test_only_registration_migrates_an_existing_ledger(self):
        original = register_intent(valid_intent(), self.db_path)
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute("DROP INDEX ux_execution_intents_single_unresolved")
            connection.execute("DROP TABLE execution_events")
            connection.execute("PRAGMA user_version = 3")
            connection.commit()

        with self.assertRaisesRegex(LedgerError, "schema version"):
            get_intent(original["decision_key"], self.db_path)

        migrated = register_intent(valid_intent(), self.db_path)
        self.assertEqual(migrated["id"], original["id"])
        self.assertEqual(
            get_intent(original["decision_key"], self.db_path)["status"],
            "REGISTERED",
        )
        with closing(sqlite3.connect(self.db_path)) as connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            intent_columns = {
                row[1]
                for row in connection.execute(
                    "PRAGMA table_info(execution_intents)"
                ).fetchall()
            }
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            triggers = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'trigger'"
                ).fetchall()
            }
        self.assertEqual(version, 4)
        self.assertTrue(
            {
                "entry_time_utc",
                "exit_time_utc",
                "reconciled_at_utc",
                "evidence_source_hash",
            }
            <= intent_columns
        )
        self.assertIn("execution_events", tables)
        self.assertEqual(
            triggers,
            {
                "execution_events_append_only_update",
                "execution_events_append_only_delete",
            },
        )

    def test_event_close_is_atomic_append_only_and_restart_idempotent(self):
        key = self.protect_intent()
        events = [
            execution_event(
                "entry-exec-1",
                "ENTRY",
                "exchange-entry-1",
                "BUY",
                1_000,
                62_510,
                "2026-07-13T13:00:00+00:00",
                "a" * 64,
                0.031255,
                -0.031255,
            ),
            execution_event(
                "exit-exec-1",
                "EXIT",
                "exchange-target-1",
                "SELL",
                1_000,
                63_000,
                "2026-07-13T13:10:00+00:00",
                "b" * 64,
                0.0315,
                0.4585,
            ),
        ]

        closed = close_intent_from_events(
            key,
            expected_statuses={"PROTECTED"},
            events=events,
            db_path=self.db_path,
        )

        self.assertEqual(closed["status"], "CLOSED")
        self.assertEqual(closed["exit_reason"], "take_profit")
        self.assertEqual(closed["entry_time_utc"], "2026-07-13T13:00:00+00:00")
        self.assertEqual(closed["exit_time_utc"], "2026-07-13T13:10:00+00:00")
        self.assertIsNotNone(closed["reconciled_at_utc"])
        self.assertRegex(closed["evidence_source_hash"], r"^[0-9a-f]{64}$")
        self.assertAlmostEqual(closed["actual_exit_price_usdt"], 63_000)
        self.assertAlmostEqual(closed["gross_pnl_usdt"], 0.49)
        self.assertAlmostEqual(closed["fees_usdt"], 0.062755)
        self.assertAlmostEqual(closed["net_pnl_usdt"], 0.427245)
        self.assertEqual(len(list_execution_events(key, self.db_path)), 2)

        replayed = close_intent_from_events(
            key,
            expected_statuses={"PROTECTED"},
            events=events,
            db_path=self.db_path,
        )
        self.assertEqual(replayed["id"], closed["id"])
        self.assertEqual(len(list_execution_events(key, self.db_path)), 2)

        changed = [dict(event) for event in events]
        changed[0]["source_hash"] = "c" * 64
        with self.assertRaisesRegex(LedgerError, "different execution evidence"):
            close_intent_from_events(
                key,
                expected_statuses={"PROTECTED"},
                events=changed,
                db_path=self.db_path,
            )

        with closing(sqlite3.connect(self.db_path)) as connection:
            with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
                connection.execute(
                    "UPDATE execution_events SET source_hash = ?",
                    ("d" * 64,),
                )
            connection.rollback()
            with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
                connection.execute("DELETE FROM execution_events")
            connection.rollback()

    def test_incomplete_event_close_rolls_back_event_and_state_together(self):
        key = self.protect_intent()
        entry_only = execution_event(
            "entry-exec-1",
            "ENTRY",
            "exchange-entry-1",
            "BUY",
            1_000,
            62_510,
            "2026-07-13T13:00:00+00:00",
            "a" * 64,
            0.031255,
            -0.031255,
        )

        with self.assertRaisesRegex(LedgerError, "filled quantity"):
            close_intent_from_events(
                key,
                expected_statuses={"PROTECTED"},
                events=[entry_only],
                db_path=self.db_path,
            )

        self.assertEqual(get_intent(key, self.db_path)["status"], "PROTECTED")
        self.assertEqual(list_execution_events(key, self.db_path), [])

    def test_missing_ledger_reads_and_mutations_fail_without_creating_it(self):
        missing = str(Path(self.tmp.name) / "absent" / "trades_v2.db")
        operations = {
            "get intent": lambda: get_intent("missing", missing),
            "client reference": lambda: get_intent_by_client_order_id(
                "missing-client", missing
            ),
            "external reference": lambda: get_intent_by_external_order_id(
                "missing-order", missing
            ),
            "list active": lambda: list_active_intents(missing),
            "list open": lambda: list_open_intents(missing),
            "transition": lambda: transition_intent(
                "missing",
                "ENTRY_PENDING",
                expected_statuses={"REGISTERED"},
                db_path=missing,
            ),
            "record evidence": lambda: record_execution_evidence(
                "missing",
                expected_statuses={"ENTRY_PENDING"},
                updates={"entry_order_id": "missing-order"},
                db_path=missing,
            ),
        }
        for label, operation in operations.items():
            with self.subTest(operation=label):
                with self.assertRaisesRegex(LedgerError, "does not exist"):
                    operation()
                self.assertFalse(Path(missing).exists())
        self.assertFalse(Path(missing).parent.exists())

    def test_existing_helpers_reject_missing_schema_without_initializing_it(self):
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)")
            connection.commit()

        operations = {
            "read": lambda: get_intent("missing", self.db_path),
            "transition": lambda: transition_intent(
                "missing",
                "ENTRY_PENDING",
                expected_statuses={"REGISTERED"},
                db_path=self.db_path,
            ),
            "evidence": lambda: record_execution_evidence(
                "missing",
                expected_statuses={"ENTRY_PENDING"},
                updates={"entry_order_id": "missing-order"},
                db_path=self.db_path,
            ),
        }
        for label, operation in operations.items():
            with self.subTest(operation=label):
                with self.assertRaisesRegex(LedgerError, "ledger schema"):
                    operation()

        with closing(sqlite3.connect(self.db_path)) as connection:
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
        self.assertNotIn("execution_intents", tables)

    def test_default_database_path_is_repo_anchored_across_working_directories(self):
        expected = Path(__file__).resolve().parent / "data" / "trades_v2.db"
        original_cwd = Path.cwd()
        alternate_cwd = Path(self.tmp.name) / "alternate-cwd"
        alternate_cwd.mkdir()
        try:
            os.chdir(alternate_cwd)
            self.assertTrue(Path(DB_PATH).is_absolute())
            self.assertEqual(Path(DB_PATH), expected)
        finally:
            os.chdir(original_cwd)

    def test_reused_decision_key_with_different_order_is_rejected(self):
        register_intent(valid_intent(), self.db_path)
        changed = valid_intent()
        changed["requested_contracts"] = 2_000
        with self.assertRaises(LedgerError):
            register_intent(changed, self.db_path)

    def test_production_intent_is_hard_disabled(self):
        intent = valid_intent()
        intent["environment"] = "production"
        with self.assertRaisesRegex(LedgerError, "refuses production"):
            register_intent(intent, self.db_path)

    def test_transitions_are_atomic_and_ordered(self):
        register_intent(valid_intent(), self.db_path)
        pending = transition_intent(
            valid_intent()["decision_key"],
            "ENTRY_PENDING",
            expected_statuses={"REGISTERED"},
            updates={"entry_order_id": "exchange-entry-1"},
            db_path=self.db_path,
        )
        self.assertEqual(pending["status"], "ENTRY_PENDING")
        protected = transition_intent(
            valid_intent()["decision_key"],
            "PROTECTED",
            expected_statuses={"ENTRY_PENDING"},
            updates={
                "filled_contracts": 1_000,
                "actual_entry_price_usdt": 62_510,
                "stop_order_id": "exchange-stop-1",
                "target_order_id": "exchange-target-1",
            },
            db_path=self.db_path,
        )
        self.assertEqual(protected["filled_contracts"], 1_000)
        persisted = get_intent(valid_intent()["decision_key"], self.db_path)
        self.assertEqual(persisted["status"], "PROTECTED")

    def test_full_fill_is_durable_before_protection(self):
        key = valid_intent()["decision_key"]
        register_intent(valid_intent(), self.db_path)
        transition_intent(
            key,
            "ENTRY_PENDING",
            expected_statuses={"REGISTERED"},
            updates={"entry_order_id": "exchange-entry-1"},
            db_path=self.db_path,
        )
        filled = transition_intent(
            key,
            "ENTRY_FILLED",
            expected_statuses={"ENTRY_PENDING"},
            updates={
                "filled_contracts": 1_000,
                "actual_entry_price_usdt": 62_510,
            },
            db_path=self.db_path,
        )
        self.assertEqual(filled["status"], "ENTRY_FILLED")
        self.assertEqual(list_open_intents(self.db_path)[0]["status"], "ENTRY_FILLED")

    def test_partial_fill_evidence_is_monotonic_and_restart_safe(self):
        key = valid_intent()["decision_key"]
        register_intent(valid_intent(), self.db_path)
        transition_intent(
            key,
            "ENTRY_PENDING",
            expected_statuses={"REGISTERED"},
            updates={"entry_order_id": "exchange-entry-1"},
            db_path=self.db_path,
        )
        first = transition_intent(
            key,
            "ENTRY_PARTIAL",
            expected_statuses={"ENTRY_PENDING"},
            updates={
                "filled_contracts": 200,
                "actual_entry_price_usdt": 62_500,
            },
            db_path=self.db_path,
        )
        second = record_execution_evidence(
            key,
            expected_statuses={"ENTRY_PARTIAL"},
            expected_updated_at_utc=first["updated_at_utc"],
            updates={
                "filled_contracts": 500,
                "actual_entry_price_usdt": 62_502,
            },
            db_path=self.db_path,
        )
        self.assertEqual(second["filled_contracts"], 500)
        self.assertEqual(second["actual_entry_price_usdt"], 62_502)

        with self.assertRaisesRegex(LedgerError, "cannot decrease"):
            record_execution_evidence(
                key,
                expected_statuses={"ENTRY_PARTIAL"},
                updates={"filled_contracts": 400},
                db_path=self.db_path,
            )
        with self.assertRaisesRegex(LedgerError, "updated entry average"):
            record_execution_evidence(
                key,
                expected_statuses={"ENTRY_PARTIAL"},
                updates={"filled_contracts": 600},
                db_path=self.db_path,
            )
        with self.assertRaisesRegex(LedgerError, "changed after it was read"):
            record_execution_evidence(
                key,
                expected_statuses={"ENTRY_PARTIAL"},
                expected_updated_at_utc=first["updated_at_utc"],
                updates={"filled_contracts": 600},
                db_path=self.db_path,
            )

    def test_stop_only_state_can_be_completed_after_restart(self):
        key = valid_intent()["decision_key"]
        register_intent(valid_intent(), self.db_path)
        transition_intent(
            key,
            "ENTRY_PENDING",
            expected_statuses={"REGISTERED"},
            updates={"entry_order_id": "exchange-entry-1"},
            db_path=self.db_path,
        )
        stop_only = transition_intent(
            key,
            "PROTECTED_NO_TP",
            expected_statuses={"ENTRY_PENDING"},
            updates={
                "filled_contracts": 1_000,
                "actual_entry_price_usdt": 62_510,
                "stop_order_id": "exchange-stop-1",
            },
            db_path=self.db_path,
        )
        self.assertEqual(stop_only["status"], "PROTECTED_NO_TP")
        protected = transition_intent(
            key,
            "PROTECTED",
            expected_statuses={"PROTECTED_NO_TP"},
            updates={"target_order_id": "exchange-target-1"},
            db_path=self.db_path,
        )
        self.assertEqual(protected["status"], "PROTECTED")

    def test_close_requires_and_persists_complete_reconciliation(self):
        key = valid_intent()["decision_key"]
        register_intent(valid_intent(), self.db_path)
        transition_intent(
            key,
            "ENTRY_PENDING",
            expected_statuses={"REGISTERED"},
            updates={"entry_order_id": "exchange-entry-1"},
            db_path=self.db_path,
        )
        transition_intent(
            key,
            "PROTECTED",
            expected_statuses={"ENTRY_PENDING"},
            updates={
                "filled_contracts": 1_000,
                "actual_entry_price_usdt": 62_510,
                "stop_order_id": "exchange-stop-1",
                "target_order_id": "exchange-target-1",
            },
            db_path=self.db_path,
        )
        with self.assertRaisesRegex(LedgerError, "updated exit average"):
            transition_intent(
                key,
                "CLOSED",
                expected_statuses={"PROTECTED"},
                updates={"closed_contracts": 1_000},
                db_path=self.db_path,
            )

        with self.assertRaisesRegex(LedgerError, "complete exchange provenance"):
            transition_intent(
                key,
                "CLOSED",
                expected_statuses={"PROTECTED"},
                updates={
                    "closed_contracts": 1_000,
                    "exit_client_order_id": "bmb-exit-0123456789",
                    "exit_order_id": "exchange-exit-1",
                    "actual_exit_price_usdt": 63_000,
                    "gross_pnl_usdt": 0.49,
                    "fees_usdt": 0.06275,
                    "funding_usdt": -0.01,
                    "net_pnl_usdt": 0.41725,
                    "exit_reason": "take_profit",
                },
                db_path=self.db_path,
            )
        self.assertEqual(get_intent(key, self.db_path)["status"], "PROTECTED")

    def test_native_realised_pnl_mismatch_rolls_back_close(self):
        key = self.protect_intent()
        events = [
            execution_event(
                "entry-native-mismatch",
                "ENTRY",
                "exchange-entry-1",
                "BUY",
                1_000,
                62_510,
                "2026-07-13T13:00:00+00:00",
                "a" * 64,
                0.031255,
                -0.031255,
            ),
            execution_event(
                "exit-native-mismatch",
                "EXIT",
                "exchange-target-1",
                "SELL",
                1_000,
                63_000,
                "2026-07-13T13:10:00+00:00",
                "b" * 64,
                0.0315,
                -999,
            ),
        ]

        with self.assertRaisesRegex(LedgerError, "native realised PnL"):
            close_intent_from_events(
                key,
                expected_statuses={"PROTECTED"},
                events=events,
                db_path=self.db_path,
            )

        self.assertEqual(get_intent(key, self.db_path)["status"], "PROTECTED")
        self.assertEqual(list_execution_events(key, self.db_path), [])

    def test_halted_manual_remains_visible_for_reconciliation(self):
        key = valid_intent()["decision_key"]
        register_intent(valid_intent(), self.db_path)
        halted = transition_intent(
            key,
            "HALTED_MANUAL",
            expected_statuses={"REGISTERED"},
            updates={"halt_reason": "ambiguous exchange state"},
            db_path=self.db_path,
        )
        self.assertEqual(list_active_intents(self.db_path), [])
        self.assertEqual(list_open_intents(self.db_path)[0]["id"], halted["id"])

    def test_order_identifiers_are_immutable(self):
        key = valid_intent()["decision_key"]
        register_intent(valid_intent(), self.db_path)
        transition_intent(
            key,
            "ENTRY_PENDING",
            expected_statuses={"REGISTERED"},
            updates={"entry_order_id": "exchange-entry-1"},
            db_path=self.db_path,
        )
        with self.assertRaisesRegex(LedgerError, "immutable"):
            record_execution_evidence(
                key,
                expected_statuses={"ENTRY_PENDING"},
                updates={"entry_order_id": "different-entry"},
                db_path=self.db_path,
            )

    def test_invalid_transition_fails_closed(self):
        register_intent(valid_intent(), self.db_path)
        with self.assertRaisesRegex(LedgerError, "invalid transition"):
            transition_intent(
                valid_intent()["decision_key"],
                "CLOSED",
                expected_statuses={"REGISTERED"},
                db_path=self.db_path,
            )

    def test_stale_compare_and_set_fails(self):
        register_intent(valid_intent(), self.db_path)
        transition_intent(
            valid_intent()["decision_key"],
            "ENTRY_PENDING",
            expected_statuses={"REGISTERED"},
            db_path=self.db_path,
        )
        with self.assertRaisesRegex(LedgerError, "expected status"):
            transition_intent(
                valid_intent()["decision_key"],
                "FAILED_FLAT",
                expected_statuses={"REGISTERED"},
                db_path=self.db_path,
            )

    def test_missing_or_malformed_evidence_is_rejected(self):
        intent = valid_intent()
        intent["requested_contracts"] = 0
        with self.assertRaises(LedgerError):
            register_intent(intent, self.db_path)
        intent = valid_intent()
        intent["unit_model"] = "legacy-ambiguous"
        with self.assertRaises(LedgerError):
            register_intent(intent, self.db_path)


if __name__ == "__main__":
    unittest.main(verbosity=2)
