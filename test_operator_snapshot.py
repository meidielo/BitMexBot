import ast
import hashlib
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import operator_snapshot


NOW = datetime(2026, 7, 14, 1, 30, tzinfo=timezone.utc)


def write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def create_ledger(path: Path, *, now: datetime = NOW, status: str = "PROTECTED") -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            PRAGMA user_version=4;
            CREATE TABLE execution_intents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                decision_key TEXT NOT NULL,
                environment TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                requested_contracts INTEGER NOT NULL,
                filled_contracts INTEGER NOT NULL,
                closed_contracts INTEGER NOT NULL,
                expected_max_loss_usdt REAL NOT NULL,
                net_pnl_usdt REAL,
                status TEXT NOT NULL,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                entry_order_id TEXT,
                halt_reason TEXT
            );
            CREATE UNIQUE INDEX ux_execution_intents_single_unresolved
            ON execution_intents((1))
            WHERE status IN (
                'REGISTERED', 'ENTRY_PENDING', 'ENTRY_PARTIAL', 'ENTRY_FILLED',
                'PROTECTED', 'PROTECTED_NO_TP', 'HALTED_MANUAL'
            );
            """
        )
        connection.execute(
            "INSERT INTO execution_intents "
            "(decision_key, environment, symbol, side, requested_contracts, "
            "filled_contracts, closed_contracts, expected_max_loss_usdt, "
            "net_pnl_usdt, status, created_at_utc, updated_at_utc, "
            "entry_order_id, halt_reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "secret-decision-key",
                "testnet",
                "BTC/USDT:USDT",
                "LONG",
                1000,
                1000,
                0,
                1.25,
                None,
                status,
                now.isoformat(),
                now.isoformat(),
                "secret-exchange-order-id",
                "secret raw halt detail",
            ),
        )
        connection.commit()
    finally:
        connection.close()


class OperatorSnapshotTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.ledger = self.root / "trades_v2.db"
        self.daily_loss = self.root / "daily_loss.json"
        self.heartbeat = self.root / "runner_status.json"
        self.watchdog = self.root / "watchdog_status.json"
        self.promotion = self.root / "promotion_evidence.json"
        self.output = self.root / "public" / "operator_status.json"

        create_ledger(self.ledger)
        write_json(
            self.daily_loss,
            {"date": "2026-07-14", "loss_usd": 2.5, "source": "trades_v2.db"},
        )
        write_json(
            self.heartbeat,
            {
                "schema_version": 1,
                "environment": "testnet",
                "status": "RUNNING",
                "generated_at_utc": NOW.isoformat(),
                "detail_code": "main_loop",
                "raw_error": "must never cross the boundary",
            },
        )
        write_json(
            self.watchdog,
            {
                "schema_version": 1,
                "environment": "testnet",
                "state": "healthy",
                "generated_at_utc": NOW.isoformat(),
                "fresh": True,
                "api_key": "must never cross the boundary",
            },
        )
        write_json(self.promotion, {})

    def tearDown(self) -> None:
        self.temp.cleanup()

    def build(self, **overrides: object) -> dict[str, object]:
        options: dict[str, object] = {
            "ledger_path": self.ledger,
            "daily_loss_path": self.daily_loss,
            "heartbeat_path": self.heartbeat,
            "watchdog_path": self.watchdog,
            "promotion_path": self.promotion,
            "now": NOW,
        }
        options.update(overrides)
        return operator_snapshot.build_snapshot(**options)

    def test_happy_path_is_sanitized_and_does_not_modify_ledger(self) -> None:
        before_hash = hashlib.sha256(self.ledger.read_bytes()).hexdigest()
        before_mtime = self.ledger.stat().st_mtime_ns

        snapshot = self.build()
        serialized = json.dumps(snapshot, allow_nan=False)

        self.assertEqual(snapshot["schema_version"], 1)
        self.assertEqual(snapshot["data_state"], "OK")
        self.assertEqual(snapshot["environment"], "TESTNET")
        self.assertIs(snapshot["production_enabled"], False)
        self.assertIs(snapshot["actions_enabled"], False)
        self.assertEqual(snapshot["ledger"]["protection_state"], "PROTECTED")
        self.assertEqual(snapshot["risk"]["daily_loss_usdt"], 2.5)
        self.assertEqual(snapshot["runner"]["state"], "RUNNING")
        self.assertEqual(snapshot["watchdog"]["state"], "HEALTHY")
        self.assertEqual(snapshot["warnings"], [])
        self.assertNotIn("secret-decision-key", serialized)
        self.assertNotIn("secret-exchange-order-id", serialized)
        self.assertNotIn("secret raw halt detail", serialized)
        self.assertNotIn("must never cross the boundary", serialized)
        self.assertNotIn("order_id", serialized)
        self.assertNotIn("decision_key", serialized)
        self.assertEqual(hashlib.sha256(self.ledger.read_bytes()).hexdigest(), before_hash)
        self.assertEqual(self.ledger.stat().st_mtime_ns, before_mtime)

    def test_missing_sources_fail_closed_and_missing_db_is_not_created(self) -> None:
        missing_ledger = self.root / "missing.db"
        snapshot = operator_snapshot.build_snapshot(
            ledger_path=missing_ledger,
            daily_loss_path=self.root / "missing-loss.json",
            heartbeat_path=self.root / "missing-heartbeat.json",
            watchdog_path=self.root / "missing-watchdog.json",
            promotion_path=self.root / "missing-promotion.json",
            now=NOW,
        )

        self.assertEqual(snapshot["data_state"], "UNAVAILABLE")
        self.assertEqual(snapshot["environment"], "UNKNOWN")
        self.assertIs(snapshot["ledger"]["available"], False)
        self.assertEqual(snapshot["runner"]["state"], "UNKNOWN")
        self.assertEqual(snapshot["watchdog"]["state"], "UNKNOWN")
        self.assertEqual(snapshot["risk"]["state"], "UNAVAILABLE")
        self.assertEqual(snapshot["readiness"]["verdict"], "NOT_READY")
        self.assertIn("LEDGER_UNAVAILABLE", snapshot["warnings"])
        self.assertIn("PROMOTION_EVIDENCE_UNAVAILABLE", snapshot["warnings"])
        self.assertFalse(missing_ledger.exists())

    def test_non_finite_and_malformed_json_are_rejected(self) -> None:
        self.daily_loss.write_text(
            '{"date":"2026-07-14","loss_usd":NaN,"source":"trades_v2.db"}',
            encoding="utf-8",
        )
        self.heartbeat.write_text("{not-json", encoding="utf-8")
        self.promotion.write_text("[]", encoding="utf-8")

        snapshot = self.build()

        self.assertEqual(snapshot["data_state"], "DEGRADED")
        self.assertEqual(snapshot["risk"]["state"], "UNAVAILABLE")
        self.assertEqual(snapshot["runner"]["state"], "UNKNOWN")
        self.assertEqual(snapshot["readiness"]["evidence_state"], "UNAVAILABLE")
        self.assertIn("DAILY_LOSS_UNAVAILABLE", snapshot["warnings"])
        self.assertIn("HEARTBEAT_UNAVAILABLE", snapshot["warnings"])
        self.assertIn("PROMOTION_EVIDENCE_UNAVAILABLE", snapshot["warnings"])

    def test_stale_timed_sources_never_report_healthy(self) -> None:
        old = NOW - timedelta(minutes=20)
        write_json(
            self.heartbeat,
            {
                "schema_version": 1,
                "environment": "testnet",
                "status": "RUNNING",
                "generated_at_utc": old.isoformat(),
            },
        )
        write_json(
            self.watchdog,
            {
                "schema_version": 1,
                "environment": "testnet",
                "state": "healthy",
                "generated_at_utc": old.isoformat(),
                "fresh": True,
            },
        )

        snapshot = self.build()

        self.assertEqual(snapshot["runner"]["state"], "STALE")
        self.assertIs(snapshot["runner"]["fresh"], False)
        self.assertEqual(snapshot["watchdog"]["state"], "STALE")
        self.assertIs(snapshot["watchdog"]["fresh"], False)
        self.assertIn("HEARTBEAT_STALE", snapshot["warnings"])
        self.assertIn("WATCHDOG_STALE", snapshot["warnings"])

    def test_real_runtime_and_watchdog_state_vocabularies_are_supported(self) -> None:
        runner_states = {
            "STARTING": "STARTING",
            "RUNNING": "RUNNING",
            "WAITING": "WAITING",
            "PAUSED": "PAUSED",
            "MANUAL_HALT": "MANUAL_HALT",
            "FAILED": "FAILED",
            "STOPPED": "STOPPED",
        }
        for source_state, expected in runner_states.items():
            with self.subTest(runner=source_state):
                write_json(
                    self.heartbeat,
                    {
                        "schema_version": 1,
                        "environment": "testnet",
                        "status": source_state,
                        "generated_at_utc": NOW.isoformat(),
                        "detail_code": "test",
                    },
                )
                self.assertEqual(self.build()["runner"]["state"], expected)

        watchdog_states = {
            "starting": "STARTING",
            "synchronizing": "SYNCHRONIZING",
            "healthy": "HEALTHY",
            "stale": "STALE",
            "failed": "FAILED",
        }
        for source_state, expected in watchdog_states.items():
            with self.subTest(watchdog=source_state):
                write_json(
                    self.watchdog,
                    {
                        "schema_version": 1,
                        "environment": "testnet",
                        "state": source_state,
                        "generated_at_utc": NOW.isoformat(),
                        "fresh": source_state == "healthy",
                    },
                )
                self.assertEqual(self.build()["watchdog"]["state"], expected)

    def test_watchdog_reported_freshness_can_never_be_overridden_by_file_age(self) -> None:
        write_json(
            self.watchdog,
            {
                "schema_version": 1,
                "environment": "testnet",
                "state": "healthy",
                "generated_at_utc": NOW.isoformat(),
                "fresh": False,
            },
        )

        snapshot = self.build()

        self.assertEqual(snapshot["watchdog"]["state"], "STALE")
        self.assertIs(snapshot["watchdog"]["fresh"], False)
        self.assertIn("WATCHDOG_STALE", snapshot["warnings"])

    def test_manual_halt_and_unprotected_states_are_explicit_warnings(self) -> None:
        self.ledger.unlink()
        create_ledger(self.ledger, status="HALTED_MANUAL")
        halted = self.build()
        self.assertEqual(halted["ledger"]["protection_state"], "MANUAL_HALT")
        self.assertIn("MANUAL_HALT_PRESENT", halted["warnings"])

        self.ledger.unlink()
        create_ledger(self.ledger, status="ENTRY_PENDING")
        unprotected = self.build()
        self.assertEqual(unprotected["ledger"]["protection_state"], "UNPROTECTED")
        self.assertIn("UNPROTECTED_INTENT_PRESENT", unprotected["warnings"])

    def test_invalid_schema_fails_closed(self) -> None:
        self.ledger.unlink()
        connection = sqlite3.connect(self.ledger)
        connection.execute("CREATE TABLE execution_intents (id INTEGER PRIMARY KEY)")
        connection.execute("PRAGMA user_version=3")
        connection.commit()
        connection.close()

        snapshot = self.build()

        self.assertEqual(snapshot["data_state"], "UNAVAILABLE")
        self.assertIs(snapshot["ledger"]["available"], False)
        self.assertIn("LEDGER_UNAVAILABLE", snapshot["warnings"])

    def test_atomic_replace_failure_preserves_previous_snapshot(self) -> None:
        self.output.parent.mkdir()
        self.output.write_text('{"old":true}\n', encoding="utf-8")
        with patch("operator_snapshot.os.replace", side_effect=OSError("simulated")):
            with self.assertRaises(operator_snapshot.SnapshotWriteError):
                operator_snapshot.write_snapshot_atomic(self.build(), self.output)

        self.assertEqual(self.output.read_text(encoding="utf-8"), '{"old":true}\n')
        self.assertEqual(list(self.output.parent.glob("*.tmp")), [])

    def test_cli_one_shot_publishes_strict_json(self) -> None:
        current = datetime.now(timezone.utc)
        self.ledger.unlink()
        create_ledger(self.ledger, now=current)
        write_json(
            self.daily_loss,
            {
                "date": current.strftime("%Y-%m-%d"),
                "loss_usd": 0,
                "source": "trades_v2.db",
            },
        )
        write_json(
            self.heartbeat,
            {
                "schema_version": 1,
                "environment": "testnet",
                "status": "RUNNING",
                "generated_at_utc": current.isoformat(),
            },
        )
        write_json(
            self.watchdog,
            {
                "schema_version": 1,
                "environment": "testnet",
                "state": "healthy",
                "generated_at_utc": current.isoformat(),
                "fresh": True,
            },
        )

        result = operator_snapshot.main(
            [
                "--ledger",
                str(self.ledger),
                "--daily-loss",
                str(self.daily_loss),
                "--heartbeat",
                str(self.heartbeat),
                "--watchdog",
                str(self.watchdog),
                "--promotion-evidence",
                str(self.promotion),
                "--output",
                str(self.output),
            ]
        )

        self.assertEqual(result, 0)
        stored = json.loads(self.output.read_text(encoding="utf-8"))
        self.assertEqual(stored["schema_version"], 1)
        self.assertIs(stored["production_enabled"], False)

    def test_polling_stops_cleanly_on_interrupt(self) -> None:
        with (
            patch("operator_snapshot.publish_once") as publish,
            patch("operator_snapshot.time.sleep", side_effect=KeyboardInterrupt),
        ):
            result = operator_snapshot.main(["--poll-seconds", "1"])

        self.assertEqual(result, 0)
        publish.assert_called_once()

    def test_exporter_has_no_execution_or_exchange_imports(self) -> None:
        tree = ast.parse(Path(operator_snapshot.__file__).read_text(encoding="utf-8"))
        imported_roots: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_roots.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_roots.add(node.module.split(".")[0])

        blocked = {
            "bitmex_client",
            "ccxt",
            "dotenv",
            "main",
            "order_manager",
            "risk",
            "trade_ledger",
        }
        self.assertTrue(imported_roots.isdisjoint(blocked), imported_roots & blocked)


if __name__ == "__main__":
    unittest.main()
