import io
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import audit


def readiness(verdict: str) -> dict:
    failed = verdict != "READY_FOR_CANARY_REVIEW"
    return {
        "verdict": verdict,
        "gates": [
            {
                "category": "history_data",
                "name": "Versioned history",
                "status": "FAIL" if failed else "PASS",
                "detail": "history gate detail",
            }
        ],
        "history_data_blockers": ["Versioned history"] if failed else [],
        "metrics": {
            "legacy_trades": {
                "path": "data/trades.db",
                "exists": True,
                "reason": "legacy rows mix BTC quantities and contract counts",
            }
        },
    }


class AuditExitTests(unittest.TestCase):
    def run_with(self, result: dict) -> tuple[int, str]:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with (
                patch.object(audit, "chk_testnet_env", return_value=(True, "ok")),
                patch.object(audit, "chk_no_hardcoded_keys", return_value=(True, "ok")),
                patch.object(audit, "chk_testnet_guards_present", return_value=(True, "ok")),
                patch.object(audit, "chk_daily_loss_state", return_value=(True, "ok")),
                patch("sys.stdout", new_callable=io.StringIO) as output,
            ):
                code = audit.run_audit(
                    env={"BITMEX_TESTNET": "true"},
                    root=root,
                    readiness_result=result,
                )
        return code, output.getvalue()

    def test_readiness_failure_returns_nonzero(self):
        code, output = self.run_with(readiness("NOT_READY"))
        self.assertEqual(code, 1)
        self.assertIn("DO NOT USE REAL FUNDS", output)
        self.assertIn("Existing history/data blockers: Versioned history", output)

    def test_canary_review_only_returns_zero(self):
        code, output = self.run_with(readiness("READY_FOR_CANARY_REVIEW"))
        self.assertEqual(code, 0)
        self.assertIn("HUMAN CANARY REVIEW ONLY", output)
        self.assertIn("Production trading remains disabled", output)

    def test_local_failure_still_returns_nonzero(self):
        with patch.object(audit, "chk_testnet_env", return_value=(False, "not true")):
            checks = [("Runtime remains testnet-only", False, "not true")]
            self.assertEqual(audit._audit_exit_code(checks, readiness("READY_FOR_CANARY_REVIEW")), 1)


class AuditLocalSafetyTests(unittest.TestCase):
    def test_testnet_env_accepts_literal_lowercase_true_only(self):
        cases = [
            ({"BITMEX_TESTNET": "true"}, True),
            ({}, False),
            ({"BITMEX_TESTNET": ""}, False),
            ({"BITMEX_TESTNET": "True"}, False),
            ({"BITMEX_TESTNET": "TRUE"}, False),
            ({"BITMEX_TESTNET": "1"}, False),
            ({"BITMEX_TESTNET": " true"}, False),
            ({"BITMEX_TESTNET": True}, False),
        ]
        for environment, expected in cases:
            with self.subTest(environment=environment):
                passed, _ = audit.chk_testnet_env(environment)
                self.assertEqual(passed, expected)

    def test_daily_loss_accepts_current_finite_v2_state(self):
        now = datetime(2026, 7, 14, 1, 2, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "daily_loss.json"
            path.write_text(
                json.dumps(
                    {
                        "date": "2026-07-14",
                        "loss_usd": 1.25,
                        "source": "trades_v2.db",
                    }
                ),
                encoding="utf-8",
            )

            passed, detail = audit.chk_daily_loss_state(path, now=now)

        self.assertTrue(passed)
        self.assertIn("$1.25", detail)

    def test_daily_loss_rejects_untrusted_state_matrix(self):
        now = datetime(2026, 7, 14, 1, 2, tzinfo=timezone.utc)
        cases = [
            ("{", "missing or untrusted"),
            ("[]", "missing or untrusted"),
            (
                json.dumps({"date": "2026-07-14", "loss_usd": 0}),
                "missing or untrusted",
            ),
            (
                json.dumps(
                    {
                        "date": "2026-7-14",
                        "loss_usd": 0,
                        "source": "trades_v2.db",
                    }
                ),
                "missing or untrusted",
            ),
            (
                json.dumps(
                    {
                        "date": "2026-02-30",
                        "loss_usd": 0,
                        "source": "trades_v2.db",
                    }
                ),
                "missing or untrusted",
            ),
            (
                json.dumps(
                    {
                        "date": "2026-07-13",
                        "loss_usd": 0,
                        "source": "trades_v2.db",
                    }
                ),
                "stale",
            ),
            (
                '{"date":"2026-07-14","loss_usd":NaN,"source":"trades_v2.db"}',
                "non-finite",
            ),
            (
                '{"date":"2026-07-14","loss_usd":Infinity,"source":"trades_v2.db"}',
                "non-finite",
            ),
            (
                json.dumps(
                    {
                        "date": "2026-07-14",
                        "loss_usd": -0.01,
                        "source": "trades_v2.db",
                    }
                ),
                "negative",
            ),
            (
                json.dumps(
                    {
                        "date": "2026-07-14",
                        "loss_usd": "0",
                        "source": "trades_v2.db",
                    }
                ),
                "missing or untrusted",
            ),
            (
                json.dumps(
                    {
                        "date": "2026-07-14",
                        "loss_usd": 0,
                        "source": "trades.db",
                    }
                ),
                "source is untrusted",
            ),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "daily_loss.json"
            for raw, expected in cases:
                with self.subTest(raw=raw):
                    path.write_text(raw, encoding="utf-8")
                    passed, detail = audit.chk_daily_loss_state(path, now=now)
                    self.assertFalse(passed)
                    self.assertIn(expected, detail)

    def test_daily_loss_rejects_missing_file(self):
        now = datetime(2026, 7, 14, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            passed, detail = audit.chk_daily_loss_state(
                Path(tmp) / "missing.json",
                now=now,
            )
        self.assertFalse(passed)
        self.assertIn("missing or untrusted", detail)


if __name__ == "__main__":
    unittest.main(verbosity=2)
