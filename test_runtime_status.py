import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from runtime_status import RuntimeStatusError, build_runner_status, write_runner_status


class RuntimeStatusTests(unittest.TestCase):
    def test_builds_only_allowlisted_sanitized_fields(self):
        observed = datetime(2026, 7, 14, 1, 2, 3, tzinfo=timezone.utc)

        result = build_runner_status(
            "running",
            detail_code="managed_position",
            observed_at=observed,
        )

        self.assertEqual(
            result,
            {
                "schema_version": 1,
                "generated_at_utc": "2026-07-14T01:02:03+00:00",
                "environment": "testnet",
                "status": "RUNNING",
                "detail_code": "managed_position",
            },
        )

    def test_rejects_free_form_or_sensitive_detail(self):
        for value in (
            "contains spaces",
            "secret=abc",
            "https://example.invalid",
            "x" * 65,
            "",
        ):
            with self.subTest(value=value):
                with self.assertRaises(RuntimeStatusError):
                    build_runner_status("running", detail_code=value)

    def test_rejects_unknown_status_and_naive_timestamp(self):
        with self.assertRaises(RuntimeStatusError):
            build_runner_status("trading_live", detail_code="loop")
        with self.assertRaises(RuntimeStatusError):
            build_runner_status(
                "running",
                detail_code="loop",
                observed_at=datetime(2026, 7, 14),
            )

    def test_atomically_replaces_valid_json(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "nested", "runner_status.json")
            first = write_runner_status(
                "starting",
                detail_code="client_init",
                path=path,
                observed_at=datetime(2026, 7, 14, tzinfo=timezone.utc),
            )
            second = write_runner_status(
                "waiting",
                detail_code="next_candle",
                path=path,
                observed_at=datetime(2026, 7, 14, 0, 0, 5, tzinfo=timezone.utc),
            )

            with open(path, encoding="utf-8") as handle:
                stored = json.load(handle)

            self.assertEqual(first["status"], "STARTING")
            self.assertEqual(stored, second)
            self.assertEqual(os.listdir(os.path.dirname(path)), ["runner_status.json"])

    def test_directory_setup_failure_is_normalized(self):
        with (
            patch("runtime_status.os.makedirs", side_effect=OSError("read only")),
            self.assertRaisesRegex(RuntimeStatusError, "OSError"),
        ):
            write_runner_status("running", detail_code="decision_cycle")

    def test_temporary_file_setup_failure_is_normalized(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "runner_status.json")
            with (
                patch(
                    "runtime_status.tempfile.mkstemp",
                    side_effect=OSError("volume full"),
                ),
                self.assertRaisesRegex(RuntimeStatusError, "OSError"),
            ):
                write_runner_status(
                    "paused",
                    detail_code="safety_reconciliation",
                    path=path,
                )


if __name__ == "__main__":
    unittest.main()
