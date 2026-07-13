import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from healthcheck import HealthcheckError, PROBES, check_status_file


class HealthcheckTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "status.json")
        self.now = datetime(2026, 7, 14, 1, 0, tzinfo=timezone.utc)

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, payload):
        with open(self.path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)

    def payload(self, **changes):
        result = {
            "schema_version": 1,
            "generated_at_utc": self.now.isoformat(),
            "status": "RUNNING",
        }
        result.update(changes)
        return result

    def test_accepts_current_allowlisted_record(self):
        self.write(self.payload())

        result = check_status_file(
            self.path,
            schema_version=1,
            max_age_seconds=60,
            status_field="status",
            allowed_statuses={"RUNNING"},
            now=self.now,
        )

        self.assertEqual(result["status"], "RUNNING")

    def test_watchdog_probe_accepts_only_non_failure_producer_states(self):
        allowed = PROBES["watchdog"]["allowed_statuses"]

        self.assertEqual(allowed, {"starting", "synchronizing", "healthy"})
        self.assertTrue({"stale", "failed"}.isdisjoint(allowed))

    def test_snapshot_probe_rejects_unavailable_data_state(self):
        self.write(self.payload(data_state="UNAVAILABLE"))

        with self.assertRaisesRegex(HealthcheckError, "unsafe data_state"):
            check_status_file(
                self.path,
                schema_version=1,
                max_age_seconds=60,
                rejected_values={"data_state": {"UNAVAILABLE"}},
                now=self.now,
            )

    def test_rejects_stale_future_wrong_schema_and_unsafe_status(self):
        cases = (
            (
                self.payload(
                    generated_at_utc=(self.now - timedelta(seconds=61)).isoformat()
                ),
                "stale",
            ),
            (
                self.payload(
                    generated_at_utc=(self.now + timedelta(seconds=6)).isoformat()
                ),
                "future",
            ),
            (self.payload(schema_version=2), "schema"),
            (self.payload(status="FAILED"), "unsafe"),
        )
        for payload, expected in cases:
            with self.subTest(expected=expected):
                self.write(payload)
                with self.assertRaisesRegex(HealthcheckError, expected):
                    check_status_file(
                        self.path,
                        schema_version=1,
                        max_age_seconds=60,
                        status_field="status",
                        allowed_statuses={"RUNNING"},
                        now=self.now,
                    )

    def test_rejects_missing_malformed_oversized_and_non_object_files(self):
        with self.assertRaises(HealthcheckError):
            check_status_file(self.path, schema_version=1, max_age_seconds=60)
        for raw in ("{", "[]", "x" * 1_000_001):
            with self.subTest(size=len(raw)):
                with open(self.path, "w", encoding="utf-8") as handle:
                    handle.write(raw)
                with self.assertRaises(HealthcheckError):
                    check_status_file(self.path, schema_version=1, max_age_seconds=60)


if __name__ == "__main__":
    unittest.main()
