import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

from tools import security_triage_ledger
from tools.dependency_audit_runner import (
    classify_pip_audit_run,
    download_epss_for_cves,
    download_with_retries,
    extract_cves_from_pip_audit,
    sha256_file,
    venv_python,
)


class DependencyAuditRunnerTests(unittest.TestCase):
    def test_classifies_clean_audit_with_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "pip-audit.json"
            path.write_text('{"dependencies":[]}\n', encoding="utf-8")
            self.assertEqual(classify_pip_audit_run(0, path), "completed_clean")

    def test_classifies_findings_with_json_as_completed(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "pip-audit.json"
            path.write_text('{"dependencies":[{"name":"x","vulns":[{}]}]}\n', encoding="utf-8")
            self.assertEqual(classify_pip_audit_run(1, path), "completed_with_findings")

    def test_classifies_missing_json_as_runner_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "missing.json"
            self.assertEqual(classify_pip_audit_run(2, path), "failed_no_json")

    def test_sha256_file_returns_digest(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sample.txt"
            path.write_text("abc", encoding="utf-8")
            self.assertEqual(
                sha256_file(path),
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            )

    def test_venv_python_is_platform_specific(self):
        path = venv_python(Path("audit-env"))
        self.assertIn(path.name, {"python", "python.exe"})

    def test_download_failure_writes_attempt_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "kev.json"
            log_path = Path(tmp) / "kev-download.log"
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                ok = download_with_retries(
                    "file:///definitely/missing/known_exploited_vulnerabilities.json",
                    output_path=output_path,
                    attempts=1,
                    timeout_s=1,
                    log_path=log_path,
                )

            self.assertFalse(ok)
            self.assertFalse(output_path.exists())
            self.assertIn("attempt 1:", log_path.read_text(encoding="utf-8"))
            self.assertIn("KEV download attempt 1:", stderr.getvalue())

    def test_extracts_cve_aliases_from_pip_audit(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "pip-audit.json"
            path.write_text(
                json.dumps(
                    {
                        "dependencies": [
                            {
                                "name": "example",
                                "vulns": [
                                    {"id": "PYSEC-2026-1", "aliases": ["CVE-2026-0002"]},
                                    {"id": "CVE-2026-0001", "aliases": ["GHSA-xxxx"]},
                                ],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            self.assertEqual(
                extract_cves_from_pip_audit(path),
                ["CVE-2026-0001", "CVE-2026-0002"],
            )

    def test_epss_not_requested_without_cves_writes_empty_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "epss.json"
            log_path = Path(tmp) / "epss-download.log"

            ok = download_epss_for_cves(
                [],
                "https://api.first.org/data/v1/epss",
                output_path,
                attempts=1,
                timeout_s=1,
                log_path=log_path,
            )

            self.assertTrue(ok)
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "not_requested")
            self.assertEqual(payload["data"], [])
            self.assertIn("no CVE aliases", log_path.read_text(encoding="utf-8"))

    def test_watchlist_finding_can_fail_ledger_without_kev(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            pip_audit_path = tmp_path / "pip-audit.json"
            kev_path = tmp_path / "kev.json"
            epss_path = tmp_path / "epss.json"
            ledger_path = tmp_path / "ledger.md"
            summary_path = tmp_path / "summary.json"

            pip_audit_path.write_text(
                json.dumps(
                    {
                        "dependencies": [
                            {
                                "name": "example",
                                "version": "1.0.0",
                                "vulns": [
                                    {
                                        "id": "PYSEC-2026-1",
                                        "aliases": ["CVE-2026-0001"],
                                        "fix_versions": ["1.0.1"],
                                    }
                                ],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            kev_path.write_text('{"vulnerabilities":[]}\n', encoding="utf-8")
            epss_path.write_text(
                json.dumps(
                    {
                        "data": [
                            {
                                "cve": "CVE-2026-0001",
                                "epss": "0.12000",
                                "percentile": "0.95000",
                                "date": "2026-05-29",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            old_argv = sys.argv
            try:
                sys.argv = [
                    "security_triage_ledger.py",
                    "--pip-audit-json",
                    str(pip_audit_path),
                    "--kev-json",
                    str(kev_path),
                    "--epss-json",
                    str(epss_path),
                    "--output",
                    str(ledger_path),
                    "--summary-json",
                    str(summary_path),
                    "--fail-on-watchlist",
                ]
                with contextlib.redirect_stderr(io.StringIO()):
                    rc = security_triage_ledger.main()
            finally:
                sys.argv = old_argv

            self.assertEqual(rc, 4)
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(summary["kev_findings"], 0)
            self.assertEqual(summary["watchlist_findings"], 1)
            self.assertIn("P2", ledger_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
