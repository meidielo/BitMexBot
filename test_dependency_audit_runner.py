import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from tools.dependency_audit_runner import (
    classify_pip_audit_run,
    download_with_retries,
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


if __name__ == "__main__":
    unittest.main()
