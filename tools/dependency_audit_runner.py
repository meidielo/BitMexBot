"""Run dependency vulnerability audit with retained diagnostics.

The GitHub workflow calls this script instead of hand-written shell fragments so
dependency audit failures produce the same artifact set locally and in CI.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import venv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_KEV_URL = (
    "https://www.cisa.gov/sites/default/files/feeds/"
    "known_exploited_vulnerabilities.json"
)
DEFAULT_OUTPUT_DIR = Path("security-triage")
PIP_AUDIT_JSON_NAME = "pip-audit.json"
KEV_JSON_NAME = "known_exploited_vulnerabilities.json"


@dataclass(frozen=True)
class CommandResult:
    args: list[str]
    returncode: int
    stdout: str
    stderr: str
    duration_s: float


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256_file(path: Path) -> str | None:
    if not path.exists():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def venv_python(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def run_command(
    args: list[str],
    cwd: Path,
    timeout_s: int,
    env: dict[str, str] | None = None,
) -> CommandResult:
    started = time.monotonic()
    completed = subprocess.run(
        args,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        timeout=timeout_s,
        env=env,
    )
    return CommandResult(
        args=args,
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        duration_s=round(time.monotonic() - started, 3),
    )


def write_command_artifacts(prefix: str, result: CommandResult, output_dir: Path) -> None:
    (output_dir / f"{prefix}.stdout.txt").write_text(result.stdout, encoding="utf-8")
    (output_dir / f"{prefix}.stderr.txt").write_text(result.stderr, encoding="utf-8")
    (output_dir / f"{prefix}.status.json").write_text(
        json.dumps(
            {
                "args": result.args,
                "returncode": result.returncode,
                "duration_s": result.duration_s,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def retry_command(
    args: list[str],
    cwd: Path,
    timeout_s: int,
    attempts: int,
    prefix: str,
    output_dir: Path,
    env: dict[str, str] | None = None,
) -> CommandResult:
    last: CommandResult | None = None
    for attempt in range(1, attempts + 1):
        result = run_command(args, cwd=cwd, timeout_s=timeout_s, env=env)
        write_command_artifacts(f"{prefix}-attempt-{attempt}", result, output_dir)
        last = result
        if result.returncode == 0:
            return result
        if attempt < attempts:
            time.sleep(min(2 * attempt, 6))
    assert last is not None
    return last


def classify_pip_audit_run(returncode: int, output_path: Path) -> str:
    if returncode == 0 and output_path.exists() and output_path.stat().st_size > 0:
        return "completed_clean"
    if returncode != 0 and output_path.exists() and output_path.stat().st_size > 0:
        return "completed_with_findings"
    return "failed_no_json"


def download_with_retries(
    url: str,
    output_path: Path,
    attempts: int,
    timeout_s: int,
    log_path: Path,
) -> bool:
    lines: list[str] = []
    for attempt in range(1, attempts + 1):
        started = time.monotonic()
        try:
            request = urllib.request.Request(
                url,
                headers={"User-Agent": "BitMexBot-security-audit/1.0"},
            )
            with urllib.request.urlopen(request, timeout=timeout_s) as response:
                payload = response.read()
            output_path.write_bytes(payload)
            json.loads(payload.decode("utf-8-sig"))
            elapsed = round(time.monotonic() - started, 3)
            lines.append(f"attempt {attempt}: ok in {elapsed}s, bytes={len(payload)}")
            log_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            return True
        except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
            elapsed = round(time.monotonic() - started, 3)
            message = f"attempt {attempt}: {type(exc).__name__}: {exc} after {elapsed}s"
            lines.append(message)
            log_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            print(f"KEV download {message}", file=sys.stderr)
            if attempt < attempts:
                time.sleep(min(2 * attempt, 6))
    log_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return False


def load_json_file(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as handle:
        data = json.load(handle)
    return data if isinstance(data, dict) else {}


def diagnostic_payload(repo_root: Path, output_dir: Path, requirements: Path) -> dict[str, Any]:
    env_keys = [
        "GITHUB_ACTIONS",
        "GITHUB_RUN_ID",
        "GITHUB_SHA",
        "RUNNER_OS",
        "RUNNER_ARCH",
        "PIP_INDEX_URL",
        "PIP_NO_INDEX",
        "PIP_DISABLE_PIP_VERSION_CHECK",
    ]
    return {
        "generated_at": utc_now(),
        "repo_root": str(repo_root),
        "output_dir": str(output_dir),
        "python_executable": sys.executable,
        "python_version": sys.version,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "requirements": str(requirements),
        "requirements_exists": requirements.exists(),
        "requirements_sha256": sha256_file(requirements),
        "env": {key: os.environ.get(key, "") for key in env_keys},
    }


def write_diagnostics_markdown(payload: dict[str, Any], output_path: Path) -> None:
    lines = [
        "# Audit Runner Diagnostics",
        "",
        f"Generated: `{payload['generated_at']}`",
        f"Python: `{payload['python_version'].splitlines()[0]}`",
        f"Executable: `{payload['python_executable']}`",
        f"Platform: `{payload['platform']}`",
        f"Requirements SHA256: `{payload.get('requirements_sha256') or 'missing'}`",
        "",
        "## Environment Hints",
        "",
    ]
    for key, value in payload.get("env", {}).items():
        lines.append(f"- `{key}`: `{value}`")
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def make_summary(
    status: str,
    pip_audit_status: str,
    pip_audit_exit: int | None,
    ledger_summary: dict[str, Any] | None,
    actionable_diagnostics: list[str],
) -> dict[str, Any]:
    return {
        "status": status,
        "pip_audit_status": pip_audit_status,
        "pip_audit_exit": pip_audit_exit,
        "ledger_summary": ledger_summary or {},
        "actionable_diagnostics": actionable_diagnostics,
        "generated_at": utc_now(),
    }


def run_audit(args: argparse.Namespace) -> int:
    repo_root = Path(args.repo_root).resolve()
    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = repo_root / output_dir
    requirements = Path(args.requirements)
    if not requirements.is_absolute():
        requirements = repo_root / requirements
    ledger_script = repo_root / "tools" / "security_triage_ledger.py"

    if output_dir.exists() and args.clean:
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    diagnostics = diagnostic_payload(repo_root, output_dir, requirements)
    (output_dir / "audit-runner-diagnostics.json").write_text(
        json.dumps(diagnostics, indent=2) + "\n",
        encoding="utf-8",
    )
    write_diagnostics_markdown(diagnostics, output_dir / "audit-runner-diagnostics.md")

    actionable: list[str] = []
    if not requirements.exists():
        actionable.append(f"requirements file is missing: {requirements}")
        summary = make_summary("failed", "not_started", None, None, actionable)
        (output_dir / "audit-runner-summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        return 10

    with tempfile.TemporaryDirectory(prefix="bitmexbot-audit-venv-") as tmp:
        venv_dir = Path(tmp) / "venv"
        venv.EnvBuilder(with_pip=True, clear=True).create(venv_dir)
        audit_python = venv_python(venv_dir)
        env = dict(os.environ)
        env["PIP_DISABLE_PIP_VERSION_CHECK"] = "1"

        install = retry_command(
            [str(audit_python), "-m", "pip", "install", "--upgrade", "pip", "pip-audit"],
            cwd=repo_root,
            timeout_s=args.install_timeout,
            attempts=args.attempts,
            prefix="pip-audit-install",
            output_dir=output_dir,
            env=env,
        )
        if install.returncode != 0:
            actionable.append(
                "pip-audit installation failed. Check pip-audit-install-attempt-*.stderr.txt "
                "for package index, DNS, proxy, or TLS errors."
            )
            summary = make_summary("failed", "install_failed", install.returncode, None, actionable)
            (output_dir / "audit-runner-summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
            return 11

        pip_json = output_dir / PIP_AUDIT_JSON_NAME
        pip_audit = run_command(
            [
                str(audit_python),
                "-m",
                "pip_audit",
                "-r",
                str(requirements),
                "--format",
                "json",
                "--output",
                str(pip_json),
            ],
            cwd=repo_root,
            timeout_s=args.audit_timeout,
            env=env,
        )
        write_command_artifacts("pip-audit", pip_audit, output_dir)

    pip_state = classify_pip_audit_run(pip_audit.returncode, pip_json)
    if pip_state == "failed_no_json":
        actionable.append(
            "pip-audit did not produce JSON. Check pip-audit.stderr.txt and "
            "pip-audit.status.json; rerun from a network-permitted environment."
        )
        if not pip_json.exists():
            pip_json.write_text('{"dependencies":[]}\n', encoding="utf-8")
        summary = make_summary("failed", pip_state, pip_audit.returncode, None, actionable)
        (output_dir / "audit-runner-summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        return 12

    kev_json = output_dir / KEV_JSON_NAME
    kev_ok = download_with_retries(
        args.kev_url,
        kev_json,
        attempts=args.attempts,
        timeout_s=args.download_timeout,
        log_path=output_dir / "kev-download.log",
    )
    if not kev_ok:
        actionable.append(
            "CISA KEV catalog download failed. Check kev-download.log and retry from "
            "a network-permitted environment."
        )
        summary = make_summary("failed", pip_state, pip_audit.returncode, None, actionable)
        (output_dir / "audit-runner-summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        return 13

    ledger_summary_path = output_dir / "security-triage-summary.json"
    ledger = run_command(
        [
            sys.executable,
            str(ledger_script),
            "--pip-audit-json",
            str(pip_json),
            "--kev-json",
            str(kev_json),
            "--output",
            str(output_dir / "security-triage-ledger.md"),
            "--summary-json",
            str(ledger_summary_path),
            *(["--fail-on-kev"] if args.fail_on_kev else []),
        ],
        cwd=repo_root,
        timeout_s=args.ledger_timeout,
    )
    write_command_artifacts("security-triage-ledger", ledger, output_dir)

    ledger_summary = load_json_file(ledger_summary_path) if ledger_summary_path.exists() else {}
    if ledger.returncode != 0:
        if ledger.returncode == 2:
            actionable.append("KEV-matched dependency finding requires immediate triage.")
        else:
            actionable.append("Security triage ledger generation failed; check security-triage-ledger.stderr.txt.")
        summary = make_summary("failed", pip_state, pip_audit.returncode, ledger_summary, actionable)
        (output_dir / "audit-runner-summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        return ledger.returncode

    summary = make_summary("passed", pip_state, pip_audit.returncode, ledger_summary, actionable)
    (output_dir / "audit-runner-summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(
        f"Dependency audit passed. pip-audit={pip_state}, "
        f"findings={ledger_summary.get('total_findings', 'unknown')}, "
        f"kev={ledger_summary.get('kev_findings', 'unknown')}"
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--requirements", default="requirements.txt")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--kev-url", default=DEFAULT_KEV_URL)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--install-timeout", type=int, default=180)
    parser.add_argument("--audit-timeout", type=int, default=240)
    parser.add_argument("--download-timeout", type=int, default=45)
    parser.add_argument("--ledger-timeout", type=int, default=60)
    parser.add_argument("--fail-on-kev", action="store_true")
    parser.add_argument("--clean", action="store_true")
    return parser


def main() -> int:
    return run_audit(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
