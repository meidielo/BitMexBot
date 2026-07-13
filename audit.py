"""One-shot, read-only safety and promotion audit for BitMexBot.

The audit intentionally excludes ``data/trades.db`` from all evidence because
that legacy database mixes BTC quantities and contract counts.  A successful
exit means only that the bot is eligible for a separate human canary review.
It does not enable or authorize real-funds trading.
"""

from __future__ import annotations

import json
import math
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from dotenv import load_dotenv

from live_readiness import (
    PROMOTION_EVIDENCE_PATH,
    TRADE_DB_PATH,
    evaluate_live_readiness,
)


ROOT = Path(__file__).resolve().parent
DAILY_LOSS_FILE = ROOT / "data" / "daily_loss.json"
W = 72


def chk_testnet_env(env: Mapping[str, str] | None = None) -> tuple[bool, str]:
    environment = os.environ if env is None else env
    value = str(environment.get("BITMEX_TESTNET", ""))
    return (
        value == "true",
        f"BITMEX_TESTNET={value!r}; must remain exactly 'true'",
    )


def chk_no_hardcoded_keys(root: Path = ROOT) -> tuple[bool, str]:
    """Scan project Python files for credential-like literal assignments."""

    assignment = re.compile(
        r"(api[_]?key|api[_]?secret|secret)\s*=\s*['\"]([A-Za-z0-9_\-]{16,})['\"]",
        re.IGNORECASE,
    )
    environment_read = re.compile(r"os\.(getenv|environ)", re.IGNORECASE)
    excluded_parts = {".git", ".venv", "venv", "__pycache__"}
    violations: list[str] = []
    files = [
        path
        for path in root.rglob("*.py")
        if not excluded_parts.intersection(path.relative_to(root).parts)
    ]
    for path in sorted(files):
        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError as exc:  # agent-quality: allow: unreadable file is accumulated as an explicit failed check
            violations.append(f"{path.relative_to(root)}: unreadable ({exc})")
            continue
        for line_number, line in enumerate(lines, start=1):
            if assignment.search(line) and not environment_read.search(line):
                violations.append(
                    f"{path.relative_to(root)}:{line_number}: credential-like literal"
                )
    if violations:
        return False, "; ".join(violations)
    return True, f"No credential-like literals found in {len(files)} Python file(s)"


def chk_testnet_guards_present(root: Path = ROOT) -> tuple[bool, str]:
    required = {
        "order_manager.py": (
            "_assert_testnet",
            "BITMEX_TESTNET",
            "attest_testnet_exchange",
            "exclusive_execution_lock",
            "raise",
        ),
        "bitmex_client.py": (
            "BITMEX_TESTNET",
            "TESTNET_API_ORIGIN",
            "set_sandbox_mode",
            "attest_testnet_exchange",
            "raise",
        ),
    }
    failures: list[str] = []
    for filename, markers in required.items():
        path = root / filename
        try:
            content = path.read_text(encoding="utf-8", errors="ignore")
        except OSError as exc:  # agent-quality: allow: unreadable guard file is accumulated as an explicit failed check
            failures.append(f"{filename} unreadable ({exc})")
            continue
        missing = [marker for marker in markers if marker not in content]
        if missing:
            failures.append(f"{filename} missing {', '.join(missing)}")
    if failures:
        return False, "; ".join(failures)
    return True, "Testnet fail-closed guards are present in client and order manager"


def chk_daily_loss_state(
    path: Path = DAILY_LOSS_FILE,
    now: datetime | None = None,
) -> tuple[bool, str]:
    """Confirm the fail-closed daily loss state is current and parseable."""

    now = now or datetime.now(timezone.utc)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise TypeError("daily loss state must be a JSON object")
        date = data["date"]
        loss_value = data["loss_usd"]
        source = data["source"]
        if not isinstance(date, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
            raise ValueError("daily loss date must use YYYY-MM-DD")
        datetime.strptime(date, "%Y-%m-%d")
        if isinstance(loss_value, bool) or not isinstance(loss_value, (int, float)):
            raise TypeError("daily loss must be a JSON number")
        loss = float(loss_value)
        if not isinstance(source, str):
            raise TypeError("daily loss source must be a string")
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:  # agent-quality: allow: parse failure is returned as a failed audit result
        return False, f"{path} is missing or untrusted ({type(exc).__name__})"
    today = now.astimezone(timezone.utc).strftime("%Y-%m-%d")
    if date != today:
        return False, f"daily loss state is stale ({date!r}); expected {today!r}"
    if not math.isfinite(loss):
        return False, "daily loss state is non-finite and therefore untrusted"
    if loss < 0:
        return False, "daily loss state is negative and therefore untrusted"
    if source != "trades_v2.db":
        return False, f"daily loss source is untrusted ({source!r})"
    return True, f"daily loss state is current; gross realised loss is ${loss:.2f}"


def _print_header() -> None:
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print("=" * W)
    print("  BitMexBot safety and real-funds promotion audit")
    print(f"  Generated: {generated}")
    print("=" * W)


def _print_section(title: str) -> None:
    print(f"\n  {title}")
    print(f"  {'-' * (W - 4)}")


def _print_check(label: str, passed: bool, detail: str) -> None:
    marker = "PASS" if passed else "FAIL"
    print(f"  [{marker}] {label}")
    print(f"         {detail}")


def _audit_exit_code(
    local_checks: list[tuple[str, bool, str]],
    readiness: dict[str, Any],
) -> int:
    local_ok = all(passed for _, passed, _ in local_checks)
    readiness_ok = readiness.get("verdict") == "READY_FOR_CANARY_REVIEW"
    return 0 if local_ok and readiness_ok else 1


def run_audit(
    *,
    env: Mapping[str, str] | None = None,
    root: Path = ROOT,
    trade_db_path: str = TRADE_DB_PATH,
    promotion_evidence_path: str = PROMOTION_EVIDENCE_PATH,
    readiness_result: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> int:
    """Print the audit and return a process exit code suitable for automation."""

    environment = os.environ if env is None else env
    local_checks = [
        ("Runtime remains testnet-only", *chk_testnet_env(environment)),
        ("No hardcoded API credentials", *chk_no_hardcoded_keys(root)),
        ("Testnet guards present", *chk_testnet_guards_present(root)),
        (
            "Current daily-loss state [history/data]",
            *chk_daily_loss_state(root / "data" / "daily_loss.json", now=now),
        ),
    ]
    readiness = readiness_result or evaluate_live_readiness(
        trade_db_path=trade_db_path,
        promotion_evidence_path=promotion_evidence_path,
        env=environment,
        now=now,
    )

    _print_header()
    _print_section("LOCAL SAFETY CHECKS")
    for label, passed, detail in local_checks:
        _print_check(label, passed, detail)

    _print_section("VERSIONED HISTORY AND PROMOTION GATES")
    for gate in readiness["gates"]:
        label = f"[{gate['category']}] {gate['name']}"
        _print_check(label, gate["status"] == "PASS", gate["detail"])

    legacy = readiness["metrics"]["legacy_trades"]
    _print_section("EXCLUDED LOCAL DATA")
    print(
        f"  [INFO] {legacy['path']} exists={legacy['exists']} and is excluded: "
        f"{legacy['reason']}"
    )

    exit_code = _audit_exit_code(local_checks, readiness)
    failed_local = [label for label, passed, _ in local_checks if not passed]
    print()
    print("=" * W)
    if exit_code == 0:
        print("  FINAL VERDICT: READY FOR HUMAN CANARY REVIEW ONLY")
        print("  Production trading remains disabled.")
    else:
        print("  FINAL VERDICT: DO NOT USE REAL FUNDS")
        if failed_local:
            print("  Local failures: " + "; ".join(failed_local))
        if readiness["history_data_blockers"]:
            print(
                "  Existing history/data blockers: "
                + "; ".join(readiness["history_data_blockers"])
            )
        print(f"  Readiness verdict: {readiness['verdict']}")
    print("=" * W)
    return exit_code


def main() -> int:
    load_dotenv()
    return run_audit()


if __name__ == "__main__":
    raise SystemExit(main())
