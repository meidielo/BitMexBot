"""Publish a sanitized, read-only operator snapshot for the web dashboard.

This process belongs on the private bot host.  It has no order authority and
never copies exchange identifiers, credentials, raw errors, or log content to
the public dashboard boundary.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sqlite3
import sys
import time
from collections.abc import Mapping, Sequence
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

from promotion import evaluate_promotion


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DEFAULT_LEDGER_PATH = DATA_DIR / "trades_v2.db"
DEFAULT_DAILY_LOSS_PATH = DATA_DIR / "daily_loss.json"
DEFAULT_HEARTBEAT_PATH = DATA_DIR / "runner_status.json"
DEFAULT_WATCHDOG_PATH = DATA_DIR / "watchdog_status.json"
DEFAULT_PROMOTION_PATH = DATA_DIR / "promotion_evidence.json"
DEFAULT_OUTPUT_PATH = DATA_DIR / "operator_dashboard" / "operator_status.json"

SNAPSHOT_SCHEMA_VERSION = 1
LEDGER_SCHEMA_VERSION = 4
MAX_RECENT_INTENTS = 20
MAX_SOURCE_JSON_BYTES = 128 * 1024
DEFAULT_HEARTBEAT_MAX_AGE_SECONDS = 120.0
DEFAULT_WATCHDOG_MAX_AGE_SECONDS = 180.0

ACTIVE_STATUSES = frozenset(
    {
        "REGISTERED",
        "ENTRY_PENDING",
        "ENTRY_PARTIAL",
        "ENTRY_FILLED",
        "PROTECTED",
        "PROTECTED_NO_TP",
    }
)
ALL_STATUSES = ACTIVE_STATUSES | frozenset(
    {"CLOSED", "FAILED_FLAT", "HALTED_MANUAL"}
)
ALLOWED_ENVIRONMENTS = frozenset({"testnet", "dry_run"})
ALLOWED_SIDES = frozenset({"LONG", "SHORT"})
PROMOTION_STAGES = frozenset(
    {
        "RESEARCH",
        "SHADOW",
        "TESTNET_ENGINEERING",
        "MAINNET_DRY_RUN",
        "CANARY_REVIEW",
    }
)

_REQUIRED_LEDGER_COLUMNS = frozenset(
    {
        "id",
        "environment",
        "symbol",
        "side",
        "requested_contracts",
        "filled_contracts",
        "closed_contracts",
        "expected_max_loss_usdt",
        "net_pnl_usdt",
        "status",
        "created_at_utc",
        "updated_at_utc",
    }
)
_REQUIRED_LEDGER_INDEX = "ux_execution_intents_single_unresolved"
_SYMBOL_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_./:-]{0,31}\Z")
_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}\Z")

_HEARTBEAT_STATE_MAP = {
    "RUNNING": "RUNNING",
    "STARTING": "STARTING",
    "WAITING": "WAITING",
    "PAUSED": "PAUSED",
    "MANUAL_HALT": "MANUAL_HALT",
    "FAILED": "FAILED",
    "STOPPED": "STOPPED",
    # Compatibility aliases for older private heartbeat producers.
    "IDLE": "WAITING",
    "HALTED": "HALTED",
    "ERROR": "ERROR",
}
_WATCHDOG_STATE_MAP = {
    "STARTING": "STARTING",
    "SYNCHRONIZING": "SYNCHRONIZING",
    "HEALTHY": "HEALTHY",
    "OK": "HEALTHY",
    "ARMED": "HEALTHY",
    "DEGRADED": "DEGRADED",
    "WARNING": "DEGRADED",
    "ALERT": "ALERT",
    "TRIPPED": "ALERT",
    "FAILED": "FAILED",
    "ERROR": "ALERT",
    "DISABLED": "DISABLED",
    "STALE": "STALE",
}


class SnapshotSourceError(RuntimeError):
    """Raised when a private source cannot be safely summarized."""


class SnapshotWriteError(RuntimeError):
    """Raised when the sanitized snapshot cannot be atomically published."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return _aware_utc(value).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or not value or len(value) > 64:
        raise ValueError("timestamp must be a bounded string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("timestamp is not ISO-8601") from exc
    return _aware_utc(parsed)


def _finite_number(value: Any, *, nullable: bool = False) -> float | None:
    if value is None and nullable:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("value must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("value must be finite")
    return result


def _non_negative_int(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("value must be a non-negative integer")
    return value


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON constant rejected: {value}")


def _read_json_object(path: Path) -> Mapping[str, Any] | None:
    """Return a bounded strict-JSON object, or ``None`` on any input failure."""

    try:
        with path.open("r", encoding="utf-8") as handle:
            raw = handle.read(MAX_SOURCE_JSON_BYTES + 1)
    except (OSError, UnicodeError):  # agent-quality: allow: caller receives an explicit fail-closed source state
        return None
    if len(raw.encode("utf-8")) > MAX_SOURCE_JSON_BYTES:
        return None
    try:
        parsed = json.loads(raw, parse_constant=_reject_json_constant)
    except (json.JSONDecodeError, ValueError):  # agent-quality: allow: caller receives an explicit fail-closed source state
        return None
    return parsed if isinstance(parsed, Mapping) else None


def _sqlite_read_only_uri(path: Path) -> str:
    normalized = path.resolve().as_posix()
    return f"file:{quote(normalized, safe='/:')}?mode=ro"


def _open_ledger_read_only(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise SnapshotSourceError("ledger is unavailable")
    try:
        connection = sqlite3.connect(
            _sqlite_read_only_uri(path),
            uri=True,
            timeout=5,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=5000")
        query_only = connection.execute("PRAGMA query_only").fetchone()
        if not query_only or int(query_only[0]) != 1:
            connection.close()
            raise SnapshotSourceError("ledger query-only mode could not be proven")
        return connection
    except sqlite3.Error as exc:
        raise SnapshotSourceError("ledger could not be opened read-only") from exc


def _sanitize_ledger_row(row: sqlite3.Row) -> dict[str, Any]:
    environment = str(row["environment"])
    side = str(row["side"])
    status = str(row["status"])
    symbol = str(row["symbol"])
    if environment not in ALLOWED_ENVIRONMENTS:
        raise SnapshotSourceError("ledger environment is invalid")
    if side not in ALLOWED_SIDES or status not in ALL_STATUSES:
        raise SnapshotSourceError("ledger lifecycle value is invalid")
    if not _SYMBOL_PATTERN.fullmatch(symbol):
        raise SnapshotSourceError("ledger symbol is invalid")

    try:
        requested = _non_negative_int(row["requested_contracts"])
        filled = _non_negative_int(row["filled_contracts"])
        closed = _non_negative_int(row["closed_contracts"])
        expected_loss = _finite_number(row["expected_max_loss_usdt"])
        net_pnl = _finite_number(row["net_pnl_usdt"], nullable=True)
        created = _timestamp(_parse_timestamp(row["created_at_utc"]))
        updated = _timestamp(_parse_timestamp(row["updated_at_utc"]))
    except (TypeError, ValueError) as exc:
        raise SnapshotSourceError("ledger row has invalid typed values") from exc

    if requested <= 0 or filled > requested or closed > filled:
        raise SnapshotSourceError("ledger contract invariants are invalid")
    if expected_loss is None or expected_loss <= 0:
        raise SnapshotSourceError("ledger loss evidence is invalid")
    if status in {"CLOSED", "FAILED_FLAT"} and filled > 0 and net_pnl is None:
        raise SnapshotSourceError("terminal ledger accounting is incomplete")

    return {
        "environment": environment.upper(),
        "symbol": symbol,
        "side": side,
        "status": status,
        "requested_contracts": requested,
        "filled_contracts": filled,
        "closed_contracts": closed,
        "expected_max_loss_usdt": round(expected_loss, 8),
        "net_pnl_usdt": None if net_pnl is None else round(net_pnl, 8),
        "created_at_utc": created,
        "updated_at_utc": updated,
    }


def _empty_ledger() -> dict[str, Any]:
    return {
        "available": False,
        "total_intents": 0,
        "open_intents": 0,
        "manual_halts": 0,
        "protection_state": "UNKNOWN",
        "latest_intent": None,
        "recent_intents": [],
    }


def read_ledger_summary(path: str | os.PathLike[str]) -> dict[str, Any]:
    """Read and validate the v3 ledger without creating or changing it."""

    ledger_path = Path(path)
    with closing(_open_ledger_read_only(ledger_path)) as connection:
        try:
            connection.execute("BEGIN")
            version_row = connection.execute("PRAGMA user_version").fetchone()
            if not version_row or int(version_row[0]) != LEDGER_SCHEMA_VERSION:
                raise SnapshotSourceError("ledger schema version is invalid")
            integrity = connection.execute("PRAGMA quick_check").fetchone()
            if not integrity or str(integrity[0]).lower() != "ok":
                raise SnapshotSourceError("ledger integrity check failed")
            columns = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(execution_intents)"
                ).fetchall()
            }
            if not _REQUIRED_LEDGER_COLUMNS.issubset(columns):
                raise SnapshotSourceError("ledger schema columns are incomplete")
            indexes = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA index_list(execution_intents)"
                ).fetchall()
            }
            if _REQUIRED_LEDGER_INDEX not in indexes:
                raise SnapshotSourceError("ledger unresolved-intent index is missing")

            cursor = connection.execute(
                "SELECT environment, symbol, side, status, requested_contracts, "
                "filled_contracts, closed_contracts, expected_max_loss_usdt, "
                "net_pnl_usdt, created_at_utc, updated_at_utc "
                "FROM execution_intents ORDER BY id DESC"
            )
            total = 0
            active = 0
            manual_halts = 0
            environments: set[str] = set()
            latest_unresolved: str | None = None
            recent: list[dict[str, Any]] = []
            for row in cursor:
                item = _sanitize_ledger_row(row)
                total += 1
                environments.add(item["environment"])
                if item["status"] in ACTIVE_STATUSES:
                    active += 1
                    if latest_unresolved is None:
                        latest_unresolved = item["status"]
                elif item["status"] == "HALTED_MANUAL":
                    manual_halts += 1
                    if latest_unresolved is None:
                        latest_unresolved = item["status"]
                if len(recent) < MAX_RECENT_INTENTS:
                    recent.append(item)

            if active + manual_halts > 1:
                raise SnapshotSourceError("multiple unresolved intents were found")
        except sqlite3.Error as exc:
            raise SnapshotSourceError("ledger query failed") from exc

    if manual_halts:
        protection_state = "MANUAL_HALT"
    elif latest_unresolved == "PROTECTED":
        protection_state = "PROTECTED"
    elif latest_unresolved == "PROTECTED_NO_TP":
        protection_state = "STOP_ONLY"
    elif latest_unresolved is not None:
        protection_state = "UNPROTECTED"
    else:
        protection_state = "FLAT"

    if environments == {"TESTNET"}:
        environment = "TESTNET"
    elif environments == {"DRY_RUN"}:
        environment = "DRY_RUN"
    elif environments:
        environment = "MIXED_NON_PRODUCTION"
    else:
        environment = "UNKNOWN"

    return {
        "available": True,
        "environment": environment,
        "total_intents": total,
        "open_intents": active,
        "manual_halts": manual_halts,
        "protection_state": protection_state,
        "latest_intent": recent[0] if recent else None,
        "recent_intents": recent,
    }


def _read_daily_loss(path: Path, now: datetime) -> tuple[dict[str, Any], str | None]:
    empty = {
        "state": "UNAVAILABLE",
        "date": None,
        "daily_loss_usdt": None,
        "fresh": False,
    }
    payload = _read_json_object(path)
    if payload is None:
        return empty, "DAILY_LOSS_UNAVAILABLE"
    date = payload.get("date")
    source = payload.get("source")
    try:
        loss = _finite_number(payload.get("loss_usd"))
    except ValueError:  # agent-quality: allow: invalid loss data returns an explicit unavailable state
        return empty, "DAILY_LOSS_UNAVAILABLE"
    if (
        not isinstance(date, str)
        or not _DATE_PATTERN.fullmatch(date)
        or source != "trades_v2.db"
        or loss is None
        or loss < 0
    ):
        return empty, "DAILY_LOSS_UNAVAILABLE"
    today = _aware_utc(now).strftime("%Y-%m-%d")
    fresh = date == today
    return (
        {
            "state": "CURRENT" if fresh else "STALE",
            "date": date,
            "daily_loss_usdt": round(loss, 8),
            "fresh": fresh,
        },
        None if fresh else "DAILY_LOSS_STALE",
    )


def _first_present(payload: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        if key in payload:
            return payload[key]
    return None


def _read_timed_state(
    path: Path,
    *,
    now: datetime,
    max_age_seconds: float,
    state_map: Mapping[str, str],
    timestamp_keys: Sequence[str],
    timestamp_output_key: str,
    unavailable_warning: str,
    stale_warning: str,
    honor_reported_fresh: bool = False,
) -> tuple[dict[str, Any], str | None]:
    empty = {"state": "UNKNOWN", timestamp_output_key: None, "fresh": False}
    payload = _read_json_object(path)
    if payload is None or payload.get("schema_version") != 1:
        return empty, unavailable_warning
    if "environment" in payload and payload.get("environment") != "testnet":
        return empty, unavailable_warning
    raw_state = _first_present(payload, ("state", "status"))
    raw_timestamp = _first_present(payload, timestamp_keys)
    if not isinstance(raw_state, str):
        return empty, unavailable_warning
    state = state_map.get(raw_state.strip().upper())
    try:
        observed = _parse_timestamp(raw_timestamp)
    except ValueError:  # agent-quality: allow: invalid telemetry returns an explicit unavailable state
        return empty, unavailable_warning
    if state is None:
        return empty, unavailable_warning
    age = (_aware_utc(now) - observed).total_seconds()
    age_fresh = -30.0 <= age <= max_age_seconds
    reported_fresh = True
    if honor_reported_fresh:
        reported_fresh = payload.get("fresh")
        if not isinstance(reported_fresh, bool):
            return empty, unavailable_warning
    if not age_fresh or state == "STALE" or (
        state == "HEALTHY" and not reported_fresh
    ):
        return (
            {
                "state": "STALE",
                timestamp_output_key: _timestamp(observed),
                "fresh": False,
            },
            stale_warning,
        )
    return (
        {
            "state": state,
            timestamp_output_key: _timestamp(observed),
            "fresh": reported_fresh,
        },
        None,
    )


def _read_promotion(path: Path) -> tuple[dict[str, Any], str | None]:
    payload = _read_json_object(path)
    if payload is None:
        return (
            {
                "evidence_state": "UNAVAILABLE",
                "verdict": "NOT_READY",
                "stage": "RESEARCH",
                "blocker_count": 1,
                "human_approval_required": True,
                "production_enabled": False,
            },
            "PROMOTION_EVIDENCE_UNAVAILABLE",
        )
    try:
        result = evaluate_promotion(payload)
    except (TypeError, ValueError, OverflowError):  # agent-quality: allow: invalid evidence returns an explicit fail-closed state
        return (
            {
                "evidence_state": "INVALID",
                "verdict": "NOT_READY",
                "stage": "RESEARCH",
                "blocker_count": 1,
                "human_approval_required": True,
                "production_enabled": False,
            },
            "PROMOTION_EVIDENCE_INVALID",
        )
    stage = result.get("stage")
    blockers = result.get("blockers")
    if stage not in PROMOTION_STAGES or not isinstance(blockers, list):
        return (
            {
                "evidence_state": "INVALID",
                "verdict": "NOT_READY",
                "stage": "RESEARCH",
                "blocker_count": 1,
                "human_approval_required": True,
                "production_enabled": False,
            },
            "PROMOTION_EVIDENCE_INVALID",
        )
    return (
        {
            "evidence_state": "VALID",
            "verdict": (
                "CANARY_REVIEW_ONLY" if stage == "CANARY_REVIEW" else "NOT_READY"
            ),
            "stage": stage,
            "blocker_count": min(len(blockers), 999),
            "human_approval_required": True,
            "production_enabled": False,
        },
        None,
    )


def build_snapshot(
    *,
    ledger_path: str | os.PathLike[str] = DEFAULT_LEDGER_PATH,
    daily_loss_path: str | os.PathLike[str] = DEFAULT_DAILY_LOSS_PATH,
    heartbeat_path: str | os.PathLike[str] = DEFAULT_HEARTBEAT_PATH,
    watchdog_path: str | os.PathLike[str] = DEFAULT_WATCHDOG_PATH,
    promotion_path: str | os.PathLike[str] = DEFAULT_PROMOTION_PATH,
    now: datetime | None = None,
    heartbeat_max_age_seconds: float = DEFAULT_HEARTBEAT_MAX_AGE_SECONDS,
    watchdog_max_age_seconds: float = DEFAULT_WATCHDOG_MAX_AGE_SECONDS,
) -> dict[str, Any]:
    """Build a fail-closed, allowlisted snapshot from private local sources."""

    current = _aware_utc(now or _utc_now())
    if heartbeat_max_age_seconds <= 0 or watchdog_max_age_seconds <= 0:
        raise ValueError("freshness thresholds must be positive")

    warnings: list[str] = []
    try:
        ledger_with_environment = read_ledger_summary(ledger_path)
        environment = ledger_with_environment.pop("environment")
        ledger = ledger_with_environment
    except (SnapshotSourceError, OSError, sqlite3.Error):  # agent-quality: allow: ledger failures are published as unavailable without private details
        ledger = _empty_ledger()
        environment = "UNKNOWN"
        warnings.append("LEDGER_UNAVAILABLE")

    risk, warning = _read_daily_loss(Path(daily_loss_path), current)
    if warning:
        warnings.append(warning)

    runner, warning = _read_timed_state(
        Path(heartbeat_path),
        now=current,
        max_age_seconds=heartbeat_max_age_seconds,
        state_map=_HEARTBEAT_STATE_MAP,
        timestamp_keys=(
            "generated_at_utc",
            "heartbeat_at_utc",
            "timestamp_utc",
            "updated_at_utc",
            "last_seen_utc",
        ),
        timestamp_output_key="heartbeat_at_utc",
        unavailable_warning="HEARTBEAT_UNAVAILABLE",
        stale_warning="HEARTBEAT_STALE",
    )
    if warning:
        warnings.append(warning)

    watchdog, warning = _read_timed_state(
        Path(watchdog_path),
        now=current,
        max_age_seconds=watchdog_max_age_seconds,
        state_map=_WATCHDOG_STATE_MAP,
        timestamp_keys=(
            "generated_at_utc",
            "checked_at_utc",
            "timestamp_utc",
            "updated_at_utc",
            "last_seen_utc",
        ),
        timestamp_output_key="checked_at_utc",
        unavailable_warning="WATCHDOG_UNAVAILABLE",
        stale_warning="WATCHDOG_STALE",
        honor_reported_fresh=True,
    )
    if warning:
        warnings.append(warning)

    readiness, warning = _read_promotion(Path(promotion_path))
    if warning:
        warnings.append(warning)

    protection = ledger["protection_state"]
    if protection == "MANUAL_HALT":
        warnings.append("MANUAL_HALT_PRESENT")
    elif protection == "UNPROTECTED":
        warnings.append("UNPROTECTED_INTENT_PRESENT")
    elif protection == "STOP_ONLY":
        warnings.append("STOP_ONLY_PROTECTION")
    if runner["state"] in {"ERROR", "FAILED", "HALTED", "MANUAL_HALT"}:
        warnings.append("RUNNER_ATTENTION")
    if watchdog["state"] in {
        "ALERT",
        "DISABLED",
        "DEGRADED",
        "FAILED",
        "STARTING",
        "SYNCHRONIZING",
    }:
        warnings.append("WATCHDOG_ATTENTION")

    warnings = list(dict.fromkeys(warnings))
    data_state = (
        "UNAVAILABLE"
        if not ledger["available"]
        else "DEGRADED"
        if warnings
        else "OK"
    )
    return {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "generated_at_utc": _timestamp(current),
        "data_state": data_state,
        "environment": environment,
        "production_enabled": False,
        "actions_enabled": False,
        "runner": runner,
        "watchdog": watchdog,
        "ledger": ledger,
        "risk": risk,
        "readiness": readiness,
        "warnings": warnings,
    }


def write_snapshot_atomic(
    snapshot: Mapping[str, Any], output_path: str | os.PathLike[str]
) -> None:
    """Atomically replace the public snapshot without publishing partial JSON."""

    try:
        serialized = json.dumps(
            snapshot,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        ) + "\n"
    except (TypeError, ValueError) as exc:
        raise SnapshotWriteError("snapshot could not be serialized safely") from exc

    destination = Path(output_path)
    temporary = destination.with_name(
        f".{destination.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        if os.name != "nt":
            # The file contains only allowlisted telemetry and must remain
            # readable by the non-root dashboard UID through a bind mount.
            os.chmod(temporary, 0o644)
        os.replace(temporary, destination)
    except OSError as exc:
        try:
            temporary.unlink(missing_ok=True)
        except OSError as cleanup_exc:
            print(
                f"warning: temporary snapshot cleanup failed: {type(cleanup_exc).__name__}",
                file=sys.stderr,
            )
        raise SnapshotWriteError("snapshot could not be atomically published") from exc


def publish_once(
    *,
    output_path: str | os.PathLike[str] = DEFAULT_OUTPUT_PATH,
    **snapshot_options: Any,
) -> dict[str, Any]:
    snapshot = build_snapshot(**snapshot_options)
    write_snapshot_atomic(snapshot, output_path)
    return snapshot


def _positive_number(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be numeric") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return parsed


def _non_negative_number(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be numeric") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise argparse.ArgumentTypeError("must be a non-negative finite number")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER_PATH))
    parser.add_argument("--daily-loss", default=str(DEFAULT_DAILY_LOSS_PATH))
    parser.add_argument("--heartbeat", default=str(DEFAULT_HEARTBEAT_PATH))
    parser.add_argument("--watchdog", default=str(DEFAULT_WATCHDOG_PATH))
    parser.add_argument("--promotion-evidence", default=str(DEFAULT_PROMOTION_PATH))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument(
        "--heartbeat-max-age-seconds",
        type=_positive_number,
        default=DEFAULT_HEARTBEAT_MAX_AGE_SECONDS,
    )
    parser.add_argument(
        "--watchdog-max-age-seconds",
        type=_positive_number,
        default=DEFAULT_WATCHDOG_MAX_AGE_SECONDS,
    )
    parser.add_argument(
        "--poll-seconds",
        type=_non_negative_number,
        default=0.0,
        help="Publish once by default; values greater than zero poll continuously.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    options = {
        "ledger_path": args.ledger,
        "daily_loss_path": args.daily_loss,
        "heartbeat_path": args.heartbeat,
        "watchdog_path": args.watchdog,
        "promotion_path": args.promotion_evidence,
        "heartbeat_max_age_seconds": args.heartbeat_max_age_seconds,
        "watchdog_max_age_seconds": args.watchdog_max_age_seconds,
    }
    try:
        while True:
            publish_once(output_path=args.output, **options)
            if args.poll_seconds == 0:
                return 0
            time.sleep(args.poll_seconds)
    except KeyboardInterrupt:  # agent-quality: allow: operator interrupt is the documented clean polling shutdown
        return 0
    except SnapshotWriteError as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
