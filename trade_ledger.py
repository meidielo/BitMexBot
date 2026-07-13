"""Durable, versioned execution ledger for safety-critical order state.

The legacy ``data/trades.db`` mixes BTC quantities and contract counts.  This
module deliberately writes to a separate v2 database so legacy rows can never
silently become live-readiness evidence.
"""

from __future__ import annotations

import argparse
import math
import os
import sqlite3
from collections.abc import Iterable, Mapping
from contextlib import closing
from datetime import datetime, timezone
from typing import Any


DB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
DB_PATH = os.path.join(DB_DIR, "trades_v2.db")
UNIT_MODEL = "xbtusdt-linear-metadata-v1"
SCHEMA_VERSION = 3

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
TERMINAL_STATUSES = frozenset({"CLOSED", "FAILED_FLAT", "HALTED_MANUAL"})
ALL_STATUSES = ACTIVE_STATUSES | TERMINAL_STATUSES
UNRESOLVED_STATUSES = ACTIVE_STATUSES | {"HALTED_MANUAL"}

_UNRESOLVED_STATUS_SQL = ", ".join(
    f"'{status}'" for status in sorted(UNRESOLVED_STATUSES)
)

_TRANSITIONS = {
    "REGISTERED": {"ENTRY_PENDING", "FAILED_FLAT", "HALTED_MANUAL"},
    "ENTRY_PENDING": {
        "ENTRY_PARTIAL",
        "ENTRY_FILLED",
        "PROTECTED",
        "PROTECTED_NO_TP",
        "FAILED_FLAT",
        "HALTED_MANUAL",
    },
    "ENTRY_PARTIAL": {
        "ENTRY_FILLED",
        "PROTECTED",
        "PROTECTED_NO_TP",
        "FAILED_FLAT",
        "HALTED_MANUAL",
    },
    "ENTRY_FILLED": {
        "PROTECTED",
        "PROTECTED_NO_TP",
        "FAILED_FLAT",
        "HALTED_MANUAL",
    },
    "PROTECTED": {"CLOSED", "HALTED_MANUAL"},
    "PROTECTED_NO_TP": {"PROTECTED", "CLOSED", "HALTED_MANUAL"},
    "CLOSED": set(),
    "FAILED_FLAT": set(),
    "HALTED_MANUAL": set(),
}

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS execution_intents (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_key              TEXT NOT NULL UNIQUE,
    strategy_version          TEXT NOT NULL,
    environment               TEXT NOT NULL CHECK(environment IN ('testnet', 'dry_run')),
    symbol                    TEXT NOT NULL,
    side                      TEXT NOT NULL CHECK(side IN ('LONG', 'SHORT')),
    decision_time_utc         TEXT NOT NULL,
    signal_entry_price_usdt   REAL NOT NULL,
    signal_stop_price_usdt    REAL NOT NULL,
    signal_target_price_usdt  REAL NOT NULL,
    requested_contracts       INTEGER NOT NULL CHECK(requested_contracts > 0),
    filled_contracts          INTEGER NOT NULL DEFAULT 0 CHECK(filled_contracts >= 0),
    closed_contracts          INTEGER NOT NULL DEFAULT 0 CHECK(closed_contracts >= 0),
    contract_size_btc         REAL NOT NULL CHECK(contract_size_btc > 0),
    requested_base_btc        REAL NOT NULL CHECK(requested_base_btc > 0),
    requested_notional_usdt   REAL NOT NULL CHECK(requested_notional_usdt > 0),
    expected_max_loss_usdt    REAL NOT NULL CHECK(expected_max_loss_usdt > 0),
    unit_model                TEXT NOT NULL,
    entry_client_order_id     TEXT NOT NULL UNIQUE,
    stop_client_order_id      TEXT NOT NULL UNIQUE,
    target_client_order_id    TEXT NOT NULL UNIQUE,
    entry_order_id            TEXT,
    stop_order_id             TEXT,
    target_order_id           TEXT,
    exit_client_order_id      TEXT,
    exit_order_id             TEXT,
    actual_entry_price_usdt   REAL,
    actual_exit_price_usdt    REAL,
    gross_pnl_usdt            REAL,
    fees_usdt                 REAL,
    funding_usdt              REAL,
    net_pnl_usdt              REAL,
    exit_reason               TEXT,
    status                    TEXT NOT NULL,
    halt_reason               TEXT,
    created_at_utc            TEXT NOT NULL,
    updated_at_utc            TEXT NOT NULL
);
"""

_SCHEMA_ADDITIONS = {
    "closed_contracts": "INTEGER NOT NULL DEFAULT 0 CHECK(closed_contracts >= 0)",
    "exit_client_order_id": "TEXT",
    "exit_order_id": "TEXT",
}

_UNIQUE_ORDER_ID_FIELDS = (
    "entry_order_id",
    "stop_order_id",
    "target_order_id",
    "exit_client_order_id",
    "exit_order_id",
)
_SINGLE_UNRESOLVED_INDEX = "ux_execution_intents_single_unresolved"
_REQUIRED_COLUMNS = frozenset(
    {
        "id",
        "decision_key",
        "strategy_version",
        "environment",
        "symbol",
        "side",
        "decision_time_utc",
        "signal_entry_price_usdt",
        "signal_stop_price_usdt",
        "signal_target_price_usdt",
        "requested_contracts",
        "filled_contracts",
        "closed_contracts",
        "contract_size_btc",
        "requested_base_btc",
        "requested_notional_usdt",
        "expected_max_loss_usdt",
        "unit_model",
        "entry_client_order_id",
        "stop_client_order_id",
        "target_client_order_id",
        "entry_order_id",
        "stop_order_id",
        "target_order_id",
        "exit_client_order_id",
        "exit_order_id",
        "actual_entry_price_usdt",
        "actual_exit_price_usdt",
        "gross_pnl_usdt",
        "fees_usdt",
        "funding_usdt",
        "net_pnl_usdt",
        "exit_reason",
        "status",
        "halt_reason",
        "created_at_utc",
        "updated_at_utc",
    }
)


class LedgerError(RuntimeError):
    """Raised when durable execution state cannot be proven or persisted."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _configure_connection(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row


def _initialize_schema(conn: sqlite3.Connection) -> None:
    conn.execute(_CREATE_TABLE)
    existing_columns = {
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(execution_intents)").fetchall()
    }
    for column, definition in _SCHEMA_ADDITIONS.items():
        if column not in existing_columns:
            conn.execute(
                f"ALTER TABLE execution_intents ADD COLUMN {column} {definition}"
            )
    for column in _UNIQUE_ORDER_ID_FIELDS:
        conn.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS ux_execution_intents_{column} "
            f"ON execution_intents({column}) WHERE {column} IS NOT NULL"
        )
    conn.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS {_SINGLE_UNRESOLVED_INDEX} "
        "ON execution_intents((1)) "
        f"WHERE status IN ({_UNRESOLVED_STATUS_SQL})"
    )
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")


def _validate_schema(conn: sqlite3.Connection) -> None:
    version = int(conn.execute("PRAGMA user_version").fetchone()[0])
    if version != SCHEMA_VERSION:
        raise LedgerError(
            f"ledger schema version {version} is not the required {SCHEMA_VERSION}"
        )
    table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        ("execution_intents",),
    ).fetchone()
    if table is None:
        raise LedgerError("ledger schema is missing execution_intents")
    columns = {
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(execution_intents)").fetchall()
    }
    missing_columns = sorted(_REQUIRED_COLUMNS - columns)
    if missing_columns:
        raise LedgerError(f"ledger schema is missing columns: {missing_columns}")
    indexes = {
        str(row["name"])
        for row in conn.execute("PRAGMA index_list(execution_intents)").fetchall()
    }
    required_indexes = {
        *(f"ux_execution_intents_{column}" for column in _UNIQUE_ORDER_ID_FIELDS),
        _SINGLE_UNRESOLVED_INDEX,
    }
    missing_indexes = sorted(required_indexes - indexes)
    if missing_indexes:
        raise LedgerError(f"ledger schema is missing indexes: {missing_indexes}")


def _connect_for_registration(db_path: str) -> sqlite3.Connection:
    absolute_path = os.path.abspath(db_path)
    os.makedirs(os.path.dirname(absolute_path), exist_ok=True)
    conn = sqlite3.connect(absolute_path, timeout=10, isolation_level=None)
    try:
        _configure_connection(conn)
    except sqlite3.Error:
        conn.close()
        raise
    return conn


def _connect_existing(db_path: str) -> sqlite3.Connection:
    absolute_path = os.path.abspath(db_path)
    if not os.path.isfile(absolute_path):
        raise LedgerError(f"ledger database does not exist: {absolute_path}")
    normalized_path = absolute_path.replace("\\", "/")
    conn = sqlite3.connect(
        f"file:{normalized_path}?mode=rw",
        timeout=10,
        isolation_level=None,
        uri=True,
    )
    try:
        _configure_connection(conn)
        _validate_schema(conn)
    except (LedgerError, sqlite3.Error):
        conn.close()
        raise
    return conn


def _require_text(values: Mapping[str, Any], key: str) -> str:
    value = str(values.get(key, "")).strip()
    if not value:
        raise LedgerError(f"{key} is required")
    return value


def _require_positive(values: Mapping[str, Any], key: str) -> float:
    try:
        value = float(values[key])
    except (KeyError, TypeError, ValueError) as exc:
        raise LedgerError(f"{key} must be numeric") from exc
    if value <= 0:
        raise LedgerError(f"{key} must be positive")
    return value


def _normalize_intent(values: Mapping[str, Any]) -> dict[str, Any]:
    environment = _require_text(values, "environment").lower()
    if environment not in {"testnet", "dry_run"}:
        raise LedgerError("v2 ledger refuses production execution intents")
    side = _require_text(values, "side").upper()
    if side not in {"LONG", "SHORT"}:
        raise LedgerError("side must be LONG or SHORT")
    requested_contracts = int(_require_positive(values, "requested_contracts"))
    if requested_contracts != float(values["requested_contracts"]):
        raise LedgerError("requested_contracts must be a whole number")

    normalized = {
        "decision_key": _require_text(values, "decision_key"),
        "strategy_version": _require_text(values, "strategy_version"),
        "environment": environment,
        "symbol": _require_text(values, "symbol"),
        "side": side,
        "decision_time_utc": _require_text(values, "decision_time_utc"),
        "signal_entry_price_usdt": _require_positive(values, "signal_entry_price_usdt"),
        "signal_stop_price_usdt": _require_positive(values, "signal_stop_price_usdt"),
        "signal_target_price_usdt": _require_positive(values, "signal_target_price_usdt"),
        "requested_contracts": requested_contracts,
        "contract_size_btc": _require_positive(values, "contract_size_btc"),
        "requested_base_btc": _require_positive(values, "requested_base_btc"),
        "requested_notional_usdt": _require_positive(values, "requested_notional_usdt"),
        "expected_max_loss_usdt": _require_positive(values, "expected_max_loss_usdt"),
        "unit_model": _require_text(values, "unit_model"),
        "entry_client_order_id": _require_text(values, "entry_client_order_id"),
        "stop_client_order_id": _require_text(values, "stop_client_order_id"),
        "target_client_order_id": _require_text(values, "target_client_order_id"),
    }
    if normalized["unit_model"] != UNIT_MODEL:
        raise LedgerError(f"unsupported unit_model {normalized['unit_model']!r}")
    client_order_ids = {
        normalized["entry_client_order_id"],
        normalized["stop_client_order_id"],
        normalized["target_client_order_id"],
    }
    if len(client_order_ids) != 3:
        raise LedgerError("client order IDs must be distinct")
    return normalized


def register_intent(values: Mapping[str, Any], db_path: str = DB_PATH) -> dict[str, Any]:
    """Persist an intent before submission, idempotently by ``decision_key``.

    Re-registering the exact same immutable intent is safe.  Reusing a decision
    key with different values raises instead of mutating execution history.
    """

    intent = _normalize_intent(values)
    now = _utc_now()
    columns = list(intent)
    placeholders = ", ".join("?" for _ in columns)
    sql = (
        f"INSERT INTO execution_intents "
        f"({', '.join(columns)}, status, created_at_utc, updated_at_utc) "
        f"VALUES ({placeholders}, 'REGISTERED', ?, ?)"
    )
    try:
        with closing(_connect_for_registration(db_path)) as conn:
            conn.execute("BEGIN IMMEDIATE")
            _initialize_schema(conn)
            _validate_schema(conn)
            existing = conn.execute(
                "SELECT * FROM execution_intents WHERE decision_key = ?",
                (intent["decision_key"],),
            ).fetchone()
            if existing is not None:
                for key, expected in intent.items():
                    actual = existing[key]
                    if isinstance(expected, float):
                        matches = abs(float(actual) - expected) <= 1e-12
                    else:
                        matches = actual == expected
                    if not matches:
                        raise LedgerError(
                            f"decision_key already exists with different {key}"
                        )
                conn.commit()
                return dict(existing)
            unresolved = conn.execute(
                f"SELECT decision_key FROM execution_intents "
                f"WHERE status IN ({_UNRESOLVED_STATUS_SQL}) LIMIT 1"
            ).fetchone()
            if unresolved is not None:
                raise LedgerError(
                    "cannot register a different decision while unresolved intent "
                    f"{unresolved['decision_key']!r} exists"
                )
            conn.execute(sql, (*intent.values(), now, now))
            row = conn.execute(
                "SELECT * FROM execution_intents WHERE decision_key = ?",
                (intent["decision_key"],),
            ).fetchone()
            conn.commit()
            if row is None:
                raise LedgerError("intent insert succeeded but row cannot be read")
            return dict(row)
    except sqlite3.Error as exc:
        raise LedgerError(f"could not persist execution intent: {exc}") from exc


def initialize_ledger(db_path: str = DB_PATH) -> str:
    """Explicitly create or migrate an empty execution ledger for startup."""

    absolute_path = os.path.abspath(db_path)
    try:
        with closing(_connect_for_registration(absolute_path)) as conn:
            conn.execute("BEGIN IMMEDIATE")
            _initialize_schema(conn)
            _validate_schema(conn)
            conn.commit()
    except sqlite3.Error as exc:
        raise LedgerError(f"could not initialize execution ledger: {exc}") from exc
    return absolute_path


_UPDATABLE_FIELDS = frozenset(
    {
        "filled_contracts",
        "closed_contracts",
        "entry_order_id",
        "stop_order_id",
        "target_order_id",
        "exit_client_order_id",
        "exit_order_id",
        "actual_entry_price_usdt",
        "actual_exit_price_usdt",
        "gross_pnl_usdt",
        "fees_usdt",
        "funding_usdt",
        "net_pnl_usdt",
        "exit_reason",
        "halt_reason",
    }
)

_ORDER_ID_FIELDS = frozenset(
    {
        "entry_order_id",
        "stop_order_id",
        "target_order_id",
        "exit_client_order_id",
        "exit_order_id",
    }
)
_POSITIVE_PRICE_FIELDS = frozenset(
    {"actual_entry_price_usdt", "actual_exit_price_usdt"}
)
_PNL_FIELDS = frozenset(
    {"gross_pnl_usdt", "fees_usdt", "funding_usdt", "net_pnl_usdt"}
)


def _whole_non_negative(value: Any, field: str) -> int:
    try:
        numeric = float(value)
        normalized = int(numeric)
    except (TypeError, ValueError, OverflowError) as exc:
        raise LedgerError(f"{field} must be a whole non-negative number") from exc
    if not math.isfinite(numeric) or numeric < 0 or normalized != numeric:
        raise LedgerError(f"{field} must be a whole non-negative number")
    return normalized


def _finite_number(value: Any, field: str) -> float:
    try:
        normalized = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise LedgerError(f"{field} must be finite numeric evidence") from exc
    if not math.isfinite(normalized):
        raise LedgerError(f"{field} must be finite numeric evidence")
    return normalized


def _normalize_updates(
    row: Mapping[str, Any], updates: Mapping[str, Any]
) -> dict[str, Any]:
    changes = dict(updates)
    unknown_fields = set(changes) - _UPDATABLE_FIELDS
    if unknown_fields:
        raise LedgerError(f"unsupported update fields: {sorted(unknown_fields)}")

    for field in _ORDER_ID_FIELDS:
        if field not in changes:
            continue
        if changes[field] is None:
            raise LedgerError(f"{field} must be non-empty")
        value = str(changes[field]).strip()
        if not value:
            raise LedgerError(f"{field} must be non-empty")
        existing = row[field]
        if existing is not None and str(existing) != value:
            raise LedgerError(f"{field} is immutable once recorded")
        changes[field] = value

    old_filled = int(row["filled_contracts"])
    new_filled = old_filled
    if "filled_contracts" in changes:
        new_filled = _whole_non_negative(changes["filled_contracts"], "filled_contracts")
        if new_filled < old_filled:
            raise LedgerError("filled_contracts cannot decrease")
        if new_filled > int(row["requested_contracts"]):
            raise LedgerError("filled_contracts cannot exceed requested_contracts")
        changes["filled_contracts"] = new_filled

    old_closed = int(row["closed_contracts"])
    new_closed = old_closed
    if "closed_contracts" in changes:
        new_closed = _whole_non_negative(changes["closed_contracts"], "closed_contracts")
        if new_closed < old_closed:
            raise LedgerError("closed_contracts cannot decrease")
        changes["closed_contracts"] = new_closed
    if new_closed > new_filled:
        raise LedgerError("closed_contracts cannot exceed filled_contracts")
    if new_filled > old_filled and "actual_entry_price_usdt" not in changes:
        raise LedgerError(
            "a cumulative entry fill increase requires an updated entry average"
        )
    if new_closed > old_closed and "actual_exit_price_usdt" not in changes:
        raise LedgerError(
            "a cumulative exit fill increase requires an updated exit average"
        )

    for field in _POSITIVE_PRICE_FIELDS:
        if field not in changes:
            continue
        value = _finite_number(changes[field], field)
        if value <= 0:
            raise LedgerError(f"{field} must be positive")
        if field == "actual_entry_price_usdt" and new_filled == 0:
            raise LedgerError("entry average requires a positive entry fill")
        if field == "actual_exit_price_usdt" and new_closed == 0:
            raise LedgerError("exit average requires a positive exit fill")
        existing = row[field]
        quantity_advanced = (
            new_filled > old_filled
            if field == "actual_entry_price_usdt"
            else new_closed > old_closed
        )
        if existing is not None and float(existing) != value and not quantity_advanced:
            raise LedgerError(
                f"{field} can change only when its cumulative fill increases"
            )
        changes[field] = value

    for field in _PNL_FIELDS:
        if field in changes:
            changes[field] = _finite_number(changes[field], field)
    if "fees_usdt" in changes and changes["fees_usdt"] < 0:
        raise LedgerError("fees_usdt must be a non-negative cost")

    for field in ("exit_reason", "halt_reason"):
        if field not in changes:
            continue
        if changes[field] is None:
            raise LedgerError(f"{field} must be non-empty")
        value = str(changes[field]).strip()
        if not value:
            raise LedgerError(f"{field} must be non-empty")
        existing = row[field]
        if existing is not None and str(existing) != value:
            raise LedgerError(f"{field} is immutable once recorded")
        changes[field] = value

    client_ids = {
        str(value)
        for value in (
            row["entry_client_order_id"],
            row["stop_client_order_id"],
            row["target_client_order_id"],
            changes.get("exit_client_order_id", row["exit_client_order_id"]),
        )
        if value is not None
    }
    expected_client_id_count = 4 if (
        changes.get("exit_client_order_id", row["exit_client_order_id"]) is not None
    ) else 3
    if len(client_ids) != expected_client_id_count:
        raise LedgerError("client order IDs must identify distinct order legs")

    external_ids = [
        value
        for value in (
            changes.get("entry_order_id", row["entry_order_id"]),
            changes.get("stop_order_id", row["stop_order_id"]),
            changes.get("target_order_id", row["target_order_id"]),
            changes.get("exit_order_id", row["exit_order_id"]),
        )
        if value is not None
    ]
    if len(set(external_ids)) != len(external_ids):
        raise LedgerError("external order IDs must identify distinct order legs")
    return changes


def _merged(row: Mapping[str, Any], changes: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result.update(changes)
    return result


def _require_state_evidence(status: str, values: Mapping[str, Any]) -> None:
    filled = int(values["filled_contracts"])
    closed = int(values["closed_contracts"])
    requested = int(values["requested_contracts"])

    if status == "ENTRY_PARTIAL":
        if (
            not 0 < filled < requested
            or values["actual_entry_price_usdt"] is None
            or values["entry_order_id"] is None
        ):
            raise LedgerError(
                "ENTRY_PARTIAL requires an order ID, partial fill, and entry average"
            )
    if status == "ENTRY_FILLED":
        if (
            filled != requested
            or values["actual_entry_price_usdt"] is None
            or values["entry_order_id"] is None
        ):
            raise LedgerError(
                "ENTRY_FILLED requires an order ID, complete fill, and entry average"
            )
    if status in {"PROTECTED", "PROTECTED_NO_TP"}:
        if (
            filled <= closed
            or values["actual_entry_price_usdt"] is None
            or values["entry_order_id"] is None
        ):
            raise LedgerError("protected state requires known open exposure")
        if values["stop_order_id"] is None:
            raise LedgerError("protected state requires a durable stop_order_id")
        if status == "PROTECTED" and values["target_order_id"] is None:
            raise LedgerError("PROTECTED requires a durable target_order_id")
    if status == "HALTED_MANUAL" and not values["halt_reason"]:
        raise LedgerError("HALTED_MANUAL requires halt_reason")
    if status in {"CLOSED", "FAILED_FLAT"} and filled > 0:
        required = (
            "actual_entry_price_usdt",
            "actual_exit_price_usdt",
            "gross_pnl_usdt",
            "fees_usdt",
            "funding_usdt",
            "net_pnl_usdt",
            "exit_reason",
        )
        if closed != filled or any(values[field] is None for field in required):
            raise LedgerError(
                f"{status} with a fill requires complete flat-position evidence"
            )
        direction = 1.0 if values["side"] == "LONG" else -1.0
        expected_gross = (
            filled
            * float(values["contract_size_btc"])
            * (float(values["actual_exit_price_usdt"]) - float(values["actual_entry_price_usdt"]))
            * direction
        )
        gross = float(values["gross_pnl_usdt"])
        gross_tolerance = max(1e-8, abs(expected_gross) * 1e-9)
        if abs(gross - expected_gross) > gross_tolerance:
            raise LedgerError("gross_pnl_usdt is inconsistent with contract economics")
        expected_net = (
            gross - float(values["fees_usdt"]) + float(values["funding_usdt"])
        )
        net_tolerance = max(1e-8, abs(expected_net) * 1e-9)
        if abs(float(values["net_pnl_usdt"]) - expected_net) > net_tolerance:
            raise LedgerError("net_pnl_usdt is inconsistent with costs and funding")


def transition_intent(
    decision_key: str,
    to_status: str,
    *,
    expected_statuses: Iterable[str],
    updates: Mapping[str, Any] | None = None,
    expected_updated_at_utc: str | None = None,
    db_path: str = DB_PATH,
) -> dict[str, Any]:
    """Atomically transition an intent using compare-and-set semantics."""

    target = str(to_status).strip().upper()
    expected = {str(value).strip().upper() for value in expected_statuses}
    if target not in ALL_STATUSES or not expected:
        raise LedgerError("invalid or missing ledger status")
    try:
        with closing(_connect_existing(db_path)) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM execution_intents WHERE decision_key = ?",
                (decision_key,),
            ).fetchone()
            if row is None:
                raise LedgerError(f"unknown decision_key {decision_key!r}")
            current = str(row["status"])
            if current not in expected:
                raise LedgerError(
                    f"expected status in {sorted(expected)}, found {current}"
                )
            if target not in _TRANSITIONS[current]:
                raise LedgerError(f"invalid transition {current} -> {target}")
            if (
                expected_updated_at_utc is not None
                and row["updated_at_utc"] != expected_updated_at_utc
            ):
                raise LedgerError("intent changed after it was read")

            changes = _normalize_updates(row, updates or {})
            _require_state_evidence(target, _merged(row, changes))

            assignments = ["status = ?", "updated_at_utc = ?"]
            params: list[Any] = [target, _utc_now()]
            for field, value in changes.items():
                assignments.append(f"{field} = ?")
                params.append(value)
            params.extend([decision_key, current])
            revision_clause = ""
            if expected_updated_at_utc is not None:
                revision_clause = " AND updated_at_utc = ?"
                params.append(expected_updated_at_utc)
            cursor = conn.execute(
                f"UPDATE execution_intents SET {', '.join(assignments)} "
                f"WHERE decision_key = ? AND status = ?{revision_clause}",
                params,
            )
            if cursor.rowcount != 1:
                raise LedgerError("intent transition lost a concurrent update race")
            updated = conn.execute(
                "SELECT * FROM execution_intents WHERE decision_key = ?",
                (decision_key,),
            ).fetchone()
            conn.commit()
            if updated is None:
                raise LedgerError("transitioned intent cannot be read")
            return dict(updated)
    except sqlite3.Error as exc:
        raise LedgerError(f"could not transition execution intent: {exc}") from exc


def record_execution_evidence(
    decision_key: str,
    *,
    expected_statuses: Iterable[str],
    updates: Mapping[str, Any],
    expected_updated_at_utc: str | None = None,
    db_path: str = DB_PATH,
) -> dict[str, Any]:
    """Durably record exchange evidence without inventing a state transition.

    This supports cumulative partial-fill and exit reconciliation. Quantities
    may only increase, external order identifiers are immutable, and callers
    can supply the last observed ``updated_at_utc`` for strict revision CAS.
    """

    expected = {str(value).strip().upper() for value in expected_statuses}
    if not expected or not expected <= ALL_STATUSES:
        raise LedgerError("invalid or missing expected ledger status")
    if not updates:
        raise LedgerError("execution evidence update cannot be empty")
    try:
        with closing(_connect_existing(db_path)) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM execution_intents WHERE decision_key = ?",
                (decision_key,),
            ).fetchone()
            if row is None:
                raise LedgerError(f"unknown decision_key {decision_key!r}")
            current = str(row["status"])
            if current not in expected:
                raise LedgerError(
                    f"expected status in {sorted(expected)}, found {current}"
                )
            if (
                expected_updated_at_utc is not None
                and row["updated_at_utc"] != expected_updated_at_utc
            ):
                raise LedgerError("intent changed after it was read")
            changes = _normalize_updates(row, updates)
            _require_state_evidence(current, _merged(row, changes))

            assignments = ["updated_at_utc = ?"]
            params: list[Any] = [_utc_now()]
            for field, value in changes.items():
                assignments.append(f"{field} = ?")
                params.append(value)
            params.extend([decision_key, current])
            revision_clause = ""
            if expected_updated_at_utc is not None:
                revision_clause = " AND updated_at_utc = ?"
                params.append(expected_updated_at_utc)
            cursor = conn.execute(
                f"UPDATE execution_intents SET {', '.join(assignments)} "
                f"WHERE decision_key = ? AND status = ?{revision_clause}",
                params,
            )
            if cursor.rowcount != 1:
                raise LedgerError("evidence update lost a concurrent update race")
            updated = conn.execute(
                "SELECT * FROM execution_intents WHERE decision_key = ?",
                (decision_key,),
            ).fetchone()
            conn.commit()
            if updated is None:
                raise LedgerError("updated intent cannot be read")
            return dict(updated)
    except sqlite3.Error as exc:
        raise LedgerError(f"could not record execution evidence: {exc}") from exc


def get_intent(decision_key: str, db_path: str = DB_PATH) -> dict[str, Any] | None:
    try:
        with closing(_connect_existing(db_path)) as conn:
            row = conn.execute(
                "SELECT * FROM execution_intents WHERE decision_key = ?",
                (decision_key,),
            ).fetchone()
            return dict(row) if row is not None else None
    except sqlite3.Error as exc:
        raise LedgerError(f"could not read execution intent: {exc}") from exc


def _get_unique_by_order_reference(
    reference: str, columns: tuple[str, ...], db_path: str
) -> dict[str, Any] | None:
    value = str(reference).strip()
    if not value:
        raise LedgerError("order reference must be non-empty")
    predicates = " OR ".join(f"{column} = ?" for column in columns)
    try:
        with closing(_connect_existing(db_path)) as conn:
            rows = conn.execute(
                f"SELECT * FROM execution_intents WHERE {predicates}",
                tuple(value for _ in columns),
            ).fetchall()
            if len(rows) > 1:
                raise LedgerError("order reference resolves to multiple intents")
            return dict(rows[0]) if rows else None
    except sqlite3.Error as exc:
        raise LedgerError(f"could not reconcile order reference: {exc}") from exc


def get_intent_by_client_order_id(
    client_order_id: str, db_path: str = DB_PATH
) -> dict[str, Any] | None:
    """Resolve any deterministic entry, stop, target, or exit client ID."""

    return _get_unique_by_order_reference(
        client_order_id,
        (
            "entry_client_order_id",
            "stop_client_order_id",
            "target_client_order_id",
            "exit_client_order_id",
        ),
        db_path,
    )


def get_intent_by_external_order_id(
    order_id: str, db_path: str = DB_PATH
) -> dict[str, Any] | None:
    """Resolve a ledger intent from any exchange-assigned order ID."""

    return _get_unique_by_order_reference(
        order_id,
        ("entry_order_id", "stop_order_id", "target_order_id", "exit_order_id"),
        db_path,
    )


def list_active_intents(db_path: str = DB_PATH) -> list[dict[str, Any]]:
    placeholders = ", ".join("?" for _ in ACTIVE_STATUSES)
    try:
        with closing(_connect_existing(db_path)) as conn:
            rows = conn.execute(
                f"SELECT * FROM execution_intents WHERE status IN ({placeholders}) "
                "ORDER BY created_at_utc",
                tuple(sorted(ACTIVE_STATUSES)),
            ).fetchall()
            return [dict(row) for row in rows]
    except sqlite3.Error as exc:
        raise LedgerError(f"could not list active execution intents: {exc}") from exc


def list_open_intents(db_path: str = DB_PATH) -> list[dict[str, Any]]:
    """List every intent still needing automated or manual reconciliation.

    ``HALTED_MANUAL`` is deliberately included because it may still represent
    exchange exposure even though automated transitions have stopped.
    """

    placeholders = ", ".join("?" for _ in UNRESOLVED_STATUSES)
    try:
        with closing(_connect_existing(db_path)) as conn:
            rows = conn.execute(
                f"SELECT * FROM execution_intents WHERE status IN ({placeholders}) "
                "ORDER BY created_at_utc, id",
                tuple(sorted(UNRESOLVED_STATUSES)),
            ).fetchall()
            return [dict(row) for row in rows]
    except sqlite3.Error as exc:
        raise LedgerError(f"could not list open execution intents: {exc}") from exc


__all__ = [
    "ACTIVE_STATUSES",
    "ALL_STATUSES",
    "DB_PATH",
    "LedgerError",
    "TERMINAL_STATUSES",
    "UNIT_MODEL",
    "UNRESOLVED_STATUSES",
    "get_intent",
    "get_intent_by_client_order_id",
    "get_intent_by_external_order_id",
    "initialize_ledger",
    "list_active_intents",
    "list_open_intents",
    "record_execution_evidence",
    "register_intent",
    "transition_intent",
]


def _main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--init",
        action="store_true",
        help="explicitly create or migrate the versioned execution ledger",
    )
    parser.add_argument("--path", default=DB_PATH, help="absolute ledger path")
    args = parser.parse_args()
    if not args.init:
        parser.error("no action selected; use --init")
    print(f"Initialized versioned execution ledger: {initialize_ledger(args.path)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
