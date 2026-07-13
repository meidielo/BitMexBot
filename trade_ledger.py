"""Durable, versioned execution ledger for safety-critical order state.

The legacy ``data/trades.db`` mixes BTC quantities and contract counts.  This
module deliberately writes to a separate v2 database so legacy rows can never
silently become live-readiness evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import math
import os
import re
import sqlite3
from collections.abc import Iterable, Mapping
from contextlib import closing
from datetime import datetime, timezone
from typing import Any


DB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
DB_PATH = os.path.join(DB_DIR, "trades_v2.db")
UNIT_MODEL = "xbtusdt-linear-metadata-v1"
SCHEMA_VERSION = 4

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

_CREATE_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS execution_events (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    intent_id                 INTEGER NOT NULL,
    exec_id                   TEXT NOT NULL UNIQUE,
    account_id                TEXT NOT NULL,
    native_symbol             TEXT NOT NULL CHECK(native_symbol = 'XBTUSDT'),
    event_type                TEXT NOT NULL CHECK(event_type IN ('TRADE', 'FUNDING')),
    event_role                TEXT NOT NULL CHECK(event_role IN ('ENTRY', 'EXIT', 'FUNDING')),
    order_id                  TEXT,
    client_order_id           TEXT,
    link_id                   TEXT,
    side                      TEXT,
    last_qty                  INTEGER NOT NULL CHECK(last_qty >= 0),
    last_price_usdt           REAL,
    commission_usdt           REAL NOT NULL,
    funding_usdt              REAL NOT NULL,
    realised_pnl_usdt         REAL,
    transact_time_utc         TEXT NOT NULL,
    source_hash               TEXT NOT NULL,
    created_at_utc            TEXT NOT NULL,
    FOREIGN KEY(intent_id) REFERENCES execution_intents(id) ON DELETE RESTRICT
);
"""

_SCHEMA_ADDITIONS = {
    "closed_contracts": "INTEGER NOT NULL DEFAULT 0 CHECK(closed_contracts >= 0)",
    "exit_client_order_id": "TEXT",
    "exit_order_id": "TEXT",
    "entry_time_utc": "TEXT",
    "exit_time_utc": "TEXT",
    "reconciled_at_utc": "TEXT",
    "evidence_source_hash": "TEXT",
}

_UNIQUE_ORDER_ID_FIELDS = (
    "entry_order_id",
    "stop_order_id",
    "target_order_id",
    "exit_client_order_id",
    "exit_order_id",
)
_SINGLE_UNRESOLVED_INDEX = "ux_execution_intents_single_unresolved"
_EVENT_INTENT_INDEX = "idx_execution_events_intent_time"
_EVENT_NO_UPDATE_TRIGGER = "execution_events_append_only_update"
_EVENT_NO_DELETE_TRIGGER = "execution_events_append_only_delete"
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
        "entry_time_utc",
        "exit_time_utc",
        "reconciled_at_utc",
        "evidence_source_hash",
        "status",
        "halt_reason",
        "created_at_utc",
        "updated_at_utc",
    }
)

_REQUIRED_EVENT_COLUMNS = frozenset(
    {
        "id",
        "intent_id",
        "exec_id",
        "account_id",
        "native_symbol",
        "event_type",
        "event_role",
        "order_id",
        "client_order_id",
        "link_id",
        "side",
        "last_qty",
        "last_price_usdt",
        "commission_usdt",
        "funding_usdt",
        "realised_pnl_usdt",
        "transact_time_utc",
        "source_hash",
        "created_at_utc",
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
    conn.execute(_CREATE_EVENTS_TABLE)
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS {_EVENT_INTENT_INDEX} "
        "ON execution_events(intent_id, transact_time_utc, exec_id)"
    )
    conn.execute(
        f"CREATE TRIGGER IF NOT EXISTS {_EVENT_NO_UPDATE_TRIGGER} "
        "BEFORE UPDATE ON execution_events BEGIN "
        "SELECT RAISE(ABORT, 'execution_events are append-only'); END"
    )
    conn.execute(
        f"CREATE TRIGGER IF NOT EXISTS {_EVENT_NO_DELETE_TRIGGER} "
        "BEFORE DELETE ON execution_events BEGIN "
        "SELECT RAISE(ABORT, 'execution_events are append-only'); END"
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

    event_table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        ("execution_events",),
    ).fetchone()
    if event_table is None:
        raise LedgerError("ledger schema is missing execution_events")
    event_columns = {
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(execution_events)").fetchall()
    }
    missing_event_columns = sorted(_REQUIRED_EVENT_COLUMNS - event_columns)
    if missing_event_columns:
        raise LedgerError(
            f"ledger schema is missing execution event columns: {missing_event_columns}"
        )
    event_indexes = {
        str(row["name"])
        for row in conn.execute("PRAGMA index_list(execution_events)").fetchall()
    }
    if _EVENT_INTENT_INDEX not in event_indexes:
        raise LedgerError("ledger schema is missing the execution-event intent index")
    trigger_rows = {
        str(row[0]): str(row[1] or "")
        for row in conn.execute(
            "SELECT name, sql FROM sqlite_master WHERE type = 'trigger'"
        ).fetchall()
    }
    required_triggers = {_EVENT_NO_UPDATE_TRIGGER, _EVENT_NO_DELETE_TRIGGER}
    if not required_triggers <= trigger_rows.keys():
        raise LedgerError("ledger schema is missing append-only event triggers")
    for name, operation in (
        (_EVENT_NO_UPDATE_TRIGGER, "UPDATE"),
        (_EVENT_NO_DELETE_TRIGGER, "DELETE"),
    ):
        expected = (
            f"CREATE TRIGGER {name} BEFORE {operation} ON execution_events BEGIN "
            "SELECT RAISE(ABORT, 'execution_events are append-only'); END"
        )
        normalized_actual = re.sub(r"\s+", " ", trigger_rows[name]).strip().casefold()
        normalized_expected = re.sub(r"\s+", " ", expected).strip().casefold()
        if normalized_actual != normalized_expected:
            raise LedgerError("ledger append-only event trigger definition is invalid")


def validate_execution_ledger_schema(conn: sqlite3.Connection) -> None:
    """Validate the complete current ledger contract on an existing connection."""

    _validate_schema(conn)


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
        "entry_time_utc",
        "exit_time_utc",
        "reconciled_at_utc",
        "evidence_source_hash",
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


def _utc_timestamp(value: Any, field: str) -> str:
    text = str(value).strip()
    if not text:
        raise LedgerError(f"{field} must be a non-empty UTC timestamp")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LedgerError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise LedgerError(f"{field} must be timezone-aware")
    return parsed.astimezone(timezone.utc).isoformat()


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

    for field in ("entry_time_utc", "exit_time_utc", "reconciled_at_utc"):
        if field not in changes:
            continue
        value = _utc_timestamp(changes[field], field)
        existing = row[field]
        if existing is not None and str(existing) != value:
            raise LedgerError(f"{field} is immutable once recorded")
        changes[field] = value

    if "evidence_source_hash" in changes:
        value = str(changes["evidence_source_hash"]).strip().lower()
        if not re.fullmatch(r"[0-9a-f]{64}", value):
            raise LedgerError("evidence_source_hash must be a SHA-256 hex digest")
        existing = row["evidence_source_hash"]
        if existing is not None and str(existing) != value:
            raise LedgerError("evidence_source_hash is immutable once recorded")
        changes["evidence_source_hash"] = value

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
        provenance = (
            values["entry_time_utc"],
            values["exit_time_utc"],
            values["reconciled_at_utc"],
            values["evidence_source_hash"],
        )
        if any(value is None for value in provenance):
            raise LedgerError("filled terminal rows require complete exchange provenance")
        if not re.fullmatch(r"[0-9a-f]{64}", str(values["evidence_source_hash"])):
            raise LedgerError("terminal evidence_source_hash must be SHA-256")
        try:
            entry_time = datetime.fromisoformat(str(values["entry_time_utc"]))
            exit_time = datetime.fromisoformat(str(values["exit_time_utc"]))
            reconciled_at = datetime.fromisoformat(str(values["reconciled_at_utc"]))
        except ValueError as exc:
            raise LedgerError("terminal exchange timestamps are invalid") from exc
        if any(value.tzinfo is None or value.utcoffset() is None for value in (entry_time, exit_time, reconciled_at)):
            raise LedgerError("terminal exchange timestamps must be timezone-aware")
        if not entry_time <= exit_time <= reconciled_at:
            raise LedgerError("terminal exchange timestamps are not chronologically ordered")


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


_EVENT_FIELDS = (
    "exec_id",
    "account_id",
    "native_symbol",
    "event_type",
    "event_role",
    "order_id",
    "client_order_id",
    "link_id",
    "side",
    "last_qty",
    "last_price_usdt",
    "commission_usdt",
    "funding_usdt",
    "realised_pnl_usdt",
    "transact_time_utc",
    "source_hash",
)


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_execution_event(event: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(event, Mapping):
        raise LedgerError("execution event must be a mapping")
    exec_id = _optional_text(event.get("exec_id"))
    account_id = _optional_text(event.get("account_id"))
    native_symbol = _optional_text(event.get("native_symbol"))
    event_type = str(event.get("event_type", "")).strip().upper()
    event_role = str(event.get("event_role", "")).strip().upper()
    source_hash = str(event.get("source_hash", "")).strip().lower()
    if not exec_id or not account_id:
        raise LedgerError("execution event requires exec_id and account_id")
    if native_symbol != "XBTUSDT":
        raise LedgerError("execution event must be native XBTUSDT evidence")
    if event_type not in {"TRADE", "FUNDING"}:
        raise LedgerError("execution event type must be TRADE or FUNDING")
    if event_role not in {"ENTRY", "EXIT", "FUNDING"}:
        raise LedgerError("execution event role is invalid")
    if not re.fullmatch(r"[0-9a-f]{64}", source_hash):
        raise LedgerError("execution event source_hash must be SHA-256")

    last_qty = _whole_non_negative(event.get("last_qty"), "last_qty")
    commission = _finite_number(event.get("commission_usdt"), "commission_usdt")
    funding = _finite_number(event.get("funding_usdt"), "funding_usdt")
    price_raw = event.get("last_price_usdt")
    price = None if price_raw is None else _finite_number(price_raw, "last_price_usdt")
    realised_raw = event.get("realised_pnl_usdt")
    if realised_raw is None:
        raise LedgerError("execution event requires native realised_pnl_usdt")
    realised = _finite_number(realised_raw, "realised_pnl_usdt")
    side = _optional_text(event.get("side"))
    if side is not None:
        side = side.upper()
    order_id = _optional_text(event.get("order_id"))

    if event_type == "TRADE":
        if event_role not in {"ENTRY", "EXIT"}:
            raise LedgerError("trade execution must be ENTRY or EXIT evidence")
        if not order_id or side not in {"BUY", "SELL"}:
            raise LedgerError("trade execution requires an order ID and side")
        if last_qty <= 0 or price is None or price <= 0:
            raise LedgerError("trade execution requires positive quantity and price")
        if funding != 0:
            raise LedgerError("trade execution cannot carry a funding amount")
    else:
        if event_role != "FUNDING":
            raise LedgerError("funding execution must use the FUNDING role")
        if price is not None or commission != 0:
            raise LedgerError("funding execution has invalid trade fields")

    return {
        "exec_id": exec_id,
        "account_id": account_id,
        "native_symbol": native_symbol,
        "event_type": event_type,
        "event_role": event_role,
        "order_id": order_id,
        "client_order_id": _optional_text(event.get("client_order_id")),
        "link_id": _optional_text(event.get("link_id")),
        "side": side,
        "last_qty": last_qty,
        "last_price_usdt": price,
        "commission_usdt": commission,
        "funding_usdt": funding,
        "realised_pnl_usdt": realised,
        "transact_time_utc": _utc_timestamp(
            event.get("transact_time_utc"), "transact_time_utc"
        ),
        "source_hash": source_hash,
    }


def _event_evidence_hash(events: Iterable[Mapping[str, Any]]) -> str:
    components = sorted(
        f"{event['exec_id']}:{event['source_hash']}:{event['event_role']}"
        for event in events
    )
    return hashlib.sha256("\n".join(components).encode("utf-8")).hexdigest()


def _weighted_average(events: list[Mapping[str, Any]], label: str) -> float:
    quantity = sum(int(event["last_qty"]) for event in events)
    if quantity <= 0:
        raise LedgerError(f"{label} execution quantity is missing")
    notional = sum(
        float(event["last_price_usdt"]) * int(event["last_qty"])
        for event in events
    )
    return notional / quantity


def _derive_close_updates(
    row: Mapping[str, Any], events: list[Mapping[str, Any]]
) -> dict[str, Any]:
    entries = [event for event in events if event["event_role"] == "ENTRY"]
    exits = [event for event in events if event["event_role"] == "EXIT"]
    funding_events = [
        event for event in events if event["event_role"] == "FUNDING"
    ]
    filled = int(row["filled_contracts"])
    entry_qty = sum(int(event["last_qty"]) for event in entries)
    exit_qty = sum(int(event["last_qty"]) for event in exits)
    if filled <= 0 or entry_qty != filled or exit_qty != filled:
        raise LedgerError(
            "execution events do not reconcile exactly to the durable filled quantity"
        )
    accounts = {str(event["account_id"]) for event in events}
    if len(accounts) != 1:
        raise LedgerError("execution events span multiple exchange accounts")

    entry_average = _weighted_average(entries, "entry")
    recorded_entry = float(row["actual_entry_price_usdt"])
    tolerance = max(1e-8, abs(recorded_entry) * 1e-9)
    if abs(entry_average - recorded_entry) > tolerance:
        raise LedgerError("entry executions disagree with the durable entry average")
    exit_average = _weighted_average(exits, "exit")

    entry_time = min(str(event["transact_time_utc"]) for event in entries)
    exit_time = max(str(event["transact_time_utc"]) for event in exits)
    if datetime.fromisoformat(entry_time) > datetime.fromisoformat(exit_time):
        raise LedgerError("exit executions precede the entry executions")
    for event in funding_events:
        timestamp = datetime.fromisoformat(str(event["transact_time_utc"]))
        if not datetime.fromisoformat(entry_time) <= timestamp <= datetime.fromisoformat(
            exit_time
        ):
            raise LedgerError("funding execution falls outside the position lifetime")

    exit_order_ids = {str(event["order_id"]) for event in exits}
    if len(exit_order_ids) != 1:
        raise LedgerError("exit executions span multiple order legs")
    exit_order_id = next(iter(exit_order_ids))
    if exit_order_id == row["stop_order_id"]:
        exit_reason = "stop_loss"
    elif exit_order_id == row["target_order_id"]:
        exit_reason = "take_profit"
    elif exit_order_id == row["exit_order_id"]:
        exit_reason = "emergency_close"
    else:
        raise LedgerError("exit execution is not attributable to a durable order leg")

    fees = sum(
        float(event["commission_usdt"])
        for event in events
        if event["event_type"] == "TRADE"
    )
    funding = sum(float(event["funding_usdt"]) for event in funding_events)
    direction = 1.0 if row["side"] == "LONG" else -1.0
    gross = (
        filled
        * float(row["contract_size_btc"])
        * (exit_average - entry_average)
        * direction
    )
    net = gross - fees + funding
    native_net = sum(float(event["realised_pnl_usdt"]) for event in events)
    native_tolerance = max(0.000001, abs(net) * 1e-9)
    if abs(native_net - net) > native_tolerance:
        raise LedgerError(
            "native realised PnL disagrees with price, fee, and funding accounting"
        )
    return {
        "closed_contracts": filled,
        "actual_exit_price_usdt": exit_average,
        "gross_pnl_usdt": gross,
        "fees_usdt": fees,
        "funding_usdt": funding,
        "net_pnl_usdt": net,
        "exit_reason": exit_reason,
        "entry_time_utc": entry_time,
        "exit_time_utc": exit_time,
        "reconciled_at_utc": _utc_now(),
        "evidence_source_hash": _event_evidence_hash(events),
    }


def verify_terminal_execution_evidence(
    conn: sqlite3.Connection,
    row: Mapping[str, Any],
) -> None:
    """Recompute one filled terminal row from its immutable native events."""

    row = dict(row)
    status = str(row.get("status", "")).strip().upper()
    filled = int(row.get("filled_contracts", 0))
    if status not in {"CLOSED", "FAILED_FLAT", "HALTED_MANUAL"} or filled <= 0:
        raise LedgerError("terminal evidence verification requires a filled terminal row")
    intent_id = int(row["id"])
    persisted = conn.execute(
        "SELECT * FROM execution_events WHERE intent_id = ? "
        "ORDER BY transact_time_utc, exec_id",
        (intent_id,),
    ).fetchall()
    normalized = [_normalize_execution_event(dict(event)) for event in persisted]
    if not normalized:
        raise LedgerError("filled terminal row has no execution events")
    expected = _derive_close_updates(row, normalized)

    for field in (
        "actual_exit_price_usdt",
        "gross_pnl_usdt",
        "fees_usdt",
        "funding_usdt",
        "net_pnl_usdt",
    ):
        actual = _finite_number(row.get(field), field)
        calculated = _finite_number(expected[field], field)
        tolerance = max(1e-8, abs(calculated) * 1e-9)
        if abs(actual - calculated) > tolerance:
            raise LedgerError(f"terminal {field} disagrees with execution events")
    for field in ("exit_reason", "entry_time_utc", "exit_time_utc"):
        if str(row.get(field)) != str(expected[field]):
            raise LedgerError(f"terminal {field} disagrees with execution events")
    actual_hash = str(row.get("evidence_source_hash", "")).strip().lower()
    if not re.fullmatch(r"[0-9a-f]{64}", actual_hash):
        raise LedgerError("terminal evidence_source_hash must be SHA-256")
    if actual_hash != expected["evidence_source_hash"]:
        raise LedgerError("terminal evidence hash disagrees with execution events")

    reconciled_at = _utc_timestamp(row.get("reconciled_at_utc"), "reconciled_at_utc")
    if datetime.fromisoformat(reconciled_at) < datetime.fromisoformat(expected["exit_time_utc"]):
        raise LedgerError("terminal reconciliation precedes the final exit execution")


def close_intent_from_events(
    decision_key: str,
    *,
    expected_statuses: Iterable[str],
    events: Iterable[Mapping[str, Any]],
    final_status: str = "CLOSED",
    halt_reason: str | None = None,
    db_path: str = DB_PATH,
) -> dict[str, Any]:
    """Append immutable exchange events and finalize accounting atomically.

    Replaying the exact same event set after a lost response is idempotent.
    Reusing an ``exec_id`` with changed source evidence or another intent is
    rejected. Emergency accounting may finalize as ``HALTED_MANUAL`` so the
    durable review gate is never cleared by successful flat-position accounting.
    """

    expected = {str(value).strip().upper() for value in expected_statuses}
    if not expected or not expected <= ACTIVE_STATUSES:
        raise LedgerError("close reconciliation requires active expected statuses")
    final_status = str(final_status).strip().upper()
    if final_status not in {"CLOSED", "HALTED_MANUAL"}:
        raise LedgerError("reconciled accounting must close or remain manually halted")
    if final_status == "HALTED_MANUAL":
        if halt_reason is None or not str(halt_reason).strip():
            raise LedgerError("manual-halt accounting requires a halt reason")
    elif halt_reason is not None:
        raise LedgerError("closed accounting cannot include a manual-halt reason")
    normalized = [_normalize_execution_event(event) for event in events]
    if not normalized:
        raise LedgerError("close reconciliation requires execution events")
    exec_ids = [event["exec_id"] for event in normalized]
    if len(exec_ids) != len(set(exec_ids)):
        raise LedgerError("close reconciliation contains duplicate exec_id values")

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
            intent_id = int(row["id"])

            if current == final_status:
                existing = conn.execute(
                    "SELECT exec_id, source_hash, event_role FROM execution_events "
                    "WHERE intent_id = ? ORDER BY exec_id",
                    (intent_id,),
                ).fetchall()
                supplied = sorted(
                    (event["exec_id"], event["source_hash"], event["event_role"])
                    for event in normalized
                )
                persisted = sorted(
                    (item["exec_id"], item["source_hash"], item["event_role"])
                    for item in existing
                )
                if supplied != persisted:
                    raise LedgerError(
                        "reconciled intent was replayed with different execution evidence"
                    )
                verify_terminal_execution_evidence(conn, row)
                conn.commit()
                return dict(row)
            if current not in expected:
                raise LedgerError(
                    f"expected status in {sorted(expected)}, found {current}"
                )

            created_at = _utc_now()
            for event in normalized:
                prior = conn.execute(
                    "SELECT intent_id, source_hash, event_role FROM execution_events "
                    "WHERE exec_id = ?",
                    (event["exec_id"],),
                ).fetchone()
                if prior is not None:
                    if (
                        int(prior["intent_id"]) != intent_id
                        or prior["source_hash"] != event["source_hash"]
                        or prior["event_role"] != event["event_role"]
                    ):
                        raise LedgerError(
                            "exec_id already exists with different execution evidence"
                        )
                    continue
                columns = ("intent_id", *_EVENT_FIELDS, "created_at_utc")
                values = (
                    intent_id,
                    *(event[field] for field in _EVENT_FIELDS),
                    created_at,
                )
                conn.execute(
                    f"INSERT INTO execution_events ({', '.join(columns)}) "
                    f"VALUES ({', '.join('?' for _ in columns)})",
                    values,
                )

            stored_events = [
                dict(item)
                for item in conn.execute(
                    "SELECT * FROM execution_events WHERE intent_id = ? "
                    "ORDER BY transact_time_utc, exec_id",
                    (intent_id,),
                ).fetchall()
            ]
            updates = _derive_close_updates(row, stored_events)
            if final_status == "HALTED_MANUAL":
                updates["halt_reason"] = str(halt_reason).strip()
            changes = _normalize_updates(row, updates)
            merged = _merged(row, changes)
            _require_state_evidence("CLOSED", merged)
            if final_status == "HALTED_MANUAL":
                _require_state_evidence("HALTED_MANUAL", merged)
            assignments = ["status = ?", "updated_at_utc = ?"]
            params: list[Any] = [final_status, _utc_now()]
            for field, value in changes.items():
                assignments.append(f"{field} = ?")
                params.append(value)
            params.extend([decision_key, current])
            cursor = conn.execute(
                f"UPDATE execution_intents SET {', '.join(assignments)} "
                "WHERE decision_key = ? AND status = ?",
                params,
            )
            if cursor.rowcount != 1:
                raise LedgerError("close reconciliation lost a concurrent update race")
            closed = conn.execute(
                "SELECT * FROM execution_intents WHERE decision_key = ?",
                (decision_key,),
            ).fetchone()
            conn.commit()
            if closed is None:
                raise LedgerError("closed intent cannot be read")
            return dict(closed)
    except sqlite3.Error as exc:
        raise LedgerError(f"could not close intent from execution events: {exc}") from exc


def list_execution_events(
    decision_key: str, db_path: str = DB_PATH
) -> list[dict[str, Any]]:
    """Return immutable normalized events for one durable intent."""

    try:
        with closing(_connect_existing(db_path)) as conn:
            rows = conn.execute(
                "SELECT event.* FROM execution_events AS event "
                "JOIN execution_intents AS intent ON intent.id = event.intent_id "
                "WHERE intent.decision_key = ? "
                "ORDER BY event.transact_time_utc, event.exec_id",
                (decision_key,),
            ).fetchall()
            return [dict(row) for row in rows]
    except sqlite3.Error as exc:
        raise LedgerError(f"could not list execution events: {exc}") from exc


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
    "SCHEMA_VERSION",
    "UNIT_MODEL",
    "UNRESOLVED_STATUSES",
    "close_intent_from_events",
    "get_intent",
    "get_intent_by_client_order_id",
    "get_intent_by_external_order_id",
    "initialize_ledger",
    "list_active_intents",
    "list_execution_events",
    "list_open_intents",
    "record_execution_evidence",
    "register_intent",
    "transition_intent",
    "validate_execution_ledger_schema",
    "verify_terminal_execution_evidence",
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
