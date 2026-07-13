"""Atomic daily-loss state derived only from the versioned execution ledger."""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from trade_ledger import (
    ACTIVE_STATUSES,
    ALL_STATUSES,
    DB_PATH,
    UNIT_MODEL,
    LedgerError,
    validate_execution_ledger_schema,
    verify_terminal_execution_evidence,
)


DAILY_LOSS_PATH = os.path.join("data", "daily_loss.json")


class DailyLossStateError(RuntimeError):
    """Raised when trustworthy daily-loss state cannot be produced."""


def _read_only_connection(path: str) -> sqlite3.Connection:
    if not os.path.isfile(path):
        raise DailyLossStateError("versioned execution ledger does not exist")
    uri = Path(path).resolve().as_posix()
    connection = sqlite3.connect(f"file:{uri}?mode=ro", uri=True, timeout=5)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA query_only=ON")
    return connection


def refresh_daily_loss_from_ledger(
    *,
    db_path: str = DB_PATH,
    output_path: str = DAILY_LOSS_PATH,
    now: datetime | None = None,
) -> dict[str, float | str]:
    """Rebuild today's gross realised loss and atomically publish it.

    Any active or manual-halt intent blocks publication. A zero-fill
    ``FAILED_FLAT`` row is harmless, while filled terminal rows must have
    complete finite net PnL evidence.
    """

    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    today = current.strftime("%Y-%m-%d")
    try:
        with closing(_read_only_connection(db_path)) as connection:
            integrity = connection.execute("PRAGMA integrity_check").fetchone()
            if not integrity or str(integrity[0]).lower() != "ok":
                raise DailyLossStateError("versioned ledger integrity check failed")
            validate_execution_ledger_schema(connection)
            placeholders = ", ".join("?" for _ in ACTIVE_STATUSES)
            unresolved = connection.execute(
                "SELECT COUNT(*) FROM execution_intents "
                f"WHERE status IN ({placeholders}) OR status = 'HALTED_MANUAL'",
                tuple(sorted(ACTIVE_STATUSES)),
            ).fetchone()[0]
            if int(unresolved) != 0:
                raise DailyLossStateError(
                    "daily loss cannot be published while execution intents are unresolved"
                )
            status_placeholders = ", ".join("?" for _ in ALL_STATUSES)
            invalid = connection.execute(
                "SELECT COUNT(*) FROM execution_intents "
                f"WHERE status NOT IN ({status_placeholders}) "
                "OR environment NOT IN ('testnet', 'dry_run') "
                "OR unit_model != ? OR requested_contracts <= 0 "
                "OR filled_contracts < 0 OR filled_contracts > requested_contracts "
                "OR closed_contracts < 0 OR closed_contracts > filled_contracts",
                (*tuple(sorted(ALL_STATUSES)), UNIT_MODEL),
            ).fetchone()[0]
            if int(invalid) != 0:
                raise DailyLossStateError(
                    "daily loss cannot be published from invalid ledger rows"
                )
            rows = connection.execute(
                "SELECT * "
                "FROM execution_intents WHERE status IN ('CLOSED', 'FAILED_FLAT')"
            ).fetchall()
            for row in rows:
                if int(row["filled_contracts"]) > 0:
                    verify_terminal_execution_evidence(connection, row)
            rows = [dict(row) for row in rows]
    except (LedgerError, sqlite3.Error) as exc:
        raise DailyLossStateError(f"could not read versioned ledger: {exc}") from exc

    gross_loss = 0.0
    for row in rows:
        filled = int(row["filled_contracts"])
        if filled == 0:
            continue
        if row["net_pnl_usdt"] is None:
            raise DailyLossStateError("filled terminal row is missing net PnL")
        try:
            net_pnl = float(row["net_pnl_usdt"])
            exited = datetime.fromisoformat(str(row["exit_time_utc"]))
        except (TypeError, ValueError) as exc:
            raise DailyLossStateError(
                "terminal ledger row has invalid PnL or timestamp evidence"
            ) from exc
        if not exited.tzinfo:
            raise DailyLossStateError("terminal ledger exit timestamp is not timezone-aware")
        if not (float("-inf") < net_pnl < float("inf")):
            raise DailyLossStateError("terminal ledger PnL is non-finite")
        if exited.astimezone(timezone.utc).strftime("%Y-%m-%d") == today and net_pnl < 0:
            gross_loss += abs(net_pnl)

    payload: dict[str, float | str] = {
        "date": today,
        "loss_usd": round(gross_loss, 8),
        "source": "trades_v2.db",
    }
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f"{destination.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    except OSError as exc:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:  # agent-quality: allow: cleanup failure cannot replace the original publish error
            pass
        raise DailyLossStateError(f"could not publish daily loss state: {exc}") from exc
    return payload


__all__ = [
    "DAILY_LOSS_PATH",
    "DailyLossStateError",
    "refresh_daily_loss_from_ledger",
]
