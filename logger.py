"""Read-only compatibility helpers for the quarantined legacy trade database.

``data/trades.db`` contains mixed quantity units and is not valid execution,
PnL, risk, or promotion evidence. New execution state is stored only by
``trade_ledger.py`` in ``data/trades_v2.db``.
"""

import os
import sqlite3


DB_DIR = "data"
DB_PATH = os.path.join(DB_DIR, "trades.db")
DAILY_LOSS_FILE = os.path.join(DB_DIR, "daily_loss.json")

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS trades (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id          TEXT,
    timestamp         TEXT    NOT NULL,
    signal            TEXT    NOT NULL,
    entry_price       REAL    NOT NULL,
    sl_price          REAL    NOT NULL,
    tp_price          REAL    NOT NULL,
    position_size_btc REAL    NOT NULL,
    leverage          INTEGER NOT NULL,
    approved_by_risk  INTEGER NOT NULL,
    order_status      TEXT,
    exit_price        REAL,
    pnl_usd           REAL,
    exit_reason       TEXT,
    duration_seconds  INTEGER
);
"""


def _connect() -> sqlite3.Connection:
    """Open the legacy database for compatibility reporting only."""

    os.makedirs(DB_DIR, exist_ok=True)
    connection = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=5000")
    connection.row_factory = sqlite3.Row
    return connection


def _init_db() -> None:
    """Ensure old monitoring tools can read an empty legacy schema."""

    with _connect() as connection:
        connection.execute(_CREATE_TABLE)
        connection.commit()


def log_trade(trade_dict: dict) -> int | None:
    """Reject writes to the ambiguous-unit legacy database."""

    del trade_dict
    raise RuntimeError(
        "legacy data/trades.db is read-only because its quantity units are "
        "ambiguous; use trade_ledger.register_intent"
    )


def compute_pnl_usdt(
    signal: str,
    entry_price: float,
    exit_price: float,
    contracts: float,
    contract_size_btc: float = 0.000001,
) -> float:
    """Compute gross PnL for a verified linear XBTUSDT contract model."""

    if signal not in {"LONG", "SHORT"}:
        raise ValueError("signal must be LONG or SHORT")
    if contracts < 0 or contract_size_btc <= 0:
        raise ValueError("contracts must be non-negative and contract size positive")
    if signal == "LONG":
        return contracts * contract_size_btc * (exit_price - entry_price)
    return contracts * contract_size_btc * (entry_price - exit_price)


def update_trade_exit(order_id: str, exit_price: float, exit_reason: str) -> bool:
    """Reject exit writes to the ambiguous-unit legacy database."""

    del order_id, exit_price, exit_reason
    raise RuntimeError(
        "legacy data/trades.db is read-only; reconcile exits in trades_v2.db"
    )


if __name__ == "__main__":
    print("Legacy data/trades.db is quarantined and read-only.")


__all__ = [
    "DAILY_LOSS_FILE",
    "DB_PATH",
    "_connect",
    "_init_db",
    "compute_pnl_usdt",
    "log_trade",
    "update_trade_exit",
]
