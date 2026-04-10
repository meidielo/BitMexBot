"""
forward_tracker.py — Forward Performance Tracking

When V4 fires live, captures structured performance data for comparing
live execution against backtest assumptions:
  - Entry price vs signal price (slippage)
  - Actual liquidation volume vs threshold
  - OI drop magnitude
  - Time from signal to execution

Usage:
  from forward_tracker import get_tracker, record_signal, record_execution, record_exit
"""

import os
import sqlite3
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_PATH = os.path.join("data", "forward_tracking.db")

_conn = None


def get_tracker() -> sqlite3.Connection:
    """Get or create the forward tracker connection."""
    global _conn
    if _conn is not None:
        return _conn

    os.makedirs("data", exist_ok=True)
    _conn = sqlite3.connect(DB_PATH, timeout=10)
    _conn.execute("PRAGMA journal_mode=WAL")
    _conn.execute("PRAGMA busy_timeout=5000")

    _conn.execute("""
        CREATE TABLE IF NOT EXISTS forward_trades (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id                TEXT,
            signal_timestamp        TEXT,
            execution_timestamp     TEXT,
            signal_price            REAL,
            entry_price             REAL,
            slippage_pct            REAL,
            liq_volume_actual       REAL,
            liq_volume_threshold    REAL,
            liq_ratio_actual        REAL,
            oi_drop_actual          REAL,
            time_signal_to_exec_sec INTEGER,
            exit_price              REAL,
            exit_reason             TEXT,
            pnl_usd                 REAL
        )
    """)

    _conn.commit()
    return _conn


def record_signal(conn: sqlite3.Connection, trade_id: str,
                  signal_price: float, trigger_data: dict) -> int:
    """Record the signal event. Returns row id."""
    now = datetime.now(timezone.utc).isoformat()
    cursor = conn.execute(
        "INSERT INTO forward_trades "
        "(trade_id, signal_timestamp, signal_price, "
        " liq_volume_actual, liq_ratio_actual, oi_drop_actual) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (trade_id, now, signal_price,
         trigger_data.get("liq_long_pct", 0),
         trigger_data.get("liq_long_ratio", 0),
         trigger_data.get("oi_delta_pct", 0))
    )
    conn.commit()
    return cursor.lastrowid


def record_execution(conn: sqlite3.Connection, trade_id: str,
                     entry_price: float, signal_price: float):
    """Record execution details after fill."""
    now = datetime.now(timezone.utc).isoformat()
    slippage_pct = ((entry_price - signal_price) / signal_price * 100
                    if signal_price > 0 else 0)

    # Calculate time from signal to execution
    row = conn.execute(
        "SELECT signal_timestamp FROM forward_trades WHERE trade_id = ? "
        "ORDER BY id DESC LIMIT 1", (trade_id,)
    ).fetchone()

    time_to_exec = 0
    if row and row[0]:
        signal_dt = datetime.fromisoformat(row[0])
        exec_dt = datetime.fromisoformat(now)
        time_to_exec = int((exec_dt - signal_dt).total_seconds())

    conn.execute(
        "UPDATE forward_trades SET "
        "execution_timestamp = ?, entry_price = ?, slippage_pct = ?, "
        "time_signal_to_exec_sec = ? "
        "WHERE trade_id = ? AND exit_price IS NULL",
        (now, entry_price, round(slippage_pct, 4), time_to_exec, trade_id)
    )
    conn.commit()


def record_exit(conn: sqlite3.Connection, trade_id: str,
                exit_price: float, exit_reason: str, pnl_usd: float = None):
    """Record exit details."""
    conn.execute(
        "UPDATE forward_trades SET "
        "exit_price = ?, exit_reason = ?, pnl_usd = ? "
        "WHERE trade_id = ? AND exit_price IS NULL",
        (exit_price, exit_reason, pnl_usd, trade_id)
    )
    conn.commit()
