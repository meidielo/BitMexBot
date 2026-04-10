"""
logger.py — Phase 6

Writes every trade attempt to data/trades.db (SQLite) and keeps
data/daily_loss.json in sync so risk.py can read the daily loss limit.

No exchange connection.  Pure read/write to local storage.
"""

import json
import os
import sqlite3
from datetime import datetime, timezone

DB_DIR          = "data"
DB_PATH         = os.path.join(DB_DIR, "trades.db")
DAILY_LOSS_FILE = os.path.join(DB_DIR, "daily_loss.json")

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _connect() -> sqlite3.Connection:
    """Return a connection with WAL mode + busy_timeout for concurrent access."""
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    return conn


def _init_db() -> None:
    """Create the trades table if it does not already exist."""
    try:
        with _connect() as conn:
            conn.execute(_CREATE_TABLE)
            conn.commit()
    except Exception as e:
        print(f"[ERROR] logger._init_db failed: {e}")


def _update_daily_loss_file() -> None:
    """
    Recalculate today's total realised loss from the DB and write it to
    data/daily_loss.json so risk.py can read it on the next loop iteration.

    Only closed trades (exit_price IS NOT NULL) with a negative PnL count.
    Uses GROSS losses (sum of all losing trades), not net PnL.
    Losses are stored as positive numbers in the file.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        with _connect() as conn:
            row = conn.execute(
                """
                SELECT COALESCE(SUM(ABS(pnl_usd)), 0.0) AS gross_loss
                FROM trades
                WHERE date(timestamp) = ?
                  AND exit_price IS NOT NULL
                  AND pnl_usd < 0
                """,
                (today,),
            ).fetchone()

        daily_loss = float(row["gross_loss"])

        os.makedirs(DB_DIR, exist_ok=True)
        with open(DAILY_LOSS_FILE, "w") as f:
            json.dump({"date": today, "loss_usd": round(daily_loss, 4)}, f, indent=2)

    except Exception as e:
        print(f"[WARN] Could not update daily loss file: {e}")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def log_trade(trade_dict: dict) -> int | None:
    """
    Insert a new trade row.  Call this immediately after an order attempt.

    Expected keys in trade_dict
    ---------------------------
    order_id          str   — ccxt order ID (may be None if order failed before placement)
    signal            str   — 'LONG' or 'SHORT'
    entry_price       float
    sl_price          float
    tp_price          float
    position_size_btc float
    leverage          int
    approved_by_risk  bool
    order_status      str   — 'placed' | 'failed'

    Returns the new row id on success, or None on failure.
    """
    _init_db()

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    try:
        with _connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO trades
                    (order_id, timestamp, signal, entry_price, sl_price, tp_price,
                     position_size_btc, leverage, approved_by_risk, order_status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade_dict.get("order_id"),
                    ts,
                    trade_dict["signal"],
                    float(trade_dict["entry_price"]),
                    float(trade_dict["sl_price"]),
                    float(trade_dict["tp_price"]),
                    float(trade_dict["position_size_btc"]),
                    int(trade_dict["leverage"]),
                    int(bool(trade_dict.get("approved_by_risk", True))),
                    trade_dict.get("order_status", "placed"),
                ),
            )
            conn.commit()
            row_id = cursor.lastrowid

        print(f"[LOG] Trade logged — DB row: {row_id}  order_id: {trade_dict.get('order_id')}")
        return row_id

    except Exception as e:
        print(f"[ERROR] log_trade failed: {e}")
        return None


def update_trade_exit(order_id: str, exit_price: float,
                      exit_reason: str) -> bool:
    """
    Update an existing row with exit data once a trade is closed.

    Parameters
    ----------
    order_id    : ccxt order ID used when the trade was logged
    exit_price  : price at which the position was closed
    exit_reason : 'TP' | 'SL' | 'MANUAL'

    PnL formula (linear approximation — accurate within ~0.5 % for small moves)
    ----------
    LONG  : pnl_usd = (exit_price - entry_price) * position_size_btc
    SHORT : pnl_usd = (entry_price - exit_price) * position_size_btc

    Returns True on success, False on failure.
    """
    _init_db()

    try:
        with _connect() as conn:
            row = conn.execute(
                "SELECT * FROM trades WHERE order_id = ?",
                (order_id,),
            ).fetchone()

            if row is None:
                print(f"[WARN] update_trade_exit: no row found for order_id '{order_id}'")
                return False

            entry_price       = float(row["entry_price"])
            position_size_btc = float(row["position_size_btc"])
            signal            = row["signal"]
            opened_at         = datetime.fromisoformat(row["timestamp"].replace(" ", "T"))
            opened_at         = opened_at.replace(tzinfo=timezone.utc)

            # PnL
            if signal == "LONG":
                pnl_usd = (exit_price - entry_price) * position_size_btc
            else:  # SHORT
                pnl_usd = (entry_price - exit_price) * position_size_btc

            duration_seconds = int(
                (datetime.now(timezone.utc) - opened_at).total_seconds()
            )

            conn.execute(
                """
                UPDATE trades
                   SET exit_price       = ?,
                       pnl_usd          = ?,
                       exit_reason      = ?,
                       duration_seconds = ?
                 WHERE order_id = ?
                """,
                (
                    float(exit_price),
                    round(pnl_usd, 4),
                    exit_reason.upper(),
                    duration_seconds,
                    order_id,
                ),
            )
            conn.commit()

        print(
            f"[LOG] Trade exit updated — order_id: {order_id}  "
            f"exit: {exit_price:.2f}  PnL: ${pnl_usd:+.4f}  reason: {exit_reason}"
        )

        # Keep daily_loss.json in sync for risk.py
        _update_daily_loss_file()
        return True

    except Exception as e:
        print(f"[ERROR] update_trade_exit failed: {e}")
        return False


# ---------------------------------------------------------------------------
# CLI — quick sanity check without touching the exchange
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("logger.py — inserting and updating a test trade\n")

    test_trade = {
        "order_id":          "TEST-001",
        "signal":            "SHORT",
        "entry_price":       69280.60,
        "sl_price":          70666.21,
        "tp_price":          67202.18,
        "position_size_btc": 0.00028868,
        "leverage":          15,
        "approved_by_risk":  True,
        "order_status":      "placed",
    }

    row_id = log_trade(test_trade)
    print(f"Inserted row id: {row_id}")

    ok = update_trade_exit("TEST-001", exit_price=67202.18, exit_reason="TP")
    print(f"Exit update success: {ok}")

    # Verify by reading back
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM trades WHERE order_id = 'TEST-001'"
        ).fetchone()
    if row:
        print("\nStored row:")
        for key in row.keys():
            print(f"  {key:<22}: {row[key]}")
