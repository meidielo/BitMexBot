"""
monitor.py -- Phase 6

Reads data/trades.db and prints a daily summary to the terminal.
No exchange connection.  Pure read from local storage.
"""

import os
import sqlite3
from datetime import datetime, timezone

from logger import DB_PATH, _connect, _init_db

# Single source of truth for the daily loss limit lives in risk.py
try:
    from risk import MAX_DAILY_LOSS_USD
except ImportError:
    MAX_DAILY_LOSS_USD = 50.0


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def _query(sql: str, params: tuple = ()) -> list:
    """Run a read query and return a list of sqlite3.Row objects."""
    _init_db()
    try:
        with _connect() as conn:
            return conn.execute(sql, params).fetchall()
    except Exception as e:
        print(f"[WARN] monitor query failed: {e}")
        return []


def _scalar(sql: str, params: tuple = (), default=None):
    """Return a single value from a query, or default on failure/empty."""
    rows = _query(sql, params)
    if rows and rows[0][0] is not None:
        return rows[0][0]
    return default


# ---------------------------------------------------------------------------
# Section printers
# ---------------------------------------------------------------------------

def _section(title: str, width: int = 60) -> None:
    print(f"\n{'-' * width}")
    print(f"  {title}")
    print(f"{'-' * width}")


def _print_todays_trades(today: str) -> None:
    _section("TODAY'S TRADES  " + today)

    rows = _query(
        """
        SELECT order_id, signal, entry_price, exit_price,
               pnl_usd, exit_reason, order_status
          FROM trades
         WHERE date(timestamp) = ?
         ORDER BY id ASC
        """,
        (today,),
    )

    if not rows:
        print("  No trades today.")
        return

    closed  = [r for r in rows if r["exit_price"] is not None]
    open_   = [r for r in rows if r["exit_price"] is None]
    wins    = [r for r in closed if (r["pnl_usd"] or 0) > 0]
    losses  = [r for r in closed if (r["pnl_usd"] or 0) < 0]
    pnl_sum = sum((r["pnl_usd"] or 0) for r in closed)

    print(f"  Total attempts  : {len(rows)}")
    print(f"  Closed trades   : {len(closed)}  "
          f"(W: {len(wins)}  L: {len(losses)})")
    print(f"  Open (unfilled) : {len(open_)}")
    print(f"  Realised PnL    : ${pnl_sum:+.4f}")

    if closed:
        print()
        header = (f"  {'Order ID':<14} {'Dir':<6} {'Entry':>9} "
                  f"{'Exit':>9} {'PnL':>10} {'Reason':<8}")
        print(header)
        print("  " + "-" * (len(header) - 2))
        for r in closed:
            pnl_str = f"${r['pnl_usd']:+.4f}" if r["pnl_usd"] is not None else "    --"
            exit_str = f"{r['exit_price']:.2f}" if r["exit_price"] else "   open"
            oid = (r["order_id"] or "--")[:13]
            print(
                f"  {oid:<14} {r['signal']:<6} {r['entry_price']:>9.2f} "
                f"{exit_str:>9} {pnl_str:>10} {(r['exit_reason'] or '--'):<8}"
            )


def _print_running_pnl() -> None:
    _section("RUNNING TOTAL PnL  (all time)")

    total = _scalar("SELECT COALESCE(SUM(pnl_usd),0) FROM trades "
                    "WHERE exit_price IS NOT NULL", default=0.0)
    count = _scalar("SELECT COUNT(*) FROM trades WHERE exit_price IS NOT NULL",
                    default=0)
    wins  = _scalar("SELECT COUNT(*) FROM trades "
                    "WHERE exit_price IS NOT NULL AND pnl_usd > 0", default=0)

    win_rate = (wins / count * 100) if count else 0.0

    print(f"  Closed trades   : {count}")
    print(f"  Win rate        : {win_rate:.1f}%  ({wins}W / {count - wins}L)")
    print(f"  Total PnL       : ${total:+.4f}")


def _print_daily_loss_status(today: str) -> None:
    _section("DAILY LOSS LIMIT")

    daily_loss = _scalar(
        """
        SELECT COALESCE(SUM(ABS(pnl_usd)), 0.0)
          FROM trades
         WHERE date(timestamp) = ?
           AND exit_price IS NOT NULL
           AND pnl_usd < 0
        """,
        (today,),
        default=0.0,
    )
    remaining  = MAX_DAILY_LOSS_USD - daily_loss
    pct_used   = daily_loss / MAX_DAILY_LOSS_USD * 100

    bar_filled = int(pct_used / 5)   # 20-char bar
    bar = "#" * bar_filled + "." * (20 - bar_filled)

    print(f"  Limit           : ${MAX_DAILY_LOSS_USD:.2f}")
    print(f"  Used today      : ${daily_loss:.4f}  ({pct_used:.1f}%)")
    print(f"  Remaining       : ${remaining:.4f}")
    print(f"  [{bar}]")

    if daily_loss >= MAX_DAILY_LOSS_USD:
        print("  *** LIMIT HIT -- bot is halted for today ***")


def _print_open_positions() -> None:
    _section("OPEN POSITIONS  (unfilled / no exit logged)")

    rows = _query(
        """
        SELECT order_id, timestamp, signal, entry_price,
               sl_price, tp_price, position_size_btc
          FROM trades
         WHERE exit_price IS NULL
           AND order_status = 'placed'
         ORDER BY id DESC
        """,
    )

    if not rows:
        print("  No open positions.")
        return

    for r in rows:
        oid = (r["order_id"] or "--")
        print(
            f"  {oid}  |  {r['signal']}  |  "
            f"entry: {r['entry_price']:.2f}  "
            f"SL: {r['sl_price']:.2f}  "
            f"TP: {r['tp_price']:.2f}  "
            f"size: {r['position_size_btc']:.8f} BTC  "
            f"opened: {r['timestamp']}"
        )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def print_summary() -> None:
    """Print the full daily summary block to terminal."""
    now   = datetime.now(timezone.utc)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    print()
    print("=" * 60)
    print(f"  BitMEX Bot -- Daily Summary   {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    _print_todays_trades(today)
    _print_running_pnl()
    _print_daily_loss_status(today)
    _print_open_positions()

    print(f"\n{'-' * 60}\n")


if __name__ == "__main__":
    print_summary()
