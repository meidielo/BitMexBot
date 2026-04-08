"""
v4_execution.py — V4: Trend-Following Liquidation Dip-Buy (Live Execution)

Standalone service. Runs separately from main.py.
Monitors for liquidation cascade dip-buy opportunities in bull regimes.

Hybrid timeframe architecture:
  - Macro context (EMA200, funding, ATR) updates daily at 00:15 UTC
  - Micro trigger (liquidation spike + OI drop) checks every 15 minutes
  - Execution fires immediately on trigger — no waiting for daily close

State machine: IDLE → ARMED → POSITION_OPEN → COOLDOWN → IDLE
State persists to SQLite (data/v4_state.db) for crash recovery.

Run: python v4_execution.py
Systemd: sudo systemctl start bitmexv4.service
"""

import logging
import os
import sqlite3
import sys
import time
import traceback
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from bitmex_client import get_client, get_data_client
from fetch_data import fetch_recent_funding
from order_manager import execute_signal, round_to_tick, SYMBOL
from risk import validate_signal
from logger import log_trade, update_trade_exit

load_dotenv()

# ---------------------------------------------------------------------------
# Constants — V4 signal parameters (frozen, matching backtest_v3.py)
# ---------------------------------------------------------------------------
EMA_PERIOD         = 200
FUNDING_THRESHOLD  = 0.0003     # 0.03% per 8h settlement
FUNDING_LOOKBACK_D = 3          # 3 days = 9 settlements
LIQ_SPIKE_MULT     = 3.0        # 3× rolling average
LIQ_LONG_DOM       = 0.60       # 60% long-dominant
LIQ_LOOKBACK_DAYS  = 20         # 20-day rolling baseline
ATR_PERIOD         = 14
SL_ATR_MULT        = 2.0
TARGET_RR          = 2.0
COOLDOWN_HOURS     = 24

# Loop timing
LOOP_SECONDS       = 15 * 60    # 15 min
LOOP_OFFSET_SEC    = 30         # wake 30s after quarter-hour
MACRO_UPDATE_HOUR  = 0          # 00:xx UTC
MACRO_UPDATE_MIN   = 15         # 00:15 UTC

# Paths
STATE_DB_PATH      = os.path.join("data", "v4_state.db")
COINALYZE_DB_PATH  = os.path.join("data", "coinalyze.db")
HEARTBEAT_PATH     = os.path.join("data", "v4_heartbeat.txt")
COLLECTOR_HB_PATH  = os.path.join("data", "coinalyze_heartbeat.txt")

# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------
class State:
    IDLE          = "IDLE"
    ARMED         = "ARMED"
    POSITION_OPEN = "POSITION_OPEN"
    COOLDOWN      = "COOLDOWN"


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _setup_logging() -> logging.Logger:
    """Configure structured logging to stdout (captured by systemd)."""
    logger = logging.getLogger("v4")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            "[%(asctime)s UTC] [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"))
        handler.formatter.converter = time.gmtime
        logger.addHandler(handler)
    return logger

log = _setup_logging()


def _log_to_db(conn: sqlite3.Connection, level: str, message: str):
    """Append to v4_log audit table."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("INSERT INTO v4_log (timestamp, level, message) VALUES (?, ?, ?)",
                 (now, level, message[:1000]))
    conn.commit()


# ---------------------------------------------------------------------------
# State DB persistence
# ---------------------------------------------------------------------------

def _init_state_db() -> sqlite3.Connection:
    """Initialize v4_state.db with schema and singleton row."""
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(STATE_DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row

    conn.execute("""
        CREATE TABLE IF NOT EXISTS v4_state (
            id              INTEGER PRIMARY KEY CHECK (id = 1),
            state           TEXT    NOT NULL DEFAULT 'IDLE',
            updated_at      TEXT    NOT NULL,

            macro_valid     INTEGER NOT NULL DEFAULT 0,
            macro_updated   TEXT,
            ema200          REAL,
            last_close      REAL,
            funding_peak    REAL,
            daily_atr       REAL,

            trigger_ts      TEXT,
            liq_long_ratio  REAL,
            liq_long_pct    REAL,
            oi_delta_pct    REAL,

            order_id        TEXT,
            entry_price     REAL,
            sl_price        REAL,
            tp_price        REAL,
            sl_order_id     TEXT,
            tp_order_id     TEXT,
            position_size   INTEGER,

            cooldown_until  TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS v4_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT    NOT NULL,
            level       TEXT    NOT NULL,
            message     TEXT    NOT NULL
        )
    """)

    # Ensure singleton row exists
    row = conn.execute("SELECT COUNT(*) FROM v4_state").fetchone()[0]
    if row == 0:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO v4_state (id, state, updated_at) VALUES (1, 'IDLE', ?)",
            (now,))
        conn.commit()

    return conn


def _load_state(conn: sqlite3.Connection) -> dict:
    """Load the singleton state row as a dict."""
    row = conn.execute("SELECT * FROM v4_state WHERE id = 1").fetchone()
    return dict(row)


def _save_state(conn: sqlite3.Connection, **kwargs):
    """Update state fields. Always sets updated_at."""
    kwargs["updated_at"] = datetime.now(timezone.utc).isoformat()
    cols = ", ".join(f"{k} = ?" for k in kwargs)
    vals = list(kwargs.values())
    conn.execute(f"UPDATE v4_state SET {cols} WHERE id = 1", vals)
    conn.commit()


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

def _write_heartbeat(state_name: str, loop_count: int):
    """Write heartbeat file for external monitoring."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    with open(HEARTBEAT_PATH, "w") as f:
        f.write(f"{now}\nstate={state_name}\nloop={loop_count}\n")


def _check_collector_heartbeat() -> bool:
    """Check Coinalyze collector freshness. Returns True if healthy."""
    if not os.path.exists(COLLECTOR_HB_PATH):
        log.warning("Collector heartbeat file missing: %s", COLLECTOR_HB_PATH)
        return False

    age_sec = time.time() - os.path.getmtime(COLLECTOR_HB_PATH)
    if age_sec > 7200:  # 2 hours
        log.error("Collector stale: heartbeat %.0f min old (>120m)", age_sec / 60)
        return False
    if age_sec > 1800:  # 30 minutes
        log.warning("Collector possibly stale: heartbeat %.0f min old", age_sec / 60)
    return True


# ---------------------------------------------------------------------------
# Macro context (daily)
# ---------------------------------------------------------------------------

def _should_run_macro_update(state: dict) -> bool:
    """Check if macro context needs refreshing (once per day after 00:15 UTC)."""
    now = datetime.now(timezone.utc)

    # Only run after 00:15 UTC
    if now.hour == MACRO_UPDATE_HOUR and now.minute < MACRO_UPDATE_MIN:
        return False

    # Check if already updated today
    if state.get("macro_updated"):
        last = datetime.fromisoformat(state["macro_updated"])
        if last.date() == now.date():
            return False

    return True


def _update_macro_context(conn: sqlite3.Connection) -> dict:
    """
    Fetch daily candles + funding rates, compute EMA200 + ATR(14).
    Returns macro context dict.
    """
    log.info("Updating macro context...")
    exchange = get_data_client()

    # 1. Daily candles (250 bars for EMA200 warmup)
    try:
        raw = exchange.fetch_ohlcv(SYMBOL, "1d", limit=250)
    except Exception as e:
        log.error("Failed to fetch daily candles: %s", e)
        return {"macro_valid": False}

    if len(raw) < EMA_PERIOD + 5:
        log.warning("Only %d daily candles (need %d+)", len(raw), EMA_PERIOD)
        return {"macro_valid": False}

    df = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "volume"])

    # EMA200
    ema = df["close"].ewm(span=EMA_PERIOD, adjust=False).mean()
    ema200 = float(ema.iloc[-1])
    last_close = float(df["close"].iloc[-1])
    bull_regime = last_close > ema200

    # ATR(14)
    tr = np.maximum(
        df["high"] - df["low"],
        np.maximum(
            abs(df["high"] - df["close"].shift(1)),
            abs(df["low"] - df["close"].shift(1))
        )
    )
    atr = float(tr.rolling(ATR_PERIOD).mean().iloc[-1])

    # 2. Funding rates (last 10 settlements = ~3.3 days)
    try:
        df_fund = fetch_recent_funding(count=10)
        funding_peak = float(df_fund["rate"].max()) if df_fund is not None and not df_fund.empty else 0
    except Exception as e:
        log.error("Failed to fetch funding rates: %s", e)
        funding_peak = 0

    funding_valid = funding_peak > FUNDING_THRESHOLD

    macro_valid = bull_regime and funding_valid

    result = {
        "macro_valid": 1 if macro_valid else 0,
        "macro_updated": datetime.now(timezone.utc).isoformat(),
        "ema200": round(ema200, 2),
        "last_close": round(last_close, 2),
        "funding_peak": round(funding_peak, 8),
        "daily_atr": round(atr, 2),
    }

    _save_state(conn, **result)

    status = "VALID" if macro_valid else "INVALID"
    reason = []
    if not bull_regime:
        reason.append(f"bear regime: close={last_close:,.0f} < EMA200={ema200:,.0f}")
    if not funding_valid:
        reason.append(f"funding low: peak={funding_peak*100:.4f}% <= {FUNDING_THRESHOLD*100:.2f}%")

    log.info("Macro %s: close=%s EMA200=%s ATR=%s fund_peak=%s%% %s",
             status, f"{last_close:,.0f}", f"{ema200:,.0f}", f"{atr:,.0f}",
             f"{funding_peak*100:.4f}",
             f"({'; '.join(reason)})" if reason else "")

    _log_to_db(conn, "INFO", f"Macro update: {status} | "
               f"close={last_close:.0f} EMA200={ema200:.0f} ATR={atr:.0f} "
               f"fund_peak={funding_peak*100:.4f}%")

    return result


# ---------------------------------------------------------------------------
# Micro trigger (every 15 min)
# ---------------------------------------------------------------------------

def _check_micro_trigger(conn: sqlite3.Connection) -> dict | None:
    """
    Query Coinalyze 15m aggregated data for liquidation spike + OI drop.
    Returns trigger metadata dict if conditions met, else None.
    """
    try:
        cz = sqlite3.connect(COINALYZE_DB_PATH, timeout=5)
        cz.execute("PRAGMA busy_timeout=3000")
    except Exception as e:
        log.error("Cannot open Coinalyze DB: %s", e)
        return None

    try:
        # Latest 4 bars (1 hour) of liquidations
        rows = cz.execute("""
            SELECT timestamp, liq_long, liq_short
            FROM liquidations_15m_agg
            ORDER BY timestamp DESC
            LIMIT 4
        """).fetchall()

        if len(rows) < 4:
            log.debug("Only %d liq rows (need 4)", len(rows))
            return None

        hourly_long = sum(r[1] or 0 for r in rows)
        hourly_short = sum(r[2] or 0 for r in rows)
        hourly_total = hourly_long + hourly_short

        if hourly_total <= 0:
            return None

        long_pct = hourly_long / hourly_total

        # 20-day baseline: average hourly long liquidations
        max_ts = rows[0][0]
        since_ts = max_ts - (LIQ_LOOKBACK_DAYS * 86400)

        baseline_row = cz.execute("""
            SELECT SUM(liq_long), COUNT(*)
            FROM liquidations_15m_agg
            WHERE timestamp >= ?
        """, (since_ts,)).fetchone()

        total_long_20d = baseline_row[0] or 0
        n_bars_20d = baseline_row[1] or 1
        # Convert to hourly: each bar is 15min, so 4 bars = 1 hour
        hours_20d = n_bars_20d / 4
        avg_hourly_long = total_long_20d / hours_20d if hours_20d > 0 else 1

        if avg_hourly_long <= 0:
            return None

        ratio = hourly_long / avg_hourly_long

        # Check spike threshold
        if ratio < LIQ_SPIKE_MULT:
            return None

        if long_pct < LIQ_LONG_DOM:
            return None

        # OI confirmation: current vs 4 bars ago
        oi_rows = cz.execute("""
            SELECT timestamp, oi_close
            FROM oi_15m_agg
            ORDER BY timestamp DESC
            LIMIT 5
        """).fetchall()

        if len(oi_rows) < 5:
            log.debug("Only %d OI rows (need 5)", len(oi_rows))
            return None

        oi_now = oi_rows[0][1] or 0
        oi_prev = oi_rows[4][1] or 0

        if oi_prev <= 0:
            return None

        oi_delta_pct = (oi_now - oi_prev) / oi_prev

        # OI must have dropped
        if oi_delta_pct >= 0:
            return None

        # All conditions met
        trigger = {
            "trigger_ts": datetime.now(timezone.utc).isoformat(),
            "liq_long_ratio": round(ratio, 2),
            "liq_long_pct": round(long_pct, 4),
            "oi_delta_pct": round(oi_delta_pct, 4),
        }

        log.info("TRIGGER FIRED: liq_long=%.1f× (need %.1f×), L%%=%.0f%% (need %.0f%%), "
                 "OI Δ=%+.2f%%",
                 ratio, LIQ_SPIKE_MULT, long_pct * 100, LIQ_LONG_DOM * 100,
                 oi_delta_pct * 100)
        _log_to_db(conn, "TRADE", f"Trigger fired: ratio={ratio:.1f}× "
                   f"long_pct={long_pct*100:.0f}% oi_delta={oi_delta_pct*100:+.2f}%")

        return trigger

    except Exception as e:
        log.error("Trigger check failed: %s", e)
        return None
    finally:
        cz.close()


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------

def _execute_entry(conn: sqlite3.Connection, macro: dict, trigger: dict) -> bool:
    """
    Place market LONG with SL/TP on testnet.
    Returns True if position opened, False if failed.
    """
    log.info("Executing LONG entry...")

    try:
        exchange = get_client()
    except Exception as e:
        log.error("Cannot connect to exchange: %s", e)
        return False

    # Get current price from order book
    try:
        ob = exchange.fetch_order_book(SYMBOL, limit=1)
        bid = ob["bids"][0][0] if ob["bids"] else None
        if bid is None:
            log.error("Empty order book")
            return False
    except Exception as e:
        log.error("Failed to fetch order book: %s", e)
        return False

    # Compute SL/TP from current bid + daily ATR
    atr = macro.get("daily_atr", 0)
    if atr <= 0:
        log.error("Invalid ATR: %s", atr)
        return False

    sl_dist = atr * SL_ATR_MULT
    sl_price = round_to_tick(bid - sl_dist)
    tp_price = round_to_tick(bid + sl_dist * TARGET_RR)

    if sl_price <= 0:
        log.error("SL price below zero: %s", sl_price)
        return False

    # Build signal dict for order_manager
    signal = {
        "signal": "LONG",
        "entry_price": bid,
        "sl_price": sl_price,
        "tp_price": tp_price,
        "reason": (f"V4 cascade dip-buy: liq_ratio={trigger['liq_long_ratio']:.1f}× "
                   f"L%={trigger['liq_long_pct']*100:.0f}% "
                   f"OI_Δ={trigger['oi_delta_pct']*100:+.1f}%"),
    }

    # Risk validation
    try:
        balance = _get_balance(exchange)
        positions = exchange.fetch_positions([SYMBOL])
        open_pos = [p for p in positions if abs(float(p.get("contracts", 0))) > 0]
    except Exception as e:
        log.error("Failed to fetch balance/positions: %s", e)
        return False

    risk_result = validate_signal(signal, balance, open_pos)
    if not risk_result["approved"]:
        log.warning("Risk VETOED: %s", risk_result["reason"])
        _log_to_db(conn, "WARN", f"Risk veto: {risk_result['reason']}")
        return False

    # Execute
    log.info("Placing LONG: entry≈%s SL=%s TP=%s contracts=%s",
             f"{bid:,.1f}", f"{sl_price:,.1f}", f"{tp_price:,.1f}",
             risk_result["position_size_btc"])

    order_result = execute_signal(signal, risk_result)

    if order_result["status"] != "placed":
        log.error("Order failed: %s", order_result.get("error", "unknown"))
        _log_to_db(conn, "ERROR", f"Order failed: {order_result.get('error')}")
        return False

    # Log trade
    order_id = order_result.get("order_id")
    fill_price = bid  # approximate; actual fill from market order
    if order_result.get("entry_order"):
        fill_price = float(order_result["entry_order"].get("average", bid) or bid)

    log_trade({
        "order_id": order_id,
        "signal": "LONG",
        "entry_price": fill_price,
        "sl_price": sl_price,
        "tp_price": tp_price,
        "position_size_btc": risk_result["position_size_btc"],
        "leverage": risk_result["leverage"],
        "approved_by_risk": 1,
        "order_status": "placed",
    })

    # Update state → POSITION_OPEN
    _save_state(conn,
                state=State.POSITION_OPEN,
                order_id=order_id,
                entry_price=fill_price,
                sl_price=sl_price,
                tp_price=tp_price,
                sl_order_id=order_result.get("sl_order", {}).get("id") if order_result.get("sl_order") else None,
                tp_order_id=order_result.get("tp_order", {}).get("id") if order_result.get("tp_order") else None,
                position_size=risk_result["position_size_btc"])

    log.info("POSITION OPEN: fill=%s SL=%s TP=%s size=%s",
             f"{fill_price:,.1f}", f"{sl_price:,.1f}", f"{tp_price:,.1f}",
             risk_result["position_size_btc"])
    _log_to_db(conn, "TRADE", f"LONG opened: fill={fill_price:.1f} "
               f"SL={sl_price:.1f} TP={tp_price:.1f}")

    return True


def _get_balance(exchange) -> float:
    """Fetch USDT balance from exchange."""
    try:
        bal = exchange.fetch_balance()
        # Try USDT first, then USD
        usdt = bal.get("USDT", {}).get("free", 0) or 0
        usd = bal.get("USD", {}).get("free", 0) or 0
        return max(float(usdt), float(usd))
    except Exception as e:
        log.error("Balance fetch failed: %s", e)
        return 0


# ---------------------------------------------------------------------------
# Exit detection
# ---------------------------------------------------------------------------

def _check_exit(conn: sqlite3.Connection, state: dict) -> bool:
    """
    Check if position has been closed by SL/TP.
    Returns True if position closed, False if still open.
    """
    try:
        exchange = get_client()
        positions = exchange.fetch_positions([SYMBOL])
        open_pos = [p for p in positions if abs(float(p.get("contracts", 0))) > 0]
    except Exception as e:
        log.error("Failed to check positions: %s", e)
        return False

    if open_pos:
        return False  # still open

    # Position closed — determine exit reason
    order_id = state.get("order_id")
    sl_order_id = state.get("sl_order_id")
    tp_order_id = state.get("tp_order_id")

    exit_reason = "MANUAL"
    exit_price = state.get("entry_price", 0)  # fallback

    # Check SL order status
    if sl_order_id:
        try:
            sl_order = exchange.fetch_order(sl_order_id, SYMBOL)
            if sl_order.get("status") == "closed":
                exit_reason = "SL"
                exit_price = float(sl_order.get("average", state["sl_price"]) or state["sl_price"])
        except Exception:
            pass

    # Check TP order status
    if tp_order_id:
        try:
            tp_order = exchange.fetch_order(tp_order_id, SYMBOL)
            if tp_order.get("status") == "closed":
                exit_reason = "TP"
                exit_price = float(tp_order.get("average", state["tp_price"]) or state["tp_price"])
        except Exception:
            pass

    # Cancel remaining orders
    try:
        exchange.cancel_all_orders(SYMBOL)
    except Exception:
        pass

    # Log exit
    if order_id:
        update_trade_exit(order_id, exit_price, exit_reason)

    # Transition to COOLDOWN
    cooldown_until = (datetime.now(timezone.utc) +
                      timedelta(hours=COOLDOWN_HOURS)).isoformat()
    _save_state(conn,
                state=State.COOLDOWN,
                cooldown_until=cooldown_until)

    pnl_dir = "WIN" if exit_reason == "TP" else "LOSS" if exit_reason == "SL" else "?"
    log.info("POSITION CLOSED: %s (%s) exit=%s",
             exit_reason, pnl_dir, f"{exit_price:,.1f}")
    _log_to_db(conn, "TRADE", f"Position closed: {exit_reason} at {exit_price:.1f}")

    return True


# ---------------------------------------------------------------------------
# Sleep alignment
# ---------------------------------------------------------------------------

def _sleep_to_next_15m(loop_start: float):
    """Sleep until 30 seconds after the next quarter-hour."""
    now = time.time()
    elapsed = now - loop_start

    # Next quarter-hour boundary + offset
    current_quarter = int(now // LOOP_SECONDS) * LOOP_SECONDS
    next_wake = current_quarter + LOOP_SECONDS + LOOP_OFFSET_SEC

    sleep_sec = max(1, next_wake - now)

    # Cap at 16 minutes (safety)
    sleep_sec = min(sleep_sec, LOOP_SECONDS + LOOP_OFFSET_SEC)

    log.info("Sleeping %.0fs (loop took %.1fs)", sleep_sec, elapsed)
    time.sleep(sleep_sec)


# ---------------------------------------------------------------------------
# Crash recovery
# ---------------------------------------------------------------------------

def _recover_state(conn: sqlite3.Connection, state: dict):
    """Handle restart in non-IDLE states."""
    current = state["state"]

    if current == State.IDLE:
        return

    if current == State.COOLDOWN:
        if state.get("cooldown_until"):
            until = datetime.fromisoformat(state["cooldown_until"])
            if datetime.now(timezone.utc) >= until:
                log.info("Cooldown expired during downtime → IDLE")
                _save_state(conn, state=State.IDLE, cooldown_until=None)
            else:
                remaining = (until - datetime.now(timezone.utc)).total_seconds()
                log.info("Resuming COOLDOWN: %.0f min remaining", remaining / 60)
        return

    if current == State.ARMED:
        log.warning("Recovering from ARMED state — attempting immediate execution")
        # Trigger metadata should still be in state
        return  # main loop will attempt execution

    if current == State.POSITION_OPEN:
        log.info("Recovering POSITION_OPEN — checking exchange...")
        try:
            exchange = get_client()
            positions = exchange.fetch_positions([SYMBOL])
            open_pos = [p for p in positions if abs(float(p.get("contracts", 0))) > 0]

            if open_pos:
                log.info("Position still open — continuing to monitor")
            else:
                log.warning("Position closed during downtime — detecting exit")
                _check_exit(conn, state)
        except Exception as e:
            log.error("Recovery check failed: %s — will retry next loop", e)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    # Testnet guard
    testnet = os.getenv("BITMEX_TESTNET", "").lower()
    if testnet != "true":
        print("[FATAL] BITMEX_TESTNET must be 'true'. Refusing to start.")
        sys.exit(1)

    log.info("=" * 60)
    log.info("  V4 EXECUTION SERVICE — Cascade Dip-Buy")
    log.info("  TESTNET ONLY | %s", SYMBOL)
    log.info("=" * 60)
    log.info("  EMA200 bull filter, funding > %.2f%%, "
             "liq spike >= %.1f×, OI confirm",
             FUNDING_THRESHOLD * 100, LIQ_SPIKE_MULT)
    log.info("  SL: %.1f× ATR, TP: %.1f× risk, cooldown: %dh",
             SL_ATR_MULT, TARGET_RR, COOLDOWN_HOURS)

    conn = _init_state_db()
    state = _load_state(conn)
    log.info("Initial state: %s", state["state"])

    # Crash recovery
    _recover_state(conn, state)

    loop_count = 0

    while True:
        loop_start = time.time()
        loop_count += 1

        try:
            # Refresh state
            state = _load_state(conn)
            current = state["state"]
            log.info("─── Loop %d | State: %s ───", loop_count, current)

            # Check collector health
            _check_collector_heartbeat()

            # --- COOLDOWN ---
            if current == State.COOLDOWN:
                if state.get("cooldown_until"):
                    until = datetime.fromisoformat(state["cooldown_until"])
                    if datetime.now(timezone.utc) >= until:
                        log.info("Cooldown expired → IDLE")
                        _save_state(conn, state=State.IDLE, cooldown_until=None)
                    else:
                        remaining = (until - datetime.now(timezone.utc)).total_seconds()
                        log.info("Cooldown: %.0f min remaining", remaining / 60)

            # --- IDLE: macro update + trigger check ---
            elif current == State.IDLE:
                # Daily macro update
                if _should_run_macro_update(state):
                    _update_macro_context(conn)
                    state = _load_state(conn)

                # Check trigger (only if macro valid)
                if state.get("macro_valid"):
                    trigger = _check_micro_trigger(conn)
                    if trigger:
                        # Transition to ARMED (persisted for crash safety)
                        _save_state(conn, state=State.ARMED, **trigger)
                        state = _load_state(conn)
                        current = State.ARMED
                        log.info("State → ARMED")
                else:
                    log.debug("Macro invalid — skipping trigger check")

            # --- ARMED: execute immediately ---
            if current == State.ARMED:
                macro = {
                    "daily_atr": state.get("daily_atr", 0),
                    "ema200": state.get("ema200", 0),
                    "last_close": state.get("last_close", 0),
                }
                trigger = {
                    "liq_long_ratio": state.get("liq_long_ratio", 0),
                    "liq_long_pct": state.get("liq_long_pct", 0),
                    "oi_delta_pct": state.get("oi_delta_pct", 0),
                }

                success = _execute_entry(conn, macro, trigger)
                if not success:
                    log.warning("Execution failed — reverting to IDLE")
                    _save_state(conn, state=State.IDLE)

            # --- POSITION_OPEN: check exit ---
            elif current == State.POSITION_OPEN:
                _check_exit(conn, state)

        except KeyboardInterrupt:
            log.info("Shutting down (Ctrl-C)")
            break
        except Exception as e:
            log.error("Unhandled error: %s", e)
            traceback.print_exc()
            _log_to_db(conn, "ERROR", f"Unhandled: {e}")

        # Heartbeat
        state = _load_state(conn)
        _write_heartbeat(state["state"], loop_count)

        # Sleep to next 15m window
        _sleep_to_next_15m(loop_start)

    conn.close()
    log.info("V4 service stopped.")


if __name__ == "__main__":
    main()
