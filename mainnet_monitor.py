"""
mainnet_monitor.py — Read-Only Mainnet V4 Condition Monitor

Monitors mainnet for V4 signal conditions without placing any orders.
Logs "WOULD_FIRE" events when all conditions are met.
Tracks condition distances every loop for forward analysis.

No API key required — uses only public data endpoints.
No testnet connection — purely observational.

Usage:
  python mainnet_monitor.py
  sudo systemctl start mainnet-monitor.service
"""

import logging
import os
import sqlite3
import sys
import time
import traceback
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from bitmex_client import get_data_client
from condition_logger import get_logger as get_condition_logger, log_v4_conditions
from fetch_data import fetch_recent_funding

# ---------------------------------------------------------------------------
# Constants — same as v4_execution.py (FROZEN)
# ---------------------------------------------------------------------------
EMA_PERIOD         = 200
FUNDING_THRESHOLD  = 0.0003
FUNDING_LOOKBACK_D = 3
LIQ_SPIKE_MULT     = 3.0
LIQ_LONG_DOM       = 0.60
LIQ_LOOKBACK_DAYS  = 20
SYMBOL             = "BTC/USDT:USDT"

LOOP_SECONDS       = 15 * 60
COINALYZE_DB_PATH  = os.path.join("data", "coinalyze.db")
EVENTS_DB_PATH     = os.path.join("data", "monitor_events.db")
HEARTBEAT_PATH     = os.path.join("data", "monitor_heartbeat.txt")


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _setup_logging() -> logging.Logger:
    logger = logging.getLogger("monitor")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            "[%(asctime)s UTC] [MONITOR] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"))
        handler.formatter.converter = time.gmtime
        logger.addHandler(handler)
    return logger

log = _setup_logging()


# ---------------------------------------------------------------------------
# Events DB
# ---------------------------------------------------------------------------
def _init_events_db() -> sqlite3.Connection:
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(EVENTS_DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS monitor_events (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp           TEXT NOT NULL,
            all_conditions_met  INTEGER NOT NULL,
            macro_valid         INTEGER NOT NULL,
            bull_regime         INTEGER,
            funding_valid       INTEGER,
            ema200              REAL,
            last_close          REAL,
            ema_gap_pct         REAL,
            funding_peak        REAL,
            liq_ratio           REAL,
            liq_long_pct        REAL,
            oi_delta_pct        REAL
        )
    """)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Macro check (same logic as v4_execution._update_macro_context)
# ---------------------------------------------------------------------------
def check_macro() -> dict:
    """Check macro conditions using mainnet public data."""
    exchange = get_data_client()

    try:
        raw = exchange.fetch_ohlcv(SYMBOL, "1d", limit=250)
    except Exception as e:
        log.error("Failed to fetch daily candles: %s", e)
        return {"macro_valid": False}

    if len(raw) < EMA_PERIOD + 5:
        return {"macro_valid": False}

    df = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "volume"])
    ema = df["close"].ewm(span=EMA_PERIOD, adjust=False).mean()
    ema200 = float(ema.iloc[-1])
    last_close = float(df["close"].iloc[-1])
    bull_regime = last_close > ema200

    try:
        df_fund = fetch_recent_funding(count=10)
        funding_peak = float(df_fund["rate"].max()) if df_fund is not None and not df_fund.empty else 0
    except Exception:
        funding_peak = 0

    funding_valid = funding_peak > FUNDING_THRESHOLD
    macro_valid = bull_regime and funding_valid

    ema_gap_pct = (last_close - ema200) / ema200 * 100 if ema200 > 0 else 0

    return {
        "macro_valid": macro_valid,
        "bull_regime": bull_regime,
        "funding_valid": funding_valid,
        "ema200": ema200,
        "last_close": last_close,
        "ema_gap_pct": ema_gap_pct,
        "funding_peak": funding_peak,
    }


# ---------------------------------------------------------------------------
# Micro check (same logic as v4_execution._check_micro_trigger)
# ---------------------------------------------------------------------------
def check_micro() -> dict:
    """Check micro trigger conditions from Coinalyze data."""
    result = {
        "liq_ratio": 0,
        "liq_long_pct": 0,
        "oi_delta_pct": 0,
        "liq_spike_ok": False,
        "long_dom_ok": False,
        "oi_confirm_ok": False,
    }

    if not os.path.exists(COINALYZE_DB_PATH):
        log.debug("Coinalyze DB not found")
        return result

    try:
        cz = sqlite3.connect(COINALYZE_DB_PATH, timeout=5)
        cz.execute("PRAGMA busy_timeout=3000")

        # Liquidation metrics
        rows = cz.execute("""
            SELECT timestamp, liq_long, liq_short
            FROM liquidations_15m_agg
            ORDER BY timestamp DESC LIMIT 4
        """).fetchall()

        if len(rows) >= 4:
            hourly_long = sum(r[1] or 0 for r in rows)
            hourly_short = sum(r[2] or 0 for r in rows)
            hourly_total = hourly_long + hourly_short

            if hourly_total > 0:
                result["liq_long_pct"] = hourly_long / hourly_total

            max_ts = rows[0][0]
            since_ts = max_ts - (LIQ_LOOKBACK_DAYS * 86400)

            baseline = cz.execute("""
                SELECT SUM(liq_long), COUNT(*)
                FROM liquidations_15m_agg WHERE timestamp >= ?
            """, (since_ts,)).fetchone()

            total_20d = baseline[0] or 0
            n_bars = baseline[1] or 1
            avg_hourly = total_20d / (n_bars / 4) if n_bars > 0 else 1

            if avg_hourly > 0:
                result["liq_ratio"] = hourly_long / avg_hourly

        result["liq_spike_ok"] = result["liq_ratio"] >= LIQ_SPIKE_MULT
        result["long_dom_ok"] = result["liq_long_pct"] >= LIQ_LONG_DOM

        # OI metrics
        oi_rows = cz.execute("""
            SELECT timestamp, oi_close
            FROM oi_15m_agg ORDER BY timestamp DESC LIMIT 5
        """).fetchall()

        if len(oi_rows) >= 5:
            oi_now = oi_rows[0][1] or 0
            oi_prev = oi_rows[4][1] or 0
            if oi_prev > 0:
                result["oi_delta_pct"] = (oi_now - oi_prev) / oi_prev
        result["oi_confirm_ok"] = result["oi_delta_pct"] < 0

        cz.close()
    except Exception as e:
        log.error("Micro check failed: %s", e)

    return result


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    log.info("=" * 60)
    log.info("  MAINNET REGIME MONITOR — Read-Only V4 Conditions")
    log.info("  NO ORDERS | NO API KEY | Public data only")
    log.info("=" * 60)

    events_conn = _init_events_db()
    loop_count = 0

    while True:
        loop_start = time.time()
        loop_count += 1

        try:
            log.info("─── Monitor loop %d ───", loop_count)

            # Check macro
            macro = check_macro()
            log.info("Macro: bull=%s fund=%s | close=%s EMA=%s (gap=%+.1f%%) "
                     "fund_peak=%s%%",
                     "Y" if macro.get("bull_regime") else "N",
                     "Y" if macro.get("funding_valid") else "N",
                     f"{macro.get('last_close', 0):,.0f}",
                     f"{macro.get('ema200', 0):,.0f}",
                     macro.get("ema_gap_pct", 0),
                     f"{macro.get('funding_peak', 0)*100:.4f}")

            # Check micro
            micro = check_micro()
            log.info("Micro: liq=%.2f×/%.1f× L%%=%.0f%%/%.0f%% OI_Δ=%+.2f%%",
                     micro["liq_ratio"], LIQ_SPIKE_MULT,
                     micro["liq_long_pct"] * 100, LIQ_LONG_DOM * 100,
                     micro["oi_delta_pct"] * 100)

            # Log to condition tracker
            try:
                cond_conn = get_condition_logger()
                log_v4_conditions(cond_conn,
                                 close=macro.get("last_close"),
                                 ema200=macro.get("ema200"),
                                 funding_peak=macro.get("funding_peak"),
                                 funding_threshold=FUNDING_THRESHOLD,
                                 liq_ratio=micro["liq_ratio"],
                                 liq_spike_mult=LIQ_SPIKE_MULT,
                                 liq_long_pct=micro["liq_long_pct"],
                                 liq_long_dom=LIQ_LONG_DOM,
                                 oi_delta_pct=micro["oi_delta_pct"])
            except Exception:
                pass

            # Check all conditions
            all_met = (macro.get("macro_valid", False) and
                      micro["liq_spike_ok"] and
                      micro["long_dom_ok"] and
                      micro["oi_confirm_ok"])

            if all_met:
                log.info("*** WOULD_FIRE *** All V4 conditions met on mainnet!")

            # Log event
            now = datetime.now(timezone.utc).isoformat()
            events_conn.execute(
                "INSERT INTO monitor_events VALUES "
                "(NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (now, 1 if all_met else 0,
                 1 if macro.get("macro_valid") else 0,
                 1 if macro.get("bull_regime") else 0,
                 1 if macro.get("funding_valid") else 0,
                 macro.get("ema200"), macro.get("last_close"),
                 macro.get("ema_gap_pct"),
                 macro.get("funding_peak"),
                 micro["liq_ratio"], micro["liq_long_pct"],
                 micro["oi_delta_pct"])
            )
            events_conn.commit()

            # Heartbeat
            with open(HEARTBEAT_PATH, "w") as f:
                f.write(f"{now}\nloop={loop_count}\nall_met={all_met}\n")

        except KeyboardInterrupt:
            log.info("Monitor stopped (Ctrl-C)")
            break
        except Exception as e:
            log.error("Unhandled: %s", e)
            traceback.print_exc()

        # Sleep to next 15m
        elapsed = time.time() - loop_start
        sleep_sec = max(1, LOOP_SECONDS - elapsed)
        log.info("Sleeping %.0fs", sleep_sec)
        time.sleep(sleep_sec)

    events_conn.close()


if __name__ == "__main__":
    main()
