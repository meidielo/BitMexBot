"""
coinalyze_collector.py — 15-minute forward collector for OI + Liquidations

Fetches aggregated global Open Interest and Liquidation data from Coinalyze
every 15 minutes and archives to SQLite. This builds the high-resolution
dataset that Coinalyze's API only retains for ~21 days.

Run as cron (every 15 min) or as a long-running daemon:
  cron:   */15 * * * * cd ~/BitMexBot && venv/bin/python coinalyze_collector.py
  daemon: python coinalyze_collector.py --daemon

Data is stored in data/coinalyze.db with WAL mode enabled to prevent
locking during concurrent reads (backtest) and writes (collector).

Heartbeat: writes last-success timestamp to data/coinalyze_heartbeat.txt.
Check staleness: if file mtime > 30 min old, the pipeline is down.
"""

import argparse
import os
import sqlite3
import sys
import time
import traceback
from datetime import datetime, timezone

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BTC_PERP_SYMBOLS = [
    "BTCUSD_PERP.A",      # Binance coin-margined
    "BTCUSDT_PERP.A",     # Binance USDT-margined
    "BTCUSD_PERP.3",      # OKX
    "BTCUSDT_PERP.3",     # OKX USDT
    "BTCUSD_PERP.4",      # Bybit
    "BTCUSDT_PERP.4",     # Bybit USDT
    "BTCUSD_PERP.0",      # BitMEX
    "BTC-PERPETUAL.2",    # Deribit
]

BASE_URL       = "https://api.coinalyze.net/v1"
DB_PATH        = os.path.join("data", "coinalyze.db")
HEARTBEAT_PATH = os.path.join("data", "coinalyze_heartbeat.txt")
CRED_PATH      = os.path.expanduser("~/.openclaw/workspace/.credentials")

RATE_LIMIT_SLEEP = 1.6  # 40 calls/min = 1.5s/call + buffer
MAX_RETRIES      = 3
REQUEST_TIMEOUT  = 30   # seconds


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_api_key() -> str:
    with open(CRED_PATH) as f:
        for line in f:
            line = line.strip()
            if line.startswith("COINALYZE_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise RuntimeError("COINALYZE_API_KEY not found in .credentials")


def _init_db() -> sqlite3.Connection:
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    # Sync WAL to disk every 1000 pages (balance durability vs performance)
    conn.execute("PRAGMA wal_autocheckpoint=1000")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS oi_15m (
            timestamp   INTEGER NOT NULL,
            symbol      TEXT NOT NULL,
            oi_open     REAL,
            oi_high     REAL,
            oi_low      REAL,
            oi_close    REAL,
            PRIMARY KEY (timestamp, symbol)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS liquidations_15m (
            timestamp   INTEGER NOT NULL,
            symbol      TEXT NOT NULL,
            liq_long    REAL,
            liq_short   REAL,
            PRIMARY KEY (timestamp, symbol)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS oi_15m_agg (
            timestamp   INTEGER PRIMARY KEY,
            oi_open     REAL,
            oi_high     REAL,
            oi_low      REAL,
            oi_close    REAL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS liquidations_15m_agg (
            timestamp   INTEGER PRIMARY KEY,
            liq_long    REAL,
            liq_short   REAL
        )
    """)

    # Heartbeat table — track every collection attempt
    conn.execute("""
        CREATE TABLE IF NOT EXISTS collector_log (
            timestamp   INTEGER PRIMARY KEY,
            oi_rows     INTEGER,
            liq_rows    INTEGER,
            status      TEXT,
            error_msg   TEXT
        )
    """)

    conn.commit()
    return conn


def _fetch(endpoint: str, params: dict, headers: dict) -> list:
    """Fetch from Coinalyze with retry on 429 and connection errors."""
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(
                f"{BASE_URL}/{endpoint}",
                headers=headers,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", 60))
                _log(f"[429] Rate limited. Sleeping {retry_after}s (attempt {attempt+1})")
                time.sleep(retry_after)
                continue
            _log(f"[ERROR] {endpoint}: HTTP {r.status_code} — {r.text[:200]}")
            return []

        except requests.exceptions.Timeout:
            _log(f"[TIMEOUT] {endpoint} attempt {attempt+1}/{MAX_RETRIES}")
            time.sleep(5)
        except requests.exceptions.ConnectionError as e:
            _log(f"[CONN_ERROR] {endpoint} attempt {attempt+1}/{MAX_RETRIES}: {e}")
            time.sleep(10)
        except Exception as e:
            _log(f"[EXCEPTION] {endpoint}: {e}")
            return []

    _log(f"[FAILED] {endpoint}: exhausted {MAX_RETRIES} retries")
    return []


def _log(msg: str):
    """Print with timestamp. Works for both cron (log file) and terminal."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}", flush=True)


def _write_heartbeat(oi_rows: int, liq_rows: int):
    """Write heartbeat file — external monitoring can check mtime."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    with open(HEARTBEAT_PATH, "w") as f:
        f.write(f"{ts}\noi_rows={oi_rows}\nliq_rows={liq_rows}\n")


def _log_collection(conn: sqlite3.Connection, oi_rows: int, liq_rows: int,
                    status: str, error_msg: str = ""):
    """Log collection attempt to DB for auditing."""
    now = int(datetime.now(timezone.utc).timestamp())
    conn.execute(
        "INSERT OR REPLACE INTO collector_log VALUES (?, ?, ?, ?, ?)",
        (now, oi_rows, liq_rows, status, error_msg[:500]),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Collection logic
# ---------------------------------------------------------------------------

def collect_once(conn: sqlite3.Connection, api_key: str):
    """Fetch latest 15m OI + liquidations for all BTC perp symbols."""
    headers = {"api_key": api_key}
    now = int(datetime.now(timezone.utc).timestamp())
    since = now - (2 * 3600)  # last 2 hours to catch gaps

    symbols_csv = ",".join(BTC_PERP_SYMBOLS)

    # --- OI ---
    params = {"symbols": symbols_csv, "interval": "15min", "from": since, "to": now}
    data = _fetch("open-interest-history", params, headers)
    oi_rows = 0
    for entry in data:
        sym = entry.get("symbol", "")
        for h in entry.get("history", []):
            conn.execute(
                "INSERT OR REPLACE INTO oi_15m VALUES (?, ?, ?, ?, ?, ?)",
                (h["t"], sym, h.get("o"), h.get("h"), h.get("l"), h.get("c")),
            )
            oi_rows += 1

    time.sleep(RATE_LIMIT_SLEEP)

    # --- Liquidations ---
    params = {"symbols": symbols_csv, "interval": "15min", "from": since, "to": now}
    data = _fetch("liquidation-history", params, headers)
    liq_rows = 0
    for entry in data:
        sym = entry.get("symbol", "")
        for h in entry.get("history", []):
            conn.execute(
                "INSERT OR REPLACE INTO liquidations_15m VALUES (?, ?, ?, ?)",
                (h["t"], sym, h.get("l", 0), h.get("s", 0)),
            )
            liq_rows += 1

    conn.commit()

    # --- Aggregate only recent data (last 3 hours, not entire table) ---
    _aggregate_recent(conn, since)

    _log(f"Collected {oi_rows} OI rows, {liq_rows} liq rows")
    _write_heartbeat(oi_rows, liq_rows)
    _log_collection(conn, oi_rows, liq_rows, "OK")


def _aggregate_recent(conn: sqlite3.Connection, since_ts: int):
    """Aggregate only timestamps >= since_ts (not the entire table)."""
    conn.execute("""
        INSERT OR REPLACE INTO oi_15m_agg (timestamp, oi_open, oi_high, oi_low, oi_close)
        SELECT timestamp,
               SUM(oi_open), SUM(oi_high), SUM(oi_low), SUM(oi_close)
        FROM oi_15m
        WHERE timestamp >= ?
        GROUP BY timestamp
    """, (since_ts,))
    conn.execute("""
        INSERT OR REPLACE INTO liquidations_15m_agg (timestamp, liq_long, liq_short)
        SELECT timestamp,
               SUM(liq_long), SUM(liq_short)
        FROM liquidations_15m
        WHERE timestamp >= ?
        GROUP BY timestamp
    """, (since_ts,))
    conn.commit()


def _aggregate_all(conn: sqlite3.Connection):
    """Full re-aggregation (used only during backfill)."""
    conn.execute("""
        INSERT OR REPLACE INTO oi_15m_agg (timestamp, oi_open, oi_high, oi_low, oi_close)
        SELECT timestamp,
               SUM(oi_open), SUM(oi_high), SUM(oi_low), SUM(oi_close)
        FROM oi_15m
        GROUP BY timestamp
    """)
    conn.execute("""
        INSERT OR REPLACE INTO liquidations_15m_agg (timestamp, liq_long, liq_short)
        SELECT timestamp,
               SUM(liq_long), SUM(liq_short)
        FROM liquidations_15m
        GROUP BY timestamp
    """)
    conn.commit()


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------

def backfill(conn: sqlite3.Connection, api_key: str):
    """Pull the full 15m retention window (~21 days) to seed the DB."""
    headers = {"api_key": api_key}
    now = int(datetime.now(timezone.utc).timestamp())
    since = now - (25 * 86400)

    symbols_csv = ",".join(BTC_PERP_SYMBOLS)

    _log("Backfilling OI 15m...")
    params = {"symbols": symbols_csv, "interval": "15min", "from": since, "to": now}
    data = _fetch("open-interest-history", params, headers)
    total = 0
    for entry in data:
        sym = entry.get("symbol", "")
        for h in entry.get("history", []):
            conn.execute(
                "INSERT OR IGNORE INTO oi_15m VALUES (?, ?, ?, ?, ?, ?)",
                (h["t"], sym, h.get("o"), h.get("h"), h.get("l"), h.get("c")),
            )
            total += 1
    _log(f"  OI: {total} rows")

    time.sleep(RATE_LIMIT_SLEEP)

    _log("Backfilling liquidations 15m...")
    params = {"symbols": symbols_csv, "interval": "15min", "from": since, "to": now}
    data = _fetch("liquidation-history", params, headers)
    total = 0
    for entry in data:
        sym = entry.get("symbol", "")
        for h in entry.get("history", []):
            conn.execute(
                "INSERT OR IGNORE INTO liquidations_15m VALUES (?, ?, ?, ?)",
                (h["t"], sym, h.get("l", 0), h.get("s", 0)),
            )
            total += 1
    _log(f"  Liquidations: {total} rows")

    conn.commit()
    _aggregate_all(conn)
    _log("Backfill complete.")


# ---------------------------------------------------------------------------
# Integrity check
# ---------------------------------------------------------------------------

def check_integrity(conn: sqlite3.Connection):
    """Report gaps in the 15m aggregated data."""
    rows = conn.execute(
        "SELECT timestamp FROM oi_15m_agg ORDER BY timestamp"
    ).fetchall()

    if len(rows) < 2:
        _log("Not enough data to check integrity")
        return

    timestamps = [r[0] for r in rows]
    gaps = []
    for i in range(1, len(timestamps)):
        delta = timestamps[i] - timestamps[i-1]
        if delta > 1200:  # > 20 min = missed at least one 15m interval
            gap_start = datetime.fromtimestamp(timestamps[i-1], tz=timezone.utc)
            gap_end   = datetime.fromtimestamp(timestamps[i], tz=timezone.utc)
            missed    = delta // 900 - 1
            gaps.append((gap_start, gap_end, missed))

    if gaps:
        _log(f"Found {len(gaps)} gaps in OI data:")
        for start, end, missed in gaps[-10:]:  # show last 10
            _log(f"  {start.strftime('%Y-%m-%d %H:%M')} → "
                 f"{end.strftime('%Y-%m-%d %H:%M')} ({missed} intervals missed)")
    else:
        first = datetime.fromtimestamp(timestamps[0], tz=timezone.utc)
        last  = datetime.fromtimestamp(timestamps[-1], tz=timezone.utc)
        _log(f"No gaps found. {len(timestamps)} intervals from "
             f"{first.strftime('%Y-%m-%d %H:%M')} → {last.strftime('%Y-%m-%d %H:%M')}")

    # DB size
    db_size = os.path.getsize(DB_PATH) / (1024 * 1024)
    _log(f"DB size: {db_size:.1f} MB")

    # Row counts
    for tbl in ["oi_15m", "liquidations_15m", "oi_15m_agg", "liquidations_15m_agg", "collector_log"]:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        _log(f"  {tbl}: {cnt:,} rows")


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Coinalyze 15m forward collector")
    parser.add_argument("--daemon", action="store_true",
                        help="Run continuously (every 15 min)")
    parser.add_argument("--backfill", action="store_true",
                        help="Backfill available 15m history (~21 days)")
    parser.add_argument("--check", action="store_true",
                        help="Check data integrity and report gaps")
    args = parser.parse_args()

    try:
        api_key = _load_api_key()
    except Exception as e:
        _log(f"[FATAL] Cannot load API key: {e}")
        sys.exit(1)

    try:
        conn = _init_db()
    except Exception as e:
        _log(f"[FATAL] Cannot init DB: {e}")
        sys.exit(1)

    if args.check:
        check_integrity(conn)
        conn.close()
        return

    if args.backfill:
        backfill(conn, api_key)
        conn.close()
        return

    if args.daemon:
        _log("=== Coinalyze 15m Collector (daemon mode) ===")
        _log(f"DB: {DB_PATH}")
        _log(f"Symbols: {len(BTC_PERP_SYMBOLS)} BTC perps")
        _log("Collecting every 900s (15 min)")

        backfill(conn, api_key)

        while True:
            try:
                collect_once(conn, api_key)
            except Exception as e:
                _log(f"[ERROR] Collection failed: {e}")
                _log_collection(conn, 0, 0, "ERROR", str(e))
                traceback.print_exc()
            time.sleep(900)
    else:
        # Single collection (for cron) — wrapped in try/except
        try:
            collect_once(conn, api_key)
        except Exception as e:
            _log(f"[ERROR] Collection failed: {e}")
            _log_collection(conn, 0, 0, "ERROR", str(e))
            traceback.print_exc()
            sys.exit(1)

    conn.close()


if __name__ == "__main__":
    main()
