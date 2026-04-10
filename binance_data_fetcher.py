"""
binance_data_fetcher.py — Historical OI + Funding from Binance Futures (free API)

Fetches Open Interest history and funding rate history from Binance Futures
public endpoints. No API key required.

Endpoints:
  GET /futures/data/openInterestHist  — daily OI, 500 per page
  GET /fapi/v1/fundingRate            — funding history, 1000 per page

Serves dual purpose:
  1. Cross-validation data source for Coinalyze OI
  2. Extended OI history for backtest (when Coinalyze data doesn't go back far enough)

Usage:
  python binance_data_fetcher.py --start 2019-01-01
  python binance_data_fetcher.py --start 2019-01-01 --refresh
  python binance_data_fetcher.py --stats
"""

import argparse
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL_FUTURES = "https://fapi.binance.com"
DB_PATH          = os.path.join("data", "binance_historical.db")
OI_CACHE_CSV     = os.path.join("data", "binance_oi_daily.csv")
FUNDING_CACHE_CSV = os.path.join("data", "binance_funding.csv")

DEFAULT_SYMBOL   = "BTCUSDT"
REQUEST_TIMEOUT  = 30
MAX_RETRIES      = 3
RATE_LIMIT_SLEEP = 0.5  # 0.5s between requests (well under 1200 weight/min)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _log(msg: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
def _init_db() -> sqlite3.Connection:
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS binance_oi_daily (
            timestamp       INTEGER NOT NULL,
            symbol          TEXT    NOT NULL,
            sum_oi          REAL    NOT NULL,
            sum_oi_value    REAL    NOT NULL,
            PRIMARY KEY (timestamp, symbol)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS binance_funding (
            timestamp       INTEGER NOT NULL,
            symbol          TEXT    NOT NULL,
            funding_rate    REAL    NOT NULL,
            PRIMARY KEY (timestamp, symbol)
        )
    """)

    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------
def _get(endpoint: str, params: dict) -> list | dict | None:
    """GET with retry on timeout/connection errors."""
    url = f"{BASE_URL_FUTURES}{endpoint}"
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", 60))
                _log(f"[429] Rate limited. Sleeping {retry_after}s")
                time.sleep(retry_after)
                continue
            _log(f"[ERROR] {endpoint}: HTTP {r.status_code} — {r.text[:200]}")
            return None
        except requests.exceptions.Timeout:
            _log(f"[TIMEOUT] attempt {attempt+1}/{MAX_RETRIES}")
            time.sleep(5)
        except requests.exceptions.ConnectionError as e:
            _log(f"[CONN_ERROR] attempt {attempt+1}/{MAX_RETRIES}: {e}")
            time.sleep(10)
        except Exception as e:
            _log(f"[ERROR] {endpoint}: {e}")
            return None

    _log(f"[FAILED] {endpoint}: exhausted retries")
    return None


# ---------------------------------------------------------------------------
# OI History
# ---------------------------------------------------------------------------
def fetch_oi_history(conn: sqlite3.Connection, symbol: str = DEFAULT_SYMBOL,
                     start_ms: int = None, end_ms: int = None):
    """
    Fetch daily OI history from Binance. Paginates backward in 500-record chunks.
    Note: Binance limits OI history to approximately the last 500 days on daily period.
    """
    if end_ms is None:
        end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    total_rows = 0
    current_end = end_ms

    _log(f"Fetching Binance OI history for {symbol}...")

    while True:
        params = {
            "symbol": symbol,
            "period": "1d",
            "limit": 500,
        }
        if start_ms:
            params["startTime"] = start_ms
        params["endTime"] = current_end

        data = _get("/futures/data/openInterestHist", params)
        if not data:
            break

        if len(data) == 0:
            break

        for row in data:
            ts_ms = int(row["timestamp"])
            conn.execute(
                "INSERT OR REPLACE INTO binance_oi_daily VALUES (?, ?, ?, ?)",
                (ts_ms, symbol,
                 float(row["sumOpenInterest"]),
                 float(row["sumOpenInterestValue"]))
            )

        conn.commit()
        total_rows += len(data)

        # Move window backward
        earliest = min(int(r["timestamp"]) for r in data)
        if start_ms and earliest <= start_ms:
            break
        current_end = earliest - 1

        if len(data) < 500:
            break  # no more pages

        time.sleep(RATE_LIMIT_SLEEP)

    _log(f"  OI: {total_rows} records fetched")
    return total_rows


# ---------------------------------------------------------------------------
# Funding Rate History
# ---------------------------------------------------------------------------
def fetch_funding_history(conn: sqlite3.Connection, symbol: str = DEFAULT_SYMBOL,
                          start_ms: int = None):
    """
    Fetch full funding rate history. Paginates forward in 1000-record chunks.
    """
    total_rows = 0

    if start_ms is None:
        # Default: start from 2019-09-01 (Binance futures launch)
        start_ms = int(datetime(2019, 9, 1, tzinfo=timezone.utc).timestamp() * 1000)

    current_start = start_ms
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    _log(f"Fetching Binance funding history for {symbol}...")

    while current_start < now_ms:
        params = {
            "symbol": symbol,
            "startTime": current_start,
            "limit": 1000,
        }

        data = _get("/fapi/v1/fundingRate", params)
        if not data:
            break

        if len(data) == 0:
            break

        for row in data:
            ts_ms = int(row["fundingTime"])
            conn.execute(
                "INSERT OR REPLACE INTO binance_funding VALUES (?, ?, ?)",
                (ts_ms, symbol, float(row["fundingRate"]))
            )

        conn.commit()
        total_rows += len(data)

        # Move forward
        latest = max(int(r["fundingTime"]) for r in data)
        current_start = latest + 1

        if len(data) < 1000:
            break

        time.sleep(RATE_LIMIT_SLEEP)

    _log(f"  Funding: {total_rows} records fetched")
    return total_rows


# ---------------------------------------------------------------------------
# Data access
# ---------------------------------------------------------------------------
def get_oi_daily(conn: sqlite3.Connection = None,
                 symbol: str = DEFAULT_SYMBOL) -> pd.DataFrame:
    """Load OI daily data as DataFrame."""
    if conn is None:
        if not os.path.exists(DB_PATH):
            return pd.DataFrame()
        conn = sqlite3.connect(DB_PATH, timeout=5)

    df = pd.read_sql_query(
        "SELECT timestamp, sum_oi, sum_oi_value FROM binance_oi_daily "
        "WHERE symbol = ? ORDER BY timestamp",
        conn, params=(symbol,)
    )
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df = df.set_index("timestamp")
    return df


def get_funding(conn: sqlite3.Connection = None,
                symbol: str = DEFAULT_SYMBOL) -> pd.DataFrame:
    """Load funding rate data as DataFrame."""
    if conn is None:
        if not os.path.exists(DB_PATH):
            return pd.DataFrame()
        conn = sqlite3.connect(DB_PATH, timeout=5)

    df = pd.read_sql_query(
        "SELECT timestamp, funding_rate FROM binance_funding "
        "WHERE symbol = ? ORDER BY timestamp",
        conn, params=(symbol,)
    )
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df = df.set_index("timestamp")
    return df


# ---------------------------------------------------------------------------
# Export to CSV
# ---------------------------------------------------------------------------
def export_csv(conn: sqlite3.Connection, symbol: str = DEFAULT_SYMBOL):
    """Export data to CSV caches for quick re-use."""
    df_oi = get_oi_daily(conn, symbol)
    if not df_oi.empty:
        df_oi.to_csv(OI_CACHE_CSV)
        _log(f"  Exported OI to {OI_CACHE_CSV} ({len(df_oi)} rows)")

    df_fund = get_funding(conn, symbol)
    if not df_fund.empty:
        df_fund.to_csv(FUNDING_CACHE_CSV)
        _log(f"  Exported funding to {FUNDING_CACHE_CSV} ({len(df_fund)} rows)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Fetch OI + funding history from Binance Futures (free API)")
    parser.add_argument("--start", type=str, default="2019-09-01",
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--symbol", type=str, default=DEFAULT_SYMBOL,
                        help="Futures symbol (default: BTCUSDT)")
    parser.add_argument("--refresh", action="store_true",
                        help="Re-fetch all data")
    parser.add_argument("--stats", action="store_true",
                        help="Print statistics only")
    parser.add_argument("--funding-only", action="store_true",
                        help="Fetch only funding rates")
    parser.add_argument("--oi-only", action="store_true",
                        help="Fetch only OI history")
    args = parser.parse_args()

    conn = _init_db()

    if args.stats:
        df_oi = get_oi_daily(conn, args.symbol)
        df_fund = get_funding(conn, args.symbol)

        _log(f"=== Binance {args.symbol} Data ===")
        if not df_oi.empty:
            _log(f"OI:      {len(df_oi)} days, "
                 f"{df_oi.index[0].date()} → {df_oi.index[-1].date()}")
            _log(f"  Mean OI value: ${df_oi['sum_oi_value'].mean():,.0f}")
        else:
            _log("OI: no data")

        if not df_fund.empty:
            _log(f"Funding: {len(df_fund)} records, "
                 f"{df_fund.index[0].date()} → {df_fund.index[-1].date()}")
            _log(f"  Mean rate: {df_fund['funding_rate'].mean()*100:.4f}%")
        else:
            _log("Funding: no data")

        conn.close()
        return

    start_dt = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    start_ms = int(start_dt.timestamp() * 1000)

    if not args.funding_only:
        fetch_oi_history(conn, args.symbol, start_ms=start_ms)

    if not args.oi_only:
        fetch_funding_history(conn, args.symbol, start_ms=start_ms)

    export_csv(conn, args.symbol)
    conn.close()
    _log("Done.")


if __name__ == "__main__":
    main()
