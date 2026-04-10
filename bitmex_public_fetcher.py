"""
bitmex_public_fetcher.py — DEPRECATED / NON-FUNCTIONAL

The original premise (audit recommendation) was that BitMEX public S3 dumps
contain historical liquidation data. Verified 2026-04-09:

  1. https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/liquidation/  → 404
     (no liquidation directory exists)
  2. https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/  → 200
     but the trade.csv.gz files only contain `trdType=Regular` — no liquidation
     flag. Liquidations are NOT distinguishable in BitMEX public dumps.

What DOES contain historical liquidation data:
  - Coinalyze (paid, daily back to ~2019, 15m back ~21 days) — already in use
  - CryptoQuant (paid, ~$50/mo) — daily liquidations back to 2019
  - Coinglass (paid pro tier) — similar
  - Building forward from collector — slow but free

This file is kept as a placeholder. The code below is the original
implementation that targets the (non-existent) /data/liquidation/ endpoint
and will return zero rows for any date range.

See lessons.md L28 for full context.
"""

import argparse
import gzip
import io
import os
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL   = "https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/liquidation"
DB_PATH    = os.path.join("data", "bitmex_historical.db")
CACHE_DIR  = os.path.join("data", "bitmex_public")

# BTC instruments on BitMEX
BTC_SYMBOLS = {"XBTUSD", "XBTUSDT", "XBTH", "XBTM", "XBTU", "XBTZ"}
# Prefix matching for quarterly futures (XBTH24, XBTM23, etc.)
BTC_PREFIXES = ("XBT",)

REQUEST_TIMEOUT = 30
MAX_RETRIES     = 3
RETRY_SLEEP     = 5
RATE_LIMIT_SLEEP = 0.3  # polite rate limiting


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
        CREATE TABLE IF NOT EXISTS bitmex_liquidations_raw (
            date        TEXT    NOT NULL,
            symbol      TEXT    NOT NULL,
            side        TEXT    NOT NULL,
            qty         REAL    NOT NULL,
            price       REAL    NOT NULL,
            notional_usd REAL,
            PRIMARY KEY (date, symbol, side, price, qty)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bitmex_liquidations_daily (
            date          TEXT PRIMARY KEY,
            liq_long      REAL NOT NULL DEFAULT 0,
            liq_short     REAL NOT NULL DEFAULT 0,
            liq_total     REAL NOT NULL DEFAULT 0,
            trade_count   INTEGER NOT NULL DEFAULT 0
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS fetch_log (
            date    TEXT PRIMARY KEY,
            status  TEXT NOT NULL,
            rows    INTEGER DEFAULT 0,
            error   TEXT DEFAULT ''
        )
    """)

    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Download + Parse
# ---------------------------------------------------------------------------
def _is_btc_symbol(symbol: str) -> bool:
    """Check if a symbol is a BTC instrument."""
    if symbol in BTC_SYMBOLS:
        return True
    for prefix in BTC_PREFIXES:
        if symbol.startswith(prefix):
            return True
    return False


def _download_day(d: date) -> pd.DataFrame | None:
    """Download a single day's liquidation CSV from BitMEX public dumps."""
    date_str = d.strftime("%Y%m%d")
    url = f"{BASE_URL}/{date_str}.csv.gz"

    # Check local cache first
    cache_path = os.path.join(CACHE_DIR, f"{date_str}.csv.gz")
    if os.path.exists(cache_path):
        try:
            with gzip.open(cache_path, "rt") as f:
                df = pd.read_csv(f)
            return df
        except Exception:
            os.remove(cache_path)  # corrupted cache, re-download

    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                # Cache the raw file
                os.makedirs(CACHE_DIR, exist_ok=True)
                with open(cache_path, "wb") as f:
                    f.write(r.content)

                # Parse
                raw = gzip.decompress(r.content)
                df = pd.read_csv(io.BytesIO(raw))
                return df

            if r.status_code == 404:
                return None  # no data for this day (weekend, or before start)
            if r.status_code == 403:
                return None  # access denied, likely before data begins

            _log(f"[WARN] HTTP {r.status_code} for {date_str}")
            return None

        except requests.exceptions.Timeout:
            _log(f"[TIMEOUT] {date_str} attempt {attempt+1}/{MAX_RETRIES}")
            time.sleep(RETRY_SLEEP)
        except requests.exceptions.ConnectionError as e:
            _log(f"[CONN_ERROR] {date_str} attempt {attempt+1}/{MAX_RETRIES}: {e}")
            time.sleep(RETRY_SLEEP)
        except Exception as e:
            _log(f"[ERROR] {date_str}: {e}")
            return None

    _log(f"[FAILED] {date_str}: exhausted retries")
    return None


def _parse_liquidations(df: pd.DataFrame, date_str: str) -> list[dict]:
    """
    Parse BitMEX liquidation CSV into structured records.

    BitMEX liquidation CSV columns vary by era but typically include:
    symbol, side, orderQty, price, leavesQty
    """
    if df is None or df.empty:
        return []

    records = []

    # Normalize column names to lowercase
    df.columns = [c.strip().lower() for c in df.columns]

    # Filter BTC instruments only
    if "symbol" not in df.columns:
        return []

    btc_df = df[df["symbol"].apply(_is_btc_symbol)]
    if btc_df.empty:
        return []

    for _, row in btc_df.iterrows():
        symbol = str(row.get("symbol", ""))
        side = str(row.get("side", "")).upper()
        qty = float(row.get("orderqty", row.get("leavesqty", 0)) or 0)
        price = float(row.get("price", 0) or 0)

        if qty <= 0 or price <= 0:
            continue

        # For XBTUSD (inverse): notional = qty (contracts are $1 each)
        # For XBTUSDT: notional = qty * price / 1e6 (or similar)
        # Simplification: use qty as USD notional for XBTUSD
        if "USDT" in symbol:
            notional_usd = qty * price
        else:
            notional_usd = qty  # XBTUSD contracts are $1 USD each

        records.append({
            "date": date_str,
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "price": price,
            "notional_usd": notional_usd,
        })

    return records


def _aggregate_daily(conn: sqlite3.Connection):
    """Aggregate raw liquidation records into daily long/short totals."""
    conn.execute("DELETE FROM bitmex_liquidations_daily")
    conn.execute("""
        INSERT INTO bitmex_liquidations_daily (date, liq_long, liq_short, liq_total, trade_count)
        SELECT
            date,
            SUM(CASE WHEN side = 'SELL' THEN notional_usd ELSE 0 END) as liq_long,
            SUM(CASE WHEN side = 'BUY'  THEN notional_usd ELSE 0 END) as liq_short,
            SUM(notional_usd) as liq_total,
            COUNT(*) as trade_count
        FROM bitmex_liquidations_raw
        GROUP BY date
        ORDER BY date
    """)
    conn.commit()

    # Note on side mapping:
    # A LONG position liquidation is a forced SELL order (closing the long)
    # A SHORT position liquidation is a forced BUY order (closing the short)
    # So: side='Sell' = long liquidation, side='Buy' = short liquidation


# ---------------------------------------------------------------------------
# Main fetch loop
# ---------------------------------------------------------------------------
def fetch_range(conn: sqlite3.Connection, start: date, end: date,
                refresh: bool = False):
    """Download and parse liquidation data for a date range."""
    d = start
    total_rows = 0
    total_days = (end - start).days
    fetched = 0
    skipped = 0
    failed = 0

    _log(f"Fetching BitMEX liquidation data: {start} → {end} ({total_days} days)")

    while d <= end:
        date_str = d.strftime("%Y-%m-%d")

        # Skip if already fetched (unless refresh)
        if not refresh:
            existing = conn.execute(
                "SELECT status FROM fetch_log WHERE date = ?", (date_str,)
            ).fetchone()
            if existing and existing[0] == "OK":
                d += timedelta(days=1)
                skipped += 1
                continue

        # Download
        df = _download_day(d)

        if df is None:
            conn.execute(
                "INSERT OR REPLACE INTO fetch_log VALUES (?, ?, ?, ?)",
                (date_str, "NO_DATA", 0, "")
            )
            conn.commit()
            d += timedelta(days=1)
            continue

        # Parse
        try:
            records = _parse_liquidations(df, date_str)
            for rec in records:
                conn.execute(
                    "INSERT OR IGNORE INTO bitmex_liquidations_raw VALUES (?, ?, ?, ?, ?, ?)",
                    (rec["date"], rec["symbol"], rec["side"],
                     rec["qty"], rec["price"], rec["notional_usd"])
                )
            conn.commit()

            conn.execute(
                "INSERT OR REPLACE INTO fetch_log VALUES (?, ?, ?, ?)",
                (date_str, "OK", len(records), "")
            )
            conn.commit()

            total_rows += len(records)
            fetched += 1

            if fetched % 50 == 0:
                _log(f"  Progress: {fetched}/{total_days} days, "
                     f"{total_rows:,} records, {skipped} skipped")

        except Exception as e:
            conn.execute(
                "INSERT OR REPLACE INTO fetch_log VALUES (?, ?, ?, ?)",
                (date_str, "ERROR", 0, str(e)[:500])
            )
            conn.commit()
            _log(f"[ERROR] {date_str}: {e}")
            failed += 1

        time.sleep(RATE_LIMIT_SLEEP)
        d += timedelta(days=1)

    # Aggregate
    _log("Aggregating daily totals...")
    _aggregate_daily(conn)

    # Report
    daily_count = conn.execute(
        "SELECT COUNT(*) FROM bitmex_liquidations_daily"
    ).fetchone()[0]

    _log(f"\nDone: {fetched} days fetched, {skipped} skipped, {failed} failed")
    _log(f"  Raw records: {total_rows:,}")
    _log(f"  Daily aggregated rows: {daily_count}")

    if daily_count > 0:
        first = conn.execute(
            "SELECT MIN(date) FROM bitmex_liquidations_daily"
        ).fetchone()[0]
        last = conn.execute(
            "SELECT MAX(date) FROM bitmex_liquidations_daily"
        ).fetchone()[0]
        _log(f"  Date range: {first} → {last}")


def get_daily_liquidations(conn: sqlite3.Connection = None) -> pd.DataFrame:
    """Load aggregated daily liquidation data as a DataFrame."""
    if conn is None:
        if not os.path.exists(DB_PATH):
            return pd.DataFrame()
        conn = sqlite3.connect(DB_PATH, timeout=5)

    df = pd.read_sql_query(
        "SELECT date, liq_long, liq_short, liq_total, trade_count "
        "FROM bitmex_liquidations_daily ORDER BY date",
        conn,
        parse_dates=["date"],
    )
    if not df.empty:
        df = df.set_index("date")
    return df


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Fetch historical liquidation data from BitMEX public dumps")
    parser.add_argument("--start", type=str, default="2019-01-01",
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="End date (YYYY-MM-DD, default: today)")
    parser.add_argument("--refresh", action="store_true",
                        help="Re-download even if data exists")
    parser.add_argument("--stats", action="store_true",
                        help="Print statistics only, don't download")
    args = parser.parse_args()

    conn = _init_db()

    if args.stats:
        df = get_daily_liquidations(conn)
        if df.empty:
            _log("No data in database. Run with --start to fetch.")
        else:
            _log(f"Date range: {df.index[0].date()} → {df.index[-1].date()}")
            _log(f"Total days: {len(df)}")
            _log(f"Mean daily long liqs:  ${df['liq_long'].mean():,.0f}")
            _log(f"Mean daily short liqs: ${df['liq_short'].mean():,.0f}")
            _log(f"Max daily long liqs:   ${df['liq_long'].max():,.0f}")
            _log(f"Max daily short liqs:  ${df['liq_short'].max():,.0f}")
        conn.close()
        return

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else date.today()

    fetch_range(conn, start, end, refresh=args.refresh)
    conn.close()


if __name__ == "__main__":
    main()
