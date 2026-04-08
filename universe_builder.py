"""
universe_builder.py — Survivorship-Bias-Free Asset Universe for Momentum Backtest

Builds a point-in-time eligible universe from BitMEX's full instrument history
(272 perpetual swaps, including 206 delisted/settled).

For any backtest date T, returns only symbols that pass three gates:
  1. EXISTENCE:  listed before T AND not settled before T
  2. SEASONING:  listed at least 30 days before T (avoid listing pump noise)
  3. LIQUIDITY:  rolling 14-day average daily dollar volume >= $1M

Data sourced exclusively from BitMEX (execution venue = data venue = no mismatch).
Daily OHLCV cached to SQLite after first fetch (~3 min for 272 symbols).

Usage:
  python universe_builder.py --fetch         # fetch all instruments + OHLCV
  python universe_builder.py --query 2024-06-15  # show eligible universe for a date
  python universe_builder.py --matrix        # full date × symbol eligibility matrix
  python universe_builder.py --stats         # universe statistics over time
"""

import argparse
import os
import sqlite3
import time
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_PATH          = os.path.join("data", "universe.db")
BITMEX_API       = "https://www.bitmex.com/api/v1"

# Universe filters
SEASONING_DAYS   = 30       # ignore first 30 days after listing
MIN_DOLLAR_VOL   = 1_000_000  # $1M minimum 14-day avg daily dollar volume
VOLUME_LOOKBACK  = 14       # days for rolling volume calculation

# Fetch config
RATE_LIMIT_SEC   = 0.35     # BitMEX rate limit buffer
OHLCV_TIMEFRAME  = "1d"
OHLCV_LIMIT      = 1000     # max candles per request
EARLIEST_DATE    = "2021-01-01"  # don't fetch before this (most perps listed 2021+)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def _init_db() -> sqlite3.Connection:
    """Initialize universe.db with schema."""
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS instruments (
            symbol          TEXT PRIMARY KEY,
            underlying      TEXT,
            listing_date    TEXT,
            settlement_date TEXT,
            quote_currency  TEXT,
            state           TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_ohlcv (
            symbol  TEXT    NOT NULL,
            date    TEXT    NOT NULL,
            open    REAL,
            high    REAL,
            low     REAL,
            close   REAL,
            volume  REAL,
            PRIMARY KEY (symbol, date)
        )
    """)

    # Index for fast volume lookups
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ohlcv_date
        ON daily_ohlcv (date, symbol)
    """)

    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Instrument fetching
# ---------------------------------------------------------------------------

def fetch_instruments(conn: sqlite3.Connection):
    """Fetch all perpetual swap instruments from BitMEX (active + settled)."""
    print("Fetching instrument list from BitMEX...")
    all_data = []
    start = 0

    while True:
        r = requests.get(f"{BITMEX_API}/instrument", params={
            "count": 500, "start": start,
            "columns": "symbol,state,typ,listing,expiry,underlying,quoteCurrency",
        }, timeout=30)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        all_data.extend(batch)
        start += 500
        time.sleep(RATE_LIMIT_SEC)

    # Filter for perpetual swaps only (typ=FFWCSX)
    perps = [d for d in all_data if d.get("typ") == "FFWCSX"]
    print(f"  Total instruments: {len(all_data)}")
    print(f"  Perpetual swaps: {len(perps)}")

    inserted = 0
    for d in perps:
        listing = d.get("listing", "")[:10] if d.get("listing") else None
        settle = d.get("expiry", "")[:10] if d.get("expiry") else None

        conn.execute("""
            INSERT OR REPLACE INTO instruments
            (symbol, underlying, listing_date, settlement_date, quote_currency, state)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            d["symbol"],
            d.get("underlying"),
            listing,
            settle,
            d.get("quoteCurrency"),
            d.get("state"),
        ))
        inserted += 1

    conn.commit()

    # Stats
    active = sum(1 for d in perps if d.get("state") == "Open")
    settled = sum(1 for d in perps if d.get("state") == "Settled")
    print(f"  Active: {active}, Settled/delisted: {settled}")
    print(f"  Stored {inserted} instruments")

    return perps


# ---------------------------------------------------------------------------
# OHLCV fetching
# ---------------------------------------------------------------------------

def fetch_all_ohlcv(conn: sqlite3.Connection):
    """
    Fetch daily OHLCV for all perpetual swaps via BitMEX REST API.

    Uses /trade/bucketed endpoint directly (not ccxt) because ccxt only
    loads active markets — delisted symbols like LUNAUSD and FTTUSD would
    be unfetchable. The REST API works for all symbols including settled ones.
    """
    instruments = conn.execute(
        "SELECT symbol, listing_date, settlement_date FROM instruments"
    ).fetchall()
    print(f"\nFetching daily OHLCV for {len(instruments)} symbols...")

    total_fetched = 0
    skipped = 0
    errors = 0

    for idx, (symbol, listing, settle) in enumerate(instruments):
        # Check if already cached
        cached = conn.execute(
            "SELECT COUNT(*) FROM daily_ohlcv WHERE symbol = ?", (symbol,)
        ).fetchone()[0]

        if cached > 30:
            skipped += 1
            continue

        # Determine fetch range
        since_str = listing if listing and listing >= EARLIEST_DATE else EARLIEST_DATE

        print(f"  [{idx+1}/{len(instruments)}] {symbol} from {since_str}...",
              end="", flush=True)

        rows = _fetch_bucketed(conn, symbol, since_str)
        if rows >= 0:
            total_fetched += rows
            print(f" {rows} candles")
        else:
            errors += 1
            print(f" ERROR")

        time.sleep(RATE_LIMIT_SEC)

    print(f"\n  Total: {total_fetched:,} new candles, {skipped} already cached, "
          f"{errors} errors")


def _fetch_bucketed(conn: sqlite3.Connection, symbol: str,
                    start_date: str) -> int:
    """
    Fetch daily OHLCV from BitMEX /trade/bucketed endpoint.
    Works for all symbols including delisted/settled.
    Returns number of rows inserted, or -1 on error.
    """
    rows = 0
    start_time = f"{start_date}T00:00:00.000Z"

    try:
        while True:
            r = requests.get(f"{BITMEX_API}/trade/bucketed", params={
                "binSize": "1d",
                "symbol": symbol,
                "count": OHLCV_LIMIT,
                "startTime": start_time,
                "columns": "timestamp,open,high,low,close,volume",
            }, timeout=30)

            if r.status_code == 429:
                # Rate limited — wait and retry
                time.sleep(2)
                continue

            if r.status_code != 200:
                return -1

            batch = r.json()
            if not batch:
                break

            for candle in batch:
                ts = candle.get("timestamp", "")[:10]  # "2024-01-15"
                if not ts or candle.get("open") is None:
                    continue

                conn.execute("""
                    INSERT OR IGNORE INTO daily_ohlcv
                    (symbol, date, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (symbol, ts,
                      candle.get("open"), candle.get("high"),
                      candle.get("low"), candle.get("close"),
                      candle.get("volume", 0)))
                rows += 1

            conn.commit()

            # Next page: start after last candle
            last_ts = batch[-1].get("timestamp", "")
            if not last_ts:
                break
            start_time = last_ts  # BitMEX paginates by startTime

            if len(batch) < OHLCV_LIMIT:
                break

            time.sleep(RATE_LIMIT_SEC)

        return rows

    except Exception as e:
        print(f" {e}", end="")
        conn.commit()
        return -1


# ---------------------------------------------------------------------------
# Universe query
# ---------------------------------------------------------------------------

def get_universe(conn: sqlite3.Connection, date: str,
                 seasoning_days: int = SEASONING_DAYS,
                 min_dollar_vol: float = MIN_DOLLAR_VOL,
                 volume_lookback: int = VOLUME_LOOKBACK) -> list[dict]:
    """
    Return eligible symbols for a given date T.

    Gates:
      1. EXISTENCE:  listing_date <= T AND (settlement_date > T OR NULL)
      2. SEASONING:  listing_date <= T - seasoning_days
      3. LIQUIDITY:  14-day avg daily dollar volume >= min_dollar_vol
    """
    seasoning_cutoff = (datetime.strptime(date, "%Y-%m-%d") -
                        timedelta(days=seasoning_days)).strftime("%Y-%m-%d")
    vol_start = (datetime.strptime(date, "%Y-%m-%d") -
                 timedelta(days=volume_lookback)).strftime("%Y-%m-%d")

    rows = conn.execute("""
        SELECT
            i.symbol,
            i.underlying,
            i.listing_date,
            i.settlement_date,
            i.quote_currency,
            v.avg_dollar_vol,
            v.n_days
        FROM instruments i
        LEFT JOIN (
            SELECT
                symbol,
                AVG(volume * close) as avg_dollar_vol,
                COUNT(*) as n_days
            FROM daily_ohlcv
            WHERE date > ? AND date <= ?
            GROUP BY symbol
        ) v ON i.symbol = v.symbol
        WHERE i.listing_date <= ?
          AND (i.settlement_date > ? OR i.settlement_date IS NULL)
          AND COALESCE(v.avg_dollar_vol, 0) >= ?
          AND COALESCE(v.n_days, 0) >= ?
        ORDER BY v.avg_dollar_vol DESC
    """, (vol_start, date, seasoning_cutoff, date, min_dollar_vol,
          volume_lookback // 2))  # require at least half the lookback days

    return [
        {
            "symbol": r[0],
            "underlying": r[1],
            "listing_date": r[2],
            "settlement_date": r[3],
            "quote_currency": r[4],
            "avg_dollar_vol": r[5],
            "vol_days": r[6],
        }
        for r in rows
    ]


def get_universe_matrix(conn: sqlite3.Connection,
                        start_date: str = "2022-01-01",
                        end_date: str = None,
                        freq_days: int = 7) -> pd.DataFrame:
    """
    Build a date × symbol eligibility matrix (weekly intervals).
    Returns DataFrame: index=date, columns=symbol, values=1/0.
    """
    if end_date is None:
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    dates = pd.date_range(start_date, end_date, freq=f"{freq_days}D")
    all_symbols = set()
    date_universes = {}

    for dt in dates:
        date_str = dt.strftime("%Y-%m-%d")
        universe = get_universe(conn, date_str)
        symbols = [u["symbol"] for u in universe]
        date_universes[date_str] = symbols
        all_symbols.update(symbols)

    # Build matrix
    all_symbols = sorted(all_symbols)
    matrix = pd.DataFrame(0, index=[d.strftime("%Y-%m-%d") for d in dates],
                          columns=all_symbols)

    for date_str, symbols in date_universes.items():
        for sym in symbols:
            matrix.loc[date_str, sym] = 1

    return matrix


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def print_universe(conn: sqlite3.Connection, date: str):
    """Print the eligible universe for a specific date."""
    universe = get_universe(conn, date)
    w = 80
    print(f"\n{'='*w}")
    print(f"  ELIGIBLE UNIVERSE — {date}")
    print(f"  Filters: seasoning={SEASONING_DAYS}d, min_vol=${MIN_DOLLAR_VOL:,.0f}/day, "
          f"lookback={VOLUME_LOOKBACK}d")
    print(f"{'='*w}")
    print(f"\n  {len(universe)} eligible symbols:\n")

    print(f"  {'Symbol':<25} {'Underlying':>10} {'Listed':>12} "
          f"{'Settled':>12} {'AvgDolVol':>14}")
    print(f"  {'─'*75}")

    for u in universe:
        settle = u["settlement_date"] or "active"
        vol = f"${u['avg_dollar_vol']:>12,.0f}" if u["avg_dollar_vol"] else "N/A"
        print(f"  {u['symbol']:<25} {u['underlying']:>10} {u['listing_date']:>12} "
              f"{settle:>12} {vol}")

    # Sanity checks: show excluded symbols
    print(f"\n{'─'*w}")
    print("  EXCLUSION EXAMPLES (sanity check):")

    # Check LUNA
    luna = conn.execute(
        "SELECT symbol, listing_date, settlement_date FROM instruments "
        "WHERE symbol LIKE '%LUNA%'"
    ).fetchall()
    for sym, listing, settle in luna:
        included = any(u["symbol"] == sym for u in universe)
        print(f"    {sym}: listed={listing} settled={settle} → "
              f"{'INCLUDED' if included else 'EXCLUDED'}")

    # Check FTT
    ftt = conn.execute(
        "SELECT symbol, listing_date, settlement_date FROM instruments "
        "WHERE symbol LIKE '%FTT%'"
    ).fetchall()
    for sym, listing, settle in ftt:
        included = any(u["symbol"] == sym for u in universe)
        print(f"    {sym}: listed={listing} settled={settle} → "
              f"{'INCLUDED' if included else 'EXCLUDED'}")


def print_stats(conn: sqlite3.Connection):
    """Print universe statistics over time."""
    w = 80
    print(f"\n{'='*w}")
    print("  UNIVERSE STATISTICS — Point-in-Time Size Over Time")
    print(f"{'='*w}")

    # Sample dates: every quarter from 2022 to now
    dates = pd.date_range("2022-01-01", datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                          freq="QS")

    print(f"\n  {'Date':>12} {'Eligible':>10} {'Total Listed':>14} "
          f"{'Already Settled':>16} {'Filtered (vol)':>16}")
    print(f"  {'─'*70}")

    for dt in dates:
        date_str = dt.strftime("%Y-%m-%d")

        # Total listed at this date (no filters)
        total = conn.execute("""
            SELECT COUNT(*) FROM instruments
            WHERE listing_date <= ?
              AND (settlement_date > ? OR settlement_date IS NULL)
        """, (date_str, date_str)).fetchone()[0]

        # Already settled
        settled = conn.execute("""
            SELECT COUNT(*) FROM instruments
            WHERE settlement_date <= ?
        """, (date_str,)).fetchone()[0]

        # Eligible (with all filters)
        eligible = get_universe(conn, date_str)

        print(f"  {date_str:>12} {len(eligible):>10} {total:>14} "
              f"{settled:>16} {total - len(eligible):>16}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Universe Builder — Survivorship-Bias-Free Asset Lists")
    parser.add_argument("--fetch", action="store_true",
                        help="Fetch instruments + daily OHLCV from BitMEX")
    parser.add_argument("--query", type=str, metavar="DATE",
                        help="Show eligible universe for a specific date (YYYY-MM-DD)")
    parser.add_argument("--matrix", action="store_true",
                        help="Build and show full eligibility matrix")
    parser.add_argument("--stats", action="store_true",
                        help="Show universe size over time")
    args = parser.parse_args()

    conn = _init_db()

    if args.fetch:
        fetch_instruments(conn)
        fetch_all_ohlcv(conn)

    if args.query:
        print_universe(conn, args.query)

    if args.matrix:
        matrix = get_universe_matrix(conn)
        print(f"\nUniverse matrix: {matrix.shape[0]} dates × {matrix.shape[1]} symbols")
        # Show summary: avg eligible per date
        avg = matrix.sum(axis=1).mean()
        print(f"Average eligible per week: {avg:.1f}")
        # Save
        out_path = os.path.join("data", "universe_matrix.csv")
        matrix.to_csv(out_path)
        print(f"Saved to {out_path}")

    if args.stats:
        print_stats(conn)

    if not any([args.fetch, args.query, args.matrix, args.stats]):
        parser.print_help()

    conn.close()


if __name__ == "__main__":
    main()
