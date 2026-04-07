"""
fetch_data.py — V2 (Funding Rate Mean Reversion)

Fetches two data streams from BitMEX mainnet (public, no key):
  1. OHLCV candles  (5m raw → 15m resample)
  2. Funding rates  (8h intervals, settled rates only)

For live mode:  fetch_ohlcv() + fetch_current_funding()
For backtest:   fetch_all_ohlcv() + fetch_all_funding() → merge_funding()

merge_funding() uses pd.merge_asof(direction='backward') to guarantee
strict causal ordering: each 15m candle only sees the most recently
SETTLED funding rate, never the upcoming one.
"""

import os
import csv
import time
from datetime import datetime, timezone

import pandas as pd
from bitmex_client import get_data_client

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SYMBOL          = "BTC/USDT:USDT"
RAW_TIMEFRAME   = "5m"
TARGET_TF       = "15min"
LIMIT           = 100          # desired 15m output candles for live mode
RAW_LIMIT       = LIMIT * 3   # 300 x 5m → 100 x 15m

OUTPUT_DIR      = "data"
RAW_CACHE       = os.path.join(OUTPUT_DIR, "raw_candles.csv")
FUNDING_CACHE   = os.path.join(OUTPUT_DIR, "funding_history.csv")

# BitMEX max per request
FETCH_LIMIT     = 1000
FUNDING_LIMIT   = 500

# Earliest XBTUSDT candle on BitMEX live
EARLIEST_DATE   = "2024-01-12T00:00:00Z"

# ---------------------------------------------------------------------------
# Instrument configs for backtest (keyed by ccxt symbol)
# ---------------------------------------------------------------------------
INSTRUMENT_CONFIG = {
    "BTC/USDT:USDT": {
        "label":        "XBTUSDT",
        "earliest":     "2024-01-12T00:00:00Z",
        "ohlcv_cache":  os.path.join(OUTPUT_DIR, "raw_candles.csv"),
        "funding_cache": os.path.join(OUTPUT_DIR, "funding_history.csv"),
        "settle_hours": [0, 8, 16],
    },
    "BTC/USD:BTC": {
        "label":        "XBTUSD",
        "earliest":     "2020-01-01T00:00:00Z",
        "ohlcv_cache":  os.path.join(OUTPUT_DIR, "xbtusd_raw_candles.csv"),
        "funding_cache": os.path.join(OUTPUT_DIR, "xbtusd_funding_history.csv"),
        "settle_hours": [4, 12, 20],
    },
}


# ---------------------------------------------------------------------------
# 1. OHLCV — live mode (100 x 15m candles)
# ---------------------------------------------------------------------------

def fetch_ohlcv(exchange=None):
    """Fetch recent 5m candles from mainnet, resample to 15m. Returns DataFrame."""
    if exchange is None:
        try:
            exchange = get_data_client()
        except Exception as e:
            print(f"[ERROR] Failed to initialise data client: {e}")
            return None

    try:
        raw = exchange.fetch_ohlcv(SYMBOL, timeframe=RAW_TIMEFRAME, limit=RAW_LIMIT)
    except Exception as e:
        print(f"[ERROR] Failed to fetch OHLCV data from BitMEX: {e}")
        return None

    if not raw:
        print("[ERROR] No candles returned from exchange.")
        return None

    try:
        df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df = df.set_index("timestamp")

        # Drop malformed 5m candles
        bad = (
            (df["high"] < df["open"]) | (df["high"] < df["close"]) |
            (df["low"]  > df["open"]) | (df["low"]  > df["close"]) |
            (df["high"] < df["low"])
        )
        n_bad = int(bad.sum())
        if n_bad > 0:
            print(f"[INFO] Dropped {n_bad} malformed 5m candle(s) before resample.")
            df = df[~bad]

        if df.empty:
            print("[ERROR] All 5m candles failed sanity check.")
            return None

        resampled = df.resample(TARGET_TF).agg({
            "open": "first", "high": "max", "low": "min",
            "close": "last", "volume": "sum",
        }).dropna()

        return resampled.tail(LIMIT)

    except Exception as e:
        print(f"[ERROR] Failed to resample candles to 15m: {e}")
        return None


# ---------------------------------------------------------------------------
# 2. Funding rate — live mode (current predicted rate)
# ---------------------------------------------------------------------------

def fetch_current_funding(exchange=None):
    """
    Fetch the current predicted funding rate for live trading.
    Returns dict: {rate, next_timestamp, timestamp} or None on error.
    """
    if exchange is None:
        exchange = get_data_client()
    try:
        fr = exchange.fetch_funding_rate(SYMBOL)
        return {
            "rate":           fr.get("fundingRate"),
            "next_timestamp": fr.get("fundingTimestamp"),
            "timestamp":      fr.get("timestamp"),
            "datetime":       fr.get("datetime"),
        }
    except Exception as e:
        print(f"[ERROR] Failed to fetch funding rate: {e}")
        return None


def fetch_recent_funding(exchange=None, count=10):
    """
    Fetch the last `count` settled funding rates.
    Returns DataFrame with columns: [timestamp, rate].
    Used by the live bot to build a short funding history window.
    """
    if exchange is None:
        exchange = get_data_client()
    try:
        history = exchange.fetch_funding_rate_history(SYMBOL, limit=count)
        if not history:
            return pd.DataFrame(columns=["timestamp", "rate"])
        rows = []
        for h in history:
            rows.append({
                "timestamp": pd.Timestamp(h["datetime"], tz="UTC"),
                "rate":      h["fundingRate"],
            })
        df = pd.DataFrame(rows).sort_values("timestamp").reset_index(drop=True)
        return df
    except Exception as e:
        print(f"[ERROR] Failed to fetch funding history: {e}")
        return pd.DataFrame(columns=["timestamp", "rate"])


# ---------------------------------------------------------------------------
# 3. Full historical fetch — for backtest
# ---------------------------------------------------------------------------

def fetch_all_ohlcv(exchange=None, use_cache=True, symbol=None):
    """
    Page through ALL 5m OHLCV from earliest date to now.
    Returns list of [ts_ms, o, h, l, c, v] rows.
    Caches to instrument-specific CSV.
    """
    sym = symbol or SYMBOL
    cfg = INSTRUMENT_CONFIG.get(sym, INSTRUMENT_CONFIG[SYMBOL])
    cache_path = cfg["ohlcv_cache"]
    earliest = cfg["earliest"]

    if use_cache and os.path.exists(cache_path):
        rows = _load_ohlcv_cache(cache_path)
        if rows:
            print(f"[CACHE] Loaded {len(rows):,} raw 5m candles from {cache_path}")
            return rows

    if exchange is None:
        exchange = get_data_client()

    since    = exchange.parse8601(earliest)
    step_ms  = 5 * 60 * 1000
    all_rows = []
    page     = 0

    print(f"Fetching 5m candles from BitMEX live for {sym}...")
    while True:
        try:
            batch = exchange.fetch_ohlcv(sym, timeframe=RAW_TIMEFRAME,
                                         since=since, limit=FETCH_LIMIT)
        except Exception as e:
            print(f"[ERROR] Fetch failed at page {page}: {e}")
            break

        if not batch:
            break

        all_rows.extend(batch)
        page += 1
        since = batch[-1][0] + step_ms

        if page % 50 == 0:
            ts_str = datetime.fromtimestamp(batch[-1][0] / 1000, tz=timezone.utc
                                            ).strftime("%Y-%m-%d %H:%M")
            print(f"  ... page {page:4d} | up to {ts_str} | {len(all_rows):,} candles")

        if len(batch) < FETCH_LIMIT:
            break

    print(f"[OK] Fetched {len(all_rows):,} raw 5m candles ({page} pages).\n")
    _save_ohlcv_cache(all_rows, cache_path)
    return all_rows


def fetch_all_funding(exchange=None, use_cache=True, symbol=None):
    """
    Page through ALL historical funding rates from earliest date to now.
    Returns DataFrame with columns: [timestamp, rate].
    Caches to instrument-specific CSV.
    """
    sym = symbol or SYMBOL
    cfg = INSTRUMENT_CONFIG.get(sym, INSTRUMENT_CONFIG[SYMBOL])
    cache_path = cfg["funding_cache"]
    earliest = cfg["earliest"]

    if use_cache and os.path.exists(cache_path):
        df = _load_funding_cache(cache_path)
        if df is not None and not df.empty:
            print(f"[CACHE] Loaded {len(df):,} funding rates from {cache_path}")
            return df

    if exchange is None:
        exchange = get_data_client()

    since    = exchange.parse8601(earliest)
    all_rows = []
    page     = 0

    print(f"Fetching funding rate history for {sym}...")
    while True:
        try:
            batch = exchange.fetch_funding_rate_history(
                sym, since=since, limit=FUNDING_LIMIT)
        except Exception as e:
            print(f"[ERROR] Funding fetch failed at page {page}: {e}")
            break

        if not batch:
            break

        all_rows.extend(batch)
        page += 1
        since = batch[-1]["timestamp"] + 1  # ms after last

        if page % 20 == 0:
            print(f"  ... page {page:4d} | {len(all_rows):,} rates")

        if len(batch) < FUNDING_LIMIT:
            break
        time.sleep(0.2)  # rate limit courtesy

    print(f"[OK] Fetched {len(all_rows):,} funding rates ({page} pages).\n")

    rows = []
    for h in all_rows:
        rows.append({
            "timestamp": pd.Timestamp(h["datetime"], tz="UTC"),
            "rate":      h["fundingRate"],
        })
    df = pd.DataFrame(rows).sort_values("timestamp").reset_index(drop=True)
    _save_funding_cache(df, cache_path)
    return df


# ---------------------------------------------------------------------------
# 4. Merge — causal alignment (backward only)
# ---------------------------------------------------------------------------

def merge_funding(df_ohlcv: pd.DataFrame, df_funding: pd.DataFrame) -> pd.DataFrame:
    """
    Merge funding rates into OHLCV using merge_asof(direction='backward').

    Each 15m candle gets the MOST RECENTLY SETTLED funding rate.
    A 14:15 candle sees the 08:00 rate, never the 16:00 rate.
    This guarantees zero look-ahead bias.

    Adds columns: funding_rate, funding_cumsum_24h (rolling 24h sum = 3 rates).
    """
    # Ensure both have timezone-aware datetime index/column
    ohlcv = df_ohlcv.copy()
    if not isinstance(ohlcv.index, pd.DatetimeIndex):
        raise ValueError("df_ohlcv must have a DatetimeIndex")

    ohlcv = ohlcv.reset_index()
    ohlcv.rename(columns={ohlcv.columns[0]: "timestamp"}, inplace=True)
    ohlcv["timestamp"] = pd.to_datetime(ohlcv["timestamp"], utc=True)

    funding = df_funding.copy()
    funding["timestamp"] = pd.to_datetime(funding["timestamp"], utc=True)
    funding = funding.sort_values("timestamp")

    # Normalize datetime resolution to avoid merge_asof dtype mismatch
    ohlcv["timestamp"] = ohlcv["timestamp"].astype("datetime64[us, UTC]")
    funding["timestamp"] = funding["timestamp"].astype("datetime64[us, UTC]")

    merged = pd.merge_asof(
        ohlcv.sort_values("timestamp"),
        funding[["timestamp", "rate"]],
        on="timestamp",
        direction="backward",
    )

    merged = merged.rename(columns={"rate": "funding_rate"})
    merged = merged.set_index("timestamp")

    # Forward-fill any NaN funding rates (before first funding observation)
    merged["funding_rate"] = merged["funding_rate"].ffill()

    # Cumulative 24h funding = sum of last 3 settled rates (8h * 3 = 24h)
    # We compute this from the funding df directly, then merge back
    funding_cum = funding.copy()
    funding_cum["funding_24h"] = funding_cum["rate"].rolling(3, min_periods=1).sum()

    merged2 = merged.reset_index()
    merged2["timestamp"] = merged2["timestamp"].astype("datetime64[us, UTC]")
    funding_cum["timestamp"] = funding_cum["timestamp"].astype("datetime64[us, UTC]")
    merged2 = pd.merge_asof(
        merged2.sort_values("timestamp"),
        funding_cum[["timestamp", "funding_24h"]],
        on="timestamp",
        direction="backward",
    )
    merged2 = merged2.set_index("timestamp")
    merged2["funding_24h"] = merged2["funding_24h"].ffill()

    return merged2


# ---------------------------------------------------------------------------
# 5. Resample helper (for backtest — same logic as live)
# ---------------------------------------------------------------------------

def resample_to_15m(raw: list) -> pd.DataFrame:
    """Convert raw 5m OHLCV list to 15m DataFrame."""
    df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df.set_index("timestamp")

    resampled = df.resample(TARGET_TF).agg({
        "open": "first", "high": "max", "low": "min",
        "close": "last", "volume": "sum",
    }).dropna()

    # Repair impossible candles
    impossible = resampled["high"] < resampled["low"]
    if impossible.any():
        resampled = resampled[~impossible]

    oc_max = resampled[["open", "close"]].max(axis=1)
    oc_min = resampled[["open", "close"]].min(axis=1)
    resampled["high"] = resampled["high"].clip(lower=oc_max)
    resampled["low"]  = resampled["low"].clip(upper=oc_min)

    return resampled


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _load_ohlcv_cache(path=None):
    path = path or RAW_CACHE
    try:
        rows = []
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            next(reader)
            for row in reader:
                rows.append([int(row[0]), float(row[1]), float(row[2]),
                             float(row[3]), float(row[4]), float(row[5])])
        return rows
    except Exception as e:
        print(f"[WARN] OHLCV cache read failed: {e}")
        return None


def _save_ohlcv_cache(raw, path=None):
    path = path or RAW_CACHE
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["timestamp_ms", "open", "high", "low", "close", "volume"])
            writer.writerows(raw)
        print(f"[CACHE] Saved {len(raw):,} candles to {path}")
    except Exception as e:
        print(f"[WARN] Could not save OHLCV cache: {e}")


def _load_funding_cache(path=None):
    path = path or FUNDING_CACHE
    try:
        df = pd.read_csv(path, parse_dates=["timestamp"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df
    except Exception as e:
        print(f"[WARN] Funding cache read failed: {e}")
        return None


def _save_funding_cache(df, path=None):
    path = path or FUNDING_CACHE
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        df.to_csv(path, index=False)
        print(f"[CACHE] Saved {len(df):,} funding rates to {path}")
    except Exception as e:
        print(f"[WARN] Could not save funding cache: {e}")


# ---------------------------------------------------------------------------
# Aliases
# ---------------------------------------------------------------------------
get_candles = fetch_ohlcv


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=== fetch_data.py — V2 (Funding Rate) ===\n")

    ex = get_data_client()

    # Live OHLCV
    df = fetch_ohlcv(ex)
    if df is not None:
        print(f"[OK] {len(df)} x 15m candles  ({df.index[0]} → {df.index[-1]})")

    # Current funding
    fr = fetch_current_funding(ex)
    if fr:
        print(f"[OK] Current predicted funding rate: {fr['rate']}")

    # Recent funding history
    hist = fetch_recent_funding(ex, count=10)
    if not hist.empty:
        print(f"\nRecent settled funding rates:")
        for _, row in hist.iterrows():
            print(f"  {row['timestamp']}  {row['rate']:+.6f}  ({row['rate']*100:+.4f}%)")
