"""
fetch_coinalyze.py — Historical OI + Liquidation data from Coinalyze

Fetches DAILY aggregated global Open Interest and Liquidation data
for BTC perpetual futures across all major exchanges.

Daily data is retained for 3+ years on Coinalyze, covering multiple
market regimes (bull, bear, consolidation).

Usage:
  from fetch_coinalyze import fetch_daily_oi, fetch_daily_liquidations, merge_oi_liq

Data is cached to CSV for fast re-use.
"""

import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Major BTC perpetual symbols — fetched individually and summed for global OI
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

BASE_URL   = "https://api.coinalyze.net/v1"
CRED_PATH  = os.path.expanduser("~/.openclaw/workspace/.credentials")
OUTPUT_DIR = "data"

OI_CACHE   = os.path.join(OUTPUT_DIR, "coinalyze_daily_oi.csv")
LIQ_CACHE  = os.path.join(OUTPUT_DIR, "coinalyze_daily_liq.csv")

RATE_LIMIT_SLEEP = 1.6


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


def _fetch(endpoint: str, params: dict, headers: dict) -> list:
    for attempt in range(3):
        r = requests.get(f"{BASE_URL}/{endpoint}", headers=headers, params=params)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", 60))
            print(f"[429] Rate limited. Sleeping {retry_after}s...")
            time.sleep(retry_after)
            continue
        print(f"[ERROR] {endpoint}: HTTP {r.status_code} — {r.text[:200]}")
        return []
    return []


# ---------------------------------------------------------------------------
# Daily OI — aggregated across all major exchanges
# ---------------------------------------------------------------------------

def fetch_daily_oi(use_cache=True) -> pd.DataFrame:
    """
    Fetch daily aggregated global OI for BTC perpetuals.
    Returns DataFrame: [timestamp, oi_open, oi_high, oi_low, oi_close]
    """
    if use_cache and os.path.exists(OI_CACHE):
        df = pd.read_csv(OI_CACHE, parse_dates=["timestamp"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        print(f"[CACHE] Loaded {len(df):,} daily OI rows from {OI_CACHE}")
        return df

    api_key = _load_api_key()
    headers = {"api_key": api_key}
    now = int(datetime.now(timezone.utc).timestamp())
    since = now - (3 * 365 * 86400)  # 3 years

    symbols_csv = ",".join(BTC_PERP_SYMBOLS)
    params = {
        "symbols": symbols_csv,
        "interval": "daily",
        "from": since,
        "to": now,
    }

    print(f"Fetching daily OI for {len(BTC_PERP_SYMBOLS)} BTC perp symbols...")
    data = _fetch("open-interest-history", params, headers)

    if not data:
        print("[ERROR] No OI data returned")
        return pd.DataFrame()

    # Aggregate: sum OI across all exchanges per timestamp
    ts_map = {}  # {timestamp: {oi_open: sum, oi_high: sum, ...}}
    for entry in data:
        sym = entry.get("symbol", "?")
        hist = entry.get("history", [])
        print(f"  {sym}: {len(hist)} entries")
        for h in hist:
            t = h["t"]
            if t not in ts_map:
                ts_map[t] = {"oi_open": 0, "oi_high": 0, "oi_low": 0, "oi_close": 0}
            ts_map[t]["oi_open"]  += h.get("o", 0)
            ts_map[t]["oi_high"]  += h.get("h", 0)
            ts_map[t]["oi_low"]   += h.get("l", 0)
            ts_map[t]["oi_close"] += h.get("c", 0)

    rows = []
    for t, vals in sorted(ts_map.items()):
        rows.append({
            "timestamp": pd.Timestamp(t, unit="s", tz="UTC"),
            **vals,
        })

    df = pd.DataFrame(rows)
    print(f"[OK] {len(df):,} daily aggregated OI rows "
          f"({df['timestamp'].min().date()} → {df['timestamp'].max().date()})")

    # Cache
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(OI_CACHE, index=False)
    print(f"[CACHE] Saved to {OI_CACHE}")
    return df


# ---------------------------------------------------------------------------
# Daily Liquidations — aggregated across all major exchanges
# ---------------------------------------------------------------------------

def fetch_daily_liquidations(use_cache=True) -> pd.DataFrame:
    """
    Fetch daily aggregated liquidation data for BTC perpetuals.
    Returns DataFrame: [timestamp, liq_long, liq_short, liq_total]
    """
    if use_cache and os.path.exists(LIQ_CACHE):
        df = pd.read_csv(LIQ_CACHE, parse_dates=["timestamp"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        print(f"[CACHE] Loaded {len(df):,} daily liquidation rows from {LIQ_CACHE}")
        return df

    api_key = _load_api_key()
    headers = {"api_key": api_key}
    now = int(datetime.now(timezone.utc).timestamp())
    since = now - (3 * 365 * 86400)

    symbols_csv = ",".join(BTC_PERP_SYMBOLS)
    params = {
        "symbols": symbols_csv,
        "interval": "daily",
        "from": since,
        "to": now,
    }

    print(f"Fetching daily liquidations for {len(BTC_PERP_SYMBOLS)} BTC perp symbols...")
    data = _fetch("liquidation-history", params, headers)

    if not data:
        print("[ERROR] No liquidation data returned")
        return pd.DataFrame()

    ts_map = {}
    for entry in data:
        sym = entry.get("symbol", "?")
        hist = entry.get("history", [])
        print(f"  {sym}: {len(hist)} entries")
        for h in hist:
            t = h["t"]
            if t not in ts_map:
                ts_map[t] = {"liq_long": 0, "liq_short": 0}
            ts_map[t]["liq_long"]  += h.get("l", 0)
            ts_map[t]["liq_short"] += h.get("s", 0)

    rows = []
    for t, vals in sorted(ts_map.items()):
        rows.append({
            "timestamp": pd.Timestamp(t, unit="s", tz="UTC"),
            "liq_long":  vals["liq_long"],
            "liq_short": vals["liq_short"],
            "liq_total": vals["liq_long"] + vals["liq_short"],
        })

    df = pd.DataFrame(rows)
    print(f"[OK] {len(df):,} daily aggregated liquidation rows "
          f"({df['timestamp'].min().date()} → {df['timestamp'].max().date()})")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(LIQ_CACHE, index=False)
    print(f"[CACHE] Saved to {LIQ_CACHE}")
    return df


# ---------------------------------------------------------------------------
# Merge into OHLCV DataFrame (same merge_asof pattern as funding)
# ---------------------------------------------------------------------------

def merge_oi_liq(df_ohlcv: pd.DataFrame,
                 df_oi: pd.DataFrame,
                 df_liq: pd.DataFrame) -> pd.DataFrame:
    """
    Merge daily OI and liquidation data into the candle DataFrame
    using merge_asof(direction='backward'). Zero look-ahead bias.

    Adds columns: oi_close, oi_delta (daily change), oi_delta_pct,
                  liq_long, liq_short, liq_total
    """
    ohlcv = df_ohlcv.copy()
    if not isinstance(ohlcv.index, pd.DatetimeIndex):
        raise ValueError("df_ohlcv must have a DatetimeIndex")

    ohlcv = ohlcv.reset_index()
    ohlcv.rename(columns={ohlcv.columns[0]: "timestamp"}, inplace=True)
    ohlcv["timestamp"] = pd.to_datetime(ohlcv["timestamp"], utc=True)

    # Prepare OI with delta columns
    oi = df_oi.copy()
    oi["timestamp"] = pd.to_datetime(oi["timestamp"], utc=True)
    oi = oi.sort_values("timestamp")
    oi["oi_delta"]     = oi["oi_close"].diff()
    oi["oi_delta_pct"] = oi["oi_close"].pct_change()

    # Prepare liquidations
    liq = df_liq.copy()
    liq["timestamp"] = pd.to_datetime(liq["timestamp"], utc=True)
    liq = liq.sort_values("timestamp")

    # Normalize datetime resolution
    ohlcv["timestamp"] = ohlcv["timestamp"].astype("datetime64[us, UTC]")
    oi["timestamp"]    = oi["timestamp"].astype("datetime64[us, UTC]")
    liq["timestamp"]   = liq["timestamp"].astype("datetime64[us, UTC]")

    # Merge OI
    merged = pd.merge_asof(
        ohlcv.sort_values("timestamp"),
        oi[["timestamp", "oi_close", "oi_delta", "oi_delta_pct"]],
        on="timestamp",
        direction="backward",
    )

    # Merge liquidations
    merged = pd.merge_asof(
        merged.sort_values("timestamp"),
        liq[["timestamp", "liq_long", "liq_short", "liq_total"]],
        on="timestamp",
        direction="backward",
    )

    merged = merged.set_index("timestamp")

    # Forward-fill NaN (before first OI/liq observation)
    for col in ["oi_close", "oi_delta", "oi_delta_pct",
                "liq_long", "liq_short", "liq_total"]:
        if col in merged.columns:
            merged[col] = merged[col].ffill()

    return merged


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== fetch_coinalyze.py — Daily OI + Liquidations ===\n")

    df_oi = fetch_daily_oi(use_cache=False)
    if not df_oi.empty:
        print(f"\nOI stats:")
        print(f"  Range: {df_oi['timestamp'].min().date()} → {df_oi['timestamp'].max().date()}")
        print(f"  Current global OI: {df_oi['oi_close'].iloc[-1]:,.0f}")
        print(f"  Max global OI:     {df_oi['oi_close'].max():,.0f}")
        print(f"  Min global OI:     {df_oi['oi_close'].min():,.0f}")

    print()
    df_liq = fetch_daily_liquidations(use_cache=False)
    if not df_liq.empty:
        print(f"\nLiquidation stats:")
        print(f"  Range: {df_liq['timestamp'].min().date()} → {df_liq['timestamp'].max().date()}")
        print(f"  Avg daily total: {df_liq['liq_total'].mean():,.0f}")
        print(f"  Max daily total: {df_liq['liq_total'].max():,.0f}")
        print(f"  Top 5 liquidation days:")
        top5 = df_liq.nlargest(5, "liq_total")
        for _, row in top5.iterrows():
            print(f"    {row['timestamp'].date()}  "
                  f"L={row['liq_long']:>12,.0f}  S={row['liq_short']:>12,.0f}  "
                  f"Total={row['liq_total']:>12,.0f}")
