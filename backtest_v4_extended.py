"""
backtest_v4_extended.py — V4 Backtest on Extended Data (2019-2026)

Runs the V4 Trend-Following Liquidation Dip-Buy backtest using:
  - BitMEX XBTUSD OHLCV + funding (fetch_data.py, 2020+)
  - BitMEX public liquidation dumps (bitmex_public_fetcher.py, 2019+)
  - Binance OI as proxy (binance_data_fetcher.py, when Coinalyze unavailable)

Critical constraint: ALL parameters are frozen from backtest_v3.py.
No parameter changes allowed. The purpose is to expand N, not to re-optimize.

Pre-registration: Parameter hash is computed and logged to data/param_registry.json
BEFORE any out-of-sample data is evaluated.

Usage:
  python backtest_v4_extended.py [--refresh]
"""

import argparse
import csv
import hashlib
import json
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from fetch_data import fetch_all_ohlcv, fetch_all_funding, INSTRUMENT_CONFIG

# Import V4 signal logic and constants (FROZEN — do not modify)
from backtest_v3 import (
    get_daily_signal, run_backtest, print_report, save_csv,
    EMA_PERIOD, FUNDING_THRESHOLD, FUNDING_LOOKBACK_D,
    LIQ_SPIKE_MULT, LIQ_LOOKBACK, LIQ_LONG_DOM,
    USE_OI_CONFIRM, ATR_PERIOD, SL_ATR_MULT, TARGET_RR,
    SLIPPAGE_PCT, SLIPPAGE_EXIT_PCT,
    INITIAL_BALANCE, RISK_PCT, MIN_WARMUP,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
OUTPUT_DIR     = "data"
PARAM_REGISTRY = os.path.join(OUTPUT_DIR, "param_registry.json")
TRADES_CSV     = os.path.join(OUTPUT_DIR, "backtest_v4_extended_trades.csv")
ANALYSIS_FILE  = os.path.join(OUTPUT_DIR, "backtest_v4_extended_analysis.txt")

# Data sources
BITMEX_HIST_DB = os.path.join("data", "bitmex_historical.db")
BINANCE_DB     = os.path.join("data", "binance_historical.db")
COINALYZE_OI   = os.path.join("data", "coinalyze_daily_oi.csv")
COINALYZE_LIQ  = os.path.join("data", "coinalyze_daily_liq.csv")


# ---------------------------------------------------------------------------
# Pre-registration
# ---------------------------------------------------------------------------
def _pre_register_params() -> str:
    """
    Hash all V4 parameters and log to registry BEFORE evaluating OOS data.
    This prevents post-hoc parameter fitting (audit item #2).
    """
    params = {
        "EMA_PERIOD": EMA_PERIOD,
        "FUNDING_THRESHOLD": FUNDING_THRESHOLD,
        "FUNDING_LOOKBACK_D": FUNDING_LOOKBACK_D,
        "LIQ_SPIKE_MULT": LIQ_SPIKE_MULT,
        "LIQ_LOOKBACK": LIQ_LOOKBACK,
        "LIQ_LONG_DOM": LIQ_LONG_DOM,
        "USE_OI_CONFIRM": USE_OI_CONFIRM,
        "ATR_PERIOD": ATR_PERIOD,
        "SL_ATR_MULT": SL_ATR_MULT,
        "TARGET_RR": TARGET_RR,
        "SLIPPAGE_PCT": SLIPPAGE_PCT,
        "SLIPPAGE_EXIT_PCT": SLIPPAGE_EXIT_PCT,
        "RISK_PCT": RISK_PCT,
    }

    # Deterministic hash
    param_str = json.dumps(params, sort_keys=True)
    param_hash = hashlib.sha256(param_str.encode()).hexdigest()[:16]

    # Log to registry
    registry = []
    if os.path.exists(PARAM_REGISTRY):
        with open(PARAM_REGISTRY) as f:
            registry = json.load(f)

    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "hash": param_hash,
        "params": params,
        "script": "backtest_v4_extended.py",
    }
    registry.append(entry)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(PARAM_REGISTRY, "w") as f:
        json.dump(registry, f, indent=2)

    print(f"[PRE-REGISTER] Parameter hash: {param_hash}")
    print(f"  Logged to {PARAM_REGISTRY}")
    print(f"  Parameters frozen — no modifications allowed after this point.")
    return param_hash


# ---------------------------------------------------------------------------
# Data building — extended dataset
# ---------------------------------------------------------------------------
def _load_bitmex_liquidations() -> pd.DataFrame:
    """Load daily liquidation data from BitMEX public dumps."""
    import sqlite3

    if not os.path.exists(BITMEX_HIST_DB):
        print(f"[WARN] BitMEX historical DB not found: {BITMEX_HIST_DB}")
        print("       Run: python bitmex_public_fetcher.py --start 2019-01-01")
        return pd.DataFrame()

    conn = sqlite3.connect(BITMEX_HIST_DB, timeout=5)
    df = pd.read_sql_query(
        "SELECT date, liq_long, liq_short, liq_total FROM bitmex_liquidations_daily "
        "ORDER BY date", conn, parse_dates=["date"]
    )
    conn.close()

    if not df.empty:
        df["timestamp"] = df["date"]
        df = df.set_index("timestamp")
        df.index = pd.to_datetime(df.index, utc=True)
        print(f"[OK] BitMEX public liquidations: {len(df):,} days "
              f"({df.index[0].date()} → {df.index[-1].date()})")

    return df


def _load_coinalyze_liquidations() -> pd.DataFrame:
    """Load Coinalyze daily liquidation data (if available)."""
    if not os.path.exists(COINALYZE_LIQ):
        return pd.DataFrame()

    try:
        from fetch_coinalyze import fetch_daily_liquidations
        df = fetch_daily_liquidations(use_cache=True)
        if not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            df = df.set_index("timestamp")
            print(f"[OK] Coinalyze liquidations: {len(df):,} days "
                  f"({df.index[0].date()} → {df.index[-1].date()})")
        return df
    except Exception as e:
        print(f"[WARN] Could not load Coinalyze liquidations: {e}")
        return pd.DataFrame()


def _load_binance_oi() -> pd.DataFrame:
    """Load Binance daily OI as proxy."""
    import sqlite3

    if not os.path.exists(BINANCE_DB):
        print(f"[WARN] Binance DB not found: {BINANCE_DB}")
        print("       Run: python binance_data_fetcher.py --start 2019-09-01")
        return pd.DataFrame()

    conn = sqlite3.connect(BINANCE_DB, timeout=5)
    df = pd.read_sql_query(
        "SELECT timestamp, sum_oi_value FROM binance_oi_daily "
        "WHERE symbol = 'BTCUSDT' ORDER BY timestamp", conn
    )
    conn.close()

    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df = df.set_index("timestamp")
        df = df.rename(columns={"sum_oi_value": "oi_close"})
        print(f"[OK] Binance OI: {len(df):,} days "
              f"({df.index[0].date()} → {df.index[-1].date()})")

    return df


def _load_coinalyze_oi() -> pd.DataFrame:
    """Load Coinalyze daily OI (if available)."""
    if not os.path.exists(COINALYZE_OI):
        return pd.DataFrame()

    try:
        from fetch_coinalyze import fetch_daily_oi
        df = fetch_daily_oi(use_cache=True)
        if not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            df = df.set_index("timestamp")
            print(f"[OK] Coinalyze OI: {len(df):,} days "
                  f"({df.index[0].date()} → {df.index[-1].date()})")
        return df
    except Exception as e:
        print(f"[WARN] Could not load Coinalyze OI: {e}")
        return pd.DataFrame()


def build_extended_dataset(use_cache: bool = True) -> pd.DataFrame:
    """
    Build merged daily DataFrame using all available data sources.
    Priority: Coinalyze > BitMEX public (for liquidations), Coinalyze > Binance (for OI).
    Falls back to secondary sources for dates without primary coverage.
    """
    symbol = "BTC/USD:BTC"  # XBTUSD inverse

    # 1. OHLCV (BitMEX XBTUSD, 2020+)
    raw = fetch_all_ohlcv(use_cache=use_cache, symbol=symbol)
    if not raw:
        raise SystemExit("[ABORT] No OHLCV data")

    df_5m = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df_5m["timestamp"] = pd.to_datetime(df_5m["timestamp"], unit="ms", utc=True)
    df_5m = df_5m.set_index("timestamp").sort_index()

    daily = df_5m.resample("1D").agg({
        "open": "first", "high": "max", "low": "min",
        "close": "last", "volume": "sum",
    }).dropna()
    print(f"[OK] {len(daily):,} daily OHLCV bars "
          f"({daily.index[0].date()} → {daily.index[-1].date()})")

    # 2. Funding rates
    df_fund = fetch_all_funding(use_cache=use_cache, symbol=symbol)
    if df_fund is None or df_fund.empty:
        raise SystemExit("[ABORT] No funding data")
    df_fund = df_fund.copy()
    df_fund["timestamp"] = pd.to_datetime(df_fund["timestamp"], utc=True)
    df_fund = df_fund.set_index("timestamp").sort_index()

    funding_daily = df_fund["rate"].resample("1D").agg(["mean", "max", "min", "count"])
    funding_daily.columns = ["funding_avg", "funding_max", "funding_min", "funding_count"]
    funding_daily["funding_peak_pos"] = funding_daily["funding_max"].rolling(
        FUNDING_LOOKBACK_D, min_periods=1).max()
    print(f"[OK] {len(funding_daily):,} daily funding observations")

    # 3. Liquidation data — merge sources with priority
    df_bitmex_liq = _load_bitmex_liquidations()
    df_coinalyze_liq = _load_coinalyze_liquidations()

    # Use Coinalyze where available (multi-exchange), fill gaps with BitMEX-only
    if not df_coinalyze_liq.empty and not df_bitmex_liq.empty:
        # Coinalyze has priority; fill earlier dates with BitMEX
        cz_start = df_coinalyze_liq.index[0]
        df_liq_early = df_bitmex_liq[df_bitmex_liq.index < cz_start]
        df_liq = pd.concat([df_liq_early, df_coinalyze_liq]).sort_index()
        print(f"[OK] Combined liquidations: {len(df_liq):,} days "
              f"(BitMEX-only before {cz_start.date()}, Coinalyze after)")
    elif not df_coinalyze_liq.empty:
        df_liq = df_coinalyze_liq
    elif not df_bitmex_liq.empty:
        df_liq = df_bitmex_liq
    else:
        raise SystemExit("[ABORT] No liquidation data. Run bitmex_public_fetcher.py first.")

    # 4. OI data — merge sources with priority
    df_coinalyze_oi = _load_coinalyze_oi()
    df_binance_oi = _load_binance_oi()

    if not df_coinalyze_oi.empty and not df_binance_oi.empty:
        cz_start = df_coinalyze_oi.index[0]
        df_oi_early = df_binance_oi[df_binance_oi.index < cz_start]
        df_oi = pd.concat([df_oi_early, df_coinalyze_oi]).sort_index()
        print(f"[OK] Combined OI: {len(df_oi):,} days "
              f"(Binance before {cz_start.date()}, Coinalyze after)")
    elif not df_coinalyze_oi.empty:
        df_oi = df_coinalyze_oi
    elif not df_binance_oi.empty:
        df_oi = df_binance_oi
    else:
        print("[WARN] No OI data available. OI confirmation will be disabled.")
        df_oi = pd.DataFrame()

    # 5. Merge into daily dataset
    merged = daily.copy()
    merged.index = merged.index.normalize()
    funding_daily.index = funding_daily.index.normalize()
    df_liq.index = df_liq.index.normalize()

    merged = merged.join(funding_daily[["funding_avg", "funding_max", "funding_peak_pos"]],
                         how="left")
    merged = merged.join(df_liq[["liq_long", "liq_short", "liq_total"]], how="left")

    if not df_oi.empty:
        df_oi.index = df_oi.index.normalize()
        merged = merged.join(df_oi[["oi_close"]], how="left")
    else:
        merged["oi_close"] = np.nan

    # 6. Derived columns (same as backtest_v3.py)
    merged["ema200"] = merged["close"].ewm(span=EMA_PERIOD, adjust=False).mean()
    merged["bull_regime"] = merged["close"] > merged["ema200"]

    merged["tr"] = np.maximum(
        merged["high"] - merged["low"],
        np.maximum(
            abs(merged["high"] - merged["close"].shift(1)),
            abs(merged["low"] - merged["close"].shift(1))
        )
    )
    merged["atr"] = merged["tr"].rolling(ATR_PERIOD).mean()

    merged["oi_delta_pct"] = merged["oi_close"].pct_change()
    merged["liq_long_avg"] = merged["liq_long"].rolling(LIQ_LOOKBACK).mean()
    merged["liq_long_ratio"] = merged["liq_long"] / merged["liq_long_avg"]
    merged["liq_avg"] = merged["liq_total"].rolling(LIQ_LOOKBACK).mean()
    merged["liq_ratio"] = merged["liq_total"] / merged["liq_avg"]
    merged["liq_long_pct"] = merged["liq_long"] / merged["liq_total"]

    merged = merged.dropna(subset=["liq_long_avg", "atr", "funding_avg", "ema200"])

    print(f"\n[OK] Extended daily dataset: {len(merged):,} rows "
          f"({merged.index[0].date()} → {merged.index[-1].date()})")

    # Summary stats
    bull_days = merged["bull_regime"].sum()
    setup_days = (merged["funding_peak_pos"] > FUNDING_THRESHOLD).sum()
    liq_spike_days = (merged["liq_long_ratio"] >= LIQ_SPIKE_MULT).sum()
    both = ((merged["funding_peak_pos"] > FUNDING_THRESHOLD) &
            (merged["liq_long_ratio"] >= LIQ_SPIKE_MULT) &
            merged["bull_regime"]).sum()

    print(f"\n     Bull regime days (close > EMA200):        {bull_days}")
    print(f"     Positive funding setup days ({FUNDING_LOOKBACK_D}d):      {setup_days}")
    print(f"     Long liq spike days (>={LIQ_SPIKE_MULT}x avg):        {liq_spike_days}")
    print(f"     All conditions met (bull+fund+liq):       {both}")

    return merged


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="V4 extended backtest (2019-2026) with frozen parameters")
    parser.add_argument("--refresh", action="store_true",
                        help="Re-fetch data from exchanges")
    args = parser.parse_args()

    print("=" * 64)
    print("  V4 EXTENDED BACKTEST — 2019-2026")
    print("  Parameters: FROZEN from backtest_v3.py (zero changes)")
    print("=" * 64)

    # Step 1: Pre-register parameters (audit item #2)
    param_hash = _pre_register_params()

    # Step 2: Build extended dataset
    df = build_extended_dataset(use_cache=not args.refresh)

    # Step 3: Run backtest with FROZEN parameters
    print(f"\n{'─' * 64}")
    print(f"  Running V4 backtest on extended data...")
    print(f"  Param hash: {param_hash} (pre-registered)")
    print(f"{'─' * 64}")

    result = run_backtest(df)
    trades = result["trades"]
    meta = result["meta"]

    # Step 4: Report
    print_report(trades, meta)

    # Step 5: Save
    save_csv(trades, TRADES_CSV)

    # Step 6: Comparison with original N=4 result
    if trades:
        n_trades = len(trades)
        wins = sum(1 for t in trades if t["outcome"] == "TP")
        losses = sum(1 for t in trades if t["outcome"] == "SL")
        gross_profit = sum(t["pnl_btc"] for t in trades if t["outcome"] == "TP")
        gross_loss = abs(sum(t["pnl_btc"] for t in trades if t["outcome"] == "SL"))
        pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")
        wr = wins / n_trades * 100

        print(f"\n{'=' * 64}")
        print(f"  COMPARISON: Original vs Extended")
        print(f"{'=' * 64}")
        print(f"  Original (Coinalyze 2023-2026):  N=4,  WR=50%, PF=1.58")
        print(f"  Extended (BitMEX+  2019-2026):   N={n_trades}, "
              f" WR={wr:.0f}%, PF={pf:.2f}")
        print(f"{'=' * 64}")

        if n_trades >= 12:
            print(f"\n  N={n_trades} approaches minimum viable sample (N>=15 preferred)")
        elif n_trades >= 8:
            print(f"\n  N={n_trades} is better than 4 but still low confidence")
        else:
            print(f"\n  N={n_trades} — sample size remains insufficient")

    # Save analysis
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(ANALYSIS_FILE, "w") as f:
        f.write(f"V4 Extended Backtest Analysis\n")
        f.write(f"Generated: {datetime.now(timezone.utc).isoformat()}\n")
        f.write(f"Param hash: {param_hash}\n")
        f.write(f"Dataset: {df.index[0].date()} → {df.index[-1].date()}\n")
        f.write(f"N trades: {len(trades)}\n")
        if trades:
            f.write(f"Win rate: {wr:.1f}%\n")
            f.write(f"Profit factor: {pf:.2f}\n")


if __name__ == "__main__":
    main()
