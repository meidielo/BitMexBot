"""
backtest_v3.py — V4: Trend-Following Liquidation Dip-Buy

Thesis: In a macro bull trend, retail longs get over-leveraged (extreme positive
funding). Market makers push price down to hunt their stops, triggering a cascade
of LONG liquidations. This violent wick clears the leverage, resetting funding
from extreme back to baseline. We buy the blood — entering LONG after the washout
to ride the trend continuation.

This is NOT mean reversion. This is trend-following with a microstructure entry.

Signal sequence:
  1. FILTER:  Close > EMA200 (confirmed bull regime)
  2. SETUP:   Positive funding was extreme recently (retail exuberance)
  3. TRIGGER: Massive spike in LONG liquidations (the washout event)
  4. CONFIRM: OI dropped on signal day (positions actually destroyed)
  5. ACTION:  Enter LONG next day's open

Key insight: In a bull trend, extreme positive funding does NOT reverse to negative.
It RESETS from +0.05% back to +0.01%. A "funding reset" is not a "funding reversal."

Data sources:
  - XBTUSD 5m candles → resampled to 1D (BitMEX, 2020-2026)
  - Funding rates (BitMEX, 8h settled) → daily aggregation
  - Global aggregated OI (Coinalyze, daily, 8 exchanges)
  - Global aggregated liquidations (Coinalyze, daily, 8 exchanges)

Overlap period: 2023-04-26 → 2026-04-05 (~3 years, bull-dominated)

Usage:
  python backtest_v3.py [--refresh]
"""

import argparse
import csv
import os

import pandas as pd
import numpy as np

from fetch_data import (
    fetch_all_ohlcv, fetch_all_funding,
    INSTRUMENT_CONFIG,
)
from fetch_coinalyze import fetch_daily_oi, fetch_daily_liquidations

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
INITIAL_BALANCE = 1000.0
RISK_PCT        = 0.02         # 2% of balance per trade

OUTPUT_DIR      = "data"

# --- Trend filter ---
EMA_PERIOD         = 200       # Bull regime: close > EMA200

# --- Funding setup ---
# Any single 8h rate in the lookback window exceeded threshold
# 0.03% is sufficient — we want "elevated" not "nuclear"
FUNDING_THRESHOLD  = 0.0003    # 0.03% per 8h settlement
FUNDING_LOOKBACK_D = 3         # look back 3 days (9 settlements)

# --- Liquidation trigger ---
# Long liquidations must spike relative to rolling average
LIQ_SPIKE_MULT     = 3.0       # 3× average daily LONG liquidations
LIQ_LOOKBACK       = 20        # 20-day rolling average baseline
LIQ_LONG_DOM       = 0.60      # long liqs must be ≥60% of total

# --- OI confirmation (optional but preferred) ---
# OI should have decreased on signal day (leverage actually destroyed)
USE_OI_CONFIRM     = True      # OI must drop on signal day (leverage actually destroyed)

# --- SL/TP ---
ATR_PERIOD         = 14
SL_ATR_MULT        = 2.0       # 2× ATR — wider SL for daily (volatility is extreme on cascade days)
TARGET_RR          = 2.0       # TP at 2× risk

# --- Slippage model ---
# During liquidation cascades the order book thins out. Spreads widen 3-7×.
# A market order on a cascade day can easily slip 0.3-1.0%.
# We model slippage as a fixed % of entry price, adverse to our direction.
# Conservative estimate based on max 5m candle range analysis on entry days.
SLIPPAGE_PCT       = 0.003     # 0.3% adverse slippage on entry (market order)
SLIPPAGE_EXIT_PCT  = 0.001     # 0.1% adverse slippage on exit (SL/TP market orders)

# --- Warm-up ---
MIN_WARMUP         = max(EMA_PERIOD, LIQ_LOOKBACK, ATR_PERIOD) + 5


# ---------------------------------------------------------------------------
# Data preparation
# ---------------------------------------------------------------------------

def build_daily_dataset(use_cache: bool = True) -> pd.DataFrame:
    """
    Build merged daily DataFrame: OHLCV + funding + OI + liquidations + EMA200.
    """
    symbol = "BTC/USD:BTC"

    # 1. OHLCV — 5m → daily
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

    # 2. Funding — 8h → daily
    df_fund = fetch_all_funding(use_cache=use_cache, symbol=symbol)
    if df_fund is None or df_fund.empty:
        raise SystemExit("[ABORT] No funding data")
    df_fund = df_fund.copy()
    df_fund["timestamp"] = pd.to_datetime(df_fund["timestamp"], utc=True)
    df_fund = df_fund.set_index("timestamp").sort_index()

    funding_daily = df_fund["rate"].resample("1D").agg(["mean", "max", "min", "count"])
    funding_daily.columns = ["funding_avg", "funding_max", "funding_min", "funding_count"]
    # Rolling max of positive funding in lookback window
    funding_daily["funding_peak_pos"] = funding_daily["funding_max"].rolling(
        FUNDING_LOOKBACK_D, min_periods=1).max()
    print(f"[OK] {len(funding_daily):,} daily funding observations")

    # 3. OI
    df_oi = fetch_daily_oi(use_cache=use_cache)
    if df_oi.empty:
        raise SystemExit("[ABORT] No OI data")
    df_oi = df_oi.copy()
    df_oi["timestamp"] = pd.to_datetime(df_oi["timestamp"], utc=True)
    df_oi = df_oi.set_index("timestamp").sort_index()

    # 4. Liquidations
    df_liq = fetch_daily_liquidations(use_cache=use_cache)
    if df_liq.empty:
        raise SystemExit("[ABORT] No liquidation data")
    df_liq = df_liq.copy()
    df_liq["timestamp"] = pd.to_datetime(df_liq["timestamp"], utc=True)
    df_liq = df_liq.set_index("timestamp").sort_index()

    # 5. Merge
    merged = daily.copy()
    merged.index = merged.index.normalize()
    funding_daily.index = funding_daily.index.normalize()
    df_oi.index = df_oi.index.normalize()
    df_liq.index = df_liq.index.normalize()

    merged = merged.join(funding_daily[["funding_avg", "funding_max", "funding_peak_pos"]],
                         how="left")
    merged = merged.join(df_oi[["oi_close"]], how="left")
    merged = merged.join(df_liq[["liq_long", "liq_short", "liq_total"]], how="left")

    # 6. Derived columns
    # EMA200 — trend filter
    merged["ema200"] = merged["close"].ewm(span=EMA_PERIOD, adjust=False).mean()
    merged["bull_regime"] = merged["close"] > merged["ema200"]

    # ATR
    merged["tr"] = np.maximum(
        merged["high"] - merged["low"],
        np.maximum(
            abs(merged["high"] - merged["close"].shift(1)),
            abs(merged["low"] - merged["close"].shift(1))
        )
    )
    merged["atr"] = merged["tr"].rolling(ATR_PERIOD).mean()

    # OI delta
    merged["oi_delta_pct"] = merged["oi_close"].pct_change()

    # Long liquidation rolling average and spike ratio
    merged["liq_long_avg"] = merged["liq_long"].rolling(LIQ_LOOKBACK).mean()
    merged["liq_long_ratio"] = merged["liq_long"] / merged["liq_long_avg"]

    # Total liq ratio (for reporting)
    merged["liq_avg"] = merged["liq_total"].rolling(LIQ_LOOKBACK).mean()
    merged["liq_ratio"] = merged["liq_total"] / merged["liq_avg"]

    # Long liq dominance
    merged["liq_long_pct"] = merged["liq_long"] / merged["liq_total"]

    # Drop incomplete rows
    merged = merged.dropna(subset=["liq_long_avg", "atr", "funding_avg", "ema200"])

    print(f"\n[OK] Merged daily dataset: {len(merged):,} rows "
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
    print(f"     Long liq spike days (≥{LIQ_SPIKE_MULT}× avg):        {liq_spike_days}")
    print(f"     All conditions met (bull+fund+liq):       {both}")

    return merged


# ---------------------------------------------------------------------------
# Signal logic — V4 Trend-Following Liquidation Dip-Buy
# ---------------------------------------------------------------------------

def get_daily_signal(df: pd.DataFrame, i: int) -> dict:
    """
    V4 signal: buy the dip after retail longs get liquidated in a bull trend.

    1. FILTER:  Close > EMA200 (bull regime)
    2. SETUP:   Positive funding extreme in last 3 days (retail was greedy)
    3. TRIGGER: Long liquidation spike ≥3× rolling avg, longs ≥60% of total
    4. CONFIRM: (optional) OI decreased on signal day
    5. ACTION:  LONG next day open
    """
    no_trade = lambda reason: {
        "signal": "NO_TRADE", "reason": reason,
        "entry_price": 0, "sl_price": 0, "tp_price": 0, "rr": 0,
    }

    row = df.iloc[i]
    close = float(row["close"])
    atr   = float(row["atr"])

    # ------------------------------------------------------------------
    # FILTER — Bull regime
    # ------------------------------------------------------------------
    if not row["bull_regime"]:
        return no_trade(f"Bear regime: close={close:,.0f} < EMA200={row['ema200']:,.0f}")

    # ------------------------------------------------------------------
    # SETUP — Recent positive funding extreme
    # ------------------------------------------------------------------
    fund_peak = float(row["funding_peak_pos"]) if pd.notna(row.get("funding_peak_pos")) else 0
    if fund_peak <= FUNDING_THRESHOLD:
        return no_trade(
            f"No funding exuberance: peak_pos={fund_peak*100:+.4f}% "
            f"(need >{FUNDING_THRESHOLD*100:.2f}%)"
        )

    # ------------------------------------------------------------------
    # TRIGGER — Long liquidation spike
    # ------------------------------------------------------------------
    liq_long_ratio = float(row["liq_long_ratio"]) if pd.notna(row.get("liq_long_ratio")) else 0
    liq_long       = float(row["liq_long"]) if pd.notna(row.get("liq_long")) else 0
    liq_total      = float(row["liq_total"]) if pd.notna(row.get("liq_total")) else 0
    liq_long_pct   = float(row["liq_long_pct"]) if pd.notna(row.get("liq_long_pct")) else 0

    if liq_long_ratio < LIQ_SPIKE_MULT:
        return no_trade(
            f"No long liq spike: ratio={liq_long_ratio:.1f}× (need {LIQ_SPIKE_MULT}×)")

    if liq_long_pct < LIQ_LONG_DOM:
        return no_trade(
            f"Long liqs not dominant: {liq_long_pct*100:.0f}% (need ≥{LIQ_LONG_DOM*100:.0f}%)")

    # ------------------------------------------------------------------
    # CONFIRM (optional) — OI decreased (leverage destroyed)
    # ------------------------------------------------------------------
    oi_delta = float(row["oi_delta_pct"]) if pd.notna(row.get("oi_delta_pct")) else 0
    if USE_OI_CONFIRM and oi_delta >= 0:
        return no_trade(f"OI didn't drop: delta={oi_delta*100:+.1f}%")

    # ------------------------------------------------------------------
    # SL + TP — ATR-based, LONG only
    # ------------------------------------------------------------------
    sl_dist  = atr * SL_ATR_MULT
    sl_price = close - sl_dist
    tp_price = close + (sl_dist * TARGET_RR)

    # Sanity: SL must be above zero and below close
    if sl_price <= 0:
        return no_trade(f"SL below zero: {sl_price:.2f}")

    reason = (
        f"V4 LONG: bull regime (EMA200={row['ema200']:,.0f}) | "
        f"fund_peak={fund_peak*100:+.4f}% | "
        f"long_liq={liq_long:,.0f} ({liq_long_ratio:.1f}× avg, "
        f"L={liq_long_pct*100:.0f}%) | "
        f"OI Δ={oi_delta*100:+.1f}%"
    )

    return {
        "signal":         "LONG",
        "reason":         reason,
        "entry_price":    close,
        "sl_price":       round(sl_price, 2),
        "tp_price":       round(tp_price, 2),
        "rr":             round(TARGET_RR, 2),
        "funding_peak":   fund_peak,
        "liq_long":       liq_long,
        "liq_long_ratio": liq_long_ratio,
        "liq_long_pct":   liq_long_pct,
        "oi_delta_pct":   oi_delta,
    }


# ---------------------------------------------------------------------------
# Backtest loop — inverse PnL (BTC-margined)
# ---------------------------------------------------------------------------

def run_backtest(df: pd.DataFrame, slippage_pct: float = None,
                 slippage_exit_pct: float = None, quiet: bool = False) -> dict:
    """
    Walk daily bars, evaluate V4 signals, simulate LONG fills.
    XBTUSD inverse: all PnL in BTC.

    SL/TP are recalculated from actual fill price (not signal close) to
    preserve the intended risk distance and R:R ratio after slippage.
    """
    if slippage_pct is None:
        slippage_pct = SLIPPAGE_PCT
    if slippage_exit_pct is None:
        slippage_exit_pct = SLIPPAGE_EXIT_PCT

    trades   = []
    n        = len(df)
    i        = MIN_WARMUP

    init_price  = float(df.iloc[0]["close"])
    balance_btc = INITIAL_BALANCE / init_price
    peak_btc    = balance_btc

    meta = {
        "init_btc":   balance_btc,
        "init_price": init_price,
    }

    if not quiet:
        print(f"\nInitial equity: {balance_btc:.6f} BTC "
              f"(${INITIAL_BALANCE:.2f} @ ${init_price:,.2f})")
        print(f"Running V4 daily backtest on {n:,} bars "
              f"(scan starts at index {MIN_WARMUP})...\n")

    while i < n - 1:
        sig = get_daily_signal(df, i)

        if sig["signal"] != "LONG":
            i += 1
            continue

        # Entry at next day's open + slippage (adverse = higher for LONG)
        fill_idx   = i + 1
        fill_ts    = df.index[fill_idx]
        raw_open   = float(df.iloc[fill_idx]["open"])
        fill_price = raw_open * (1 + slippage_pct)  # LONG: pay more

        # Recalculate SL/TP from actual fill price (not signal close).
        # This preserves the intended ATR-based risk distance and R:R
        # ratio regardless of how much slippage was incurred.
        atr = float(df.iloc[i]["atr"])
        sl_dist = atr * SL_ATR_MULT
        sl_price = fill_price - sl_dist
        tp_price = fill_price + (sl_dist * TARGET_RR)

        # Sanity: SL must be above zero
        if sl_price <= 0:
            i += 1
            continue

        # Find exit
        exit_idx, exit_price_raw, outcome = _check_exit(
            df, fill_idx, "LONG", sl_price, tp_price)

        # Apply exit slippage (adverse = lower for LONG exit)
        if outcome in ("SL", "TP"):
            exit_price = exit_price_raw * (1 - slippage_exit_pct)
        else:
            # OPEN — closing at last bar's close, still slipped
            exit_price = exit_price_raw * (1 - slippage_exit_pct)

        exit_ts       = df.index[exit_idx]
        duration_days = (exit_ts - fill_ts).days

        # --- Inverse PnL (BTC-native) ---
        risk_btc    = balance_btc * RISK_PCT
        sl_dist_btc = (1.0 / sl_price) - (1.0 / fill_price)

        if sl_dist_btc <= 0:
            i += 1
            continue

        contracts = risk_btc / sl_dist_btc
        pnl_btc   = contracts * ((1.0 / fill_price) - (1.0 / exit_price))
        r_multiple = pnl_btc / risk_btc if risk_btc > 0 else 0.0

        balance_btc += pnl_btc
        if balance_btc > peak_btc:
            peak_btc = balance_btc

        drawdown_pct = (peak_btc - balance_btc) / peak_btc * 100
        bal_usd_ref  = balance_btc * exit_price

        trade = {
            "trade_num":      len(trades) + 1,
            "entry_ts":       fill_ts.strftime("%Y-%m-%d"),
            "exit_ts":        exit_ts.strftime("%Y-%m-%d"),
            "direction":      "LONG",
            "fill_price":     round(fill_price, 2),
            "sl_price":       round(sl_price, 2),
            "tp_price":       round(tp_price, 2),
            "exit_price":     round(exit_price, 2),
            "outcome":        outcome,
            "actual_r":       round(r_multiple, 2),
            "risk_btc":       round(risk_btc, 8),
            "pnl_btc":        round(pnl_btc, 8),
            "balance_btc":    round(balance_btc, 8),
            "balance_usd_ref": round(bal_usd_ref, 2),
            "contracts":      round(contracts, 0),
            "drawdown_pct":   round(drawdown_pct, 2),
            "duration_days":  duration_days,
            "funding_peak":   round(sig.get("funding_peak", 0), 6),
            "liq_long":       round(sig.get("liq_long", 0), 0),
            "liq_long_ratio": round(sig.get("liq_long_ratio", 0), 1),
            "liq_long_pct":   round(sig.get("liq_long_pct", 0) * 100, 1),
            "oi_delta_pct":   round(sig.get("oi_delta_pct", 0) * 100, 2),
            "reason":         sig["reason"],
        }
        trades.append(trade)

        tag = " WIN" if outcome == "TP" else ("LOSS" if outcome == "SL" else "OPEN")
        if not quiet:
            print(
                f"  [{len(trades):3d}] {fill_ts.strftime('%Y-%m-%d')} "
                f"LONG  {tag}  "
                f"R={r_multiple:+.2f}  PnL={pnl_btc:+.6f} BTC  "
                f"Bal={balance_btc:.6f} BTC (${bal_usd_ref:>9,.2f})  "
                f"LiqL={sig.get('liq_long_ratio', 0):.1f}× "
                f"OIΔ={sig.get('oi_delta_pct', 0)*100:+.1f}%"
            )

        i = exit_idx + 1

    return {"trades": trades, "meta": meta}


def _check_exit(df, start_idx, direction, sl_price, tp_price):
    """Scan daily bars for SL or TP hit. SL wins on same-day."""
    for j in range(start_idx, len(df)):
        row = df.iloc[j]
        h, lo = float(row["high"]), float(row["low"])

        if direction == "LONG":
            sl_hit = lo <= sl_price
            tp_hit = h  >= tp_price
        else:
            sl_hit = h  >= sl_price
            tp_hit = lo <= tp_price

        if sl_hit and tp_hit:
            return j, sl_price, "SL"
        if tp_hit:
            return j, tp_price, "TP"
        if sl_hit:
            return j, sl_price, "SL"

    last_close = float(df.iloc[-1]["close"])
    return len(df) - 1, last_close, "OPEN"


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def print_report(trades, meta):
    if not trades:
        print("\n  NO TRADES GENERATED.")
        print("  Bull regime + funding exuberance + long liq spike never co-occurred.")
        print("  Review thresholds or data range.\n")
        return

    total    = len(trades)
    wins     = [t for t in trades if t["outcome"] == "TP"]
    losses   = [t for t in trades if t["outcome"] == "SL"]
    open_t   = [t for t in trades if t["outcome"] == "OPEN"]

    win_rate = len(wins) / total * 100
    avg_r    = sum(t["actual_r"] for t in trades) / total
    avg_dur  = sum(t["duration_days"] for t in trades) / total

    total_pnl  = sum(t["pnl_btc"] for t in trades)
    final_btc  = trades[-1]["balance_btc"]
    final_usd  = trades[-1]["balance_usd_ref"]
    init_btc   = meta["init_btc"]
    init_price = meta["init_price"]
    return_pct = (final_btc - init_btc) / init_btc * 100
    max_dd     = max(t["drawdown_pct"] for t in trades)

    gross_profit = sum(t["pnl_btc"] for t in wins) if wins else 0
    gross_loss   = abs(sum(t["pnl_btc"] for t in losses)) if losses else 0
    pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    avg_win  = sum(t["pnl_btc"] for t in wins) / len(wins) if wins else 0
    avg_loss = sum(t["pnl_btc"] for t in losses) / len(losses) if losses else 0

    w = 64
    print()
    print("=" * w)
    print("  V4 TREND-FOLLOWING LIQ DIP-BUY — Daily, Inverse (BTC)")
    print("=" * w)
    print(f"  Period       : {trades[0]['entry_ts']} → {trades[-1]['exit_ts']}")
    print(f"  Total trades : {total}  "
          f"(wins={len(wins)}, losses={len(losses)}, open={len(open_t)})")
    print(f"  Win rate     : {win_rate:.1f}%")
    print(f"  Profit factor: {pf:.2f}")
    print("-" * w)
    print(f"  Start equity  : {init_btc:.6f} BTC  (${INITIAL_BALANCE:,.2f} @ ${init_price:,.2f})")
    print(f"  End equity    : {final_btc:.6f} BTC  (${final_usd:,.2f} at last exit)")
    print(f"  BTC return    : {return_pct:+.1f}%")
    print(f"  Total PnL     : {total_pnl:+.6f} BTC")
    print(f"  Trading DD    : {max_dd:.2f}%  (BTC equity)")
    print("-" * w)
    print(f"  Avg win  (BTC): {avg_win:+.8f}")
    print(f"  Avg loss (BTC): {avg_loss:+.8f}")
    print(f"  Avg R achieved: {avg_r:>+.2f}R")
    print(f"  Avg duration  : {avg_dur:.1f} days")
    print("=" * w)

    # Per-trade detail
    top_n = min(3, total)
    if top_n > 0:
        print(f"\n  Best {top_n} by R:")
        for t in sorted(trades, key=lambda x: x["actual_r"], reverse=True)[:top_n]:
            print(f"    {t['entry_ts']}  R={t['actual_r']:+.2f}  "
                  f"PnL={t['pnl_btc']:+.6f} BTC  "
                  f"LiqL={t['liq_long_ratio']:.1f}× OIΔ={t['oi_delta_pct']:+.1f}%")

        print(f"\n  Worst {top_n} by R:")
        for t in sorted(trades, key=lambda x: x["actual_r"])[:top_n]:
            print(f"    {t['entry_ts']}  R={t['actual_r']:+.2f}  "
                  f"PnL={t['pnl_btc']:+.6f} BTC  "
                  f"LiqL={t['liq_long_ratio']:.1f}× OIΔ={t['oi_delta_pct']:+.1f}%")


def save_csv(trades, path):
    if not trades:
        return
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    fieldnames = list(trades[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(trades)
    print(f"\n[OK] Saved {len(trades)} trades to {path}")


# ---------------------------------------------------------------------------
# Slippage sweep — stress test across catastrophic slippage levels
# ---------------------------------------------------------------------------

def run_slippage_sweep(df: pd.DataFrame):
    """
    Run V4 backtest at escalating slippage levels to find the breakeven
    where PF drops below 1.0 and the edge disappears.

    Tests both percentage-based slippage AND R-term equivalents.
    With SL = 2× ATR, 0.5R slippage = 1× ATR ≈ 2-4% of price.
    """
    w = 72
    print("\n" + "=" * w)
    print("  SLIPPAGE STRESS TEST — V4 Catastrophic Scenario Analysis")
    print("  SL/TP recalculated from actual fill price (not signal close)")
    print("=" * w)

    # Percentage-based sweep: 0% to 5% entry slippage
    # Exit slippage scales proportionally (1/3 of entry)
    pct_levels = [0.0, 0.001, 0.003, 0.005, 0.01, 0.015, 0.02, 0.03, 0.04, 0.05]

    print(f"\n  {'Slip%':>6} {'ExitSlip':>8} {'Trades':>7} {'WR':>6} "
          f"{'PF':>7} {'AvgR':>7} {'BTC Ret':>8} {'MaxDD':>7}  R-cost")
    print(f"  {'-'*72}")

    breakeven_pct = None
    last_pf = None

    for slip in pct_levels:
        exit_slip = slip / 3.0  # exit slippage ~1/3 of entry
        result = run_backtest(df, slippage_pct=slip, slippage_exit_pct=exit_slip,
                              quiet=True)
        trades = result["trades"]

        if not trades:
            print(f"  {slip*100:>5.1f}% {exit_slip*100:>7.2f}%   0 trades — all gapped past SL")
            continue

        total = len(trades)
        wins = sum(1 for t in trades if t["outcome"] == "TP")
        losses = sum(1 for t in trades if t["outcome"] == "SL")
        wr = wins / total * 100

        gross_profit = sum(t["pnl_btc"] for t in trades if t["outcome"] == "TP")
        gross_loss = abs(sum(t["pnl_btc"] for t in trades if t["outcome"] == "SL"))
        pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")

        avg_r = sum(t["actual_r"] for t in trades) / total
        init_btc = result["meta"]["init_btc"]
        final_btc = trades[-1]["balance_btc"]
        btc_ret = (final_btc - init_btc) / init_btc * 100
        max_dd = max(t["drawdown_pct"] for t in trades)

        # Estimate R-cost of slippage: compare avg_r to frictionless
        r_cost_note = ""
        if slip == 0:
            frictionless_avg_r = avg_r
        else:
            r_cost = frictionless_avg_r - avg_r
            r_cost_note = f"  -{r_cost:.2f}R"

        marker = ""
        if last_pf is not None and last_pf >= 1.0 and pf < 1.0:
            breakeven_pct = slip
            marker = "  ← BREAKEVEN"
        last_pf = pf

        print(f"  {slip*100:>5.1f}% {exit_slip*100:>7.2f}%  "
              f"{total:>3}/{total}  {wr:>4.0f}%  "
              f"{pf:>6.2f}  {avg_r:>+5.2f}R  {btc_ret:>+6.1f}%  {max_dd:>5.1f}%"
              f"{r_cost_note}{marker}")

    # R-term analysis
    print(f"\n  {'─'*72}")
    print(f"  R-TERM EQUIVALENTS (SL = {SL_ATR_MULT}× ATR)")
    print(f"  {'─'*72}")

    # Calculate average ATR and price across the 4 signals to show R→% mapping
    # Use the sweep results to identify exact breakeven
    if breakeven_pct is not None:
        print(f"\n  ⚠ BREAKEVEN at {breakeven_pct*100:.1f}% entry slippage (PF < 1.0)")
        print(f"    At SL = {SL_ATR_MULT}× ATR, this equals ~{breakeven_pct / (SL_ATR_MULT * 0.03):.2f}R")
    else:
        if last_pf is not None and last_pf >= 1.0:
            print(f"\n  Edge survives up to {pct_levels[-1]*100:.0f}% slippage (PF={last_pf:.2f})")
        else:
            print(f"\n  Edge already broken at lowest tested level")

    # Fine-grained binary search around breakeven
    if breakeven_pct is not None:
        _find_precise_breakeven(df, breakeven_pct)


def _find_precise_breakeven(df: pd.DataFrame, approx_pct: float):
    """Binary search to find exact slippage % where PF crosses 1.0."""
    lo = approx_pct * 0.5  # search below
    hi = approx_pct * 1.5  # search above
    print(f"\n  Fine-tuning breakeven (binary search {lo*100:.2f}%–{hi*100:.2f}%)...")

    for _ in range(10):
        mid = (lo + hi) / 2
        exit_slip = mid / 3.0
        result = run_backtest(df, slippage_pct=mid, slippage_exit_pct=exit_slip,
                              quiet=True)
        trades = result["trades"]
        if not trades:
            hi = mid
            continue

        gross_profit = sum(t["pnl_btc"] for t in trades if t["outcome"] == "TP")
        gross_loss = abs(sum(t["pnl_btc"] for t in trades if t["outcome"] == "SL"))
        pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")

        if pf >= 1.0:
            lo = mid
        else:
            hi = mid

    breakeven = (lo + hi) / 2
    # Convert to R-terms: slippage_pct * price ≈ slippage_pct / (ATR/price * SL_ATR_MULT) in R
    # Approximate: for XBTUSD, ATR/price ≈ 3% (historical), so SL dist ≈ 6%
    # R_cost ≈ slippage_pct / (ATR/price * SL_ATR_MULT)
    print(f"\n  PRECISE BREAKEVEN: {breakeven*100:.3f}% entry slippage")
    print(f"    Exit slippage:  {breakeven/3*100:.3f}%")
    print(f"    At avg ATR/price ~3%, SL dist ~{SL_ATR_MULT*3:.0f}%: "
          f"≈ {breakeven/(0.03*SL_ATR_MULT):.2f}R slippage cost")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="V4 Trend-Following Liq Dip-Buy Backtest")
    parser.add_argument("--refresh", action="store_true",
                        help="Force fresh fetch from APIs (skip cache)")
    parser.add_argument("--sweep", action="store_true",
                        help="Run slippage stress test across catastrophic levels")
    args = parser.parse_args()

    use_cache = not args.refresh

    w = 64
    print("=" * w)
    print("  V4 TREND-FOLLOWING LIQUIDATION DIP-BUY")
    print("  Buy the blood after retail longs get washed out")
    print("  XBTUSD Inverse (BTC-margined)")
    print("=" * w + "\n")

    print(f"  Thresholds:")
    print(f"    Trend filter: Close > EMA({EMA_PERIOD})")
    print(f"    Funding:      >{FUNDING_THRESHOLD*100:.2f}% (any 8h in {FUNDING_LOOKBACK_D}d)")
    print(f"    Long liq:     ≥{LIQ_SPIKE_MULT}× rolling {LIQ_LOOKBACK}d avg, ≥{LIQ_LONG_DOM*100:.0f}% dominance")
    print(f"    OI confirm:   {'ON' if USE_OI_CONFIRM else 'OFF'}")
    print(f"    SL:           {SL_ATR_MULT}× ATR({ATR_PERIOD})")
    print(f"    TP:           {TARGET_RR}× risk")
    print(f"    Slippage:     {SLIPPAGE_PCT*100:.1f}% entry, {SLIPPAGE_EXIT_PCT*100:.1f}% exit")
    print(f"    SL/TP basis:  Recalculated from fill price (not signal close)")
    print()

    # Build dataset
    df = build_daily_dataset(use_cache=use_cache)

    if args.sweep:
        # Slippage stress test
        run_slippage_sweep(df)
    else:
        # Standard backtest
        result = run_backtest(df)
        trades = result["trades"]
        meta   = result["meta"]
        print_report(trades, meta)
        out_path = os.path.join(OUTPUT_DIR, "backtest_v4_daily.csv")
        save_csv(trades, out_path)
