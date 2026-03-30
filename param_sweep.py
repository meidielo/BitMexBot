"""
param_sweep.py — Backtest parameter sweep

Tests combinations of strategy parameters against cached candle data
to find the best-performing configuration.

Usage:
    cd ~/BitMexBot && source venv/bin/activate && python param_sweep.py
"""

import itertools
import math
import os
import sys
import io
import pandas as pd
from datetime import datetime

from backtest import load_cache, resample_to_15m, enrich, OUTPUT_DIR

# ---------------------------------------------------------------------------
# Parameter grid
# ---------------------------------------------------------------------------
GRID = {
    "min_trend_age": [10, 20, 30, 55],
    "wick_min_pct":  [0.0005, 0.001, 0.002, 0.003],
    "min_rr":        [1.2, 1.5, 2.0],
    "long_enabled":  [False, True],
}

# Fixed params (same as signals.py)
SL_BUFFER_PCT   = 0.001
SL_MIN_DIST_PCT = 0.003
SL_LOOKBACK     = 5
TP_LOOKBACK     = 20
TP_ROUND_STEP   = 500
INITIAL_BALANCE = 1000.0
RISK_PCT        = 0.02
MIN_WARMUP      = 60


# ---------------------------------------------------------------------------
# TP helpers (copied from signals.py to avoid import side effects)
# ---------------------------------------------------------------------------
def nearest_round_support(price, step=TP_ROUND_STEP):
    return math.floor(price / step) * step

def nearest_round_resistance(price, step=TP_ROUND_STEP):
    return math.ceil(price / step) * step


# ---------------------------------------------------------------------------
# Trend age computation (vectorized prefix for speed)
# ---------------------------------------------------------------------------
def precompute_trend_ages(df):
    """
    For each row, count consecutive candles backwards where EMA20 < EMA50
    (short trend) and EMA20 > EMA50 (long trend).
    Returns two arrays: short_trend_age[], long_trend_age[]
    """
    n = len(df)
    short_age = [0] * n
    long_age = [0] * n

    ema20 = df["ema_20"].values
    ema50 = df["ema_50"].values

    for i in range(n):
        if pd.isna(ema20[i]) or pd.isna(ema50[i]):
            continue
        # Short trend (EMA20 < EMA50)
        if ema20[i] < ema50[i]:
            short_age[i] = (short_age[i - 1] + 1) if i > 0 else 1
        # Long trend (EMA20 > EMA50)
        if ema20[i] > ema50[i]:
            long_age[i] = (long_age[i - 1] + 1) if i > 0 else 1

    return short_age, long_age


# ---------------------------------------------------------------------------
# Fast backtest for one parameter set (no get_signal call — inline logic)
# ---------------------------------------------------------------------------
def run_fast_backtest(df, short_age, long_age, params):
    """
    Inline signal + backtest logic for speed.
    Returns dict with summary stats.
    """
    min_trend_age = params["min_trend_age"]
    wick_min_pct  = params["wick_min_pct"]
    min_rr        = params["min_rr"]
    long_enabled  = params["long_enabled"]

    n       = len(df)
    balance = INITIAL_BALANCE
    peak    = INITIAL_BALANCE
    trades  = []
    i       = MIN_WARMUP

    # Pre-extract arrays for speed
    opens  = df["open"].values.astype(float)
    highs  = df["high"].values.astype(float)
    lows   = df["low"].values.astype(float)
    closes = df["close"].values.astype(float)
    ema20  = df["ema_20"].values.astype(float)
    ema50  = df["ema_50"].values.astype(float)

    while i < n - 1:
        o, h, lo, c = opens[i], highs[i], lows[i], closes[i]
        e20, e50 = ema20[i], ema50[i]

        if pd.isna(e20) or pd.isna(e50):
            i += 1
            continue

        entry = c
        wick_min = entry * wick_min_pct
        upper_wick = h - max(o, c)
        lower_wick = min(o, c) - lo
        direction = None

        # --- SHORT check ---
        if (e20 < e50
            and short_age[i] >= min_trend_age
            and h > e20 and c < e20
            and upper_wick > wick_min):
            direction = "SHORT"

        # --- LONG check ---
        elif (long_enabled
              and e20 > e50
              and long_age[i] >= min_trend_age
              and lo < e20 and c > e20
              and lower_wick > wick_min):
            direction = "LONG"

        if direction is None:
            i += 1
            continue

        # --- SL/TP computation ---
        sl_start = max(0, i - SL_LOOKBACK + 1)
        tp_start = max(0, i - TP_LOOKBACK + 1)

        if direction == "SHORT":
            swing_high = highs[sl_start:i + 1].max()
            sl_price = round(swing_high + entry * SL_BUFFER_PCT, 2)
            sl_dist = sl_price - entry

            if sl_dist < entry * SL_MIN_DIST_PCT:
                i += 1
                continue

            low_20 = lows[tp_start:i + 1].min()
            tp_round = nearest_round_support(entry)
            tp_price = round(min(low_20, tp_round), 2)
            gain = entry - tp_price
            risk = sl_dist
        else:  # LONG
            swing_low = lows[sl_start:i + 1].min()
            sl_price = round(swing_low - entry * SL_BUFFER_PCT, 2)
            sl_dist = entry - sl_price

            if sl_dist < entry * SL_MIN_DIST_PCT:
                i += 1
                continue

            high_20 = highs[tp_start:i + 1].max()
            tp_round = nearest_round_resistance(entry)
            tp_price = round(max(high_20, tp_round), 2)
            gain = tp_price - entry
            risk = sl_dist

        if risk <= 0 or gain <= 0:
            i += 1
            continue

        rr = gain / risk
        if rr < min_rr:
            i += 1
            continue

        # --- Fill at next candle open ---
        fill_idx = i + 1
        fill_price = opens[fill_idx]

        # Gap check
        if direction == "SHORT" and fill_price >= sl_price:
            i += 1
            continue
        if direction == "LONG" and fill_price <= sl_price:
            i += 1
            continue

        # --- Scan for exit ---
        exit_idx = None
        exit_price = None
        outcome = None

        for j in range(fill_idx, n):
            jh, jl = highs[j], lows[j]
            if direction == "SHORT":
                sl_hit = jh >= sl_price
                tp_hit = jl <= tp_price
            else:
                sl_hit = jl <= sl_price
                tp_hit = jh >= tp_price

            if sl_hit and tp_hit:
                exit_idx, exit_price, outcome = j, sl_price, "SL"
                break
            if tp_hit:
                exit_idx, exit_price, outcome = j, tp_price, "TP"
                break
            if sl_hit:
                exit_idx, exit_price, outcome = j, sl_price, "SL"
                break

        if exit_idx is None:
            # Data ended — mark to last close
            exit_idx = n - 1
            exit_price = closes[-1]
            outcome = "OPEN"

        # --- PnL ---
        risk_usd = balance * RISK_PCT
        risk_dist = abs(fill_price - sl_price)

        if outcome == "TP":
            r_mult = abs(tp_price - fill_price) / risk_dist
        elif outcome == "SL":
            r_mult = -abs(exit_price - fill_price) / risk_dist
        else:
            if direction == "LONG":
                r_mult = (exit_price - fill_price) / risk_dist
            else:
                r_mult = (fill_price - exit_price) / risk_dist

        pnl = risk_usd * r_mult
        balance += pnl
        if balance > peak:
            peak = balance

        trades.append({
            "direction": direction,
            "outcome": outcome,
            "r_mult": r_mult,
            "pnl": pnl,
        })

        i = exit_idx + 1

    # --- Summary ---
    if not trades:
        return None

    total = len(trades)
    wins = [t for t in trades if t["outcome"] == "TP"]
    losses = [t for t in trades if t["outcome"] == "SL"]
    longs = [t for t in trades if t["direction"] == "LONG"]
    shorts = [t for t in trades if t["direction"] == "SHORT"]
    long_wins = [t for t in longs if t["outcome"] == "TP"]
    short_wins = [t for t in shorts if t["outcome"] == "TP"]

    total_pnl = sum(t["pnl"] for t in trades)
    gross_profit = sum(t["pnl"] for t in wins) if wins else 0
    gross_loss = abs(sum(t["pnl"] for t in losses)) if losses else 0
    pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    # Compute max drawdown
    running_bal = INITIAL_BALANCE
    running_peak = INITIAL_BALANCE
    max_drawdown = 0
    for t in trades:
        running_bal += t["pnl"]
        if running_bal > running_peak:
            running_peak = running_bal
        dd = (running_peak - running_bal) / running_peak * 100
        if dd > max_drawdown:
            max_drawdown = dd

    # Best single trade contribution
    best_r = max(t["r_mult"] for t in trades)
    best_pnl = max(t["pnl"] for t in trades)
    # PnL without the best trade
    pnl_without_best = total_pnl - best_pnl

    return {
        "total": total,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / total * 100,
        "long_trades": len(longs),
        "short_trades": len(shorts),
        "long_wr": len(long_wins) / len(longs) * 100 if longs else 0,
        "short_wr": len(short_wins) / len(shorts) * 100 if shorts else 0,
        "total_pnl": total_pnl,
        "final_balance": balance,
        "return_pct": (balance - INITIAL_BALANCE) / INITIAL_BALANCE * 100,
        "profit_factor": pf,
        "max_drawdown": max_drawdown,
        "avg_r": sum(t["r_mult"] for t in trades) / total,
        "best_single_r": best_r,
        "pnl_without_best": pnl_without_best,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 70)
    print("  PARAMETER SWEEP  —  XBTUSDT 15m EMA Rejection Strategy")
    print("=" * 70)

    # Load data
    raw = load_cache()
    if raw is None:
        sys.exit("[ABORT] No cache found. Run: python backtest.py first.")
    print(f"\n[OK] Loaded {len(raw):,} raw candles from cache.\n")

    df = resample_to_15m(raw)
    df = enrich(df)

    # Precompute trend ages (big speedup vs per-candle backward scan)
    print("Precomputing trend ages...")
    short_age, long_age = precompute_trend_ages(df)
    print("[OK] Done.\n")

    # Generate all parameter combinations
    keys = list(GRID.keys())
    combos = list(itertools.product(*[GRID[k] for k in keys]))
    print(f"Testing {len(combos)} parameter combinations...\n")

    results = []
    for idx, combo in enumerate(combos):
        params = dict(zip(keys, combo))
        stats = run_fast_backtest(df, short_age, long_age, params)

        if stats is not None:
            stats["params"] = params
            results.append(stats)

        if (idx + 1) % 12 == 0 or idx == len(combos) - 1:
            print(f"  ... {idx + 1}/{len(combos)} done  "
                  f"({len(results)} produced trades)")

    if not results:
        print("\n[WARN] No parameter set produced any trades!")
        return

    # Sort by profit factor (with min trade count filter)
    MIN_TRADES = 15
    viable = [r for r in results if r["total"] >= MIN_TRADES]

    if not viable:
        print(f"\n[WARN] No parameter set produced >= {MIN_TRADES} trades.")
        print("Showing all results instead.\n")
        viable = results

    # Primary sort: profit factor. Secondary: total trades (more = more reliable)
    viable.sort(key=lambda r: (r["profit_factor"], r["total"]), reverse=True)

    # Print top 15
    W = 120
    print("\n" + "=" * W)
    print(f"  TOP RESULTS  (min {MIN_TRADES} trades, sorted by profit factor)")
    print("=" * W)
    print(f"  {'#':>3}  {'TrendAge':>8}  {'Wick%':>6}  {'MinRR':>5}  {'LONG':>5}"
          f"  {'Trades':>6}  {'Wins':>4}  {'WR%':>5}  {'PF':>6}  {'PnL$':>8}"
          f"  {'Ret%':>6}  {'MaxDD':>6}  {'AvgR':>6}  {'BestR':>6}  {'w/o Best':>9}")
    print("-" * W)

    for rank, r in enumerate(viable[:15], 1):
        p = r["params"]
        long_str = "Y" if p["long_enabled"] else "N"
        print(
            f"  {rank:>3}  {p['min_trend_age']:>8}  {p['wick_min_pct']*100:>5.1f}%"
            f"  {p['min_rr']:>5.1f}  {long_str:>5}"
            f"  {r['total']:>6}  {r['wins']:>4}  {r['win_rate']:>4.0f}%"
            f"  {r['profit_factor']:>6.2f}  {r['total_pnl']:>+8.0f}"
            f"  {r['return_pct']:>+5.1f}%  {r['max_drawdown']:>5.1f}%"
            f"  {r['avg_r']:>+5.2f}  {r['best_single_r']:>+5.2f}"
            f"  {r['pnl_without_best']:>+9.0f}"
        )

    print("=" * W)

    # Direction breakdown for top 3
    print("\n  DIRECTION BREAKDOWN (top 3):")
    print("-" * 70)
    for rank, r in enumerate(viable[:3], 1):
        p = r["params"]
        print(f"  #{rank}  LONG: {r['long_trades']} trades ({r['long_wr']:.0f}% WR)"
              f"  |  SHORT: {r['short_trades']} trades ({r['short_wr']:.0f}% WR)")

    # Print worst results too for contrast
    print("\n" + "=" * W)
    print(f"  WORST 5 RESULTS  (for comparison)")
    print("=" * W)
    worst = sorted(viable, key=lambda r: r["profit_factor"])[:5]
    for rank, r in enumerate(worst, 1):
        p = r["params"]
        long_str = "Y" if p["long_enabled"] else "N"
        print(
            f"  {rank:>3}  {p['min_trend_age']:>8}  {p['wick_min_pct']*100:>5.1f}%"
            f"  {p['min_rr']:>5.1f}  {long_str:>5}"
            f"  {r['total']:>6}  {r['wins']:>4}  {r['win_rate']:>4.0f}%"
            f"  {r['profit_factor']:>6.2f}  {r['total_pnl']:>+8.0f}"
            f"  {r['return_pct']:>+5.1f}%  {r['max_drawdown']:>5.1f}%"
            f"  {r['avg_r']:>+5.2f}  {r['best_single_r']:>+5.2f}"
            f"  {r['pnl_without_best']:>+9.0f}"
        )
    print("=" * W)

    # Save full results to CSV
    out_path = os.path.join(OUTPUT_DIR, "param_sweep_results.csv")
    rows = []
    for r in viable:
        p = r["params"]
        rows.append({
            "min_trend_age": p["min_trend_age"],
            "wick_min_pct": p["wick_min_pct"],
            "min_rr": p["min_rr"],
            "long_enabled": p["long_enabled"],
            "trades": r["total"],
            "wins": r["wins"],
            "win_rate": round(r["win_rate"], 1),
            "profit_factor": round(r["profit_factor"], 2),
            "total_pnl": round(r["total_pnl"], 2),
            "return_pct": round(r["return_pct"], 1),
            "max_drawdown": round(r["max_drawdown"], 1),
            "avg_r": round(r["avg_r"], 3),
            "best_single_r": round(r["best_single_r"], 2),
            "pnl_without_best": round(r["pnl_without_best"], 2),
        })
    pd.DataFrame(rows).to_csv(out_path, index=False)
    print(f"\n[OK] Full results saved to {out_path}")


if __name__ == "__main__":
    main()
