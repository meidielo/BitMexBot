"""
analyze_binding_constraint.py — Which V4 condition is the binding frequency constraint?

Answers the meta-question before committing to a 4H adaptation:
  - If bull regime is the binding constraint, 4H won't help (filter applies at daily anyway)
  - If funding setup is binding, 4H might help (funding changes intraday)
  - If liq spike is binding, 4H definitely helps (spikes are intraday events)

Analyzes:
  1. Base rate of each V4 condition on daily data (3 years)
  2. Conditional rates: P(A | B) for each pair
  3. Intraday spike frequency from the ~3 months of 15m Coinalyze data we have
"""

import os
import sqlite3
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from backtest_v4_extended import build_extended_dataset
from backtest_v3 import (
    FUNDING_THRESHOLD, LIQ_SPIKE_MULT, LIQ_LONG_DOM,
    LIQ_LOOKBACK, FUNDING_LOOKBACK_D, EMA_PERIOD,
)

COINALYZE_DB = os.path.join("data", "coinalyze.db")


def analyze_daily():
    """Base rates + conditional rates from the existing daily dataset."""
    print("=" * 72)
    print("  DAILY BASE RATES (3 years Coinalyze data)")
    print("=" * 72)

    df = build_extended_dataset(use_cache=True)
    n = len(df)

    # Individual conditions
    c1_bull = df["bull_regime"].astype(bool)
    c2_fund = df["funding_peak_pos"] > FUNDING_THRESHOLD
    c3_liq_spike = df["liq_long_ratio"] >= LIQ_SPIKE_MULT
    c3_long_dom = df["liq_long_pct"] >= LIQ_LONG_DOM
    c4_oi = df["oi_delta_pct"] < 0

    print(f"\n  Total days: {n}")
    print(f"\n  Individual condition pass rates:")
    print(f"    C1 Bull regime (close > EMA200):   {c1_bull.sum():>4} / {n} = {c1_bull.mean()*100:5.1f}%")
    print(f"    C2 Funding setup (peak_pos > thr): {c2_fund.sum():>4} / {n} = {c2_fund.mean()*100:5.1f}%")
    print(f"    C3a Liq spike (>= 3.0x avg):       {c3_liq_spike.sum():>4} / {n} = {c3_liq_spike.mean()*100:5.1f}%")
    print(f"    C3b Long dominance (>= 60%):       {c3_long_dom.sum():>4} / {n} = {c3_long_dom.mean()*100:5.1f}%")
    print(f"    C4 OI drop (delta < 0):            {c4_oi.sum():>4} / {n} = {c4_oi.mean()*100:5.1f}%")

    # Intersections — cumulative AND
    print(f"\n  Cumulative intersection (each filter applied in order):")
    step1 = c1_bull
    print(f"    Bull:                                   {step1.sum():>4} days")
    step2 = step1 & c2_fund
    print(f"    + Funding setup:                        {step2.sum():>4} days "
          f"(kept {step2.sum()/step1.sum()*100:.0f}% of bull days)")
    step3 = step2 & c3_liq_spike
    print(f"    + Liq spike:                            {step3.sum():>4} days "
          f"(kept {step3.sum()/step2.sum()*100 if step2.sum()>0 else 0:.0f}% of bull+fund)")
    step4 = step3 & c3_long_dom
    print(f"    + Long dominance:                       {step4.sum():>4} days "
          f"(kept {step4.sum()/step3.sum()*100 if step3.sum()>0 else 0:.0f}% of prev)")
    step5 = step4 & c4_oi
    print(f"    + OI drop:                              {step5.sum():>4} days "
          f"(kept {step5.sum()/step4.sum()*100 if step4.sum()>0 else 0:.0f}% of prev)")

    # Reverse: what if we remove bull regime filter?
    print(f"\n  Alt order — what if we DROP bull regime filter?")
    alt = c2_fund & c3_liq_spike & c3_long_dom & c4_oi
    print(f"    Fund + liq_spike + dom + OI (no bull): {alt.sum():>4} days "
          f"(vs {step5.sum()} with bull)")

    # Reverse: which single condition is most restrictive?
    print(f"\n  Which condition is most restrictive when others are held?")
    other_conds = c1_bull & c2_fund & c3_liq_spike & c3_long_dom & c4_oi
    print(f"    All conditions ANDed:                   {other_conds.sum():>4} days")
    print(f"\n    Drop C1 (bull):        {(c2_fund & c3_liq_spike & c3_long_dom & c4_oi).sum():>4}  "
          f"(+{(c2_fund & c3_liq_spike & c3_long_dom & c4_oi).sum() - other_conds.sum()})")
    print(f"    Drop C2 (funding):     {(c1_bull & c3_liq_spike & c3_long_dom & c4_oi).sum():>4}  "
          f"(+{(c1_bull & c3_liq_spike & c3_long_dom & c4_oi).sum() - other_conds.sum()})")
    print(f"    Drop C3a (liq spike):  {(c1_bull & c2_fund & c3_long_dom & c4_oi).sum():>4}  "
          f"(+{(c1_bull & c2_fund & c3_long_dom & c4_oi).sum() - other_conds.sum()})")
    print(f"    Drop C3b (long dom):   {(c1_bull & c2_fund & c3_liq_spike & c4_oi).sum():>4}  "
          f"(+{(c1_bull & c2_fund & c3_liq_spike & c4_oi).sum() - other_conds.sum()})")
    print(f"    Drop C4 (OI):          {(c1_bull & c2_fund & c3_liq_spike & c3_long_dom).sum():>4}  "
          f"(+{(c1_bull & c2_fund & c3_liq_spike & c3_long_dom).sum() - other_conds.sum()})")

    # The gain from dropping a condition tells you how much ADDITIONAL frequency
    # that condition was blocking.
    return df


def analyze_intraday():
    """
    Look at 15m Coinalyze data and estimate intraday spike frequency.
    We have ~3 months of 15m data, which gives us a rough sense of
    how much more often spikes occur at finer resolution.
    """
    print(f"\n{'=' * 72}")
    print(f"  INTRADAY SPIKE FREQUENCY (15m Coinalyze data)")
    print(f"{'=' * 72}")

    if not os.path.exists(COINALYZE_DB):
        print("  [WARN] Coinalyze DB not found. Skipping intraday analysis.")
        return

    conn = sqlite3.connect(COINALYZE_DB, timeout=5)

    # Load 15m liquidation aggregates
    df = pd.read_sql_query(
        "SELECT timestamp, liq_long, liq_short FROM liquidations_15m_agg "
        "ORDER BY timestamp", conn
    )

    if df.empty:
        print("  [WARN] No 15m data. Run coinalyze_collector.py to start building.")
        conn.close()
        return

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df = df.set_index("timestamp")

    first = df.index[0]
    last = df.index[-1]
    n_bars = len(df)
    days = (last - first).total_seconds() / 86400

    print(f"\n  15m data available: {first.date()} → {last.date()} "
          f"({days:.0f} days, {n_bars:,} bars)")

    if n_bars < 200:
        print("  Not enough 15m data for meaningful analysis")
        conn.close()
        return

    # Resample to different timeframes
    for rule, label in [("15min", "15m"), ("1h", "1h"), ("4h", "4h"), ("1D", "daily")]:
        resampled = df.resample(rule).agg({"liq_long": "sum", "liq_short": "sum"})
        resampled = resampled.dropna()
        if len(resampled) < 20:
            continue

        # Rolling average (20 periods like V4)
        resampled["long_avg"] = resampled["liq_long"].rolling(20).mean()
        resampled["ratio"] = resampled["liq_long"] / resampled["long_avg"]
        resampled = resampled.dropna()

        spikes = (resampled["ratio"] >= LIQ_SPIKE_MULT).sum()
        bars = len(resampled)
        rate = spikes / bars * 100 if bars > 0 else 0
        rate_per_day = spikes / days if days > 0 else 0

        print(f"    {label:>6}: {spikes:>4} spikes / {bars:>5} bars = "
              f"{rate:5.2f}% ({rate_per_day:.2f}/day)")

    conn.close()


def main():
    df = analyze_daily()
    analyze_intraday()

    print(f"\n{'=' * 72}")
    print("  INTERPRETATION")
    print(f"{'=' * 72}")
    print("""
  The numbers above tell you:

  1. If bull regime is already 70%+ of days, it's NOT the binding constraint.
     Moving to 4H won't help because the EMA200 filter applies at any resolution.

  2. If dropping OI confirmation adds many signals, the filter is overfitting
     your small sample. L07 claimed OI is a real mechanism — verify that here.

  3. If 4H intraday spike rate × bull regime × funding ≈ N=20-30/year,
     4H V4 is worth building. If not, pivot.

  4. Watch for the biggest delta when dropping conditions — that's the real
     frequency killer.
    """)


if __name__ == "__main__":
    main()
