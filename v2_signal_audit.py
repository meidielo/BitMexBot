"""
V2 Signal Historical Audit
===========================
Question: How often would V2 (Funding Rate Mean-Reversion) have fired
          across 6 years of cached data?

Verdict criteria (pre-registered):
  - Fires 0 times historically          -> signal logic is broken
  - Fires >0 but not in last 12 months  -> regime problem, not a bug
  - Fires regularly including recent     -> correctly strict right now
  - Never fires despite valid funding    -> threshold calibration issue

V2 signal logic (from signals.py / V2 era):
  Condition: abs(funding_rate) >= FUNDING_THRESHOLD (typically 0.05%)
  Direction: funding > 0 -> SHORT (longs over-leveraged)
             funding < 0 -> LONG  (shorts over-leveraged)
  This is the original mean-reversion thesis (killed in V3 rewrite).

We test the EXACT thresholds from the original codebase.
Do not tune after seeing results.
"""

import pandas as pd
import numpy as np
from pathlib import Path

# -- Pre-registered thresholds (from V2 era codebase) ------------------------
FUNDING_THRESHOLD_HIGH = 0.0005   # 0.05% - original extreme threshold (V2 single-rate)
FUNDING_THRESHOLD_MED  = 0.0003   # 0.03% - V4 setup threshold for comparison
FUNDING_THRESHOLD_LOW  = 0.0001   # 0.01% - baseline / near-zero

FUNDING_PATHS = [
    Path("data/xbtusd_funding_history.csv"),   # 6+ years (XBTUSD inverse, 2020+)
    Path("data/funding_history.csv"),          # ~2 years (XBTUSDT linear, 2024+)
    Path("data/funding_rates.csv"),
    Path("data/btc_funding.csv"),
    Path("data/XBTUSD_funding.csv"),
    Path("data/funding.csv"),
]


# -- Load funding data --------------------------------------------------------
def load_funding():
    for p in FUNDING_PATHS:
        if p.exists():
            print(f"[DATA] Loading {p}")
            df = pd.read_csv(p)
            df.columns = [c.lower().strip() for c in df.columns]

            ts_col   = next((c for c in df.columns if 'time' in c or 'date' in c), None)
            rate_col = next((c for c in df.columns
                             if 'fund' in c or c == 'rate' or 'rate' in c), None)

            if not ts_col or not rate_col:
                print(f"  [WARN] Could not identify columns. Columns: {list(df.columns)}")
                continue

            # Handle both ms-epoch ints and ISO datetime strings
            if pd.api.types.is_numeric_dtype(df[ts_col]):
                df[ts_col] = pd.to_datetime(df[ts_col].astype('int64'),
                                            unit='ms', utc=True)
            else:
                df[ts_col] = pd.to_datetime(df[ts_col], utc=True, errors='coerce')

            df = df.rename(columns={ts_col: 'timestamp', rate_col: 'funding_rate'})
            df = df[['timestamp', 'funding_rate']].dropna()
            df = df.set_index('timestamp').sort_index()
            return df

    raise FileNotFoundError(
        f"No funding file found. Tried: {FUNDING_PATHS}\n"
        f"Files in data/: {sorted([p.name for p in Path('data').glob('*.csv')])}"
    )


# -- Signal audit -------------------------------------------------------------
def audit_v2_signals(df: pd.DataFrame) -> dict:
    """
    Apply V2 signal logic at each threshold.
    Report: total fires, fire rate, first/last fire dates,
            fires in last 12/6 months, median inter-fire gap.
    """
    results = {}
    end_date = df.index.max()
    last_12m = end_date - pd.Timedelta(days=365)
    last_6m  = end_date - pd.Timedelta(days=180)

    for label, threshold in [
        ('HIGH (0.05%)', FUNDING_THRESHOLD_HIGH),
        ('MED  (0.03%)', FUNDING_THRESHOLD_MED),
        ('LOW  (0.01%)', FUNDING_THRESHOLD_LOW),
    ]:
        fired = df[df['funding_rate'].abs() >= threshold]
        n_total    = len(fired)
        n_last_12m = len(fired[fired.index >= last_12m])
        n_last_6m  = len(fired[fired.index >= last_6m])
        total_bars = len(df)
        fire_rate  = n_total / total_bars if total_bars > 0 else 0

        if n_total > 1:
            gaps = fired.index.to_series().diff().dt.total_seconds() / 3600
            median_gap_h = gaps.median()
        else:
            median_gap_h = None

        # Direction breakdown: positive funding (→ SHORT) vs negative (→ LONG)
        n_short = (fired['funding_rate'] > 0).sum() if n_total > 0 else 0
        n_long  = (fired['funding_rate'] < 0).sum() if n_total > 0 else 0

        results[label] = {
            'threshold'    : threshold,
            'n_total'      : n_total,
            'total_bars'   : total_bars,
            'fire_pct'     : round(fire_rate * 100, 2),
            'first_fire'   : str(fired.index.min().date()) if n_total > 0 else None,
            'last_fire'    : str(fired.index.max().date()) if n_total > 0 else None,
            'n_last_12m'   : n_last_12m,
            'n_last_6m'    : n_last_6m,
            'median_gap_h' : round(median_gap_h, 1) if median_gap_h else None,
            'n_short_setup': int(n_short),
            'n_long_setup' : int(n_long),
        }

    return results


def print_audit(results: dict, df: pd.DataFrame):
    print("\n-- FUNDING RATE STATISTICS ---------------------------------")
    print(f"  Total settlements  : {len(df):,}")
    print(f"  Date range         : {df.index.min().date()} -> {df.index.max().date()}")
    print(f"  Rate range         : {df['funding_rate'].min():+.4%} -> {df['funding_rate'].max():+.4%}")
    print(f"  Mean rate          : {df['funding_rate'].mean():+.5%}")
    print(f"  Median rate        : {df['funding_rate'].median():+.5%}")
    print(f"  % positive funding : {(df['funding_rate'] > 0).mean():.1%}")
    print(f"  % zero/baseline    : {(df['funding_rate'].abs() < 0.00005).mean():.1%}")

    # Year-by-year mean funding (reveals regime change)
    print("\n-- FUNDING BY YEAR (mean rate, max, % extreme) ------------")
    print(f"  {'Year':>4}  {'Mean':>10}  {'Max':>10}  {'Min':>10}  {'>=0.05%':>8}  {'N':>6}")
    df_year = df.groupby(df.index.year)
    for year, group in df_year:
        mean_r = group['funding_rate'].mean()
        max_r  = group['funding_rate'].max()
        min_r  = group['funding_rate'].min()
        pct_ex = (group['funding_rate'].abs() >= 0.0005).mean() * 100
        n      = len(group)
        print(f"  {year}  {mean_r:+.4%}  {max_r:+.4%}  {min_r:+.4%}  {pct_ex:>6.1f}%  {n:>6}")

    print("\n-- V2 SIGNAL FIRE AUDIT ------------------------------------")
    for label, r in results.items():
        print(f"\n  Threshold {label}:")
        print(f"    Total fires     : {r['n_total']:,} / {r['total_bars']:,} ({r['fire_pct']}%)")
        if r['n_total'] > 0:
            print(f"    SHORT setups    : {r['n_short_setup']:,}  (funding > threshold)")
            print(f"    LONG  setups    : {r['n_long_setup']:,}  (funding < -threshold)")
            print(f"    First fire      : {r['first_fire']}")
            print(f"    Last fire       : {r['last_fire']}")
            print(f"    Last 12 months  : {r['n_last_12m']}")
            print(f"    Last 6 months   : {r['n_last_6m']}")
            if r['median_gap_h']:
                print(f"    Median gap      : {r['median_gap_h']:.1f}h between fires")
        else:
            print(f"    -> NEVER FIRED - signal logic may be broken")

    print("\n-- VERDICT -------------------------------------------------")
    high = results['HIGH (0.05%)']
    med  = results['MED  (0.03%)']

    if high['n_total'] == 0:
        print("  ! BROKEN - 0.05% threshold never hit in full history")
        print("    -> Either funding data is wrong or signal threshold is unreachable")
    elif high['n_last_12m'] == 0:
        print("  + REGIME PROBLEM - fired historically, silent in last 12 months")
        print(f"    -> {high['n_total']} historical fires, 0 in last 12m, last on {high['last_fire']}")
        print("    -> V2 is not broken, just irrelevant in current market structure")
    elif high['n_last_6m'] > 0:
        print(f"  + ACTIVE - fires in recent data ({high['n_last_6m']} in last 6m)")
        print("    -> If live bot never triggered, check signal integration in main.py")
    else:
        print(f"  ~ BORDERLINE - {high['n_last_12m']} fires in last 12m, 0 in last 6m")
        print("    -> Regime thinning; signal still valid but frequency dropping")

    if med['n_total'] > 0:
        print(f"\n  Note: 0.03% threshold (V4 setup level) fires {med['n_total']}x total,")
        print(f"        {med['n_last_12m']}x in last 12m - context for funding exhaustion (Path X)")


# -- Main ---------------------------------------------------------------------
def main():
    print("=" * 60)
    print("V2 SIGNAL HISTORICAL AUDIT")
    print("=" * 60)

    df = load_funding()
    print(f"[DATA] {len(df):,} funding records loaded")

    results = audit_v2_signals(df)
    print_audit(results, df)

    df.to_csv("data/funding_audit_raw.csv")
    pd.DataFrame(results).T.to_csv("data/v2_signal_audit.csv")
    print("\n[SAVE] data/funding_audit_raw.csv, data/v2_signal_audit.csv")


if __name__ == '__main__':
    main()
