"""
funding_study.py — Funding Settlement Arbitrage: Phase 1 Historical Study

Tests whether extreme funding rates create predictable order flow around
BitMEX 8-hour settlement times (04:00, 12:00, 20:00 UTC for XBTUSD).

Thesis: When funding is highly positive, longs close before settlement to
avoid payment → selling pressure pre-settlement → rebound post-settlement.
Vice versa for negative funding.

Uses existing cached data only:
  - data/xbtusd_funding_history.csv  (6,860 funding records, 2020-2026)
  - data/xbtusd_raw_candles.csv      (658,520 5m candles, 2020-2026)

Go/No-Go gate:
  PASS if: p < 0.01 (corrected), effect ≥ 5 bps, consistent across ≥4 years
  FAIL → stop, do not build real-time infrastructure

Usage:
  python funding_study.py
  python funding_study.py --save-csv    # also save event-level CSV
  python funding_study.py --verbose     # per-event details
"""

import argparse
import os

import numpy as np
import pandas as pd
from scipy import stats

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
FUNDING_CSV = os.path.join("data", "xbtusd_funding_history.csv")
CANDLE_CSV  = os.path.join("data", "xbtusd_raw_candles.csv")
OUTPUT_CSV  = os.path.join("data", "funding_study_events.csv")

# Settlement times for XBTUSD inverse
SETTLE_HOURS = [4, 12, 20]

# Window around settlement (minutes)
WINDOW_MIN = 60

# Funding rate bin thresholds
BINS = {
    "extreme_neg": (-np.inf, -0.0005),
    "mod_neg":     (-0.0005, -0.0003),
    "mild_neg":    (-0.0003, 0.0),
    "baseline":    (0.0, 0.00015),      # 0.01% ± small buffer
    "mild_pos":    (0.00015, 0.0003),
    "mod_pos":     (0.0003, 0.0005),
    "extreme_pos": (0.0005, np.inf),
}

# Go/No-Go thresholds
P_THRESHOLD   = 0.01   # after multiple testing correction
BPS_THRESHOLD = 5.0    # minimum average move in basis points
MIN_YEARS     = 4      # effect must appear in ≥4 of 6 yearly cohorts
MIN_N         = 50     # minimum events in actionable bin

# BitMEX fee structure (for practical edge calculation)
TAKER_FEE_BPS = 7.5    # 0.075% round trip (entry + exit)


# ---------------------------------------------------------------------------
# Step 1: Load data
# ---------------------------------------------------------------------------

def load_data():
    """Load funding history and 5m candles, return with DatetimeIndex."""
    # Funding
    df_fund = pd.read_csv(FUNDING_CSV)
    df_fund["timestamp"] = pd.to_datetime(df_fund["timestamp"], utc=True)
    df_fund = df_fund.set_index("timestamp").sort_index()
    print(f"[OK] Funding: {len(df_fund):,} records "
          f"({df_fund.index[0].date()} → {df_fund.index[-1].date()})")

    # 5m candles
    df_5m = pd.read_csv(CANDLE_CSV)
    df_5m["timestamp"] = pd.to_datetime(df_5m["timestamp_ms"], unit="ms", utc=True)
    df_5m = df_5m.set_index("timestamp").sort_index()
    df_5m = df_5m[["open", "high", "low", "close", "volume"]]
    print(f"[OK] 5m candles: {len(df_5m):,} bars "
          f"({df_5m.index[0].date()} → {df_5m.index[-1].date()})")

    return df_fund, df_5m


# ---------------------------------------------------------------------------
# Step 2: Build settlement event table
# ---------------------------------------------------------------------------

def build_events(df_fund, df_5m):
    """
    For each funding settlement, extract price window and compute returns.

    Vectorized approach: build a close-price lookup Series indexed by
    rounded 5-min timestamps, then do fast .loc[] joins for all events.

    Price convention: "price at time T" = close of the 5m candle ending at T,
    i.e., the candle starting at T-5min. Applied consistently to all groups.
    """
    # Build fast lookup: close price keyed by candle start time (already 5m-aligned)
    close_lookup = df_5m["close"]

    # Precompute 5m log returns for volatility calculation
    df_5m_ret = np.log(df_5m["close"] / df_5m["close"].shift(1))

    offsets_min = [-60, -30, -5, 0, 5, 30, 60]
    offset_deltas = {m: pd.Timedelta(minutes=m) for m in offsets_min}

    # For each offset, the candle we want starts at (settlement + offset - 5min)
    candle_shift = pd.Timedelta(minutes=5)

    events = []
    n_dropped = 0

    for ts, row in df_fund.iterrows():
        rate = row["rate"]

        # Fast price lookup at each offset
        prices = {}
        valid = True
        for m in offsets_min:
            candle_start = ts + offset_deltas[m] - candle_shift
            try:
                prices[m] = close_lookup.at[candle_start]
            except KeyError:
                valid = False
                break

        if not valid:
            n_dropped += 1
            continue

        p_T = prices[0]
        if p_T <= 0 or prices[-60] <= 0:
            n_dropped += 1
            continue

        # Volume: slice the pre/post windows using searchsorted for speed
        ts_pre_start = ts - pd.Timedelta(minutes=60)
        ts_post_end = ts + pd.Timedelta(minutes=60)

        i_pre_start = df_5m.index.searchsorted(ts_pre_start, side="left")
        i_settle = df_5m.index.searchsorted(ts, side="left")
        i_post_end = df_5m.index.searchsorted(ts_post_end, side="left")

        vol_pre = df_5m.iloc[i_pre_start:i_settle]["volume"].sum()
        vol_post = df_5m.iloc[i_settle:i_post_end]["volume"].sum()

        # Volatility: std of 5m log returns in pre/post windows
        rets_pre = df_5m_ret.iloc[i_pre_start:i_settle]
        rets_post = df_5m_ret.iloc[i_settle:i_post_end]

        events.append({
            "timestamp":    ts,
            "rate":         rate,
            "hour":         ts.hour,
            "year":         ts.year,
            "price_T":      p_T,
            "ret_pre_60":   np.log(p_T / prices[-60]),
            "ret_pre_30":   np.log(p_T / prices[-30]),
            "ret_post_30":  np.log(prices[30] / p_T),
            "ret_post_60":  np.log(prices[60] / p_T),
            "ret_reversal": np.log(prices[60] / p_T) - np.log(p_T / prices[-60]),
            "vol_pre":      rets_pre.std() if len(rets_pre) > 2 else np.nan,
            "vol_post":     rets_post.std() if len(rets_post) > 2 else np.nan,
            "volume_pre":   vol_pre,
            "volume_post":  vol_post,
        })

    df_events = pd.DataFrame(events)
    df_events = df_events.dropna(subset=["ret_pre_60", "ret_post_30"])
    print(f"[OK] Built {len(df_events):,} settlement events "
          f"(dropped {n_dropped + len(df_fund) - len(df_events) - n_dropped} with missing data)")
    return df_events


# ---------------------------------------------------------------------------
# Step 3: Bin events by funding rate
# ---------------------------------------------------------------------------

def bin_events(df_events):
    """Assign funding rate bins."""
    conditions = []
    labels = []
    for label, (lo, hi) in BINS.items():
        mask = (df_events["rate"] > lo) & (df_events["rate"] <= hi)
        conditions.append(mask)
        labels.append(label)

    df_events["bin"] = np.select(conditions, labels, default="other")
    return df_events


# ---------------------------------------------------------------------------
# Step 4: Statistical tests
# ---------------------------------------------------------------------------

def run_stat_tests(df_events):
    """Run all statistical tests. Returns dict of results for the report."""
    results = {}
    metrics = ["ret_pre_60", "ret_pre_30", "ret_post_30", "ret_post_60", "ret_reversal"]

    # ---------------------------------------------------------------
    # Test 1: Conditional means by bin
    # ---------------------------------------------------------------
    bin_stats = {}
    all_pvals = []

    for bin_name in BINS:
        subset = df_events[df_events["bin"] == bin_name]
        n = len(subset)
        if n < 5:
            continue

        bin_stats[bin_name] = {"n": n}
        for metric in metrics:
            vals = subset[metric].dropna()
            if len(vals) < 5:
                continue
            mean = vals.mean()
            std = vals.std()
            t_stat, p_val = stats.ttest_1samp(vals, 0)
            all_pvals.append(p_val)
            bin_stats[bin_name][metric] = {
                "mean": mean, "std": std,
                "t": t_stat, "p": p_val,
                "mean_bps": mean * 10000,
            }

    results["bin_stats"] = bin_stats

    # ---------------------------------------------------------------
    # Test 2: Extreme vs baseline (Welch t-test)
    # ---------------------------------------------------------------
    baseline = df_events[df_events["bin"] == "baseline"]
    comparisons = {}

    for bin_name in ["extreme_pos", "extreme_neg", "mod_pos", "mod_neg"]:
        subset = df_events[df_events["bin"] == bin_name]
        if len(subset) < 5 or len(baseline) < 5:
            continue

        comparisons[bin_name] = {}
        for metric in metrics:
            b_vals = baseline[metric].dropna()
            s_vals = subset[metric].dropna()
            if len(b_vals) < 5 or len(s_vals) < 5:
                continue
            t_stat, p_val = stats.ttest_ind(s_vals, b_vals, equal_var=False)
            all_pvals.append(p_val)
            comparisons[bin_name][metric] = {
                "diff_bps": (s_vals.mean() - b_vals.mean()) * 10000,
                "t": t_stat, "p": p_val,
            }

    results["comparisons"] = comparisons

    # ---------------------------------------------------------------
    # Test 3: Continuous regression (ret ~ funding_rate)
    # ---------------------------------------------------------------
    regressions = {}
    for metric in metrics:
        x = df_events["rate"].values
        y = df_events[metric].values
        mask = ~(np.isnan(x) | np.isnan(y))
        x, y = x[mask], y[mask]
        if len(x) < 10:
            continue

        slope, intercept = np.polyfit(x, y, 1)
        y_pred = slope * x + intercept
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - y.mean()) ** 2)
        r_sq = 1 - ss_res / ss_tot if ss_tot > 0 else 0

        # t-stat and p-value for slope
        n = len(x)
        se_slope = np.sqrt(ss_res / (n - 2) / np.sum((x - x.mean()) ** 2))
        t_slope = slope / se_slope if se_slope > 0 else 0
        p_slope = 2 * stats.t.sf(abs(t_slope), n - 2)
        all_pvals.append(p_slope)

        regressions[metric] = {
            "slope": slope, "intercept": intercept,
            "r_sq": r_sq, "t": t_slope, "p": p_slope, "n": n,
        }

    results["regressions"] = regressions

    # ---------------------------------------------------------------
    # Test 4: Directional thesis test (one-sided)
    # ---------------------------------------------------------------
    directional = {}

    # Positive funding: expect ret_pre_60 < 0, ret_post_30 > 0
    for bin_name in ["extreme_pos", "mod_pos"]:
        subset = df_events[df_events["bin"] == bin_name]
        if len(subset) < 5:
            continue

        d = {}
        # Pre-settlement: expect negative (selling pressure)
        vals = subset["ret_pre_60"].dropna()
        if len(vals) >= 5:
            t_stat, p_two = stats.ttest_1samp(vals, 0)
            p_one = p_two / 2 if t_stat < 0 else 1 - p_two / 2
            all_pvals.append(p_one)
            d["pre_60"] = {"mean_bps": vals.mean() * 10000, "t": t_stat,
                           "p_one": p_one, "direction": "expected" if t_stat < 0 else "opposite"}

        # Post-settlement: expect positive (rebound)
        vals = subset["ret_post_30"].dropna()
        if len(vals) >= 5:
            t_stat, p_two = stats.ttest_1samp(vals, 0)
            p_one = p_two / 2 if t_stat > 0 else 1 - p_two / 2
            all_pvals.append(p_one)
            d["post_30"] = {"mean_bps": vals.mean() * 10000, "t": t_stat,
                            "p_one": p_one, "direction": "expected" if t_stat > 0 else "opposite"}

        directional[bin_name] = d

    # Negative funding: expect ret_pre_60 > 0, ret_post_30 < 0
    for bin_name in ["extreme_neg", "mod_neg"]:
        subset = df_events[df_events["bin"] == bin_name]
        if len(subset) < 5:
            continue

        d = {}
        vals = subset["ret_pre_60"].dropna()
        if len(vals) >= 5:
            t_stat, p_two = stats.ttest_1samp(vals, 0)
            p_one = p_two / 2 if t_stat > 0 else 1 - p_two / 2
            all_pvals.append(p_one)
            d["pre_60"] = {"mean_bps": vals.mean() * 10000, "t": t_stat,
                           "p_one": p_one, "direction": "expected" if t_stat > 0 else "opposite"}

        vals = subset["ret_post_30"].dropna()
        if len(vals) >= 5:
            t_stat, p_two = stats.ttest_1samp(vals, 0)
            p_one = p_two / 2 if t_stat < 0 else 1 - p_two / 2
            all_pvals.append(p_one)
            d["post_30"] = {"mean_bps": vals.mean() * 10000, "t": t_stat,
                            "p_one": p_one, "direction": "expected" if t_stat < 0 else "opposite"}

        directional[bin_name] = d

    results["directional"] = directional

    # ---------------------------------------------------------------
    # Test 5: Volume confirmation
    # ---------------------------------------------------------------
    vol_tests = {}
    baseline_vol = baseline["volume_pre"].dropna()
    for bin_name in ["extreme_pos", "extreme_neg"]:
        subset = df_events[df_events["bin"] == bin_name]
        if len(subset) < 5:
            continue
        s_vol = subset["volume_pre"].dropna()
        if len(s_vol) >= 5 and len(baseline_vol) >= 5:
            t_stat, p_val = stats.ttest_ind(s_vol, baseline_vol, equal_var=False)
            vol_tests[bin_name] = {
                "mean_extreme": s_vol.mean(),
                "mean_baseline": baseline_vol.mean(),
                "ratio": s_vol.mean() / baseline_vol.mean() if baseline_vol.mean() > 0 else 0,
                "t": t_stat, "p": p_val,
            }

    results["volume_tests"] = vol_tests

    # ---------------------------------------------------------------
    # Multiple testing correction (Holm-Bonferroni)
    # ---------------------------------------------------------------
    all_pvals = [p for p in all_pvals if not np.isnan(p)]
    if all_pvals:
        sorted_idx = np.argsort(all_pvals)
        n_tests = len(all_pvals)
        corrected = np.zeros(n_tests)
        for rank, idx in enumerate(sorted_idx):
            corrected[idx] = min(all_pvals[idx] * (n_tests - rank), 1.0)
        results["n_tests"] = n_tests
        results["min_corrected_p"] = min(corrected)
    else:
        results["n_tests"] = 0
        results["min_corrected_p"] = 1.0

    return results


# ---------------------------------------------------------------------------
# Step 5: Robustness checks
# ---------------------------------------------------------------------------

def robustness_checks(df_events):
    """Check effect stability across time-of-day, year, and vol regime."""
    robustness = {}

    # Only test bins with enough data for the thesis
    test_bins = ["extreme_pos", "extreme_neg"]
    metric = "ret_post_30"

    # ---------------------------------------------------------------
    # By settlement hour
    # ---------------------------------------------------------------
    by_hour = {}
    for hour in SETTLE_HOURS:
        subset = df_events[df_events["hour"] == hour]
        for bin_name in test_bins:
            bin_sub = subset[subset["bin"] == bin_name]
            vals = bin_sub[metric].dropna()
            if len(vals) < 5:
                continue
            mean = vals.mean()
            t_stat, p_val = stats.ttest_1samp(vals, 0)
            by_hour[(hour, bin_name)] = {
                "n": len(vals), "mean_bps": mean * 10000,
                "t": t_stat, "p": p_val,
            }
    robustness["by_hour"] = by_hour

    # ---------------------------------------------------------------
    # By year
    # ---------------------------------------------------------------
    by_year = {}
    years = sorted(df_events["year"].unique())
    for year in years:
        subset = df_events[df_events["year"] == year]
        for bin_name in test_bins:
            bin_sub = subset[subset["bin"] == bin_name]
            vals = bin_sub[metric].dropna()
            if len(vals) < 3:
                by_year[(year, bin_name)] = {"n": len(vals), "mean_bps": np.nan,
                                              "insufficient": True}
                continue
            mean = vals.mean()
            t_stat, p_val = stats.ttest_1samp(vals, 0)
            by_year[(year, bin_name)] = {
                "n": len(vals), "mean_bps": mean * 10000,
                "t": t_stat, "p": p_val,
            }
    robustness["by_year"] = by_year

    # ---------------------------------------------------------------
    # By volatility regime (trailing 24h realized vol)
    # ---------------------------------------------------------------
    # Compute trailing vol from 5m returns in 24h window (288 candles)
    # Use pre-computed vol_pre as a proxy for local volatility
    median_vol = df_events["vol_pre"].median()
    df_events = df_events.copy()
    df_events["vol_regime"] = np.where(
        df_events["vol_pre"] > median_vol, "high_vol", "low_vol")

    by_vol = {}
    for regime in ["high_vol", "low_vol"]:
        subset = df_events[df_events["vol_regime"] == regime]
        for bin_name in test_bins:
            bin_sub = subset[subset["bin"] == bin_name]
            vals = bin_sub[metric].dropna()
            if len(vals) < 5:
                continue
            mean = vals.mean()
            t_stat, p_val = stats.ttest_1samp(vals, 0)
            by_vol[(regime, bin_name)] = {
                "n": len(vals), "mean_bps": mean * 10000,
                "t": t_stat, "p": p_val,
            }
    robustness["by_vol"] = by_vol

    return robustness


# ---------------------------------------------------------------------------
# Step 6: Practical edge estimate
# ---------------------------------------------------------------------------

def practical_edge(df_events):
    """Estimate tradeable edge: win rate, EV, Sharpe."""
    edge = {}

    for bin_name in ["extreme_pos", "mod_pos", "extreme_neg", "mod_neg"]:
        subset = df_events[df_events["bin"] == bin_name]
        if len(subset) < 10:
            continue

        # Determine trade direction based on thesis
        if "pos" in bin_name:
            # Positive funding → short pre-settlement, long post-settlement
            # We test "long post-settlement" (ret_post_30 > 0)
            returns = subset["ret_post_30"].dropna()
            direction = "LONG post-settle"
        else:
            # Negative funding → short post-settlement (ret_post_30 < 0)
            returns = -subset["ret_post_30"].dropna()  # flip sign for SHORT
            direction = "SHORT post-settle"

        returns_bps = returns * 10000
        net_returns_bps = returns_bps - TAKER_FEE_BPS  # subtract fees

        wins = (net_returns_bps > 0).sum()
        total = len(net_returns_bps)
        wr = wins / total * 100

        avg_win = net_returns_bps[net_returns_bps > 0].mean() if wins > 0 else 0
        avg_loss = net_returns_bps[net_returns_bps <= 0].mean() if (total - wins) > 0 else 0

        ev = net_returns_bps.mean()
        std = net_returns_bps.std()

        # Annualized Sharpe (3 trades/day × 365 days)
        sharpe = (ev / std * np.sqrt(3 * 365)) if std > 0 else 0

        edge[bin_name] = {
            "direction": direction,
            "n": total,
            "wr": wr,
            "avg_win_bps": avg_win,
            "avg_loss_bps": avg_loss,
            "ev_bps": ev,
            "ev_after_fees_bps": ev,  # already subtracted
            "sharpe": sharpe,
        }

    return edge


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def print_report(df_events, stat_results, robust, edge):
    """Print structured report to stdout."""
    w = 76
    metrics = ["ret_pre_60", "ret_pre_30", "ret_post_30", "ret_post_60", "ret_reversal"]
    metric_labels = {
        "ret_pre_60": "Pre-60m", "ret_pre_30": "Pre-30m",
        "ret_post_30": "Post-30m", "ret_post_60": "Post-60m",
        "ret_reversal": "Reversal",
    }

    print("\n" + "=" * w)
    print("  FUNDING SETTLEMENT STUDY — Phase 1 Historical Analysis")
    print("  XBTUSD Inverse, 04:00/12:00/20:00 UTC, 2020-2026")
    print("=" * w)

    # Section 1: Sample sizes
    print(f"\n{'─'*w}")
    print("  SECTION 1: Sample Sizes by Funding Rate Bin")
    print(f"{'─'*w}")
    bin_order = ["extreme_neg", "mod_neg", "mild_neg", "baseline",
                 "mild_pos", "mod_pos", "extreme_pos"]
    print(f"  {'Bin':<15} {'Range':>25} {'N':>6}")
    for b in bin_order:
        lo, hi = BINS[b]
        lo_s = f"{lo*100:.3f}%" if lo > -np.inf else "-∞"
        hi_s = f"{hi*100:.3f}%" if hi < np.inf else "+∞"
        n = len(df_events[df_events["bin"] == b])
        print(f"  {b:<15} ({lo_s:>8}, {hi_s:>8}]  {n:>5}")
    print(f"  {'TOTAL':<15} {'':>25} {len(df_events):>5}")

    # Section 2: Mean returns by bin
    print(f"\n{'─'*w}")
    print("  SECTION 2: Mean Returns by Bin (basis points)")
    print(f"{'─'*w}")
    bs = stat_results["bin_stats"]

    header = f"  {'Bin':<13} {'N':>5}"
    for m in metrics:
        header += f"  {metric_labels[m]:>8}"
    print(header)

    for b in bin_order:
        if b not in bs:
            continue
        row = f"  {b:<13} {bs[b]['n']:>5}"
        for m in metrics:
            if m in bs[b]:
                bps = bs[b][m]["mean_bps"]
                p = bs[b][m]["p"]
                sig = "*" if p < 0.05 else (" " if p < 0.1 else " ")
                row += f"  {bps:>+7.1f}{sig}"
            else:
                row += f"  {'N/A':>8}"
        print(row)
    print("  (* = p < 0.05)")

    # Section 3: Extreme vs baseline
    print(f"\n{'─'*w}")
    print("  SECTION 3: Extreme vs Baseline (Welch t-test, bps difference)")
    print(f"{'─'*w}")
    comp = stat_results["comparisons"]
    for bin_name in ["extreme_pos", "extreme_neg", "mod_pos", "mod_neg"]:
        if bin_name not in comp:
            continue
        print(f"\n  {bin_name} vs baseline:")
        for m in metrics:
            if m not in comp[bin_name]:
                continue
            c = comp[bin_name][m]
            sig = "***" if c["p"] < 0.001 else ("**" if c["p"] < 0.01 else
                  ("*" if c["p"] < 0.05 else ""))
            print(f"    {metric_labels[m]:<10}: diff={c['diff_bps']:>+6.1f} bps  "
                  f"t={c['t']:>+5.2f}  p={c['p']:.4f} {sig}")

    # Section 4: Regression
    print(f"\n{'─'*w}")
    print("  SECTION 4: Linear Regression (return ~ funding_rate)")
    print(f"{'─'*w}")
    reg = stat_results["regressions"]
    for m in metrics:
        if m not in reg:
            continue
        r = reg[m]
        sig = "***" if r["p"] < 0.001 else ("**" if r["p"] < 0.01 else
              ("*" if r["p"] < 0.05 else ""))
        print(f"  {metric_labels[m]:<10}: slope={r['slope']:>+10.4f}  "
              f"R²={r['r_sq']:.6f}  t={r['t']:>+6.2f}  "
              f"p={r['p']:.4f} {sig}  (N={r['n']})")

    # Section 5: Directional thesis test
    print(f"\n{'─'*w}")
    print("  SECTION 5: Directional Thesis Test (one-sided)")
    print(f"{'─'*w}")
    dt = stat_results["directional"]
    for bin_name in ["extreme_pos", "mod_pos", "extreme_neg", "mod_neg"]:
        if bin_name not in dt:
            continue
        d = dt[bin_name]
        thesis = "LONG post" if "pos" in bin_name else "SHORT post"
        print(f"\n  {bin_name} (thesis: {thesis}):")
        for window, data in d.items():
            sig = "***" if data["p_one"] < 0.001 else ("**" if data["p_one"] < 0.01 else
                  ("*" if data["p_one"] < 0.05 else ""))
            print(f"    {window:<8}: mean={data['mean_bps']:>+6.1f} bps  "
                  f"t={data['t']:>+5.2f}  p(one)={data['p_one']:.4f}  "
                  f"[{data['direction']}] {sig}")

    # Section 6: Volume confirmation
    print(f"\n{'─'*w}")
    print("  SECTION 6: Volume Confirmation (pre-settlement)")
    print(f"{'─'*w}")
    vt = stat_results["volume_tests"]
    for bin_name, v in vt.items():
        sig = "*" if v["p"] < 0.05 else ""
        print(f"  {bin_name}: extreme/baseline ratio={v['ratio']:.2f}×  "
              f"t={v['t']:>+5.2f}  p={v['p']:.4f} {sig}")

    # Section 7: Practical edge
    print(f"\n{'─'*w}")
    print(f"  SECTION 7: Practical Edge (after {TAKER_FEE_BPS:.1f} bps round-trip fees)")
    print(f"{'─'*w}")
    print(f"  {'Bin':<13} {'Dir':<18} {'N':>5} {'WR':>6} {'AvgW':>7} "
          f"{'AvgL':>7} {'EV':>7} {'Sharpe':>7}")
    for bin_name in ["extreme_pos", "mod_pos", "extreme_neg", "mod_neg"]:
        if bin_name not in edge:
            continue
        e = edge[bin_name]
        print(f"  {bin_name:<13} {e['direction']:<18} {e['n']:>5} "
              f"{e['wr']:>5.1f}% {e['avg_win_bps']:>+6.1f} "
              f"{e['avg_loss_bps']:>+6.1f} {e['ev_bps']:>+6.1f} "
              f"{e['sharpe']:>6.2f}")

    # Section 8: Robustness
    print(f"\n{'─'*w}")
    print("  SECTION 8: Robustness — Post-30m Return by Segment")
    print(f"{'─'*w}")

    print("\n  By settlement hour:")
    print(f"  {'Hour':>6} {'Bin':<13} {'N':>5} {'Mean(bps)':>10} {'p':>8}")
    for (hour, bin_name), v in sorted(robust["by_hour"].items()):
        print(f"  {hour:>5}h {bin_name:<13} {v['n']:>5} "
              f"{v['mean_bps']:>+9.1f} {v['p']:>8.4f}")

    print("\n  By year:")
    print(f"  {'Year':>6} {'Bin':<13} {'N':>5} {'Mean(bps)':>10} {'p':>8}")
    for (year, bin_name), v in sorted(robust["by_year"].items()):
        if v.get("insufficient"):
            print(f"  {year:>6} {bin_name:<13} {v['n']:>5}  insufficient data")
        else:
            print(f"  {year:>6} {bin_name:<13} {v['n']:>5} "
                  f"{v['mean_bps']:>+9.1f} {v['p']:>8.4f}")

    print("\n  By volatility regime:")
    print(f"  {'Regime':<10} {'Bin':<13} {'N':>5} {'Mean(bps)':>10} {'p':>8}")
    for (regime, bin_name), v in sorted(robust["by_vol"].items()):
        print(f"  {regime:<10} {bin_name:<13} {v['n']:>5} "
              f"{v['mean_bps']:>+9.1f} {v['p']:>8.4f}")

    # ---------------------------------------------------------------
    # VERDICT
    # ---------------------------------------------------------------
    print(f"\n{'='*w}")
    print("  VERDICT")
    print(f"{'='*w}")

    # Check go/no-go criteria
    passes = []
    fails = []

    # Criterion 1: Statistical significance after correction
    min_p = stat_results["min_corrected_p"]
    n_tests = stat_results["n_tests"]
    if min_p < P_THRESHOLD:
        passes.append(f"Statistical significance: min corrected p={min_p:.4f} < {P_THRESHOLD} "
                       f"({n_tests} tests)")
    else:
        fails.append(f"Statistical significance: min corrected p={min_p:.4f} >= {P_THRESHOLD} "
                      f"({n_tests} tests)")

    # Criterion 2: Economic significance (≥5 bps in any extreme bin post-30m)
    max_effect = 0
    max_bin = ""
    for bin_name in ["extreme_pos", "extreme_neg"]:
        if bin_name in stat_results["directional"]:
            d = stat_results["directional"][bin_name]
            if "post_30" in d:
                effect = abs(d["post_30"]["mean_bps"])
                if effect > max_effect:
                    max_effect = effect
                    max_bin = bin_name

    if max_effect >= BPS_THRESHOLD:
        passes.append(f"Economic significance: {max_bin} post-30m = {max_effect:.1f} bps "
                       f">= {BPS_THRESHOLD} bps")
    else:
        fails.append(f"Economic significance: max effect = {max_effect:.1f} bps "
                      f"< {BPS_THRESHOLD} bps threshold")

    # Criterion 3: Year-over-year consistency
    by_year = robust["by_year"]
    for bin_name in ["extreme_pos", "extreme_neg"]:
        consistent_years = 0
        total_years = 0
        for (year, bn), v in by_year.items():
            if bn != bin_name:
                continue
            if v.get("insufficient"):
                continue
            total_years += 1
            # "Consistent" = effect in predicted direction
            if "pos" in bin_name and v["mean_bps"] > 0:
                consistent_years += 1
            elif "neg" in bin_name and v["mean_bps"] < 0:
                consistent_years += 1

        if total_years > 0:
            if consistent_years >= MIN_YEARS:
                passes.append(f"Year consistency ({bin_name}): {consistent_years}/{total_years} "
                               f">= {MIN_YEARS}")
            else:
                fails.append(f"Year consistency ({bin_name}): {consistent_years}/{total_years} "
                              f"< {MIN_YEARS}")

    # Criterion 4: Minimum N in actionable bin
    for bin_name in ["extreme_pos", "extreme_neg"]:
        n = len(df_events[df_events["bin"] == bin_name])
        if n >= MIN_N:
            passes.append(f"Sample size ({bin_name}): N={n} >= {MIN_N}")
        else:
            fails.append(f"Sample size ({bin_name}): N={n} < {MIN_N}")

    for p in passes:
        print(f"  [PASS] {p}")
    for f in fails:
        print(f"  [FAIL] {f}")

    if not fails:
        print(f"\n  >>> VERDICT: PASS — Proceed to Phase 2 (real-time infrastructure) <<<")
    else:
        print(f"\n  >>> VERDICT: FAIL — {len(fails)} criteria not met. "
              f"Do NOT build infrastructure. <<<")

    print("=" * w)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Funding Settlement Arbitrage — Phase 1 Historical Study")
    parser.add_argument("--save-csv", action="store_true",
                        help="Save event-level data to CSV")
    parser.add_argument("--verbose", action="store_true",
                        help="Print per-event details")
    args = parser.parse_args()

    # Load
    df_fund, df_5m = load_data()

    # Build events
    df_events = build_events(df_fund, df_5m)

    # Bin
    df_events = bin_events(df_events)

    # Stats
    stat_results = run_stat_tests(df_events)

    # Robustness
    robust = robustness_checks(df_events)

    # Practical edge
    edge = practical_edge(df_events)

    # Report
    print_report(df_events, stat_results, robust, edge)

    # Optional CSV export
    if args.save_csv:
        df_events.to_csv(OUTPUT_CSV, index=False)
        print(f"\n[OK] Saved {len(df_events)} events to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
