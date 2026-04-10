"""
walk_forward.py — Walk-Forward Out-of-Sample Validation Engine

Tests whether V4 parameters generalize across time by:
  1. Split data into rolling train/test windows
  2. Evaluate signal quality on each IS (in-sample) and OOS (out-of-sample) window
  3. Compare IS vs OOS metrics — degradation indicates overfitting

Window structure (default):
  Train: 2 years, Test: 1 year, Step: 6 months
  Example folds with 2020-2026 data:
    Fold 1: Train 2020-2021, Test 2022
    Fold 2: Train 2020.5-2022.5, Test 2023
    Fold 3: Train 2021-2023, Test 2024
    ...

Parameters are FROZEN from backtest_v3.py. No optimization is performed.
This is pure out-of-sample evaluation.

Usage:
  python walk_forward.py [--refresh]
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from backtest_v3 import get_daily_signal, run_backtest, MIN_WARMUP
from backtest_v4_extended import build_extended_dataset, _pre_register_params

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
TRAIN_YEARS   = 2
TEST_YEARS    = 1
STEP_MONTHS   = 6

OUTPUT_DIR    = "data"
RESULTS_CSV   = os.path.join(OUTPUT_DIR, "walk_forward_results.csv")
SUMMARY_FILE  = os.path.join(OUTPUT_DIR, "walk_forward_summary.txt")


# ---------------------------------------------------------------------------
# Walk-forward splits
# ---------------------------------------------------------------------------
def generate_folds(df: pd.DataFrame, train_years: int = TRAIN_YEARS,
                   test_years: int = TEST_YEARS,
                   step_months: int = STEP_MONTHS) -> list[dict]:
    """
    Generate rolling (train, test) date ranges.
    Returns list of dicts with train_start, train_end, test_start, test_end.
    """
    start_date = df.index[0]
    end_date = df.index[-1]

    folds = []
    current_start = start_date

    while True:
        train_end = current_start + pd.DateOffset(years=train_years)
        test_start = train_end
        test_end = test_start + pd.DateOffset(years=test_years)

        # Don't create fold if test window extends beyond data
        if test_start >= end_date:
            break

        # Clip test_end to data end
        if test_end > end_date:
            test_end = end_date

        folds.append({
            "train_start": current_start,
            "train_end": train_end,
            "test_start": test_start,
            "test_end": test_end,
        })

        current_start += pd.DateOffset(months=step_months)

    return folds


# ---------------------------------------------------------------------------
# Fold evaluation
# ---------------------------------------------------------------------------
def evaluate_fold(df: pd.DataFrame, train_start, train_end,
                  test_start, test_end) -> dict:
    """
    Run backtest on train and test windows independently.
    Returns metrics for both windows.
    """
    # Slice data
    df_train = df[(df.index >= train_start) & (df.index < train_end)].copy()
    df_test = df[(df.index >= test_start) & (df.index < test_end)].copy()

    result = {
        "train_start": train_start.strftime("%Y-%m-%d"),
        "train_end": train_end.strftime("%Y-%m-%d"),
        "test_start": test_start.strftime("%Y-%m-%d"),
        "test_end": test_end.strftime("%Y-%m-%d"),
        "train_days": len(df_train),
        "test_days": len(df_test),
    }

    # Evaluate IS (in-sample)
    is_metrics = _run_and_measure(df_train)
    for k, v in is_metrics.items():
        result[f"is_{k}"] = v

    # Evaluate OOS (out-of-sample)
    oos_metrics = _run_and_measure(df_test)
    for k, v in oos_metrics.items():
        result[f"oos_{k}"] = v

    # Degradation ratios
    if is_metrics["n_trades"] > 0 and oos_metrics["n_trades"] > 0:
        if is_metrics["pf"] > 0:
            result["pf_degradation"] = round(oos_metrics["pf"] / is_metrics["pf"], 4)
        else:
            result["pf_degradation"] = 0

        if is_metrics["win_rate"] > 0:
            result["wr_degradation"] = round(
                oos_metrics["win_rate"] / is_metrics["win_rate"], 4)
        else:
            result["wr_degradation"] = 0
    else:
        result["pf_degradation"] = None
        result["wr_degradation"] = None

    return result


def _run_and_measure(df: pd.DataFrame) -> dict:
    """Run backtest on a DataFrame slice and return summary metrics."""
    if len(df) < MIN_WARMUP + 10:
        return {
            "n_trades": 0, "wins": 0, "losses": 0,
            "win_rate": 0, "pf": 0, "avg_r": 0,
            "total_pnl_btc": 0, "max_dd": 0,
        }

    result = run_backtest(df, quiet=True)
    trades = result["trades"]

    if not trades:
        return {
            "n_trades": 0, "wins": 0, "losses": 0,
            "win_rate": 0, "pf": 0, "avg_r": 0,
            "total_pnl_btc": 0, "max_dd": 0,
        }

    total = len(trades)
    wins = sum(1 for t in trades if t["outcome"] == "TP")
    losses = sum(1 for t in trades if t["outcome"] == "SL")
    wr = wins / total * 100 if total > 0 else 0

    gross_profit = sum(t["pnl_btc"] for t in trades if t["outcome"] == "TP")
    gross_loss = abs(sum(t["pnl_btc"] for t in trades if t["outcome"] == "SL"))
    pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")
    if pf == float("inf"):
        pf = 99.0  # cap for display

    avg_r = sum(t["actual_r"] for t in trades) / total
    total_pnl = sum(t["pnl_btc"] for t in trades)
    max_dd = max(t["drawdown_pct"] for t in trades) if trades else 0

    return {
        "n_trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate": round(wr, 1),
        "pf": round(pf, 2),
        "avg_r": round(avg_r, 2),
        "total_pnl_btc": round(total_pnl, 8),
        "max_dd": round(max_dd, 2),
    }


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
def print_walk_forward_report(folds_results: list):
    """Print formatted walk-forward results."""
    w = 90
    print(f"\n{'=' * w}")
    print(f"  WALK-FORWARD VALIDATION — V4 Trend-Following Liq Dip-Buy")
    print(f"  Train: {TRAIN_YEARS}yr, Test: {TEST_YEARS}yr, Step: {STEP_MONTHS}mo")
    print(f"{'=' * w}")

    # Header
    print(f"\n  {'Fold':>4} {'Train Period':>23} {'Test Period':>23} "
          f"{'IS N':>4} {'IS WR':>6} {'IS PF':>6}  "
          f"{'OOS N':>5} {'OOS WR':>7} {'OOS PF':>7} {'Degrad':>7}")
    print(f"  {'─' * 86}")

    total_oos_trades = 0
    total_oos_wins = 0
    pf_degradations = []

    for i, r in enumerate(folds_results):
        train_str = f"{r['train_start'][:7]}→{r['train_end'][:7]}"
        test_str = f"{r['test_start'][:7]}→{r['test_end'][:7]}"

        degrad_str = ""
        if r.get("pf_degradation") is not None:
            degrad_str = f"{r['pf_degradation']:.2f}×"
            pf_degradations.append(r['pf_degradation'])

        total_oos_trades += r.get("oos_n_trades", 0)
        total_oos_wins += r.get("oos_wins", 0)

        print(f"  {i+1:>4} {train_str:>23} {test_str:>23} "
              f"{r.get('is_n_trades', 0):>4} {r.get('is_win_rate', 0):>5.0f}% "
              f"{r.get('is_pf', 0):>5.2f}  "
              f"{r.get('oos_n_trades', 0):>5} {r.get('oos_win_rate', 0):>6.0f}% "
              f"{r.get('oos_pf', 0):>6.2f}  {degrad_str:>6}")

    # Summary
    print(f"\n  {'─' * 86}")
    oos_wr = total_oos_wins / total_oos_trades * 100 if total_oos_trades > 0 else 0
    avg_degrad = np.mean(pf_degradations) if pf_degradations else 0

    print(f"  AGGREGATE OOS: N={total_oos_trades}, WR={oos_wr:.0f}%")
    if pf_degradations:
        print(f"  Mean PF degradation: {avg_degrad:.2f}× "
              f"(1.0 = no degradation, <1.0 = OOS worse)")

    if avg_degrad < 0.5 and pf_degradations:
        print(f"\n  WARNING: Severe OOS degradation ({avg_degrad:.2f}×) suggests overfitting")
    elif avg_degrad < 0.8 and pf_degradations:
        print(f"\n  CAUTION: Moderate OOS degradation — parameters may not generalize well")
    elif pf_degradations:
        print(f"\n  OK: OOS degradation within acceptable range")

    print(f"{'=' * w}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Walk-forward validation for V4 strategy")
    parser.add_argument("--refresh", action="store_true",
                        help="Re-fetch data from exchanges")
    parser.add_argument("--train-years", type=int, default=TRAIN_YEARS)
    parser.add_argument("--test-years", type=int, default=TEST_YEARS)
    parser.add_argument("--step-months", type=int, default=STEP_MONTHS)
    args = parser.parse_args()

    print("=" * 64)
    print("  WALK-FORWARD VALIDATION ENGINE")
    print("  V4 parameters: FROZEN (zero optimization)")
    print("=" * 64)

    # Pre-register
    param_hash = _pre_register_params()

    # Build dataset
    df = build_extended_dataset(use_cache=not args.refresh)

    # Generate folds
    folds = generate_folds(df, args.train_years, args.test_years, args.step_months)
    print(f"\nGenerated {len(folds)} walk-forward folds")

    if not folds:
        print("[ERROR] Not enough data for walk-forward validation")
        print(f"  Need at least {args.train_years + args.test_years} years of data")
        sys.exit(1)

    # Evaluate each fold
    results = []
    for i, fold in enumerate(folds):
        print(f"\n  Evaluating fold {i+1}/{len(folds)}: "
              f"Train {fold['train_start'].strftime('%Y-%m')}→"
              f"{fold['train_end'].strftime('%Y-%m')}, "
              f"Test {fold['test_start'].strftime('%Y-%m')}→"
              f"{fold['test_end'].strftime('%Y-%m')}")

        r = evaluate_fold(df,
                         fold["train_start"], fold["train_end"],
                         fold["test_start"], fold["test_end"])
        results.append(r)

    # Report
    print_walk_forward_report(results)

    # Save CSV
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if results:
        fieldnames = list(results[0].keys())
        with open(RESULTS_CSV, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        print(f"\n[OK] Results saved to {RESULTS_CSV}")

    # Save summary
    with open(SUMMARY_FILE, "w") as f:
        f.write(f"Walk-Forward Validation Summary\n")
        f.write(f"Generated: {datetime.now(timezone.utc).isoformat()}\n")
        f.write(f"Param hash: {param_hash}\n")
        f.write(f"Folds: {len(folds)}\n")
        total_oos = sum(r.get("oos_n_trades", 0) for r in results)
        f.write(f"Total OOS trades: {total_oos}\n")


if __name__ == "__main__":
    main()
