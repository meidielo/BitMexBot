"""
cointegration_study.py — Statistical Arbitrage Phase 1: Co-integration Scan

Tests whether any crypto perpetual swap pairs on BitMEX are co-integrated,
which is the mathematical prerequisite for pairs trading.

Co-integration ≠ correlation. Two assets can be perfectly correlated yet
drift apart forever. Co-integration guarantees their linear combination
(the spread) is stationary — it reverts to a constant mean.

Tests performed per pair:
  1. Engle-Granger two-step test (ADF on OLS residuals)
  2. Johansen trace test (multivariate, rank detection)
  3. Half-life of mean reversion (Ornstein-Uhlenbeck)
  4. Hurst exponent (persistence measure: H < 0.5 = mean-reverting)

Go/No-Go gate:
  PASS if: ≥1 pair passes both EG and Johansen at p < 0.05,
           half-life is 1-30 days (tradeable), Hurst < 0.5,
           and the relationship is stable across time

Uses 15m OHLCV from BitMEX mainnet via ccxt. Fetches and caches per-symbol.

Usage:
  python cointegration_study.py              # full scan
  python cointegration_study.py --quick      # top 5 pairs only
  python cointegration_study.py --save-csv   # export results
"""

import argparse
import os
import time
from datetime import datetime, timezone
from itertools import combinations

import numpy as np
import pandas as pd
from scipy import stats as sp_stats
from statsmodels.tsa.stattools import adfuller, coint
from statsmodels.tsa.vector_ar.vecm import coint_johansen

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATA_DIR   = "data"
CACHE_DIR  = os.path.join(DATA_DIR, "pairs_cache")
OUTPUT_CSV = os.path.join(DATA_DIR, "cointegration_results.csv")

# Candidate symbols (USDT-margined linear perps — highest liquidity on BitMEX)
# Only majors with deep order books and multi-year history
CANDIDATE_SYMBOLS = [
    "BTC/USDT:USDT",
    "ETH/USDT:USDT",
    "SOL/USDT:USDT",
    "XRP/USDT:USDT",
    "DOGE/USDT:USDT",
    "LINK/USDT:USDT",
    "AVAX/USDT:USDT",
    "LTC/USDT:USDT",
    "BNB/USDT:USDT",
    "ADA/USD:BTC",       # inverse — tests cross-margining pair
]

# Quick mode: test only the most likely pairs
QUICK_PAIRS = [
    ("BTC/USDT:USDT", "ETH/USDT:USDT"),
    ("ETH/USDT:USDT", "SOL/USDT:USDT"),
    ("LTC/USDT:USDT", "BTC/USDT:USDT"),
    ("LINK/USDT:USDT", "ETH/USDT:USDT"),
    ("DOGE/USDT:USDT", "XRP/USDT:USDT"),
]

# Timeframe and history
TIMEFRAME      = "15m"
RAW_TIMEFRAME  = "5m"       # fetch 5m, resample to 15m for consistency
FETCH_LIMIT    = 1000
EARLIEST       = "2023-01-01T00:00:00Z"  # 3+ years of history
RATE_LIMIT_S   = 0.3

# Statistical thresholds
P_THRESHOLD         = 0.05   # co-integration test significance
MAX_HALF_LIFE_DAYS  = 30     # must mean-revert within 30 days
MIN_HALF_LIFE_DAYS  = 1      # not too fast (noise)
HURST_THRESHOLD     = 0.5    # H < 0.5 = mean-reverting

# Rolling window for stability check
STABILITY_WINDOW    = 90     # days
MIN_STABLE_WINDOWS  = 3      # must pass in ≥3 of rolling windows


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def _get_cache_path(symbol: str) -> str:
    """Return cache file path for a symbol."""
    safe_name = symbol.replace("/", "_").replace(":", "_")
    return os.path.join(CACHE_DIR, f"{safe_name}_5m.csv")


def fetch_symbol(symbol: str, use_cache: bool = True) -> pd.DataFrame:
    """Fetch 5m OHLCV for a symbol, cache to CSV."""
    import ccxt

    cache_path = _get_cache_path(symbol)

    if use_cache and os.path.exists(cache_path):
        df = pd.read_csv(cache_path)
        df["timestamp"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
        df = df.set_index("timestamp").sort_index()
        print(f"  [CACHE] {symbol}: {len(df):,} 5m candles")
        return df

    print(f"  [FETCH] {symbol} from {EARLIEST}...")
    exchange = ccxt.bitmex()
    exchange.load_markets()

    since = exchange.parse8601(EARLIEST)
    step_ms = 5 * 60 * 1000
    all_rows = []
    page = 0

    while True:
        try:
            batch = exchange.fetch_ohlcv(symbol, timeframe=RAW_TIMEFRAME,
                                          since=since, limit=FETCH_LIMIT)
        except Exception as e:
            print(f"    [ERROR] page {page}: {e}")
            break

        if not batch:
            break

        all_rows.extend(batch)
        page += 1
        since = batch[-1][0] + step_ms

        if page % 100 == 0:
            ts_str = datetime.fromtimestamp(batch[-1][0] / 1000, tz=timezone.utc
                                             ).strftime("%Y-%m-%d")
            print(f"    ... page {page} | {ts_str} | {len(all_rows):,} candles")

        if len(batch) < FETCH_LIMIT:
            break

        time.sleep(RATE_LIMIT_S)

    if not all_rows:
        print(f"    [WARN] No data for {symbol}")
        return pd.DataFrame()

    # Save cache
    os.makedirs(CACHE_DIR, exist_ok=True)
    df = pd.DataFrame(all_rows, columns=["timestamp_ms", "open", "high", "low", "close", "volume"])
    df.to_csv(cache_path, index=False)

    df["timestamp"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    df = df.set_index("timestamp").sort_index()
    print(f"    [OK] {symbol}: {len(df):,} 5m candles ({page} pages)")
    return df


def load_all_symbols(symbols: list, use_cache: bool = True) -> dict:
    """Fetch and return 15m close series for all symbols."""
    series = {}
    for sym in symbols:
        df_5m = fetch_symbol(sym, use_cache=use_cache)
        if df_5m.empty:
            continue

        # Resample 5m → 15m
        df_15m = df_5m["close"].resample("15min").last().dropna()
        series[sym] = df_15m
        print(f"    → {sym}: {len(df_15m):,} 15m bars")

    return series


# ---------------------------------------------------------------------------
# Co-integration tests
# ---------------------------------------------------------------------------

def test_pair(s1: pd.Series, s2: pd.Series, name1: str, name2: str) -> dict:
    """
    Run the full co-integration test battery on a pair.
    Returns dict with all test results.
    """
    # Align on common timestamps
    df = pd.DataFrame({"y": s1, "x": s2}).dropna()
    if len(df) < 500:
        return {"pair": f"{name1} / {name2}", "error": f"Insufficient overlap: {len(df)}"}

    y = df["y"].values
    x = df["x"].values
    n = len(y)

    result = {
        "pair": f"{name1} / {name2}",
        "sym1": name1, "sym2": name2,
        "n_obs": n,
        "overlap_days": (df.index[-1] - df.index[0]).days,
    }

    # --- Correlation (for reference, not for trading decisions) ---
    corr = np.corrcoef(y, x)[0, 1]
    result["correlation"] = round(corr, 4)

    # --- Engle-Granger test ---
    try:
        eg_stat, eg_pval, eg_crit = coint(y, x, trend="c", maxlag=None, autolag="AIC")
        result["eg_stat"] = round(eg_stat, 4)
        result["eg_pval"] = round(eg_pval, 4)
        result["eg_pass"] = eg_pval < P_THRESHOLD
    except Exception as e:
        result["eg_error"] = str(e)
        result["eg_pass"] = False

    # --- OLS hedge ratio ---
    # y = β·x + α + ε → spread = y - β·x
    beta = np.polyfit(x, y, 1)
    hedge_ratio = beta[0]
    intercept = beta[1]
    spread = y - hedge_ratio * x
    result["hedge_ratio"] = round(hedge_ratio, 6)
    result["intercept"] = round(intercept, 4)

    # --- ADF on spread (direct stationarity test) ---
    try:
        adf_stat, adf_pval, _, _, adf_crit, _ = adfuller(spread, maxlag=None, autolag="AIC")
        result["adf_stat"] = round(adf_stat, 4)
        result["adf_pval"] = round(adf_pval, 4)
        result["adf_pass"] = adf_pval < P_THRESHOLD
    except Exception as e:
        result["adf_error"] = str(e)
        result["adf_pass"] = False

    # --- Johansen trace test ---
    try:
        joh = coint_johansen(df[["y", "x"]], det_order=0, k_ar_diff=1)
        # Trace stat for rank=0 (null: no co-integration)
        trace_stat = joh.lr1[0]
        trace_crit_95 = joh.cvt[0, 1]  # 95% critical value
        result["johansen_trace"] = round(trace_stat, 4)
        result["johansen_crit95"] = round(trace_crit_95, 4)
        result["johansen_pass"] = trace_stat > trace_crit_95
    except Exception as e:
        result["johansen_error"] = str(e)
        result["johansen_pass"] = False

    # --- Half-life of mean reversion (Ornstein-Uhlenbeck) ---
    spread_lag = spread[:-1]
    spread_diff = np.diff(spread)
    if len(spread_lag) > 10:
        # ΔS = θ·(μ - S_lag) + ε → regress ΔS on S_lag
        slope_ou, _ = np.polyfit(spread_lag, spread_diff, 1)
        if slope_ou < 0:
            # Half-life = -ln(2) / θ, where θ = slope (negative = mean-reverting)
            half_life_bars = -np.log(2) / slope_ou
            half_life_days = half_life_bars * 15 / (60 * 24)  # 15m bars → days
        else:
            half_life_days = np.inf  # not mean-reverting
    else:
        half_life_days = np.inf

    result["half_life_days"] = round(half_life_days, 2) if half_life_days < 1e6 else np.inf
    result["half_life_pass"] = MIN_HALF_LIFE_DAYS <= half_life_days <= MAX_HALF_LIFE_DAYS

    # --- Hurst exponent (rescaled range method) ---
    try:
        hurst = _hurst_exponent(spread)
        result["hurst"] = round(hurst, 4)
        result["hurst_pass"] = hurst < HURST_THRESHOLD
    except Exception:
        result["hurst"] = np.nan
        result["hurst_pass"] = False

    # --- Spread statistics ---
    spread_mean = spread.mean()
    spread_std = spread.std()
    result["spread_mean"] = round(spread_mean, 4)
    result["spread_std"] = round(spread_std, 4)
    result["spread_zscore_now"] = round((spread[-1] - spread_mean) / spread_std, 2) if spread_std > 0 else 0

    # --- Overall pass ---
    result["overall_pass"] = (
        result.get("eg_pass", False) and
        result.get("johansen_pass", False) and
        result.get("half_life_pass", False) and
        result.get("hurst_pass", False)
    )

    return result


def _hurst_exponent(series: np.ndarray, max_lag: int = 100) -> float:
    """Compute Hurst exponent via rescaled range (R/S) method."""
    lags = range(2, min(max_lag, len(series) // 4))
    rs_values = []

    for lag in lags:
        # Split into chunks of size `lag`
        n_chunks = len(series) // lag
        if n_chunks < 1:
            continue

        rs_chunk = []
        for i in range(n_chunks):
            chunk = series[i * lag:(i + 1) * lag]
            mean_chunk = chunk.mean()
            deviations = np.cumsum(chunk - mean_chunk)
            r = deviations.max() - deviations.min()
            s = chunk.std()
            if s > 0:
                rs_chunk.append(r / s)

        if rs_chunk:
            rs_values.append((lag, np.mean(rs_chunk)))

    if len(rs_values) < 5:
        return 0.5  # inconclusive

    log_lags = np.log([v[0] for v in rs_values])
    log_rs = np.log([v[1] for v in rs_values])

    hurst, _ = np.polyfit(log_lags, log_rs, 1)
    return hurst


# ---------------------------------------------------------------------------
# Stability check (rolling window)
# ---------------------------------------------------------------------------

def check_stability(s1: pd.Series, s2: pd.Series,
                    window_days: int = STABILITY_WINDOW) -> dict:
    """
    Roll a window across the data and check if co-integration holds
    in multiple sub-periods.
    """
    df = pd.DataFrame({"y": s1, "x": s2}).dropna()
    if len(df) < 1000:
        return {"stable": False, "reason": "insufficient data"}

    bars_per_day = 24 * 4  # 15m bars per day
    window_bars = window_days * bars_per_day
    step_bars = window_bars // 2  # 50% overlap

    passes = 0
    total = 0
    window_results = []

    for start in range(0, len(df) - window_bars, step_bars):
        chunk = df.iloc[start:start + window_bars]
        y = chunk["y"].values
        x = chunk["x"].values

        try:
            _, pval, _ = coint(y, x, trend="c", maxlag=None, autolag="AIC")
            passed = pval < P_THRESHOLD
        except Exception:
            passed = False
            pval = 1.0

        total += 1
        if passed:
            passes += 1

        window_results.append({
            "start": chunk.index[0].strftime("%Y-%m-%d"),
            "end": chunk.index[-1].strftime("%Y-%m-%d"),
            "eg_pval": round(pval, 4),
            "pass": passed,
        })

    return {
        "stable": passes >= MIN_STABLE_WINDOWS,
        "passes": passes,
        "total": total,
        "pass_rate": round(passes / total * 100, 1) if total > 0 else 0,
        "windows": window_results,
    }


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def print_report(results: list, stability: dict):
    """Print structured report."""
    w = 80
    print("\n" + "=" * w)
    print("  CO-INTEGRATION STUDY — Statistical Arbitrage Phase 1")
    print("  BitMEX Perpetual Swaps, 15m resolution")
    print("=" * w)

    # Section 1: All pairs summary
    print(f"\n{'─'*w}")
    print("  SECTION 1: Pair Scan Results")
    print(f"{'─'*w}")
    print(f"  {'Pair':<30} {'Corr':>5} {'EG p':>7} {'Joh':>5} "
          f"{'H-L(d)':>7} {'Hurst':>6} {'Pass':>5}")
    print(f"  {'─'*72}")

    for r in sorted(results, key=lambda x: x.get("eg_pval", 1)):
        if "error" in r:
            print(f"  {r['pair']:<30} — {r['error']}")
            continue

        eg_p = r.get("eg_pval", 1)
        joh = "Y" if r.get("johansen_pass") else "N"
        hl = r.get("half_life_days", np.inf)
        hl_s = f"{hl:>6.1f}" if hl < 1000 else "   inf"
        hurst = r.get("hurst", np.nan)
        hurst_s = f"{hurst:>5.3f}" if not np.isnan(hurst) else "  N/A"
        overall = "YES" if r.get("overall_pass") else "no"

        print(f"  {r['pair']:<30} {r['correlation']:>5.3f} "
              f"{eg_p:>7.4f} {joh:>5} {hl_s} {hurst_s} {overall:>5}")

    # Section 2: Detailed results for passing pairs
    passing = [r for r in results if r.get("overall_pass")]
    print(f"\n{'─'*w}")
    print(f"  SECTION 2: Detailed Results for Passing Pairs ({len(passing)})")
    print(f"{'─'*w}")

    if not passing:
        print("  No pairs passed all four tests.")
    else:
        for r in passing:
            print(f"\n  {r['pair']}")
            print(f"    Observations:    {r['n_obs']:,} (15m bars, {r['overlap_days']} days)")
            print(f"    Correlation:     {r['correlation']:.4f}")
            print(f"    Engle-Granger:   stat={r['eg_stat']:.4f}  p={r['eg_pval']:.4f}")
            print(f"    Johansen trace:  {r['johansen_trace']:.4f} > {r['johansen_crit95']:.4f}")
            print(f"    Hedge ratio:     β={r['hedge_ratio']:.6f}")
            print(f"    Half-life:       {r['half_life_days']:.1f} days")
            print(f"    Hurst exponent:  {r['hurst']:.4f}")
            print(f"    Spread z-score:  {r['spread_zscore_now']:.2f} (current)")

    # Section 3: Stability
    print(f"\n{'─'*w}")
    print("  SECTION 3: Rolling Window Stability")
    print(f"{'─'*w}")

    for pair_name, stab in stability.items():
        if not stab.get("windows"):
            print(f"  {pair_name}: {stab.get('reason', 'not tested')}")
            continue

        print(f"\n  {pair_name}: {stab['passes']}/{stab['total']} windows passed "
              f"({stab['pass_rate']:.0f}%) — {'STABLE' if stab['stable'] else 'UNSTABLE'}")
        for win in stab["windows"]:
            tag = "PASS" if win["pass"] else "fail"
            print(f"    {win['start']} → {win['end']}: p={win['eg_pval']:.4f} [{tag}]")

    # Section 4: Verdict
    print(f"\n{'='*w}")
    print("  VERDICT")
    print(f"{'='*w}")

    stable_passing = [
        r for r in passing
        if stability.get(r["pair"], {}).get("stable", False)
    ]

    if stable_passing:
        print(f"\n  >>> PASS — {len(stable_passing)} pair(s) are co-integrated and stable <<<")
        for r in stable_passing:
            stab = stability[r["pair"]]
            print(f"    {r['pair']}: β={r['hedge_ratio']:.4f}, "
                  f"HL={r['half_life_days']:.1f}d, H={r['hurst']:.3f}, "
                  f"stability={stab['pass_rate']:.0f}%")
        print(f"\n  Proceed to Phase 2: Spread backtest with entry/exit signals.")
    else:
        if passing:
            print(f"\n  >>> FAIL — {len(passing)} pair(s) passed static tests but failed stability <<<")
            print("    Co-integration exists momentarily but is not persistent.")
            print("    The hedge ratio drifts, making the spread non-stationary over time.")
        else:
            print(f"\n  >>> FAIL — No pairs passed all four co-integration tests <<<")
            print("    None of the tested pairs have a stationary spread.")
        print(f"\n  Do NOT build pairs trading infrastructure.")

    print("=" * w)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Co-integration Study — Statistical Arbitrage Phase 1")
    parser.add_argument("--quick", action="store_true",
                        help="Test only the top 5 most likely pairs")
    parser.add_argument("--save-csv", action="store_true",
                        help="Save results to CSV")
    parser.add_argument("--refresh", action="store_true",
                        help="Force fresh fetch from BitMEX (skip cache)")
    args = parser.parse_args()

    use_cache = not args.refresh

    # Determine which symbols to fetch
    if args.quick:
        symbols_needed = set()
        for a, b in QUICK_PAIRS:
            symbols_needed.add(a)
            symbols_needed.add(b)
        symbols_needed = sorted(symbols_needed)
        pairs_to_test = QUICK_PAIRS
    else:
        symbols_needed = CANDIDATE_SYMBOLS
        pairs_to_test = list(combinations(CANDIDATE_SYMBOLS, 2))

    print(f"Fetching {len(symbols_needed)} symbols...")
    series = load_all_symbols(symbols_needed, use_cache=use_cache)

    print(f"\nTesting {len(pairs_to_test)} pairs...")
    results = []
    for sym1, sym2 in pairs_to_test:
        if sym1 not in series or sym2 not in series:
            results.append({"pair": f"{sym1} / {sym2}",
                            "error": "missing data for one or both symbols"})
            continue
        r = test_pair(series[sym1], series[sym2], sym1, sym2)
        results.append(r)

    # Stability check for passing pairs
    passing = [r for r in results if r.get("overall_pass")]
    stability = {}
    for r in passing:
        pair_name = r["pair"]
        sym1, sym2 = r["sym1"], r["sym2"]
        print(f"\nStability check: {pair_name}...")
        stability[pair_name] = check_stability(series[sym1], series[sym2])

    # Report
    print_report(results, stability)

    # Optional CSV
    if args.save_csv:
        df_results = pd.DataFrame([r for r in results if "error" not in r])
        if not df_results.empty:
            df_results.to_csv(OUTPUT_CSV, index=False)
            print(f"\n[OK] Saved results to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
