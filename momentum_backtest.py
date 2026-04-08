"""
momentum_backtest.py — Cross-Sectional Momentum: Market-Neutral Long/Short

Ranks the BitMEX perpetual swap universe by Volatility-Adjusted Momentum,
goes LONG the top quartile and SHORT the bottom quartile. Rebalances weekly.

Market-neutral design: crypto beta cancels out. Profit comes from the spread
between strong and weak assets, not from directional market exposure.

Ranking formula:
  Score_i = R_i / σ_i
  R_i   = total return over lookback window (default 30 days)
  σ_i   = std dev of daily returns over that window

This buys smooth grinders and shorts volatile bleeders.

Data: universe_builder.py's SQLite DB (132K daily candles, 272 symbols,
survivorship-bias-free with seasoning + volume filters).

Usage:
  python momentum_backtest.py
  python momentum_backtest.py --lookback 60 --top-pct 0.25 --fee-bps 15
  python momentum_backtest.py --save-csv
"""

import argparse
import os
import sqlite3
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from universe_builder import get_universe, DB_PATH as UNIVERSE_DB

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
OUTPUT_DIR        = "data"

# Momentum parameters
LOOKBACK_DAYS     = 30       # ranking lookback window
TOP_PCT           = 0.25     # top 25% = long, bottom 25% = short
REBALANCE_FREQ    = 7        # days between rebalances

# Costs (applied per trade leg, each way)
FEE_BPS           = 7.5      # BitMEX taker fee (0.075%)
SLIPPAGE_BPS      = 5.0      # estimated slippage (0.05%)
TOTAL_COST_BPS    = FEE_BPS + SLIPPAGE_BPS  # 12.5 bps per trade per leg

# Rank buffer: existing holdings stay unless they drop past this extended boundary
# e.g., a long position stays until it drops below top 40% (not just 25%)
RANK_BUFFER_PCT   = 0.15     # buffer beyond the quartile boundary

# Portfolio
INITIAL_CAPITAL   = 100_000  # USD notional
LEVERAGE          = 1.0      # no leverage (market-neutral, risk managed by hedging)

# Backtest range
START_DATE        = "2022-06-01"  # after enough instruments are seasoned
END_DATE          = None          # default: latest available

# Emergency close penalty for mid-week delistings
DELIST_PENALTY_BPS = 50      # 0.5% forced liquidation cost


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_price_matrix(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Load all daily close prices into a date × symbol matrix.
    Returns DataFrame: index=date (str), columns=symbol, values=close price.
    """
    df = pd.read_sql_query(
        "SELECT symbol, date, close FROM daily_ohlcv ORDER BY date, symbol",
        conn)
    matrix = df.pivot(index="date", columns="symbol", values="close")
    matrix = matrix.sort_index()
    print(f"[OK] Price matrix: {matrix.shape[0]} dates × {matrix.shape[1]} symbols")
    return matrix


def load_settlement_dates(conn: sqlite3.Connection) -> dict:
    """Load settlement dates for all instruments (for mid-week delist detection)."""
    rows = conn.execute(
        "SELECT symbol, settlement_date FROM instruments WHERE settlement_date IS NOT NULL"
    ).fetchall()
    return {r[0]: r[1] for r in rows}


# ---------------------------------------------------------------------------
# Cross-Sectional Dispersion
# ---------------------------------------------------------------------------

def compute_csd(prices: pd.DataFrame, date: str, lookback: int = 7,
                eligible: list = None) -> float:
    """
    Compute Cross-Sectional Dispersion (CSD) for the universe.

    CSD_t = sqrt( (1/N) * sum( (R_i - R_mean)^2 ) )

    Where R_i is the return of asset i over the lookback window,
    and R_mean is the mean return of all eligible assets.

    High CSD = assets moving differently (momentum profits).
    Low CSD = assets moving in lockstep (momentum bleeds on fees).
    """
    date_idx = prices.index.get_loc(date) if date in prices.index else None
    if date_idx is None or date_idx < lookback:
        return np.nan

    window = prices.iloc[date_idx - lookback:date_idx + 1]
    if eligible:
        cols = [s for s in eligible if s in window.columns]
        window = window[cols]

    if window.empty or len(window.columns) < 4:
        return np.nan

    # Returns over the lookback
    rets = (window.iloc[-1] / window.iloc[0] - 1).dropna()
    if len(rets) < 4:
        return np.nan

    mean_ret = rets.mean()
    csd = np.sqrt(((rets - mean_ret) ** 2).mean())
    return csd


# ---------------------------------------------------------------------------
# Momentum scoring
# ---------------------------------------------------------------------------

def compute_momentum_scores(prices: pd.DataFrame, date: str,
                            lookback: int = LOOKBACK_DAYS,
                            eligible: list = None) -> pd.Series:
    """
    Compute volatility-adjusted momentum score for each symbol.

    Score_i = R_i / σ_i
      R_i = log return over lookback window
      σ_i = std dev of daily log returns over lookback window

    Only scores eligible symbols. Returns Series indexed by symbol.
    """
    # Get lookback window
    date_idx = prices.index.get_loc(date) if date in prices.index else None
    if date_idx is None or date_idx < lookback:
        return pd.Series(dtype=float)

    window = prices.iloc[date_idx - lookback:date_idx + 1]

    if eligible is not None:
        # Only score eligible symbols
        cols = [s for s in eligible if s in window.columns]
        window = window[cols]

    if window.empty:
        return pd.Series(dtype=float)

    # Log returns
    log_rets = np.log(window / window.shift(1)).iloc[1:]

    # Total return over window
    total_ret = log_rets.sum()

    # Volatility (std of daily log returns)
    vol = log_rets.std()

    # Volatility-adjusted momentum
    scores = total_ret / vol
    scores = scores.replace([np.inf, -np.inf], np.nan).dropna()

    return scores.sort_values(ascending=False)


# ---------------------------------------------------------------------------
# Portfolio construction
# ---------------------------------------------------------------------------

def construct_portfolio(scores: pd.Series, top_pct: float = TOP_PCT,
                        current_portfolio: dict = None,
                        buffer_pct: float = RANK_BUFFER_PCT) -> dict:
    """
    Assign symbols to long/short legs based on momentum rank.

    Top quartile → LONG (equal weight)
    Bottom quartile → SHORT (equal weight)
    Middle 50% → no position

    RANK BUFFER: Existing holdings get a wider boundary to prevent churning.
    A long position stays as long as it's in the top (top_pct + buffer_pct).
    A short position stays as long as it's in the bottom (top_pct + buffer_pct).
    New entries still require strict top/bottom quartile rank.

    Returns dict: {symbol: weight}, where weight > 0 = long, < 0 = short.
    Weights sum to ~0 (market neutral).
    """
    n = len(scores)
    if n < 4:
        return {}

    if current_portfolio is None:
        current_portfolio = {}

    n_long_strict = max(1, int(n * top_pct))
    n_short_strict = max(1, int(n * top_pct))
    n_long_buffer = max(1, int(n * (top_pct + buffer_pct)))
    n_short_buffer = max(1, int(n * (top_pct + buffer_pct)))

    # Strict entry zones
    long_entry_zone = set(scores.index[:n_long_strict].tolist())
    short_entry_zone = set(scores.index[-n_short_strict:].tolist())

    # Extended buffer zones (for existing positions only)
    long_buffer_zone = set(scores.index[:n_long_buffer].tolist())
    short_buffer_zone = set(scores.index[-n_short_buffer:].tolist())

    # Build portfolio: existing positions stay if within buffer, new entries need strict zone
    long_symbols = []
    short_symbols = []

    for sym in scores.index:
        was_long = current_portfolio.get(sym, 0) > 0
        was_short = current_portfolio.get(sym, 0) < 0

        if sym in long_entry_zone:
            long_symbols.append(sym)
        elif was_long and sym in long_buffer_zone:
            long_symbols.append(sym)  # keep — still in buffer
        elif sym in short_entry_zone:
            short_symbols.append(sym)
        elif was_short and sym in short_buffer_zone:
            short_symbols.append(sym)  # keep — still in buffer

    # Balance legs: ensure roughly equal count
    n_target = max(1, min(len(long_symbols), len(short_symbols)))
    # Don't trim below strict count
    n_target = max(n_target, min(n_long_strict, n_short_strict))
    long_symbols = long_symbols[:max(n_target, len(long_symbols))]
    short_symbols = short_symbols[:max(n_target, len(short_symbols))]

    if not long_symbols or not short_symbols:
        return {}

    # Equal weight within each leg
    long_weight = 0.5 / len(long_symbols)
    short_weight = -0.5 / len(short_symbols)

    portfolio = {}
    for s in long_symbols:
        portfolio[s] = long_weight
    for s in short_symbols:
        portfolio[s] = short_weight

    return portfolio


# ---------------------------------------------------------------------------
# Backtest engine
# ---------------------------------------------------------------------------

def run_backtest(prices: pd.DataFrame, conn: sqlite3.Connection,
                 lookback: int = LOOKBACK_DAYS,
                 top_pct: float = TOP_PCT,
                 fee_bps: float = TOTAL_COST_BPS,
                 start_date: str = START_DATE,
                 end_date: str = END_DATE) -> dict:
    """
    Run the market-neutral momentum backtest.

    Weekly rebalance cycle:
    1. Get eligible universe for rebalance date
    2. Compute vol-adjusted momentum scores
    3. Construct target portfolio (long top Q, short bottom Q)
    4. Calculate turnover and trading costs
    5. Compute portfolio return until next rebalance
    6. Check for mid-week delistings (forced close with penalty)
    """
    settlement_dates = load_settlement_dates(conn)

    # Align dates
    all_dates = sorted(prices.index.tolist())
    if start_date:
        all_dates = [d for d in all_dates if d >= start_date]
    if end_date:
        all_dates = [d for d in all_dates if d <= end_date]

    if len(all_dates) < lookback + REBALANCE_FREQ:
        print("[ERROR] Insufficient data for backtest")
        return {}

    # Rebalance dates (every N days)
    rebal_dates = all_dates[lookback::REBALANCE_FREQ]

    portfolio = {}  # current holdings: {symbol: weight}
    capital = INITIAL_CAPITAL
    peak_capital = capital
    max_dd = 0

    trades = []
    weekly_returns = []
    total_cost_paid = 0

    print(f"\nRunning momentum backtest: {rebal_dates[0]} → {rebal_dates[-1]}")
    print(f"  Lookback: {lookback}d, Rebalance: {REBALANCE_FREQ}d")
    print(f"  Long/Short: top/bottom {top_pct*100:.0f}%")
    print(f"  Costs: {fee_bps:.1f} bps per trade leg")
    print(f"  Initial capital: ${INITIAL_CAPITAL:,.0f}\n")

    for i, rebal_date in enumerate(rebal_dates[:-1]):
        next_rebal = rebal_dates[i + 1]

        # 1. Get eligible universe
        eligible = get_universe(conn, rebal_date)
        eligible_symbols = [u["symbol"] for u in eligible]

        # 2. Check for mid-week delistings in current portfolio
        delist_cost = 0
        for sym in list(portfolio.keys()):
            settle = settlement_dates.get(sym)
            if settle and settle <= next_rebal and settle > rebal_date:
                # Forced close at settlement — apply penalty
                delist_cost += abs(portfolio[sym]) * (DELIST_PENALTY_BPS / 10000) * capital
                del portfolio[sym]

        # 3. Score eligible symbols
        scores = compute_momentum_scores(prices, rebal_date, lookback, eligible_symbols)

        if len(scores) < 4:
            # Not enough symbols to form quartiles
            weekly_returns.append({
                "date": rebal_date, "return": 0, "n_long": 0, "n_short": 0,
                "turnover": 0, "cost": delist_cost,
            })
            continue

        # 4. Construct target portfolio (with rank buffer to reduce turnover)
        target = construct_portfolio(scores, top_pct, current_portfolio=portfolio)

        # 5. Calculate turnover
        all_symbols = set(list(portfolio.keys()) + list(target.keys()))
        turnover = 0
        for sym in all_symbols:
            old_w = portfolio.get(sym, 0)
            new_w = target.get(sym, 0)
            turnover += abs(new_w - old_w)

        # Trading cost = turnover × cost_bps × capital
        trade_cost = turnover * (fee_bps / 10000) * capital + delist_cost
        total_cost_paid += trade_cost

        # 6. Compute portfolio return over holding period
        hold_start_idx = all_dates.index(rebal_date)
        hold_end_idx = all_dates.index(next_rebal)
        hold_dates = all_dates[hold_start_idx:hold_end_idx + 1]

        if len(hold_dates) < 2:
            portfolio = target
            continue

        # Portfolio return = sum of (weight_i × return_i) for each symbol
        port_ret = 0
        n_long = sum(1 for w in target.values() if w > 0)
        n_short = sum(1 for w in target.values() if w < 0)

        for sym, weight in target.items():
            if sym not in prices.columns:
                continue
            p_start = prices.loc[rebal_date, sym] if rebal_date in prices.index else None
            p_end = prices.loc[next_rebal, sym] if next_rebal in prices.index else None

            if p_start is None or p_end is None or pd.isna(p_start) or pd.isna(p_end):
                continue
            if p_start <= 0:
                continue

            sym_ret = (p_end - p_start) / p_start
            port_ret += weight * sym_ret

        # Net return after costs
        cost_drag = trade_cost / capital if capital > 0 else 0
        net_ret = port_ret - cost_drag

        capital *= (1 + net_ret)
        if capital > peak_capital:
            peak_capital = capital
        dd = (peak_capital - capital) / peak_capital * 100
        if dd > max_dd:
            max_dd = dd

        # Cross-Sectional Dispersion
        csd = compute_csd(prices, rebal_date, lookback=REBALANCE_FREQ,
                          eligible=eligible_symbols)

        weekly_returns.append({
            "date": rebal_date,
            "next_date": next_rebal,
            "return": round(net_ret * 100, 4),
            "gross_return": round(port_ret * 100, 4),
            "n_long": n_long,
            "n_short": n_short,
            "n_universe": len(eligible_symbols),
            "turnover": round(turnover * 100, 2),
            "cost": round(trade_cost, 2),
            "capital": round(capital, 2),
            "drawdown": round(dd, 2),
            "csd": round(csd, 4) if not np.isnan(csd) else None,
        })

        # Log top/bottom
        if i % 13 == 0:  # ~quarterly
            long_syms = [s for s, w in target.items() if w > 0]
            short_syms = [s for s, w in target.items() if w < 0]
            print(f"  {rebal_date}: L={n_long} S={n_short} "
                  f"U={len(eligible_symbols)} "
                  f"ret={net_ret*100:+.2f}% cap=${capital:,.0f} "
                  f"DD={dd:.1f}%")
            print(f"    LONG:  {', '.join(long_syms[:5])}")
            print(f"    SHORT: {', '.join(short_syms[:5])}")

        portfolio = target

    return {
        "weekly_returns": weekly_returns,
        "final_capital": capital,
        "max_drawdown": max_dd,
        "total_cost": total_cost_paid,
    }


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def print_report(result: dict):
    """Print backtest results."""
    wr = result["weekly_returns"]
    if not wr:
        print("\n  NO TRADES GENERATED.")
        return

    w = 76
    df = pd.DataFrame(wr)

    total_weeks = len(df)
    pos_weeks = (df["return"] > 0).sum()
    neg_weeks = (df["return"] <= 0).sum()
    win_rate = pos_weeks / total_weeks * 100

    total_ret = (result["final_capital"] / INITIAL_CAPITAL - 1) * 100
    ann_ret = ((result["final_capital"] / INITIAL_CAPITAL) **
               (365 / (total_weeks * REBALANCE_FREQ)) - 1) * 100

    avg_ret = df["return"].mean()
    std_ret = df["return"].std()
    sharpe = (avg_ret / std_ret * np.sqrt(52)) if std_ret > 0 else 0  # annualized

    avg_turnover = df["turnover"].mean()
    total_cost = result["total_cost"]

    avg_long = df["n_long"].mean()
    avg_short = df["n_short"].mean()

    # Best and worst weeks
    best = df.loc[df["return"].idxmax()]
    worst = df.loc[df["return"].idxmin()]

    # Profit factor
    gross_wins = df[df["return"] > 0]["return"].sum()
    gross_losses = abs(df[df["return"] <= 0]["return"].sum())
    pf = gross_wins / gross_losses if gross_losses > 0 else float("inf")

    print(f"\n{'='*w}")
    print("  CROSS-SECTIONAL MOMENTUM — Market-Neutral Long/Short")
    print("  Volatility-Adjusted Ranking | Weekly Rebalance")
    print(f"{'='*w}")
    print(f"  Period:         {df.iloc[0]['date']} → {df.iloc[-1].get('next_date', 'N/A')}")
    print(f"  Weeks:          {total_weeks} ({pos_weeks}W / {neg_weeks}L)")
    print(f"  Win rate:       {win_rate:.1f}%")
    print(f"  Profit factor:  {pf:.2f}")
    print(f"{'─'*w}")
    print(f"  Start capital:  ${INITIAL_CAPITAL:,.0f}")
    print(f"  End capital:    ${result['final_capital']:,.0f}")
    print(f"  Total return:   {total_ret:+.1f}%")
    print(f"  Annualized:     {ann_ret:+.1f}%")
    print(f"  Sharpe ratio:   {sharpe:.2f}")
    print(f"  Max drawdown:   {result['max_drawdown']:.1f}%")
    print(f"{'─'*w}")
    print(f"  Avg weekly ret: {avg_ret:+.3f}%")
    print(f"  Std weekly ret: {std_ret:.3f}%")
    print(f"  Best week:      {best['date']} {best['return']:+.2f}%")
    print(f"  Worst week:     {worst['date']} {worst['return']:+.2f}%")
    print(f"{'─'*w}")
    print(f"  Avg turnover:   {avg_turnover:.1f}% per rebalance")
    print(f"  Total costs:    ${total_cost:,.0f} "
          f"({total_cost/INITIAL_CAPITAL*100:.1f}% of initial)")
    print(f"  Avg positions:  {avg_long:.1f} long / {avg_short:.1f} short")
    print(f"{'='*w}")

    # Yearly breakdown
    df["year"] = df["date"].str[:4]
    print(f"\n  Yearly breakdown:")
    print(f"  {'Year':>6} {'Weeks':>6} {'Return':>8} {'Sharpe':>8} {'MaxDD':>7} {'AvgCSD':>8}")
    for year, grp in df.groupby("year"):
        yr_ret = (1 + grp["return"] / 100).prod() - 1
        yr_std = grp["return"].std()
        yr_sharpe = (grp["return"].mean() / yr_std * np.sqrt(52)) if yr_std > 0 else 0
        yr_dd = grp["drawdown"].max()
        yr_csd = grp["csd"].dropna().mean() if "csd" in grp.columns else 0
        print(f"  {year:>6} {len(grp):>6} {yr_ret*100:>+7.1f}% {yr_sharpe:>7.2f} "
              f"{yr_dd:>6.1f}% {yr_csd:>7.2%}")

    # CSD vs Return correlation
    if "csd" in df.columns:
        csd_valid = df.dropna(subset=["csd"])
        if len(csd_valid) > 10:
            corr = csd_valid["csd"].corr(csd_valid["return"])
            print(f"\n  CSD-Return correlation: {corr:.3f}")

            # Quartile analysis: performance in high vs low CSD environments
            median_csd = csd_valid["csd"].median()
            high_csd = csd_valid[csd_valid["csd"] > median_csd]
            low_csd = csd_valid[csd_valid["csd"] <= median_csd]

            high_ret = high_csd["return"].mean()
            low_ret = low_csd["return"].mean()
            high_sharpe = (high_csd["return"].mean() / high_csd["return"].std()
                           * np.sqrt(52)) if high_csd["return"].std() > 0 else 0
            low_sharpe = (low_csd["return"].mean() / low_csd["return"].std()
                          * np.sqrt(52)) if low_csd["return"].std() > 0 else 0

            print(f"\n  CSD Regime Analysis (median CSD = {median_csd:.4f}):")
            print(f"  {'Regime':<15} {'Weeks':>6} {'AvgRet':>8} {'Sharpe':>8} {'AvgCSD':>8}")
            print(f"  {'High CSD':<15} {len(high_csd):>6} {high_ret:>+7.3f}% "
                  f"{high_sharpe:>7.2f} {high_csd['csd'].mean():>7.2%}")
            print(f"  {'Low CSD':<15} {len(low_csd):>6} {low_ret:>+7.3f}% "
                  f"{low_sharpe:>7.2f} {low_csd['csd'].mean():>7.2%}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Cross-Sectional Momentum — Market-Neutral Long/Short Backtest")
    parser.add_argument("--lookback", type=int, default=LOOKBACK_DAYS,
                        help=f"Momentum lookback days (default: {LOOKBACK_DAYS})")
    parser.add_argument("--top-pct", type=float, default=TOP_PCT,
                        help=f"Top/bottom percentile (default: {TOP_PCT})")
    parser.add_argument("--fee-bps", type=float, default=TOTAL_COST_BPS,
                        help=f"Total cost per trade in bps (default: {TOTAL_COST_BPS})")
    parser.add_argument("--start", type=str, default=START_DATE,
                        help=f"Start date (default: {START_DATE})")
    parser.add_argument("--end", type=str, default=END_DATE,
                        help="End date (default: latest)")
    parser.add_argument("--save-csv", action="store_true",
                        help="Save weekly returns to CSV")
    args = parser.parse_args()

    conn = sqlite3.connect(UNIVERSE_DB, timeout=10)

    # Load price data
    prices = load_price_matrix(conn)

    # Run backtest
    result = run_backtest(
        prices, conn,
        lookback=args.lookback,
        top_pct=args.top_pct,
        fee_bps=args.fee_bps,
        start_date=args.start,
        end_date=args.end,
    )

    # Report
    print_report(result)

    # Save
    if args.save_csv and result.get("weekly_returns"):
        df = pd.DataFrame(result["weekly_returns"])
        out = os.path.join(OUTPUT_DIR, "momentum_weekly_returns.csv")
        df.to_csv(out, index=False)
        print(f"\n[OK] Saved {len(df)} weekly returns to {out}")

    conn.close()


if __name__ == "__main__":
    main()
