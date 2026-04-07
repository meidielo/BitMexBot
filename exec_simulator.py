"""
exec_simulator.py — Micro-Execution Simulator for Cascade Dip-Buy

Simulates LONG entry execution on 1-minute data during liquidation cascade
events. Models two execution strategies and compares outcomes:

Strategy A: MARKET order at entry day open
  - Guaranteed fill, but pays dynamic slippage proportional to 1m volatility
  - Slippage model: max(base_slippage, candle_range * slippage_fraction)

Strategy B: LIMIT order placed below entry day open
  - Better fill price IF hit, but risks:
    1. No fill (price doesn't reach limit) → missed trade
    2. Adverse selection (price slices through limit = catching falling knife)
  - Fill probability: P(fill) = 1.0 if low ≤ limit_price, else 0.0
  - Adverse selection: if filled, measure subsequent drawdown from fill price

Strategy C: CONFIRMATION market order after first bullish 1m close above prior high
  - Waits for V-bottom reversal signal before entering
  - Pays more slippage (entering later) but avoids catching falling knife
  - Risk: confirmation comes too late, most of the move is gone

For each strategy, computes:
  - Fill price (with slippage model)
  - Fill probability (for limit orders)
  - Max adverse excursion (MAE) from fill within next 60 minutes
  - Max favorable excursion (MFE) from fill within 24 hours
  - R-multiple using the V4 SL/TP from the daily signal

Usage:
  python exec_simulator.py
"""

import os
import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# Configuration — matches V4 backtest_v3.py
# ---------------------------------------------------------------------------
ATR_SL_MULT   = 2.0
TARGET_RR     = 2.0
RISK_PCT      = 0.02

# Slippage model for market orders
BASE_SLIPPAGE_PCT      = 0.001   # 0.1% minimum slippage
VOLATILITY_SLIPPAGE    = 0.15    # pay 15% of the 1m candle range as slippage

# Limit order placement
LIMIT_OFFSETS_PCT = [0.005, 0.01, 0.015, 0.02, 0.03]  # 0.5% to 3% below open

# Confirmation: number of consecutive bullish 1m closes before entry
CONFIRM_BARS = 1  # first bullish close above prior candle high

# ---------------------------------------------------------------------------
# Event definitions — from V4 backtest results
# ---------------------------------------------------------------------------
EVENTS = {
    "cascade_jan03": {
        "signal_date": "2024-01-03",
        "entry_date":  "2024-01-04",
        "signal_close": 42853,   # close on signal day (used for SL/TP calc)
        "atr":          1679,    # ATR(14) on signal day
        "outcome":      "LOSS",  # V4 daily backtest result
    },
    "cascade_feb20": {
        "signal_date": "2024-02-20",
        "entry_date":  "2024-02-21",
        "signal_close": 52312,
        "atr":          1855,
        "outcome":      "WIN",
    },
    "cascade_nov12": {
        "signal_date": "2024-11-12",
        "entry_date":  "2024-11-13",
        "signal_close": 88014,
        "atr":          3877,
        "outcome":      "WIN",
    },
    "cascade_jul25": {
        "signal_date": "2025-07-25",
        "entry_date":  "2025-07-26",
        "signal_close": 117651,
        "atr":          2304,
        "outcome":      "LOSS",
    },
}


def load_1m(label: str) -> pd.DataFrame:
    """Load 1-minute candle data for a cascade event."""
    path = f"data/cascade_1m/{label}.csv"
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    df = df.set_index("timestamp").sort_index()
    return df


def get_entry_day(df: pd.DataFrame, entry_date: str) -> pd.DataFrame:
    """Extract entry day candles."""
    mask = df.index.strftime("%Y-%m-%d") == entry_date
    return df[mask].copy()


# ---------------------------------------------------------------------------
# Strategy A: Market order at open
# ---------------------------------------------------------------------------

def simulate_market_at_open(entry_df: pd.DataFrame, signal_close: float,
                            atr: float) -> dict:
    """Market order at entry day open with dynamic slippage."""
    if len(entry_df) == 0:
        return {"filled": False, "reason": "no data"}

    open_price = entry_df.iloc[0]["open"]
    first_range = entry_df.iloc[0]["high"] - entry_df.iloc[0]["low"]

    # Dynamic slippage: max of base or fraction of first candle range
    slippage = max(
        open_price * BASE_SLIPPAGE_PCT,
        first_range * VOLATILITY_SLIPPAGE,
    )
    fill_price = open_price + slippage  # adverse for LONG = pay more

    # SL/TP from daily signal
    sl_dist = atr * ATR_SL_MULT
    sl_price = signal_close - sl_dist
    tp_price = signal_close + (sl_dist * TARGET_RR)

    return _compute_outcome(entry_df, fill_price, sl_price, tp_price, 0,
                            strategy="MARKET_OPEN")


def simulate_market_with_slippage_range(entry_df: pd.DataFrame,
                                         signal_close: float,
                                         atr: float) -> dict:
    """Market order with slippage based on P95 1m range (worst-case model)."""
    if len(entry_df) == 0:
        return {"filled": False, "reason": "no data"}

    open_price = entry_df.iloc[0]["open"]
    # Use P95 of entry day 1m ranges as worst-case slippage
    ranges = entry_df["high"] - entry_df["low"]
    p95_range = ranges.quantile(0.95)
    slippage = p95_range * 0.5  # assume we pay half the P95 range
    fill_price = open_price + slippage

    sl_dist = atr * ATR_SL_MULT
    sl_price = signal_close - sl_dist
    tp_price = signal_close + (sl_dist * TARGET_RR)

    return _compute_outcome(entry_df, fill_price, sl_price, tp_price, 0,
                            strategy="MARKET_P95")


# ---------------------------------------------------------------------------
# Strategy B: Limit order below open
# ---------------------------------------------------------------------------

def simulate_limit_order(entry_df: pd.DataFrame, signal_close: float,
                         atr: float, offset_pct: float) -> dict:
    """Limit order placed offset_pct below entry day open."""
    if len(entry_df) == 0:
        return {"filled": False, "reason": "no data"}

    open_price = entry_df.iloc[0]["open"]
    limit_price = open_price * (1 - offset_pct)

    # Scan candles for fill
    fill_idx = None
    for i in range(len(entry_df)):
        if entry_df.iloc[i]["low"] <= limit_price:
            fill_idx = i
            break

    if fill_idx is None:
        return {
            "filled": False,
            "strategy": f"LIMIT_{offset_pct*100:.1f}%",
            "limit_price": round(limit_price, 2),
            "reason": f"Price never reached {limit_price:,.0f} (low={entry_df['low'].min():,.0f})",
        }

    # Filled at limit price (best case) — no slippage on limit orders
    fill_price = limit_price

    # Volume-based fill probability heuristic
    # If the candle that triggered our fill had volume V, and the wick below
    # our limit is W, then roughly P(fill) ≈ volume_at_price / total_volume
    # We can't know exact volume-at-price from OHLCV, so we use a simple
    # heuristic: if the close is above our limit, we likely got filled.
    # If close is below, the wick sliced through — adverse selection.
    fill_candle = entry_df.iloc[fill_idx]
    adverse_selection = fill_candle["close"] < limit_price

    sl_dist = atr * ATR_SL_MULT
    sl_price = signal_close - sl_dist
    tp_price = signal_close + (sl_dist * TARGET_RR)

    result = _compute_outcome(entry_df, fill_price, sl_price, tp_price,
                              fill_idx, strategy=f"LIMIT_{offset_pct*100:.1f}%")
    result["adverse_selection"] = adverse_selection
    result["fill_candle_idx"] = fill_idx
    result["fill_delay_min"] = fill_idx  # each candle is 1 min
    return result


# ---------------------------------------------------------------------------
# Strategy C: Confirmation entry
# ---------------------------------------------------------------------------

def simulate_confirmation_entry(entry_df: pd.DataFrame, signal_close: float,
                                atr: float) -> dict:
    """Wait for first bullish 1m close above prior candle's high, then market buy."""
    if len(entry_df) < 3:
        return {"filled": False, "reason": "insufficient data"}

    confirm_idx = None
    for i in range(2, len(entry_df)):
        curr = entry_df.iloc[i]
        prev = entry_df.iloc[i-1]
        # Bullish: close > open AND close > prior high
        if curr["close"] > curr["open"] and curr["close"] > prev["high"]:
            confirm_idx = i
            break

    if confirm_idx is None:
        return {"filled": False, "strategy": "CONFIRM", "reason": "No bullish confirmation"}

    # Market order at confirmation candle close + slippage
    confirm_close = entry_df.iloc[confirm_idx]["close"]
    candle_range = entry_df.iloc[confirm_idx]["high"] - entry_df.iloc[confirm_idx]["low"]
    slippage = max(confirm_close * BASE_SLIPPAGE_PCT, candle_range * VOLATILITY_SLIPPAGE)
    fill_price = confirm_close + slippage

    sl_dist = atr * ATR_SL_MULT
    sl_price = signal_close - sl_dist
    tp_price = signal_close + (sl_dist * TARGET_RR)

    result = _compute_outcome(entry_df, fill_price, sl_price, tp_price,
                              confirm_idx, strategy="CONFIRM")
    result["confirm_delay_min"] = confirm_idx
    result["confirm_time"] = entry_df.index[confirm_idx].strftime("%H:%M")
    return result


# ---------------------------------------------------------------------------
# Outcome computation
# ---------------------------------------------------------------------------

def _compute_outcome(entry_df: pd.DataFrame, fill_price: float,
                     sl_price: float, tp_price: float,
                     fill_idx: int, strategy: str) -> dict:
    """
    From fill_idx forward, compute MAE, MFE, and SL/TP outcome.
    """
    remaining = entry_df.iloc[fill_idx:]

    # Check SL/TP
    outcome = "OPEN"
    exit_price = remaining.iloc[-1]["close"]
    exit_idx = len(remaining) - 1

    for j in range(len(remaining)):
        row = remaining.iloc[j]
        sl_hit = row["low"] <= sl_price
        tp_hit = row["high"] >= tp_price

        if sl_hit and tp_hit:
            outcome = "SL"
            exit_price = sl_price
            exit_idx = j
            break
        if tp_hit:
            outcome = "TP"
            exit_price = tp_price
            exit_idx = j
            break
        if sl_hit:
            outcome = "SL"
            exit_price = sl_price
            exit_idx = j
            break

    # MAE: worst point after fill (within entry day)
    post_fill = remaining["low"].min()
    mae = (fill_price - post_fill) / fill_price * 100  # positive = adverse

    # MFE: best point after fill (within entry day)
    post_fill_high = remaining["high"].max()
    mfe = (post_fill_high - fill_price) / fill_price * 100

    # R-multiple (using SL distance as risk unit, not inverse PnL for simplicity)
    sl_dist = abs(fill_price - sl_price)
    if sl_dist > 0:
        raw_pnl = exit_price - fill_price
        r_multiple = raw_pnl / sl_dist
    else:
        r_multiple = 0

    return {
        "filled":       True,
        "strategy":     strategy,
        "fill_price":   round(fill_price, 2),
        "sl_price":     round(sl_price, 2),
        "tp_price":     round(tp_price, 2),
        "exit_price":   round(exit_price, 2),
        "outcome":      outcome,
        "r_multiple":   round(r_multiple, 2),
        "mae_pct":      round(mae, 3),
        "mfe_pct":      round(mfe, 3),
        "exit_min":     exit_idx,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_all():
    w = 70
    print("=" * w)
    print("  MICRO-EXECUTION SIMULATOR — V4 Cascade Dip-Buy")
    print("  1-minute resolution, 4 historical cascade events")
    print("=" * w)

    all_results = []

    for label, cfg in EVENTS.items():
        print(f"\n{'='*w}")
        print(f"  {label} (signal={cfg['signal_date']}, entry={cfg['entry_date']})")
        print(f"  Daily backtest outcome: {cfg['outcome']}")
        print(f"{'='*w}")

        df = load_1m(label)
        entry_df = get_entry_day(df, cfg["entry_date"])
        sc = cfg["signal_close"]
        atr = cfg["atr"]

        sl_price = sc - atr * ATR_SL_MULT
        tp_price = sc + atr * ATR_SL_MULT * TARGET_RR
        print(f"  SL={sl_price:,.0f}  TP={tp_price:,.0f}  "
              f"(ATR={atr:,.0f}, SL dist={atr*ATR_SL_MULT:,.0f})")
        print(f"  Entry day: {len(entry_df)} 1m candles, "
              f"open={entry_df.iloc[0]['open']:,.0f}")

        # Strategy A: Market at open
        print(f"\n  --- Strategy A: Market at Open ---")
        res_a = simulate_market_at_open(entry_df, sc, atr)
        _print_result(res_a)
        all_results.append({"event": label, **res_a})

        # Strategy B: Limit orders at various offsets
        print(f"\n  --- Strategy B: Limit Orders ---")
        for offset in LIMIT_OFFSETS_PCT:
            res_b = simulate_limit_order(entry_df, sc, atr, offset)
            _print_result(res_b)
            all_results.append({"event": label, **res_b})

        # Strategy C: Confirmation entry
        print(f"\n  --- Strategy C: Confirmation ---")
        res_c = simulate_confirmation_entry(entry_df, sc, atr)
        _print_result(res_c)
        all_results.append({"event": label, **res_c})

    # Summary
    print(f"\n{'='*w}")
    print("  SUMMARY — Strategy Comparison")
    print(f"{'='*w}")

    strategies = {}
    for r in all_results:
        s = r.get("strategy", "?")
        if s not in strategies:
            strategies[s] = {"fills": 0, "total": 0, "wins": 0, "r_sum": 0}
        strategies[s]["total"] += 1
        if r.get("filled"):
            strategies[s]["fills"] += 1
            if r.get("outcome") == "TP":
                strategies[s]["wins"] += 1
            strategies[s]["r_sum"] += r.get("r_multiple", 0)

    print(f"\n  {'Strategy':<20} {'Fill%':>6} {'WR':>6} {'Avg R':>7} {'Trades':>7}")
    print(f"  {'-'*50}")
    for s, d in sorted(strategies.items()):
        fill_pct = d["fills"] / d["total"] * 100 if d["total"] > 0 else 0
        wr = d["wins"] / d["fills"] * 100 if d["fills"] > 0 else 0
        avg_r = d["r_sum"] / d["fills"] if d["fills"] > 0 else 0
        print(f"  {s:<20} {fill_pct:>5.0f}% {wr:>5.0f}% {avg_r:>+6.2f}R  {d['fills']}/{d['total']}")


def _print_result(res: dict):
    if not res.get("filled"):
        print(f"    {res.get('strategy', '?')}: NO FILL — {res.get('reason', '')}")
        return
    s = res["strategy"]
    adv = " [ADVERSE]" if res.get("adverse_selection") else ""
    delay = ""
    if "fill_delay_min" in res:
        delay = f" fill@+{res['fill_delay_min']}m"
    if "confirm_delay_min" in res:
        delay = f" confirm@{res.get('confirm_time', '?')}(+{res['confirm_delay_min']}m)"

    print(f"    {s}: fill={res['fill_price']:>9,.0f}  "
          f"exit={res['exit_price']:>9,.0f} ({res['outcome']})  "
          f"R={res['r_multiple']:+.2f}  "
          f"MAE={res['mae_pct']:.2f}%  MFE={res['mfe_pct']:.2f}%"
          f"{adv}{delay}")


if __name__ == "__main__":
    run_all()
