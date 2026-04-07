"""
backtest.py — V2: Funding Rate Mean Reversion

Replays the funding rate strategy against all available historical data.

Supports two instruments:
  - BTC/USDT:USDT (XBTUSDT) — linear perpetual, USDT-margined, linear PnL
  - BTC/USD:BTC    (XBTUSD)  — inverse perpetual, BTC-margined, non-linear PnL

Inverse PnL:
  LONG:   pnl_btc = contracts * (1/entry - 1/exit)
  SHORT:  pnl_btc = contracts * (1/exit - 1/entry)
  USD equivalent = pnl_btc * exit_price

Data alignment:
  - 15m OHLCV candles from BitMEX mainnet (public, no key)
  - 8h settled funding rates from BitMEX mainnet
  - Merged via pd.merge_asof(direction='backward')
  - Each candle only sees the most recently SETTLED funding rate
  - Zero look-ahead bias: a 14:15 candle sees 08:00 rate, never 16:00

Usage:
  python backtest.py [--refresh] [--symbol XBTUSD]
"""

import argparse
import csv
import io
import os
import sys
import contextlib

import pandas as pd

from fetch_data import (
    fetch_all_ohlcv, fetch_all_funding,
    resample_to_15m, merge_funding, SYMBOL,
    INSTRUMENT_CONFIG,
)
from signals import get_signal
import signals as signals_mod

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
INITIAL_BALANCE = 1000.0
RISK_PCT        = 0.02        # 2% of balance risked per trade
MIN_WARMUP      = 60          # skip first N candles for warm-up

OUTPUT_DIR      = "data"
OUTPUT_FILE     = os.path.join(OUTPUT_DIR, "backtest_trades.csv")
ANALYSIS_FILE   = os.path.join(OUTPUT_DIR, "backtest_analysis.txt")

# Symbol label → ccxt symbol mapping
SYMBOL_MAP = {
    "XBTUSDT": "BTC/USDT:USDT",
    "XBTUSD":  "BTC/USD:BTC",
}

# Inverse contracts: PnL is non-linear (BTC-margined)
INVERSE_SYMBOLS = {"BTC/USD:BTC"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@contextlib.contextmanager
def _silence():
    buf = io.StringIO()
    old = sys.stdout
    sys.stdout = buf
    try:
        yield
    finally:
        sys.stdout = old


def _check_exit(df, start_idx, direction, sl_price, tp_price):
    """
    Scan candles from start_idx for SL or TP hit.
    Conservative: if both hit on same candle, SL wins.
    """
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
# Main backtest loop
# ---------------------------------------------------------------------------

def run_backtest(df: pd.DataFrame, is_inverse: bool = False) -> dict:
    """
    Walk the merged DataFrame, fire signals, simulate fills and exits.

    Linear mode:  balance tracked in USD, PnL in USD
    Inverse mode: balance tracked in BTC, PnL in BTC (USD shown for reference)

    Returns dict with 'trades' list and 'meta' dict (init_btc, init_price for inverse).
    """
    trades   = []
    meta     = {}
    n        = len(df)
    i        = MIN_WARMUP

    if is_inverse:
        # Convert initial USD balance to BTC at first candle price
        init_price  = float(df.iloc[0]["close"])
        balance_btc = INITIAL_BALANCE / init_price
        peak_btc    = balance_btc
        pnl_label   = "INVERSE"
        meta["init_btc"]   = balance_btc
        meta["init_price"] = init_price
        print(f"Initial equity: {balance_btc:.6f} BTC "
              f"(${INITIAL_BALANCE:.2f} @ {init_price:.2f})")
    else:
        balance_usd = INITIAL_BALANCE
        peak_usd    = INITIAL_BALANCE
        pnl_label   = "LINEAR"

    print(f"Running backtest on {n:,} candles ({pnl_label} PnL) "
          f"(signal scan starts at index {MIN_WARMUP})...\n")

    while i < n - 1:
        with _silence():
            sig_result = get_signal(df.iloc[:i + 1])

        direction = sig_result["signal"]
        if direction not in ("LONG", "SHORT"):
            i += 1
            continue

        # Signal fired — fill at NEXT candle open
        fill_idx   = i + 1
        fill_ts    = df.index[fill_idx]
        fill_price = float(df.iloc[fill_idx]["open"])

        sl_price   = float(sig_result["sl_price"])
        tp_price   = float(sig_result["tp_price"])
        signal_rr  = sig_result.get("rr") or 0.0
        fr         = sig_result.get("funding_rate") or 0.0
        fr_24h     = sig_result.get("funding_24h") or 0.0

        # Gap check
        if direction == "LONG" and fill_price <= sl_price:
            i += 1
            continue
        if direction == "SHORT" and fill_price >= sl_price:
            i += 1
            continue

        # Find exit
        exit_idx, exit_price, outcome = _check_exit(
            df, fill_idx, direction, sl_price, tp_price)

        exit_ts      = df.index[exit_idx]
        duration_min = int((exit_ts - fill_ts).total_seconds() / 60)

        # ---- P&L calculation ----
        if is_inverse:
            # Inverse: all math in BTC
            # Risk = 2% of BTC equity
            risk_btc = balance_btc * RISK_PCT

            # SL distance in BTC terms (per contract)
            if direction == "LONG":
                sl_dist_btc = (1.0 / sl_price) - (1.0 / fill_price)
            else:
                sl_dist_btc = (1.0 / fill_price) - (1.0 / sl_price)

            if sl_dist_btc <= 0:
                i += 1
                continue

            # Size so that hitting SL loses exactly risk_btc
            # contracts = risk_btc / sl_dist_btc
            contracts = risk_btc / sl_dist_btc

            # Actual PnL in BTC
            if direction == "LONG":
                pnl_btc = contracts * ((1.0 / fill_price) - (1.0 / exit_price))
            else:
                pnl_btc = contracts * ((1.0 / exit_price) - (1.0 / fill_price))

            r_multiple = pnl_btc / risk_btc if risk_btc > 0 else 0.0

            balance_btc += pnl_btc
            if balance_btc > peak_btc:
                peak_btc = balance_btc

            # Metrics in native BTC
            drawdown_pct = (peak_btc - balance_btc) / peak_btc * 100
            pnl_usd_ref  = pnl_btc * exit_price  # USD reference only
            bal_usd_ref   = balance_btc * exit_price

            trade = {
                "trade_num":     len(trades) + 1,
                "entry_ts":      fill_ts.strftime("%Y-%m-%d %H:%M UTC"),
                "exit_ts":       exit_ts.strftime("%Y-%m-%d %H:%M UTC"),
                "direction":     direction,
                "fill_price":    round(fill_price, 2),
                "sl_price":      round(sl_price, 2),
                "tp_price":      round(tp_price, 2),
                "exit_price":    round(exit_price, 2),
                "outcome":       outcome,
                "signal_rr":     round(signal_rr, 2),
                "actual_r":      round(r_multiple, 2),
                "risk_btc":      round(risk_btc, 8),
                "pnl_btc":       round(pnl_btc, 8),
                "balance_btc":   round(balance_btc, 8),
                "pnl_usd_ref":   round(pnl_usd_ref, 2),
                "balance_usd_ref": round(bal_usd_ref, 2),
                "contracts":     round(contracts, 0),
                "drawdown_pct":  round(drawdown_pct, 2),
                "duration_min":  duration_min,
                "funding_rate":  round(fr, 6),
                "funding_24h":   round(fr_24h, 6),
            }
            trades.append(trade)

            tag = " WIN" if outcome == "TP" else ("LOSS" if outcome == "SL" else "OPEN")
            print(
                f"  [{len(trades):4d}] {fill_ts.strftime('%Y-%m-%d %H:%M')} "
                f"{direction:5s} {tag}  "
                f"R={r_multiple:+.2f}  PnL={pnl_btc:+.6f} BTC  "
                f"Bal={balance_btc:.6f} BTC (${bal_usd_ref:>9,.2f})  "
                f"FR={fr*100:+.4f}%"
            )

        else:
            # Linear: all math in USD
            risk_usd  = balance_usd * RISK_PCT
            risk_dist = abs(fill_price - sl_price)

            if direction == "LONG":
                raw_pnl = exit_price - fill_price
            else:
                raw_pnl = fill_price - exit_price
            r_multiple = raw_pnl / risk_dist
            pnl_usd = risk_usd * r_multiple

            balance_usd += pnl_usd
            if balance_usd > peak_usd:
                peak_usd = balance_usd

            drawdown_pct = (peak_usd - balance_usd) / peak_usd * 100

            trade = {
                "trade_num":     len(trades) + 1,
                "entry_ts":      fill_ts.strftime("%Y-%m-%d %H:%M UTC"),
                "exit_ts":       exit_ts.strftime("%Y-%m-%d %H:%M UTC"),
                "direction":     direction,
                "fill_price":    round(fill_price, 2),
                "sl_price":      round(sl_price, 2),
                "tp_price":      round(tp_price, 2),
                "exit_price":    round(exit_price, 2),
                "outcome":       outcome,
                "signal_rr":     round(signal_rr, 2),
                "actual_r":      round(r_multiple, 2),
                "risk_usd":      round(risk_usd, 2),
                "pnl_usd":       round(pnl_usd, 2),
                "balance_after":  round(balance_usd, 2),
                "drawdown_pct":  round(drawdown_pct, 2),
                "duration_min":  duration_min,
                "funding_rate":  round(fr, 6),
                "funding_24h":   round(fr_24h, 6),
            }
            trades.append(trade)

            tag = " WIN" if outcome == "TP" else ("LOSS" if outcome == "SL" else "OPEN")
            print(
                f"  [{len(trades):4d}] {fill_ts.strftime('%Y-%m-%d %H:%M')} "
                f"{direction:5s} {tag}  "
                f"R={r_multiple:+.2f}  PnL=${pnl_usd:+7.2f}  "
                f"Bal=${balance_usd:>9,.2f}  "
                f"FR={fr*100:+.4f}%"
            )

        i = exit_idx + 1

    return {"trades": trades, "meta": meta}


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_report(trades, is_inverse=False, meta=None):
    if not trades:
        print("\nNo trades generated — funding never reached extreme levels in this dataset.")
        return

    total      = len(trades)
    wins       = [t for t in trades if t["outcome"] == "TP"]
    losses     = [t for t in trades if t["outcome"] == "SL"]
    open_t     = [t for t in trades if t["outcome"] == "OPEN"]

    win_rate   = len(wins) / total * 100
    avg_r      = sum(t["actual_r"] for t in trades) / total
    avg_dur    = sum(t["duration_min"] for t in trades) / total
    max_dd     = max(t["drawdown_pct"] for t in trades)
    avg_fr     = sum(abs(t["funding_rate"]) for t in trades) / total

    w = 64
    print()
    print("=" * w)

    if is_inverse:
        # BTC-native metrics — use actual starting equity from meta
        pnl_key = "pnl_btc"
        total_pnl  = sum(t[pnl_key] for t in trades)
        avg_win    = sum(t[pnl_key] for t in wins) / len(wins) if wins else 0
        avg_loss   = sum(t[pnl_key] for t in losses) / len(losses) if losses else 0
        final_btc  = trades[-1]["balance_btc"]
        final_usd  = trades[-1]["balance_usd_ref"]
        init_btc   = meta["init_btc"] if meta else INITIAL_BALANCE / trades[0]["fill_price"]
        init_price = meta["init_price"] if meta else trades[0]["fill_price"]
        return_pct = (final_btc - init_btc) / init_btc * 100

        gross_profit = sum(t[pnl_key] for t in wins) if wins else 0
        gross_loss   = abs(sum(t[pnl_key] for t in losses)) if losses else 0
        pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")

        print("  BACKTEST SUMMARY  —  V2 Funding Rate (INVERSE / BTC-margined)")
        print("=" * w)
        print(f"  Period       : {trades[0]['entry_ts']} → {trades[-1]['exit_ts']}")
        print(f"  Total trades : {total}  "
              f"(wins={len(wins)}, losses={len(losses)}, open={len(open_t)})")
        print(f"  Win rate     : {win_rate:.1f}%")
        print(f"  Profit factor: {pf:.2f}")
        print("-" * w)
        print(f"  Start equity  : {init_btc:.6f} BTC  (${INITIAL_BALANCE:,.2f} @ {init_price:,.2f})")
        print(f"  End equity    : {final_btc:.6f} BTC  (${final_usd:,.2f} at last exit)")
        print(f"  BTC return    : {return_pct:+.1f}%")
        print(f"  Total PnL     : {total_pnl:+.6f} BTC")
        print(f"  Trading DD    : {max_dd:.2f}%  (BTC equity, not fiat)")
        print("-" * w)
        print(f"  Avg win  (BTC): {avg_win:+.8f}")
        print(f"  Avg loss (BTC): {avg_loss:+.8f}")
        print(f"  Avg R achieved: {avg_r:>+.2f}R")
        print(f"  Avg duration  : {avg_dur:.0f} min  ({avg_dur / 60:.1f} h)")
        print(f"  Avg |funding| : {avg_fr*100:.4f}%")
        print("=" * w)

    else:
        # USD-native metrics (linear)
        total_pnl  = sum(t["pnl_usd"] for t in trades)
        avg_win    = sum(t["pnl_usd"] for t in wins) / len(wins) if wins else 0
        avg_loss   = sum(t["pnl_usd"] for t in losses) / len(losses) if losses else 0
        final_bal  = trades[-1]["balance_after"]
        return_pct = (final_bal - INITIAL_BALANCE) / INITIAL_BALANCE * 100

        gross_profit = sum(t["pnl_usd"] for t in wins) if wins else 0
        gross_loss   = abs(sum(t["pnl_usd"] for t in losses)) if losses else 0
        pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")

        print("  BACKTEST SUMMARY  —  V2 Funding Rate Mean Reversion")
        print("=" * w)
        print(f"  Period       : {trades[0]['entry_ts']} → {trades[-1]['exit_ts']}")
        print(f"  Total trades : {total}  "
              f"(wins={len(wins)}, losses={len(losses)}, open={len(open_t)})")
        print(f"  Win rate     : {win_rate:.1f}%")
        print(f"  Profit factor: {pf:.2f}")
        print("-" * w)
        print(f"  Start balance : ${INITIAL_BALANCE:>10,.2f}")
        print(f"  End balance   : ${final_bal:>10,.2f}   ({return_pct:+.1f}%)")
        print(f"  Total PnL     : ${total_pnl:>+10,.2f}")
        print(f"  Max drawdown  : {max_dd:.2f}%")
        print("-" * w)
        print(f"  Avg win (USD) : ${avg_win:>+8.2f}")
        print(f"  Avg loss (USD): ${avg_loss:>+8.2f}")
        print(f"  Avg R achieved: {avg_r:>+.2f}R")
        print(f"  Avg duration  : {avg_dur:.0f} min  ({avg_dur / 60:.1f} h)")
        print(f"  Avg |funding| : {avg_fr*100:.4f}%")
        print("=" * w)

    # Direction breakdown (same for both modes)
    longs  = [t for t in trades if t["direction"] == "LONG"]
    shorts = [t for t in trades if t["direction"] == "SHORT"]
    print(f"\n  Direction:  LONG={len(longs)}  SHORT={len(shorts)}")
    for label, group in [("LONG", longs), ("SHORT", shorts)]:
        if group:
            gw = [t for t in group if t["outcome"] == "TP"]
            wr = len(gw) / len(group) * 100
            print(f"    {label}: {len(group)} trades, {wr:.0f}% WR")


def save_csv(trades, path=None):
    if not trades:
        return
    path = path or OUTPUT_FILE
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    fieldnames = list(trades[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(trades)
    print(f"\n[OK] Saved {len(trades)} trades to {path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="V2 Funding Rate backtest")
    parser.add_argument("--refresh", action="store_true",
                        help="Force fresh fetch from BitMEX.")
    parser.add_argument("--symbol", default="XBTUSDT",
                        choices=list(SYMBOL_MAP.keys()),
                        help="Instrument to backtest (default: XBTUSDT)")
    args = parser.parse_args()

    ccxt_symbol = SYMBOL_MAP[args.symbol]
    cfg = INSTRUMENT_CONFIG.get(ccxt_symbol, {})
    is_inverse = ccxt_symbol in INVERSE_SYMBOLS

    # Configure settlement hours for the chosen instrument
    if cfg.get("settle_hours"):
        signals_mod.SETTLEMENT_HOURS = cfg["settle_hours"]

    w = 64
    pnl_type = "INVERSE (BTC-margined)" if is_inverse else "LINEAR (USDT-margined)"
    print("=" * w)
    print("  BACKTEST  —  V2 Funding Rate Mean Reversion")
    print(f"  {args.symbol} 15m  |  {pnl_type}")
    print(f"  Settlement hours: {cfg.get('settle_hours', [0, 8, 16])} UTC")
    print("=" * w + "\n")

    use_cache = not args.refresh

    # 1. Fetch OHLCV
    raw_ohlcv = fetch_all_ohlcv(use_cache=use_cache, symbol=ccxt_symbol)
    if not raw_ohlcv:
        raise SystemExit("[ABORT] No OHLCV data.")

    # 2. Fetch funding history
    df_funding = fetch_all_funding(use_cache=use_cache, symbol=ccxt_symbol)
    if df_funding is None or df_funding.empty:
        raise SystemExit("[ABORT] No funding data.")

    # 3. Resample OHLCV to 15m
    df_15m = resample_to_15m(raw_ohlcv)
    print(f"[OK] {len(df_15m):,} x 15m candles  "
          f"({df_15m.index[0].strftime('%Y-%m-%d')} → "
          f"{df_15m.index[-1].strftime('%Y-%m-%d')})")

    # 4. Merge funding into OHLCV (backward — zero look-ahead)
    df_merged = merge_funding(df_15m, df_funding)
    n_with_funding = df_merged["funding_rate"].notna().sum()
    print(f"[OK] Merged funding data. {n_with_funding:,} candles have funding rates.")
    print(f"     Funding range: {df_funding['rate'].min()*100:+.4f}% "
          f"to {df_funding['rate'].max()*100:+.4f}%\n")

    # 5. Run backtest with correct PnL model
    result = run_backtest(df_merged, is_inverse=is_inverse)
    trades = result["trades"]
    meta   = result["meta"]

    # 6. Report
    print_report(trades, is_inverse=is_inverse, meta=meta)

    # 7. Save (instrument-specific filename)
    out_file = os.path.join(OUTPUT_DIR, f"backtest_trades_{args.symbol.lower()}.csv")
    save_csv(trades, out_file)
