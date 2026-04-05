"""
backtest.py

Replays the EMA-rejection signal strategy against all available
XBTUSDT 15m historical data from BitMEX live (read-only, no API key needed).

Usage
-----
    python backtest.py

What it does
------------
1.  Fetches all available 5m candles from BitMEX live via paginated OHLCV.
2.  Resamples to 15m (same method as the live bot uses in fetch_data.py).
3.  Computes all indicators on the full dataset in one pass (no look-ahead:
    EMA/RSI/BB are recursive forward-only, so computing on full data is
    identical to computing on a rolling window).
4.  Walks every candle oldest-to-newest:
      - Calls get_signal() on the candles seen so far (stdout suppressed).
      - On a LONG/SHORT signal, simulates fill at the NEXT candle's open.
      - Skips the trade if the fill open gaps past the stop-loss.
      - Scans subsequent candles for SL or TP hit.
      - After an exit, advances the loop pointer past the exit candle
        (one trade at a time — no overlapping positions).
5.  Tracks balance, peak balance, and per-trade metrics.
6.  Prints a summary report to terminal.
7.  Saves all simulated trades to data/backtest_trades.csv.

P&L model
---------
Risk per trade = current_balance * 2%  (matches live bot)
R-multiple     = actual_gain_or_loss / risk_distance_at_entry
PnL (USD)      = risk_usd * R-multiple
"""

import argparse
import csv
import io
import os
import sys
import contextlib
from datetime import datetime, timezone

import ccxt
import pandas as pd

from indicators import add_indicators
from signals import get_signal

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SYMBOL         = "BTC/USDT:USDT"
LIVE_URL       = "https://www.bitmex.com"
RAW_TIMEFRAME  = "5m"
TARGET_TF      = "15min"
FETCH_LIMIT    = 1000             # BitMEX max candles per OHLCV request

# Earliest available XBTUSDT candle on BitMEX live (confirmed by pre-flight check)
EARLIEST_DATE  = "2024-01-12T00:00:00Z"

INITIAL_BALANCE = 1000.0          # starting paper USD balance
RISK_PCT        = 0.02            # 2 % of balance risked per trade

# Start scanning for signals only after this many candles.
# EMA50 warm-up = 50, plus margin.  get_signal() also guards against NaN rows.
MIN_WARMUP = 60

OUTPUT_DIR       = "data"
OUTPUT_FILE      = os.path.join(OUTPUT_DIR, "backtest_trades.csv")
ANALYSIS_FILE    = os.path.join(OUTPUT_DIR, "backtest_analysis.txt")
RAW_CACHE        = os.path.join(OUTPUT_DIR, "raw_candles.csv")


# ---------------------------------------------------------------------------
# Step 1 — Fetch all 5m candles from live BitMEX (no API key needed)
# ---------------------------------------------------------------------------

def _make_live_exchange() -> ccxt.bitmex:
    """Return a ccxt.bitmex pointed at live (read-only, no credentials)."""
    ex = ccxt.bitmex({
        "urls": {"api": {"public": LIVE_URL, "private": LIVE_URL}},
        "options": {"defaultType": "swap"},
    })
    ex.load_markets()
    return ex


def fetch_all_raw(exchange: ccxt.bitmex) -> list:
    """
    Page through 5m OHLCV from EARLIEST_DATE to now.
    Returns a flat list of [timestamp_ms, o, h, l, c, v] rows.
    """
    since    = exchange.parse8601(EARLIEST_DATE)
    step_ms  = 5 * 60 * 1000          # 5 minutes in milliseconds
    all_rows = []
    page     = 0

    print(f"Fetching 5m candles from BitMEX live for {SYMBOL}...")
    print(f"Starting from {EARLIEST_DATE}\n")

    while True:
        try:
            batch = exchange.fetch_ohlcv(
                SYMBOL,
                timeframe=RAW_TIMEFRAME,
                since=since,
                limit=FETCH_LIMIT,
            )
        except Exception as e:
            print(f"[ERROR] Fetch failed at page {page}: {e}")
            break

        if not batch:
            break

        all_rows.extend(batch)
        page   += 1
        last_ts = batch[-1][0]
        since   = last_ts + step_ms   # next candle after the last fetched

        # Print progress every 50 pages
        if page % 50 == 0:
            ts_str = datetime.fromtimestamp(
                last_ts / 1000, tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M")
            print(f"  ... page {page:4d}  |  up to {ts_str}  |"
                  f"  {len(all_rows):,} 5m candles so far")

        if len(batch) < FETCH_LIMIT:
            break   # partial page means we've reached the live edge

    print(f"\n[OK] Fetched {len(all_rows):,} raw 5m candles ({page} pages).\n")
    return all_rows


# ---------------------------------------------------------------------------
# Step 1b — Raw candle cache  (data/raw_candles.csv)
# ---------------------------------------------------------------------------

def load_cache() -> list | None:
    """
    Read raw 5m candles from the on-disk cache.
    Returns a list of [timestamp_ms, o, h, l, c, v] rows, or None if the
    cache file does not exist.
    """
    if not os.path.exists(RAW_CACHE):
        return None
    try:
        rows = []
        with open(RAW_CACHE, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            next(reader)                          # skip header
            for row in reader:
                rows.append([
                    int(row[0]),                  # timestamp_ms
                    float(row[1]),                # open
                    float(row[2]),                # high
                    float(row[3]),                # low
                    float(row[4]),                # close
                    float(row[5]),                # volume
                ])
        return rows
    except Exception as e:
        print(f"[WARN] Cache read failed ({e}) — will fetch fresh data.")
        return None


def save_cache(raw: list) -> None:
    """Write raw 5m candles to data/raw_candles.csv."""
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(RAW_CACHE, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["timestamp_ms", "open", "high", "low", "close", "volume"])
            writer.writerows(raw)
        print(f"[CACHE] Saved {len(raw):,} candles to {RAW_CACHE}\n")
    except Exception as e:
        print(f"[WARN] Could not save cache: {e}")


# ---------------------------------------------------------------------------
# Step 2 — Resample 5m → 15m  (same logic as fetch_data.py)
# ---------------------------------------------------------------------------

def resample_to_15m(raw: list) -> pd.DataFrame:
    df = pd.DataFrame(
        raw, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df.set_index("timestamp")

    resampled = df.resample(TARGET_TF).agg(
        {"open": "first", "high": "max", "low": "min",
         "close": "last", "volume": "sum"}
    ).dropna()

    # Repair minor OHLC violations (resample artifacts)
    impossible = resampled["high"] < resampled["low"]
    n_impossible = int(impossible.sum())
    if n_impossible > 0:
        print(f"[WARN] Dropped {n_impossible} impossible candle(s) (H < L).")
        resampled = resampled[~impossible]

    oc_max = resampled[["open", "close"]].max(axis=1)
    oc_min = resampled[["open", "close"]].min(axis=1)
    needs_fix = (resampled["high"] < oc_max) | (resampled["low"] > oc_min)
    n_fix = int(needs_fix.sum())
    if n_fix > 0:
        resampled.loc[:, "high"] = resampled["high"].clip(lower=oc_max)
        resampled.loc[:, "low"]  = resampled["low"].clip(upper=oc_min)
        print(f"[INFO] Repaired {n_fix} candle(s) (H/L adjusted to cover O/C).")

    print(
        f"[OK] Resampled to {len(resampled):,} x 15m candles  "
        f"({resampled.index[0].strftime('%Y-%m-%d')} "
        f"-> {resampled.index[-1].strftime('%Y-%m-%d')})\n"
    )
    return resampled


# ---------------------------------------------------------------------------
# Step 3 — Compute indicators once on the full dataset
# ---------------------------------------------------------------------------

def enrich(df: pd.DataFrame) -> pd.DataFrame:
    enriched = add_indicators(df)
    if enriched is None:
        raise RuntimeError("[ABORT] add_indicators() returned None.")
    nan_info = {c: int(enriched[c].isna().sum())
                for c in ["ema_20", "ema_50", "rsi_14", "bb_mid"]}
    print(f"[OK] Indicators computed.  NaN warm-up rows: {nan_info}\n")
    return enriched


# ---------------------------------------------------------------------------
# Step 4 — Helpers
# ---------------------------------------------------------------------------

@contextlib.contextmanager
def _silence():
    """Redirect stdout to a buffer so get_signal diagnostics stay quiet."""
    buf = io.StringIO()
    old = sys.stdout
    sys.stdout = buf
    try:
        yield
    finally:
        sys.stdout = old


def _check_exit(df: pd.DataFrame, start_idx: int,
                direction: str, sl_price: float, tp_price: float):
    """
    Scan candles from start_idx onwards for the first SL or TP hit.

    Returns (exit_idx, exit_price, outcome)
        outcome : "TP" | "SL" | "OPEN"  (OPEN = data ended before resolution)

    Rule for candles where both SL and TP are touched (e.g. long shadow and
    long wick in the same candle): conservative assumption — SL filled first.
    """
    for j in range(start_idx, len(df)):
        row   = df.iloc[j]
        h, lo = float(row["high"]), float(row["low"])

        if direction == "LONG":
            sl_hit = lo <= sl_price
            tp_hit = h  >= tp_price
        else:  # SHORT
            sl_hit = h  >= sl_price
            tp_hit = lo <= tp_price

        if sl_hit and tp_hit:
            return j, sl_price, "SL"   # conservative: SL filled first

        if tp_hit:
            return j, tp_price, "TP"

        if sl_hit:
            return j, sl_price, "SL"

    # Data ended without resolution — mark to last close
    last_close = float(df.iloc[-1]["close"])
    return len(df) - 1, last_close, "OPEN"


# ---------------------------------------------------------------------------
# Step 5 — Context helpers for analysis fields
# ---------------------------------------------------------------------------

def _count_ema_trend_candles(df: pd.DataFrame, idx: int, direction: str) -> int:
    """
    Count consecutive candles ending AT idx (inclusive) where the EMA trend
    condition holds:
        SHORT -> EMA20 < EMA50
        LONG  -> EMA20 > EMA50

    Stops as soon as the condition breaks or a NaN row is hit.
    Minimum return value is 1 (the signal candle itself always qualifies,
    because get_signal() already confirmed the trend condition).
    """
    count = 0
    for k in range(idx, -1, -1):
        row = df.iloc[k]
        e20 = row["ema_20"]
        e50 = row["ema_50"]
        if pd.isna(e20) or pd.isna(e50):
            break
        if direction == "SHORT" and float(e20) < float(e50):
            count += 1
        elif direction == "LONG" and float(e20) > float(e50):
            count += 1
        else:
            break
    return max(count, 1)


# ---------------------------------------------------------------------------
# Step 6 — Main backtest loop
# ---------------------------------------------------------------------------

def run_backtest(df: pd.DataFrame) -> list:
    """
    Walk the enriched DataFrame, fire signals, simulate fills and exits.
    Returns a list of trade dicts.  Each dict includes two analysis fields
    captured at signal time (index i, the candle that fired the signal):

        rsi_at_entry      -- RSI(14) value on the signal candle
        ema_trend_candles -- consecutive candles where the EMA trend held
                             (EMA20 < EMA50 for SHORT, > for LONG)
    """
    trades   = []
    balance  = INITIAL_BALANCE
    peak_bal = INITIAL_BALANCE
    n        = len(df)
    i        = MIN_WARMUP   # first index we evaluate signals at

    # Disable ML filter during backtest — it was trained on backtest data,
    # so using it here is circular look-ahead bias.
    import signals as _sig_mod
    _orig_ml = getattr(_sig_mod, "_ML_AVAILABLE", False)
    _sig_mod._ML_AVAILABLE = False

    print(f"Running backtest on {n:,} candles "
          f"(signal scan starts at index {MIN_WARMUP})...\n")

    while i < n - 1:   # need at least one more candle ahead for the fill

        with _silence():
            sig_result = get_signal(df.iloc[: i + 1])

        direction = sig_result["signal"]

        if direction not in ("LONG", "SHORT"):
            i += 1
            continue

        # Capture analysis fields at signal time (candle i)
        strategy          = sig_result.get("strategy", "ema_rejection")
        rsi_at_entry      = round(float(df.iloc[i]["rsi_14"]), 2)
        ema_trend_candles = _count_ema_trend_candles(df, i, direction)

        # ------------------------------------------------------------------
        # Signal fired — fill at NEXT candle open
        # ------------------------------------------------------------------
        fill_idx    = i + 1
        fill_ts     = df.index[fill_idx]
        fill_candle = df.iloc[fill_idx]
        fill_price  = float(fill_candle["open"])

        sl_price    = float(sig_result["sl_price"])
        tp_price    = float(sig_result["tp_price"])
        signal_rr   = sig_result.get("rr") or 0.0

        # Gap check — if the fill open is already past our stop, skip.
        if direction == "LONG"  and fill_price <= sl_price:
            i += 1
            continue
        if direction == "SHORT" and fill_price >= sl_price:
            i += 1
            continue

        # ------------------------------------------------------------------
        # Find exit — start scanning from the fill candle itself
        # (the fill open could already be on the wrong side of TP by H/L)
        # ------------------------------------------------------------------
        exit_idx, exit_price, outcome = _check_exit(
            df, fill_idx, direction, sl_price, tp_price
        )

        # ------------------------------------------------------------------
        # P&L
        # ------------------------------------------------------------------
        risk_usd  = balance * RISK_PCT
        risk_dist = abs(fill_price - sl_price)

        if outcome == "TP":
            r_multiple = abs(tp_price - fill_price) / risk_dist
        elif outcome == "SL":
            # If we gapped to a worse-than-SL exit (shouldn't happen for
            # intermediate candles, but covered for OPEN edge candles)
            r_multiple = -abs(exit_price - fill_price) / risk_dist
        else:   # OPEN — mark-to-market at last close
            if direction == "LONG":
                r_multiple = (exit_price - fill_price) / risk_dist
            else:
                r_multiple = (fill_price - exit_price) / risk_dist

        pnl_usd  = risk_usd * r_multiple
        balance += pnl_usd
        if balance > peak_bal:
            peak_bal = balance

        drawdown_pct = (peak_bal - balance) / peak_bal * 100

        # ------------------------------------------------------------------
        # Record
        # ------------------------------------------------------------------
        exit_ts      = df.index[exit_idx]
        duration_min = int((exit_ts - fill_ts).total_seconds() / 60)

        trade = {
            "trade_num":          len(trades) + 1,
            "strategy":           strategy,
            "entry_ts":           fill_ts.strftime("%Y-%m-%d %H:%M UTC"),
            "exit_ts":            exit_ts.strftime("%Y-%m-%d %H:%M UTC"),
            "direction":          direction,
            "fill_price":         round(fill_price, 2),
            "sl_price":           round(sl_price,   2),
            "tp_price":           round(tp_price,   2),
            "exit_price":         round(exit_price, 2),
            "outcome":            outcome,
            "signal_rr":          round(signal_rr,  2),
            "actual_r":           round(r_multiple, 2),
            "risk_usd":           round(risk_usd,   2),
            "pnl_usd":            round(pnl_usd,    2),
            "balance_after":      round(balance,    2),
            "drawdown_pct":       round(drawdown_pct, 2),
            "duration_min":       duration_min,
            "rsi_at_entry":       rsi_at_entry,
            "ema_trend_candles":  ema_trend_candles,
        }
        trades.append(trade)

        # One-line progress per trade
        tag = " WIN" if outcome == "TP" else ("LOSS" if outcome == "SL" else "OPEN")
        strat_tag = strategy[:8].ljust(8)
        print(
            f"  [{len(trades):4d}] {fill_ts.strftime('%Y-%m-%d %H:%M')} "
            f"{strat_tag} {direction:5s} {tag}  "
            f"R={r_multiple:+.2f}  PnL=${pnl_usd:+7.2f}  "
            f"Bal=${balance:>9,.2f}"
        )

        # Advance past the exit candle — no overlapping trades
        i = exit_idx + 1

    # Restore ML flag
    _sig_mod._ML_AVAILABLE = _orig_ml

    return trades


# ---------------------------------------------------------------------------
# Step 7 — Summary report
# ---------------------------------------------------------------------------

def print_report(trades: list) -> None:
    if not trades:
        print("\nNo trades generated — no signals fired in the dataset.")
        return

    total        = len(trades)
    wins         = [t for t in trades if t["outcome"] == "TP"]
    losses       = [t for t in trades if t["outcome"] == "SL"]
    open_trades  = [t for t in trades if t["outcome"] == "OPEN"]

    win_rate     = len(wins) / total * 100

    total_pnl    = sum(t["pnl_usd"] for t in trades)
    avg_win_usd  = sum(t["pnl_usd"] for t in wins)   / len(wins)   if wins   else 0.0
    avg_loss_usd = sum(t["pnl_usd"] for t in losses) / len(losses) if losses else 0.0
    avg_r        = sum(t["actual_r"] for t in trades) / total
    avg_dur      = sum(t["duration_min"] for t in trades) / total

    final_bal    = trades[-1]["balance_after"]
    max_dd       = max(t["drawdown_pct"] for t in trades)
    return_pct   = (final_bal - INITIAL_BALANCE) / INITIAL_BALANCE * 100

    gross_profit = sum(t["pnl_usd"] for t in wins)   if wins   else 0.0
    gross_loss   = abs(sum(t["pnl_usd"] for t in losses)) if losses else 0.0
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float("inf")

    longs  = [t for t in trades if t["direction"] == "LONG"]
    shorts = [t for t in trades if t["direction"] == "SHORT"]
    long_wr  = (sum(1 for t in longs  if t["outcome"] == "TP") / len(longs)  * 100) if longs  else 0.0
    short_wr = (sum(1 for t in shorts if t["outcome"] == "TP") / len(shorts) * 100) if shorts else 0.0

    w = 64
    print()
    print("=" * w)
    print("  BACKTEST SUMMARY  --  XBTUSDT 15m  (EMA Rejection Strategy)")
    print("=" * w)
    print(f"  Period       : {trades[0]['entry_ts']} -> {trades[-1]['exit_ts']}")
    print(f"  Total trades : {total}  "
          f"(wins={len(wins)}, losses={len(losses)}, open={len(open_trades)})")
    print(f"  Win rate     : {win_rate:.1f}%  "
          f"(LONG {long_wr:.0f}%  |  SHORT {short_wr:.0f}%)")
    print(f"  Profit factor: {profit_factor:.2f}")
    print("-" * w)
    print(f"  Start balance : ${INITIAL_BALANCE:>10,.2f}")
    print(f"  End balance   : ${final_bal:>10,.2f}   ({return_pct:+.1f}%)")
    print(f"  Total PnL     : ${total_pnl:>+10,.2f}")
    print(f"  Max drawdown  : {max_dd:.2f}%")
    print("-" * w)
    print(f"  Avg win (USD) : ${avg_win_usd:>+8.2f}")
    print(f"  Avg loss (USD): ${avg_loss_usd:>+8.2f}")
    print(f"  Avg R achieved: {avg_r:>+.2f}R")
    print(f"  Avg duration  : {avg_dur:.0f} min  ({avg_dur / 60:.1f} h)")
    print("=" * w)

    # Per-strategy breakdown
    strategies = sorted(set(t.get("strategy", "ema_rejection") for t in trades))
    if len(strategies) > 1 or strategies[0] != "ema_rejection":
        print()
        print("-" * w)
        print("  PER-STRATEGY BREAKDOWN")
        print("-" * w)
        print(f"  {'Strategy':<16} {'Trades':>7} {'Wins':>6} {'WR%':>6} {'PF':>6} {'Avg R':>7} {'PnL':>10}")
        print("-" * w)
        for strat in strategies:
            st = [t for t in trades if t.get("strategy", "ema_rejection") == strat]
            sw = [t for t in st if t["outcome"] == "TP"]
            sl = [t for t in st if t["outcome"] == "SL"]
            s_wr = len(sw) / len(st) * 100 if st else 0
            s_pnl = sum(t["pnl_usd"] for t in st)
            s_avg_r = sum(t["actual_r"] for t in st) / len(st) if st else 0
            gp = sum(t["pnl_usd"] for t in sw) if sw else 0
            gl = abs(sum(t["pnl_usd"] for t in sl)) if sl else 0
            s_pf = gp / gl if gl > 0 else float("inf")
            print(f"  {strat:<16} {len(st):>7} {len(sw):>6} {s_wr:>5.1f}% {s_pf:>5.2f} {s_avg_r:>+6.2f}R ${s_pnl:>+9.2f}")
        print("-" * w)


# ---------------------------------------------------------------------------
# Step 8 — Save to CSV
# ---------------------------------------------------------------------------

def save_csv(trades: list) -> None:
    if not trades:
        return
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        fieldnames = list(trades[0].keys())
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(trades)
        print(f"\n[OK] Saved {len(trades)} trades to {OUTPUT_FILE}")
    except Exception as e:
        print(f"[ERROR] Failed to save CSV: {e}")


# ---------------------------------------------------------------------------
# Step 9 — Winners vs Losers analysis
# ---------------------------------------------------------------------------

def _avg(group: list, key: str) -> float:
    """Mean of a numeric field across a list of trade dicts."""
    vals = [t[key] for t in group if t.get(key) is not None]
    return sum(vals) / len(vals) if vals else 0.0


def _dir_pct(group: list, direction: str) -> float:
    """Percentage of trades in group that are the given direction."""
    if not group:
        return 0.0
    return sum(1 for t in group if t["direction"] == direction) / len(group) * 100


def analyze_trades(trades: list) -> None:
    """
    Split trades into winners (TP) and losers (SL).
    OPEN trades are excluded — outcome is unknown.

    For each group compute:
        - avg RSI at entry
        - avg signal R:R
        - avg duration (minutes)
        - direction breakdown (% LONG / % SHORT)
        - avg EMA trend candles before entry

    Prints a side-by-side comparison to the terminal and saves to
    data/backtest_analysis.txt.
    """
    winners = [t for t in trades if t["outcome"] == "TP"]
    losers  = [t for t in trades if t["outcome"] == "SL"]
    open_t  = [t for t in trades if t["outcome"] == "OPEN"]

    if not winners and not losers:
        print("\n[ANALYSIS] No resolved trades to analyse.")
        return

    # ------------------------------------------------------------------
    # Build output lines
    # ------------------------------------------------------------------
    lines = []
    W = 66        # total line width
    C = 22        # label column width
    V = 16        # each value column width

    def rule(char="-"):
        lines.append(char * W)

    def header(label, w_val, l_val):
        lines.append(
            f"  {label:<{C}}  {str(w_val):>{V}}  {str(l_val):>{V}}"
        )

    def section(title):
        lines.append(f"  {title}")

    # Compute stats for both groups
    stats = {}
    for name, group in [("winners", winners), ("losers", losers)]:
        stats[name] = {
            "n":               len(group),
            "avg_rsi":         _avg(group, "rsi_at_entry"),
            "avg_signal_rr":   _avg(group, "signal_rr"),
            "avg_duration":    _avg(group, "duration_min"),
            "long_pct":        _dir_pct(group, "LONG"),
            "short_pct":       _dir_pct(group, "SHORT"),
            "avg_ema_candles": _avg(group, "ema_trend_candles"),
            "avg_actual_r":    _avg(group, "actual_r"),
            "avg_pnl":         _avg(group, "pnl_usd"),
        }

    w = stats["winners"]
    l = stats["losers"]

    # Header block
    rule("=")
    lines.append(
        "  TRADE ANALYSIS  --  Winners vs Losers  (OPEN trades excluded)"
    )
    rule("=")
    lines.append(
        f"  Resolved trades: {len(winners) + len(losers)}"
        f"  (winners={len(winners)}, losers={len(losers)}, open excluded={len(open_t)})"
    )
    rule()

    # Column headers
    lines.append(
        f"  {'Metric':<{C}}  {'WINNERS (' + str(w['n']) + ')':>{V}}"
        f"  {'LOSERS ('  + str(l['n']) + ')':>{V}}"
    )
    rule()

    # ---- RSI at entry ----
    section("Entry conditions")
    header(
        "Avg RSI at entry",
        f"{w['avg_rsi']:.1f}",
        f"{l['avg_rsi']:.1f}",
    )
    header(
        "Avg signal R:R",
        f"{w['avg_signal_rr']:.2f}",
        f"{l['avg_signal_rr']:.2f}",
    )
    header(
        "Avg EMA trend candles",
        f"{w['avg_ema_candles']:.1f}",
        f"{l['avg_ema_candles']:.1f}",
    )

    rule()

    # ---- Direction ----
    section("Direction breakdown")
    header(
        "LONG  trades %",
        f"{w['long_pct']:.0f}%",
        f"{l['long_pct']:.0f}%",
    )
    header(
        "SHORT trades %",
        f"{w['short_pct']:.0f}%",
        f"{l['short_pct']:.0f}%",
    )

    rule()

    # ---- Outcome metrics ----
    section("Outcome metrics")
    header(
        "Avg duration (min)",
        f"{w['avg_duration']:.0f}",
        f"{l['avg_duration']:.0f}",
    )
    header(
        "Avg duration (hours)",
        f"{w['avg_duration'] / 60:.1f}h",
        f"{l['avg_duration'] / 60:.1f}h",
    )
    header(
        "Avg actual R",
        f"{w['avg_actual_r']:+.2f}R",
        f"{l['avg_actual_r']:+.2f}R",
    )
    header(
        "Avg PnL (USD)",
        f"${w['avg_pnl']:+.2f}",
        f"${l['avg_pnl']:+.2f}",
    )

    rule("=")

    # Per-direction breakdown (winners vs losers by LONG/SHORT)
    long_wins  = [t for t in winners if t["direction"] == "LONG"]
    long_loss  = [t for t in losers  if t["direction"] == "LONG"]
    short_wins = [t for t in winners if t["direction"] == "SHORT"]
    short_loss = [t for t in losers  if t["direction"] == "SHORT"]

    lines.append("  DIRECTION DETAIL")
    rule()
    lines.append(
        f"  {'':>{C}}  {'LONG':>{V}}  {'SHORT':>{V}}"
    )
    header(
        "Trades taken",
        len(long_wins) + len(long_loss),
        len(short_wins) + len(short_loss),
    )
    header(
        "Wins",
        len(long_wins),
        len(short_wins),
    )
    header(
        "Losses",
        len(long_loss),
        len(short_loss),
    )

    lw_wr = (len(long_wins)  / (len(long_wins)  + len(long_loss))  * 100
             if (long_wins  or long_loss)  else 0.0)
    sw_wr = (len(short_wins) / (len(short_wins) + len(short_loss)) * 100
             if (short_wins or short_loss) else 0.0)
    header(
        "Win rate",
        f"{lw_wr:.0f}%",
        f"{sw_wr:.0f}%",
    )
    header(
        "Avg RSI (winners)",
        f"{_avg(long_wins,  'rsi_at_entry'):.1f}" if long_wins  else "n/a",
        f"{_avg(short_wins, 'rsi_at_entry'):.1f}" if short_wins else "n/a",
    )
    header(
        "Avg RSI (losers)",
        f"{_avg(long_loss,  'rsi_at_entry'):.1f}" if long_loss  else "n/a",
        f"{_avg(short_loss, 'rsi_at_entry'):.1f}" if short_loss else "n/a",
    )
    header(
        "Avg EMA candles (W)",
        f"{_avg(long_wins,  'ema_trend_candles'):.1f}" if long_wins  else "n/a",
        f"{_avg(short_wins, 'ema_trend_candles'):.1f}" if short_wins else "n/a",
    )
    header(
        "Avg EMA candles (L)",
        f"{_avg(long_loss,  'ema_trend_candles'):.1f}" if long_loss  else "n/a",
        f"{_avg(short_loss, 'ema_trend_candles'):.1f}" if short_loss else "n/a",
    )
    rule("=")

    # ------------------------------------------------------------------
    # Print to terminal
    # ------------------------------------------------------------------
    print()
    for line in lines:
        print(line)

    # ------------------------------------------------------------------
    # Save to file
    # ------------------------------------------------------------------
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(ANALYSIS_FILE, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
        print(f"\n[OK] Analysis saved to {ANALYSIS_FILE}")
    except Exception as e:
        print(f"[ERROR] Failed to save analysis: {e}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="XBTUSDT 15m backtest")
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Force a fresh fetch from BitMEX and overwrite the local cache.",
    )
    args = parser.parse_args()

    w = 64
    print("=" * w)
    print("  BACKTEST  --  XBTUSDT 15m  |  BitMEX live (read-only, no key)")
    print("=" * w + "\n")

    # 1. Raw candle data — cache or fetch
    raw = None

    if not args.refresh:
        raw = load_cache()
        if raw is not None:
            print(f"[CACHE] Loading from {RAW_CACHE} ({len(raw):,} candles)\n")
        else:
            print(f"[FETCH] No cache found — fetching from BitMEX...\n")

    if raw is None:
        # Either --refresh was passed or no cache exists
        if args.refresh:
            print(f"[FETCH] --refresh flag set — fetching from BitMEX...\n")
        try:
            exchange = _make_live_exchange()
        except Exception as exc:
            raise SystemExit(f"[ABORT] Could not connect to BitMEX live: {exc}")

        raw = fetch_all_raw(exchange)
        if not raw:
            raise SystemExit("[ABORT] No candles returned from exchange.")

        save_cache(raw)

    # 2. Resample to 15m
    df_15m = resample_to_15m(raw)
    if len(df_15m) < MIN_WARMUP + 2:
        raise SystemExit(
            f"[ABORT] Too few 15m candles ({len(df_15m)}) after resampling."
        )

    # 3. Compute indicators once (no look-ahead for forward-recursive indicators)
    df_enriched = enrich(df_15m)

    # 4. Walk and simulate
    trades = run_backtest(df_enriched)

    # 5. Summary report
    print_report(trades)

    # 6. Save trade log
    save_csv(trades)

    # 7. Winners vs losers analysis
    analyze_trades(trades)
