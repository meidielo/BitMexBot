"""
signals.py — Phase 3 (rewritten)

Real strategy: EMA-rejection candles with wick confirmation, RSI filter,
swing-high/low SL, and round-number / recent-extreme TP targets.

Function signature is backward-compatible:
    get_signal(df, exchange=None) -> dict

The exchange argument is optional.  When supplied, the current
funding rate is fetched and added to the reason string as
confirmation (never blocks a signal).
"""

import math
import pandas as pd

# ML filter — optional, loaded lazily; absence is not an error
try:
    from ml_filter import score_signal as _ml_score_signal
    _ML_AVAILABLE = True
except ImportError:
    _ML_AVAILABLE = False

# ---------------------------------------------------------------------------
# Strategy constants  (never overridden)
# ---------------------------------------------------------------------------
FUNDING_SYMBOL   = "BTC/USDT:USDT"

WICK_MIN_PCT     = 0.0005  # 0.05% — lowered to generate trades on testnet
SL_BUFFER_PCT    = 0.001   # 0.1% buffer beyond swing high/low
SL_MIN_DIST_PCT  = 0.003   # 0.3% — if SL is closer than this, skip (noise)
MIN_RR           = 1.5     # minimum reward-to-risk ratio
MIN_TREND_AGE    = 15      # consecutive candles in trend required for entry
TP_ROUND_STEP    = 500     # round-number level spacing for TP targets

SHORT_RSI_MIN    = 40      # display only — RSI is no longer a signal gate
SHORT_RSI_MAX    = 65
LONG_RSI_MIN     = 35
LONG_RSI_MAX     = 60

SL_LOOKBACK      = 5       # candles back for swing high/low SL
TP_LOOKBACK      = 20      # candles back for extreme-level TP target

REQUIRED_COLS    = ["open", "high", "low", "close", "ema_20", "ema_50", "rsi_14"]


# ---------------------------------------------------------------------------
# TP level helpers
# ---------------------------------------------------------------------------

def nearest_round_support(price: float, step: int = TP_ROUND_STEP) -> float:
    """Largest multiple of `step` that is <= price."""
    return math.floor(price / step) * step


def nearest_round_resistance(price: float, step: int = TP_ROUND_STEP) -> float:
    """Smallest multiple of `step` that is >= price."""
    return math.ceil(price / step) * step


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _no_trade(reason: str, rr: float = None,
              funding_rate: float = None) -> dict:
    return {
        "signal":       "NO_TRADE",
        "reason":       reason,
        "entry_price":  None,
        "sl_price":     None,
        "tp_price":     None,
        "rr":           rr,
        "funding_rate": funding_rate,
    }


def _fetch_funding_rate(exchange) -> "float | None":
    """
    Fetch the current XBTUSDT funding rate from the exchange.
    Returns None on any failure — funding rate is confirmation only,
    never a blocker.
    """
    if exchange is None:
        return None
    try:
        data = exchange.fetch_funding_rate(FUNDING_SYMBOL)
        return float(data.get("fundingRate", 0.0))
    except Exception as exc:
        print(f"  [WARN] Could not fetch funding rate: {exc}")
        return None


def _pf(passed: bool) -> str:
    """'[PASS]' or '[FAIL]' label for terminal diagnostics."""
    return "[PASS]" if passed else "[FAIL]"


def _count_trend_age(df: pd.DataFrame, direction: str = "SHORT") -> int:
    """
    Count consecutive candles going backwards from the last row where
    the trend condition holds:
        SHORT: EMA20 < EMA50 (downtrend)
        LONG:  EMA20 > EMA50 (uptrend)

    Stops as soon as the condition breaks or a NaN is hit.
    Returns 0 if the condition does not hold on the last candle itself.
    """
    count = 0
    for k in range(len(df) - 1, -1, -1):
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
    return count


# ---------------------------------------------------------------------------
# Main signal function
# ---------------------------------------------------------------------------

def get_signal(df: pd.DataFrame, exchange=None) -> dict:
    """
    Evaluate the latest candle in an enriched OHLCV+indicator DataFrame.

    Returns
    -------
    dict with keys:
        signal       : "LONG" | "SHORT" | "NO_TRADE"
        reason       : plain-English explanation of every condition
        entry_price  : float | None
        sl_price     : float | None
        tp_price     : float | None
        rr           : float | None
        funding_rate : float | None

    SHORT conditions (all required)
    --------------------------------
    C1  Trend     : EMA20 < EMA50
    C2  Trend age : >= MIN_TREND_AGE (20) consecutive candles in downtrend
    C3  Rejection : high > EMA20  AND  close < EMA20
    C4  Wick      : upper_wick  = high - max(open, close) > 0.2% of close

    LONG conditions (all required)
    --------------------------------
    C1  Trend     : EMA20 > EMA50
    C2  Trend age : >= MIN_TREND_AGE (20) consecutive candles in uptrend
    C3  Rejection : low < EMA20  AND  close > EMA20
    C4  Wick      : lower_wick  = min(open, close) - low > 0.2% of close

    SL  = swing high/low of last 5 candles ± 0.1% buffer
    TP  = min/max of (20-bar extreme, nearest round level)
    Gate: SL distance < 0.3% → NO_TRADE; R:R < 2.0 → NO_TRADE
    """

    # ------------------------------------------------------------------
    # Guards
    # ------------------------------------------------------------------
    if df is None or df.empty:
        return _no_trade("DataFrame is empty or None.")

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        return _no_trade(f"Missing required columns: {missing}")

    if len(df) < SL_LOOKBACK:
        return _no_trade(
            f"Insufficient data: need >= {SL_LOOKBACK} candles, got {len(df)}."
        )

    latest = df.iloc[-1]
    ts     = df.index[-1]

    nan_cols = [c for c in REQUIRED_COLS if pd.isna(latest[c])]
    if nan_cols:
        return _no_trade(
            f"Indicators not warmed up at {ts}. NaN in: {nan_cols}"
        )

    # ------------------------------------------------------------------
    # Extract values
    # ------------------------------------------------------------------
    o     = float(latest["open"])
    h     = float(latest["high"])
    lo    = float(latest["low"])
    c     = float(latest["close"])
    ema20 = float(latest["ema_20"])
    ema50 = float(latest["ema_50"])
    rsi   = float(latest["rsi_14"])

    entry = c   # entry is always current close

    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - lo

    last_5      = df.iloc[-SL_LOOKBACK:]
    last_20     = df.iloc[-TP_LOOKBACK:]
    swing_high5 = float(last_5["high"].max())
    swing_low5  = float(last_5["low"].min())
    low_20      = float(last_20["low"].min())
    high_20     = float(last_20["high"].max())

    wick_min       = entry * WICK_MIN_PCT
    short_trend_age = _count_trend_age(df, direction="SHORT")
    long_trend_age  = _count_trend_age(df, direction="LONG")

    # ------------------------------------------------------------------
    # Print diagnostics
    # ------------------------------------------------------------------
    print(f"\n  -- Signal diagnostics  [{ts}] --")
    print(f"  Candle  O={o:.2f}  H={h:.2f}  L={lo:.2f}  C={c:.2f}")
    print(f"  EMA20={ema20:.2f}  EMA50={ema50:.2f}  RSI={rsi:.2f} (display only)")
    print(f"  SHORT trend age: {short_trend_age}  |  LONG trend age: {long_trend_age}"
          f"  (min={MIN_TREND_AGE})")
    print(f"  Upper wick={upper_wick:.2f}  Lower wick={lower_wick:.2f}"
          f"  (min={wick_min:.2f}, {WICK_MIN_PCT*100:.1f}% of close)")
    print(f"  {SL_LOOKBACK}-bar: swing_high={swing_high5:.2f}"
          f"  swing_low={swing_low5:.2f}")
    print(f"  {TP_LOOKBACK}-bar: high={high_20:.2f}  low={low_20:.2f}")

    # ------------------------------------------------------------------
    # Evaluate SHORT conditions
    # C1 trend      : EMA20 < EMA50
    # C2 trend age  : >= MIN_TREND_AGE consecutive candles
    # C3 rejection  : high > EMA20 AND close < EMA20
    # C4 wick       : upper_wick > 0.2% of close
    # ------------------------------------------------------------------
    sc1 = ema20 < ema50
    sc2 = short_trend_age >= MIN_TREND_AGE
    sc3 = h > ema20 and c < ema20
    sc4 = upper_wick > wick_min

    print(f"\n  SHORT conditions:")
    print(f"  {_pf(sc1)} C1 trend:      EMA20 ({ema20:.2f}) < EMA50 ({ema50:.2f})")
    print(f"  {_pf(sc2)} C2 trend age:  {short_trend_age} candles >= {MIN_TREND_AGE}")
    print(f"  {_pf(sc3)} C3 rejection:  high ({h:.2f}) > EMA20, close ({c:.2f}) < EMA20")
    print(f"  {_pf(sc4)} C4 upper wick: {upper_wick:.2f} > {wick_min:.2f}")

    # ------------------------------------------------------------------
    # Evaluate LONG conditions
    # C1 trend      : EMA20 > EMA50
    # C2 trend age  : >= MIN_TREND_AGE consecutive candles
    # C3 rejection  : low < EMA20 AND close > EMA20
    # C4 wick       : lower_wick > 0.2% of close
    # ------------------------------------------------------------------
    lc1 = ema20 > ema50
    lc2 = long_trend_age >= MIN_TREND_AGE
    lc3 = lo < ema20 and c > ema20
    lc4 = lower_wick > wick_min

    print(f"\n  LONG conditions:")
    print(f"  {_pf(lc1)} C1 trend:      EMA20 ({ema20:.2f}) > EMA50 ({ema50:.2f})")
    print(f"  {_pf(lc2)} C2 trend age:  {long_trend_age} candles >= {MIN_TREND_AGE}")
    print(f"  {_pf(lc3)} C3 rejection:  low ({lo:.2f}) < EMA20, close ({c:.2f}) > EMA20")
    print(f"  {_pf(lc4)} C4 lower wick: {lower_wick:.2f} > {wick_min:.2f}")

    # ------------------------------------------------------------------
    # Fetch funding rate (optional confirmation, never blocks)
    # ------------------------------------------------------------------
    funding_rate = _fetch_funding_rate(exchange)

    # ------------------------------------------------------------------
    # === SHORT path ===
    # ------------------------------------------------------------------
    if sc1 and sc2 and sc3 and sc4:

        sl_raw  = swing_high5 + entry * SL_BUFFER_PCT
        sl_price = round(sl_raw, 2)
        sl_dist  = sl_price - entry

        # Gate 1 — SL distance
        if sl_dist < entry * SL_MIN_DIST_PCT:
            reason = (
                f"SHORT setup valid but SL too tight: "
                f"distance {sl_dist:.2f} ({sl_dist/entry*100:.3f}%) "
                f"< minimum {SL_MIN_DIST_PCT*100:.1f}% "
                f"(SL {sl_price:.2f} vs entry {entry:.2f})"
            )
            print(f"\n  [NO_TRADE] {reason}")
            return _no_trade(reason, funding_rate=funding_rate)

        # TP
        tp_c1 = low_20
        tp_c2 = nearest_round_support(entry, step=TP_ROUND_STEP)
        tp_price = round(min(tp_c1, tp_c2), 2)

        gain = entry - tp_price
        risk = sl_price - entry

        if risk <= 0 or gain <= 0:
            reason = (
                f"SHORT setup valid but TP ({tp_price:.2f}) >= entry ({entry:.2f}). "
                f"No viable downside target."
            )
            print(f"\n  [NO_TRADE] {reason}")
            return _no_trade(reason, funding_rate=funding_rate)

        rr = round(gain / risk, 2)

        # Gate 2 — R:R
        if rr < MIN_RR:
            reason = (
                f"R:R {rr:.2f} below minimum {MIN_RR} "
                f"(entry={entry:.2f}, SL={sl_price:.2f}, TP={tp_price:.2f})"
            )
            print(f"\n  [NO_TRADE] {reason}")
            return _no_trade(reason, rr=rr, funding_rate=funding_rate)

        # Funding confirmation note
        funding_note = ""
        if funding_rate is not None and funding_rate > 0:
            funding_note = (
                f" | Funding rate {funding_rate:.6f} confirms bearish "
                f"(longs paying shorts)"
            )

        reason = (
            f"SHORT: downtrend (EMA20={ema20:.2f} < EMA50={ema50:.2f}), "
            f"trend age {short_trend_age} candles, "
            f"rejection candle (high={h:.2f} pierced EMA20, closed below at {c:.2f}), "
            f"upper wick {upper_wick:.2f} ({upper_wick/entry*100:.3f}% of close), "
            f"RSI {rsi:.2f} (info only), "
            f"SL={sl_price:.2f} (swing_high5={swing_high5:.2f} + {SL_BUFFER_PCT*100:.1f}% buffer), "
            f"TP={tp_price:.2f} (min of 20-bar low {tp_c1:.2f} / round support {tp_c2:.2f}), "
            f"R:R={rr:.2f}"
            f"{funding_note}"
        )

        print(f"\n  [SHORT] entry={entry:.2f}  SL={sl_price:.2f}"
              f"  TP={tp_price:.2f}  R:R={rr:.2f}")

        signal_result = {
            "signal":       "SHORT",
            "reason":       reason,
            "entry_price":  entry,
            "sl_price":     sl_price,
            "tp_price":     tp_price,
            "rr":           rr,
            "funding_rate": funding_rate,
            "ml_score":     None,
        }

        # ------------------------------------------------------------------
        # ML filter (soft gate — requires data/ml_filter.pkl)
        # Run:  python ml_filter.py --retrain  to generate the model.
        # ------------------------------------------------------------------
        if _ML_AVAILABLE:
            try:
                from ml_filter import MODEL_PATH as _ML_MODEL_PATH
                import os as _os
                if _os.path.exists(_ML_MODEL_PATH):
                    ml_score = _ml_score_signal(signal_result)
                    signal_result["ml_score"] = ml_score
                    ML_THRESHOLD = 0.55
                    if ml_score < ML_THRESHOLD:
                        ml_reason = (
                            f"ML filter blocked: score {ml_score:.2f} < {ML_THRESHOLD}"
                        )
                        print(f"  [ML] score={ml_score:.2f}  BLOCKED")
                        return _no_trade(
                            ml_reason,
                            rr=rr,
                            funding_rate=funding_rate,
                        )
                    else:
                        print(f"  [ML] score={ml_score:.2f}  APPROVED")
                else:
                    print("  [ML] No model found — skipping filter")
            except Exception as _ml_exc:
                print(f"  [ML] Filter error ({_ml_exc}) — skipping")
        else:
            print("  [ML] No model found — skipping filter")

        return signal_result

    # ------------------------------------------------------------------
    # === LONG path ===
    # ------------------------------------------------------------------
    if lc1 and lc2 and lc3 and lc4:

        sl_raw   = swing_low5 - entry * SL_BUFFER_PCT
        sl_price = round(sl_raw, 2)
        sl_dist  = entry - sl_price

        # Gate 1 — SL distance
        if sl_dist < entry * SL_MIN_DIST_PCT:
            reason = (
                f"LONG setup valid but SL too tight: "
                f"distance {sl_dist:.2f} ({sl_dist/entry*100:.3f}%) "
                f"< minimum {SL_MIN_DIST_PCT*100:.1f}% "
                f"(SL {sl_price:.2f} vs entry {entry:.2f})"
            )
            print(f"\n  [NO_TRADE] {reason}")
            return _no_trade(reason, funding_rate=funding_rate)

        # TP
        tp_c1 = high_20
        tp_c2 = nearest_round_resistance(entry, step=TP_ROUND_STEP)
        tp_price = round(max(tp_c1, tp_c2), 2)

        gain = tp_price - entry
        risk = entry - sl_price

        if risk <= 0 or gain <= 0:
            reason = (
                f"LONG setup valid but TP ({tp_price:.2f}) <= entry ({entry:.2f}). "
                f"No viable upside target."
            )
            print(f"\n  [NO_TRADE] {reason}")
            return _no_trade(reason, funding_rate=funding_rate)

        rr = round(gain / risk, 2)

        # Gate 2 — R:R
        if rr < MIN_RR:
            reason = (
                f"R:R {rr:.2f} below minimum {MIN_RR} "
                f"(entry={entry:.2f}, SL={sl_price:.2f}, TP={tp_price:.2f})"
            )
            print(f"\n  [NO_TRADE] {reason}")
            return _no_trade(reason, rr=rr, funding_rate=funding_rate)

        # Funding confirmation note (negative funding = shorts paying longs = bullish)
        funding_note = ""
        if funding_rate is not None and funding_rate < 0:
            funding_note = (
                f" | Funding rate {funding_rate:.6f} confirms bullish "
                f"(shorts paying longs)"
            )

        reason = (
            f"LONG: uptrend (EMA20={ema20:.2f} > EMA50={ema50:.2f}), "
            f"trend age {long_trend_age} candles, "
            f"rejection candle (low={lo:.2f} pierced EMA20, closed above at {c:.2f}), "
            f"lower wick {lower_wick:.2f} ({lower_wick/entry*100:.3f}% of close), "
            f"RSI {rsi:.2f} (info only), "
            f"SL={sl_price:.2f} (swing_low5={swing_low5:.2f} - {SL_BUFFER_PCT*100:.1f}% buffer), "
            f"TP={tp_price:.2f} (max of 20-bar high {tp_c1:.2f} / round resistance {tp_c2:.2f}), "
            f"R:R={rr:.2f}"
            f"{funding_note}"
        )

        print(f"\n  [LONG] entry={entry:.2f}  SL={sl_price:.2f}"
              f"  TP={tp_price:.2f}  R:R={rr:.2f}")

        signal_result = {
            "signal":       "LONG",
            "reason":       reason,
            "entry_price":  entry,
            "sl_price":     sl_price,
            "tp_price":     tp_price,
            "rr":           rr,
            "funding_rate": funding_rate,
            "ml_score":     None,
        }

        # ML filter (same soft gate as SHORT)
        if _ML_AVAILABLE:
            try:
                from ml_filter import MODEL_PATH as _ML_MODEL_PATH
                import os as _os
                if _os.path.exists(_ML_MODEL_PATH):
                    ml_score = _ml_score_signal(signal_result)
                    signal_result["ml_score"] = ml_score
                    ML_THRESHOLD = 0.55
                    if ml_score < ML_THRESHOLD:
                        ml_reason = (
                            f"ML filter blocked: score {ml_score:.2f} < {ML_THRESHOLD}"
                        )
                        print(f"  [ML] score={ml_score:.2f}  BLOCKED")
                        return _no_trade(
                            ml_reason,
                            rr=rr,
                            funding_rate=funding_rate,
                        )
                    else:
                        print(f"  [ML] score={ml_score:.2f}  APPROVED")
                else:
                    print("  [ML] No model found — skipping filter")
            except Exception as _ml_exc:
                print(f"  [ML] Filter error ({_ml_exc}) — skipping")
        else:
            print("  [ML] No model found — skipping filter")

        return signal_result

    # ------------------------------------------------------------------
    # === NO_TRADE — explain failed conditions ===
    # ------------------------------------------------------------------
    short_fails = []
    if not sc1: short_fails.append(
        f"EMA20 ({ema20:.2f}) not < EMA50 ({ema50:.2f}) — no downtrend")
    if not sc2: short_fails.append(
        f"trend too young: {short_trend_age} candles (min {MIN_TREND_AGE})")
    if not sc3: short_fails.append(
        f"no rejection (H={h:.2f}, C={c:.2f}, EMA20={ema20:.2f})")
    if not sc4: short_fails.append(
        f"upper wick {upper_wick:.2f} < min {wick_min:.2f}")

    long_fails = []
    if not lc1: long_fails.append(
        f"EMA20 ({ema20:.2f}) not > EMA50 ({ema50:.2f}) — no uptrend")
    if not lc2: long_fails.append(
        f"trend too young: {long_trend_age} candles (min {MIN_TREND_AGE})")
    if not lc3: long_fails.append(
        f"no rejection (L={lo:.2f}, C={c:.2f}, EMA20={ema20:.2f})")
    if not lc4: long_fails.append(
        f"lower wick {lower_wick:.2f} < min {wick_min:.2f}")

    parts = []
    if short_fails:
        parts.append("SHORT: " + "; ".join(short_fails))
    if long_fails:
        parts.append("LONG: " + "; ".join(long_fails))
    reason = " | ".join(parts) if parts else "No conditions met."
    print(f"\n  [NO_TRADE] {reason}")
    return _no_trade(reason, funding_rate=funding_rate)


# ---------------------------------------------------------------------------
# Print helper
# ---------------------------------------------------------------------------

def print_signal(result: dict) -> None:
    """Print the signal dict in a readable summary block."""
    sig = result["signal"]
    tag = {"LONG": "[  LONG  ]", "SHORT": "[ SHORT  ]",
           "NO_TRADE": "[NO TRADE]"}[sig]
    width = 66
    print("\n" + "=" * width)
    print(f"  SIGNAL: {tag}")
    print("=" * width)
    print(f"  Reason      : {result['reason']}")
    if result["entry_price"] is not None:
        ep = result["entry_price"]
        sl = result["sl_price"]
        tp = result["tp_price"]
        print(f"  Entry       : {ep:.2f}")
        print(f"  Stop-loss   : {sl:.2f}  ({abs(sl-ep)/ep*100:.3f}% from entry)")
        print(f"  Take-profit : {tp:.2f}  ({abs(tp-ep)/ep*100:.3f}% from entry)")
    if result.get("rr") is not None:
        print(f"  R:R         : {result['rr']:.2f}")
    if result.get("funding_rate") is not None:
        print(f"  Funding rate: {result['funding_rate']:.6f}")
    print("=" * width)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from fetch_data import fetch_ohlcv
    from indicators import compute_indicators
    from bitmex_client import get_client

    print("Phase 3 (rewritten) — Signal generation on XBTUSDT 15m candles\n")

    print("Fetching OHLCV data from BitMEX testnet...")
    df_raw = fetch_ohlcv()
    if df_raw is None:
        raise SystemExit("[ABORT] Could not fetch OHLCV data.")
    print(f"[OK] Received {len(df_raw)} candles.\n")

    print("Computing indicators...")
    df_enriched = compute_indicators(df_raw)
    if df_enriched is None:
        raise SystemExit("[ABORT] Indicator computation failed.")
    print("[OK] Indicators computed.\n")

    try:
        exchange = get_client()
    except Exception as e:
        print(f"[WARN] Could not connect to exchange for funding rate: {e}")
        exchange = None

    result = get_signal(df_enriched, exchange=exchange)
    print_signal(result)
