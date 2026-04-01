"""
test_signals.py — Unit tests for signals.py

Run:  python test_signals.py
No network calls — all tests use synthetic DataFrames.

Tests
-----
SHORT:
1. SHORT fires when all conditions are met (including trend age >= 20)
2. NO_TRADE when R:R < 2.0
3. NO_TRADE when trend is too young (< MIN_TREND_AGE candles)
4. NO_TRADE when upper wick is too small
5. SL is always above entry price for SHORT signals
6. TP is always below entry price for SHORT signals

LONG:
7.  LONG fires when all conditions are met
8.  SL is always below entry for LONG signals
9.  TP is always above entry for LONG signals
10. NO_TRADE when LONG trend is too young
11. NO_TRADE when lower wick is too small for LONG

Helpers:
12. round level helpers
"""

import io
import sys
import contextlib
import pandas as pd

import signals as _sig_mod
from signals import (
    get_signal,
    nearest_round_support,
    nearest_round_resistance,
    MIN_RR,
    MIN_TREND_AGE,
    SL_LOOKBACK,
    TP_LOOKBACK,
    WICK_MIN_PCT,
    SL_MIN_DIST_PCT,
    TP_ROUND_STEP,
    BB_RSI_OVERSOLD,
    BB_RSI_OVERBOUGHT,
    EMA_CROSS_BODY_MIN_PCT,
    RSI_EXTREME_LOW,
    RSI_EXTREME_HIGH,
    ADX_MIN,
)

# Disable ML filter for unit tests — it would block synthetic signals
_sig_mod._ML_AVAILABLE = False


# ---------------------------------------------------------------------------
# Synthetic DataFrame factory — SHORT
# ---------------------------------------------------------------------------

# Default candle designed so SHORT fires cleanly.
# _df() uses n=60 by default so trend_age=60 >= MIN_TREND_AGE (20).
#
#   C1 EMA20 (69800) < EMA50 (70000)                     → PASS
#   C2 trend_age = 60 >= 20                               → PASS
#   C3 high (70100) > EMA20, close (69600) < EMA20        → PASS
#   C4 upper_wick = 70100 - max(69650,69600) = 450
#      0.2% of 69600 = 139.2  →  450 > 139.2             → PASS
#
#   swing_high5 = 70100  →  SL = 70100 + 69.6 = 70169.6
#   sl_dist = 569.6  >  208.8 (0.3%)                     → gate PASS
#   tp = min(low_20=66000, round_support=69500) = 66000
#   R:R = 3600/569.6 ≈ 6.32 ≥ 2.0                       → gate PASS

BASE_ROW = {
    "open":   69650.0,
    "high":   70100.0,
    "low":    66000.0,
    "close":  69600.0,
    "volume": 1000.0,
    "ema_20": 69800.0,
    "ema_50": 70000.0,
    "rsi_14": 55.0,
    # BB columns exist in real df; not used by signals.py
    "bb_upper": 71000.0,
    "bb_mid":   69500.0,
    "bb_lower": 68000.0,
    "adx_14":   30.0,
}


def _df(n: int = 60, prev_overrides: dict = None, **last_overrides) -> pd.DataFrame:
    """
    Build a synthetic indicator DataFrame.

    Parameters
    ----------
    n               : total number of candles
    prev_overrides  : overrides applied to every candle EXCEPT the last
    **last_overrides: overrides applied to the last candle only
    """
    rows = [{**BASE_ROW} for _ in range(n)]
    if prev_overrides:
        for i in range(n - 1):
            rows[i].update(prev_overrides)
    rows[-1].update(last_overrides)
    idx = pd.date_range("2026-01-01", periods=n, freq="15min")
    return pd.DataFrame(rows, index=idx)


# ---------------------------------------------------------------------------
# Synthetic DataFrame factory — LONG
# ---------------------------------------------------------------------------

# Default candle designed so LONG fires cleanly.
#
#   C1 EMA20 (70000) > EMA50 (69800)                     → PASS
#   C2 trend_age = 60 >= 20                               → PASS
#   C3 low (69400) < EMA20 (70000), close (70200) > EMA20 → PASS
#   C4 lower_wick = min(70100,70200) - 69400 = 700
#      0.2% of 70200 = 140.4  →  700 > 140.4             → PASS
#
#   swing_low5 = 69400   →  SL = 69400 - 70.2 = 69329.8
#   sl_dist = 70200 - 69329.8 = 870.2  >  210.6 (0.3%)  → gate PASS
#   tp = max(high_20=71500, round_resistance=70500) = 71500
#   R:R = 1300/870.2 ≈ 1.49  → TOO LOW — need higher TP
#
# Fix: set high on prev candles to 73000 so high_20 = 73000
#   tp = max(73000, 70500) = 73000
#   R:R = 2800/870.2 ≈ 3.22 ≥ 2.0                       → gate PASS

LONG_BASE_ROW = {
    "open":   70100.0,
    "high":   73000.0,     # high enough for 20-bar high TP
    "low":    69400.0,
    "close":  70200.0,
    "volume": 1000.0,
    "ema_20": 70000.0,
    "ema_50": 69800.0,     # EMA20 > EMA50 → uptrend
    "rsi_14": 50.0,
    "bb_upper": 73000.0,
    "bb_mid":   70000.0,
    "bb_lower": 68000.0,
    "adx_14":   30.0,
}


def _df_long(n: int = 60, prev_overrides: dict = None, **last_overrides) -> pd.DataFrame:
    """Build a synthetic DataFrame where LONG conditions fire."""
    rows = [{**LONG_BASE_ROW} for _ in range(n)]
    if prev_overrides:
        for i in range(n - 1):
            rows[i].update(prev_overrides)
    rows[-1].update(last_overrides)
    idx = pd.date_range("2026-01-01", periods=n, freq="15min")
    return pd.DataFrame(rows, index=idx)


@contextlib.contextmanager
def _silent():
    """Suppress stdout so test output is clean (get_signal prints diagnostics)."""
    buf = io.StringIO()
    old = sys.stdout
    sys.stdout = buf
    try:
        yield
    finally:
        sys.stdout = old


# ---------------------------------------------------------------------------
# Test 1 — SHORT fires when all conditions met
# ---------------------------------------------------------------------------

def test_short_fires_on_all_conditions():
    with _silent():
        result = get_signal(_df())

    assert result["signal"] == "SHORT", (
        f"Expected SHORT, got {result['signal']}. Reason: {result['reason']}"
    )
    assert result["entry_price"] is not None
    assert result["sl_price"]    is not None
    assert result["tp_price"]    is not None
    assert result["rr"]          is not None
    assert result["rr"] >= MIN_RR, (
        f"R:R {result['rr']} should be >= {MIN_RR}"
    )
    print("[PASS] test_short_fires_on_all_conditions")


# ---------------------------------------------------------------------------
# Test 2 — NO_TRADE when R:R < 2.0
# ---------------------------------------------------------------------------

def test_no_trade_when_rr_below_minimum():
    # Push all candle lows close to entry so both TP candidates are near entry.
    #
    # Previous candles: low = 69500  (close to entry 69600)
    # Last candle:      low = 69400  (last candle retains other SHORT values)
    #
    # lowest_low_20 = 69400
    # round_support  = floor(69600/500)*500 = 69500
    # tp_price       = min(69400, 69500) = 69400
    # gain           = 69600 - 69400 = 200
    # risk           = 70169.6 - 69600 ≈ 570
    # R:R ≈ 0.35  →  NO_TRADE
    with _silent():
        result = get_signal(_df(prev_overrides={"low": 69500.0}, low=69400.0))

    assert result["signal"] == "NO_TRADE", (
        f"Expected NO_TRADE (low R:R), got {result['signal']}. "
        f"Reason: {result['reason']}"
    )
    assert "R:R" in result["reason"], (
        f"Reason should mention R:R, got: {result['reason']}"
    )
    print("[PASS] test_no_trade_when_rr_below_minimum")


# ---------------------------------------------------------------------------
# Test 3 — NO_TRADE when trend is too young (< MIN_TREND_AGE candles)
# ---------------------------------------------------------------------------

def test_no_trade_when_trend_too_young():
    # Only 10 candles in the DataFrame → trend_age = 10 < MIN_TREND_AGE (20)
    with _silent():
        result = get_signal(_df(n=10))

    assert result["signal"] == "NO_TRADE", (
        f"Expected NO_TRADE (trend too young), got {result['signal']}. "
        f"Reason: {result['reason']}"
    )
    assert "trend" in result["reason"].lower(), (
        f"Reason should mention trend age, got: {result['reason']}"
    )

    # Boundary: exactly MIN_TREND_AGE candles should pass C2
    with _silent():
        result_boundary = get_signal(_df(n=MIN_TREND_AGE))

    assert result_boundary["signal"] == "SHORT", (
        f"Expected SHORT at exactly MIN_TREND_AGE={MIN_TREND_AGE} candles, "
        f"got {result_boundary['signal']}. Reason: {result_boundary['reason']}"
    )
    print("[PASS] test_no_trade_when_trend_too_young")


# ---------------------------------------------------------------------------
# Test 4 — NO_TRADE when upper wick is too small
# ---------------------------------------------------------------------------

def test_no_trade_when_wick_too_small():
    # Construct a candle where:
    #   high (69810) is just above EMA20 (69800) — C3 still passes
    #   open (69790) is just below EMA20              — body is at EMA20
    #   close stays at 69600 < EMA20                  — C3 close passes
    #   upper_wick = 69810 - max(69790, 69600) = 69810 - 69790 = 20
    #   0.2% of 69600 = 139.2  →  20 < 139.2  → C4 FAIL
    with _silent():
        result = get_signal(_df(open=69790.0, high=69810.0))

    assert result["signal"] == "NO_TRADE", (
        f"Expected NO_TRADE (wick too small), got {result['signal']}. "
        f"Reason: {result['reason']}"
    )
    assert "wick" in result["reason"].lower(), (
        f"Reason should mention wick, got: {result['reason']}"
    )
    print("[PASS] test_no_trade_when_wick_too_small")


# ---------------------------------------------------------------------------
# Test 5 — SL is always ABOVE entry for SHORT signals
# ---------------------------------------------------------------------------

def test_sl_above_entry_for_short():
    with _silent():
        result = get_signal(_df())

    assert result["signal"] == "SHORT", (
        f"Precondition failed — expected SHORT, got {result['signal']}"
    )
    assert result["sl_price"] > result["entry_price"], (
        f"SHORT SL ({result['sl_price']}) must be > entry ({result['entry_price']})"
    )
    print("[PASS] test_sl_above_entry_for_short  "
          f"(entry={result['entry_price']:.2f}, SL={result['sl_price']:.2f})")


# ---------------------------------------------------------------------------
# Test 6 — TP is always BELOW entry for SHORT signals
# ---------------------------------------------------------------------------

def test_tp_below_entry_for_short():
    with _silent():
        result = get_signal(_df())

    assert result["signal"] == "SHORT", (
        f"Precondition failed — expected SHORT, got {result['signal']}"
    )
    assert result["tp_price"] < result["entry_price"], (
        f"SHORT TP ({result['tp_price']}) must be < entry ({result['entry_price']})"
    )
    print("[PASS] test_tp_below_entry_for_short  "
          f"(entry={result['entry_price']:.2f}, TP={result['tp_price']:.2f})")


# ---------------------------------------------------------------------------
# Test 7 — LONG fires when all conditions met
# ---------------------------------------------------------------------------

def test_long_fires_on_all_conditions():
    with _silent():
        result = get_signal(_df_long())

    assert result["signal"] == "LONG", (
        f"Expected LONG, got {result['signal']}. Reason: {result['reason']}"
    )
    assert result["entry_price"] is not None
    assert result["sl_price"]    is not None
    assert result["tp_price"]    is not None
    assert result["rr"]          is not None
    assert result["rr"] >= MIN_RR, (
        f"R:R {result['rr']} should be >= {MIN_RR}"
    )
    print("[PASS] test_long_fires_on_all_conditions")


# ---------------------------------------------------------------------------
# Test 8 — SL is always BELOW entry for LONG signals
# ---------------------------------------------------------------------------

def test_sl_below_entry_for_long():
    with _silent():
        result = get_signal(_df_long())

    assert result["signal"] == "LONG", (
        f"Precondition failed — expected LONG, got {result['signal']}"
    )
    assert result["sl_price"] < result["entry_price"], (
        f"LONG SL ({result['sl_price']}) must be < entry ({result['entry_price']})"
    )
    print("[PASS] test_sl_below_entry_for_long  "
          f"(entry={result['entry_price']:.2f}, SL={result['sl_price']:.2f})")


# ---------------------------------------------------------------------------
# Test 9 — TP is always ABOVE entry for LONG signals
# ---------------------------------------------------------------------------

def test_tp_above_entry_for_long():
    with _silent():
        result = get_signal(_df_long())

    assert result["signal"] == "LONG", (
        f"Precondition failed — expected LONG, got {result['signal']}"
    )
    assert result["tp_price"] > result["entry_price"], (
        f"LONG TP ({result['tp_price']}) must be > entry ({result['entry_price']})"
    )
    print("[PASS] test_tp_above_entry_for_long  "
          f"(entry={result['entry_price']:.2f}, TP={result['tp_price']:.2f})")


# ---------------------------------------------------------------------------
# Test 10 — NO_TRADE when LONG trend is too young
# ---------------------------------------------------------------------------

def test_no_trade_when_long_trend_too_young():
    # Only 10 candles → long trend_age = 10 < MIN_TREND_AGE (20)
    with _silent():
        result = get_signal(_df_long(n=10))

    assert result["signal"] == "NO_TRADE", (
        f"Expected NO_TRADE (LONG trend too young), got {result['signal']}. "
        f"Reason: {result['reason']}"
    )
    assert "trend" in result["reason"].lower(), (
        f"Reason should mention trend age, got: {result['reason']}"
    )

    # Boundary: exactly MIN_TREND_AGE candles should pass C2
    with _silent():
        result_boundary = get_signal(_df_long(n=MIN_TREND_AGE))

    assert result_boundary["signal"] == "LONG", (
        f"Expected LONG at exactly MIN_TREND_AGE={MIN_TREND_AGE} candles, "
        f"got {result_boundary['signal']}. Reason: {result_boundary['reason']}"
    )
    print("[PASS] test_no_trade_when_long_trend_too_young")


# ---------------------------------------------------------------------------
# Test 11 — NO_TRADE when lower wick is too small for LONG
# ---------------------------------------------------------------------------

def test_no_trade_when_long_wick_too_small():
    # Construct a candle where open/close are near EMA20 so wick is tiny:
    #   open (70010), close (70020), low (69990)
    #   lower_wick = min(70010, 70020) - 69990 = 20
    #   0.05% of 70020 = 35.01  →  20 < 35.01  → C4 FAIL
    with _silent():
        result = get_signal(_df_long(open=70010.0, close=70020.0, low=69990.0))

    assert result["signal"] == "NO_TRADE", (
        f"Expected NO_TRADE (LONG wick too small), got {result['signal']}. "
        f"Reason: {result['reason']}"
    )
    assert "wick" in result["reason"].lower(), (
        f"Reason should mention wick, got: {result['reason']}"
    )
    print("[PASS] test_no_trade_when_long_wick_too_small")


# ---------------------------------------------------------------------------
# Synthetic DataFrame factory — BB Bounce
# ---------------------------------------------------------------------------

# BB LONG: price dips below lower BB, closes back inside, RSI oversold, bullish body
# We need Strategy 1 (EMA rejection) to NOT fire, so set EMA20 ≈ EMA50 (no trend).
#
#   bb_lower = 69000, low = 68900 (pierces below), close = 69200 (back inside)
#   open = 69050, close = 69200 → bullish body
#   RSI = 30 < 35 (oversold)
#   EMA20 = 69500, EMA50 = 69500 → no trend → Strategy 1 won't fire
#
#   swing_low5 = 68900  → SL = 68900 - 69.2 = 68830.8
#   sl_dist = 69200 - 68830.8 = 369.2 > 207.6 (0.3%) → gate PASS
#   TP = bb_mid = 70000
#   R:R = 800 / 369.2 ≈ 2.17 ≥ 1.5 → gate PASS

BB_LONG_ROW = {
    "open":     69050.0,
    "high":     69300.0,
    "low":      68900.0,
    "close":    69200.0,
    "volume":   1000.0,
    "ema_20":   69500.0,
    "ema_50":   69500.0,   # no trend — Strategy 1 won't fire
    "rsi_14":   30.0,      # oversold
    "bb_upper": 71000.0,
    "bb_mid":   70000.0,
    "bb_lower": 69000.0,
    "adx_14":   30.0,
}

# BB SHORT: price spikes above upper BB, closes back inside, RSI overbought, bearish body
#
#   bb_upper = 71000, high = 71200 (pierces above), close = 70800 (back inside)
#   open = 71100, close = 70800 → bearish body
#   RSI = 70 > 65 (overbought)
#
#   swing_high5 = 71200  → SL = 71200 + 70.8 = 71270.8
#   sl_dist = 71270.8 - 70800 = 470.8 > 212.4 (0.3%) → gate PASS
#   TP = bb_mid = 70000
#   R:R = 800 / 470.8 ≈ 1.70 ≥ 1.5 → gate PASS

BB_SHORT_ROW = {
    "open":     71100.0,
    "high":     71200.0,
    "low":      70700.0,
    "close":    70800.0,
    "volume":   1000.0,
    "ema_20":   70500.0,
    "ema_50":   70500.0,   # no trend
    "rsi_14":   70.0,      # overbought
    "bb_upper": 71000.0,
    "bb_mid":   70000.0,
    "bb_lower": 69000.0,
    "adx_14":   30.0,
}


def _df_bb(base_row: dict, n: int = 60, **last_overrides) -> pd.DataFrame:
    """Build a synthetic DataFrame for BB bounce tests."""
    rows = [{**base_row} for _ in range(n)]
    rows[-1].update(last_overrides)
    idx = pd.date_range("2026-01-01", periods=n, freq="15min")
    return pd.DataFrame(rows, index=idx)


# ---------------------------------------------------------------------------
# Test 13 — BB LONG fires when all conditions met
# ---------------------------------------------------------------------------

def test_bb_long_fires():
    with _silent():
        result = get_signal(_df_bb(BB_LONG_ROW))

    assert result["signal"] == "LONG", (
        f"Expected BB LONG, got {result['signal']}. Reason: {result['reason']}"
    )
    assert "BB LONG" in result["reason"], (
        f"Reason should mention BB LONG, got: {result['reason']}"
    )
    assert result["rr"] >= MIN_RR
    print("[PASS] test_bb_long_fires")


# ---------------------------------------------------------------------------
# Test 14 — BB SHORT fires when all conditions met
# ---------------------------------------------------------------------------

def test_bb_short_fires():
    with _silent():
        result = get_signal(_df_bb(BB_SHORT_ROW))

    assert result["signal"] == "SHORT", (
        f"Expected BB SHORT, got {result['signal']}. Reason: {result['reason']}"
    )
    assert "BB SHORT" in result["reason"], (
        f"Reason should mention BB SHORT, got: {result['reason']}"
    )
    assert result["rr"] >= MIN_RR
    print("[PASS] test_bb_short_fires")


# ---------------------------------------------------------------------------
# Test 15 — BB LONG NO_TRADE when RSI not oversold
# ---------------------------------------------------------------------------

def test_bb_long_no_trade_rsi_not_oversold():
    with _silent():
        result = get_signal(_df_bb(BB_LONG_ROW, rsi_14=50.0))

    assert result["signal"] == "NO_TRADE", (
        f"Expected NO_TRADE (RSI not oversold), got {result['signal']}"
    )
    print("[PASS] test_bb_long_no_trade_rsi_not_oversold")


# ---------------------------------------------------------------------------
# Test 16 — BB SHORT NO_TRADE when RSI not overbought
# ---------------------------------------------------------------------------

def test_bb_short_no_trade_rsi_not_overbought():
    with _silent():
        result = get_signal(_df_bb(BB_SHORT_ROW, rsi_14=50.0))

    assert result["signal"] == "NO_TRADE", (
        f"Expected NO_TRADE (RSI not overbought), got {result['signal']}"
    )
    print("[PASS] test_bb_short_no_trade_rsi_not_overbought")


# ---------------------------------------------------------------------------
# Test 17 — Strategy 1 takes priority over Strategy 2
# ---------------------------------------------------------------------------

def test_strategy_1_priority_over_bb():
    """When EMA rejection (S1) fires, BB bounce (S2) should not be reached."""
    with _silent():
        result = get_signal(_df())  # default _df fires SHORT via Strategy 1

    assert result["signal"] == "SHORT", (
        f"Expected SHORT from S1, got {result['signal']}"
    )
    # Strategy 1 reason mentions EMA/rejection, not BB
    assert "BB" not in result["reason"], (
        f"S1 should fire before S2, but reason mentions BB: {result['reason']}"
    )
    print("[PASS] test_strategy_1_priority_over_bb")


# ---------------------------------------------------------------------------
# Synthetic DataFrame factory — EMA Crossover (S3)
# ---------------------------------------------------------------------------

# EMA Cross LONG: prev candle EMA20 <= EMA50, current EMA20 > EMA50, bullish body
# No S1 or S2 should fire: no rejection candle, no BB touch.
#
# Previous candle: EMA20=69500, EMA50=69500 (equal — no trend for S1)
# Current candle:  EMA20=69600, EMA50=69500 (just crossed above)
# Bullish body: open=69300, close=69600 (body=0.43%)
# Low stays above EMA20 → no S1 rejection
# BB bounds far away → no S2

EMA_CROSS_LONG_ROW = {
    "open":     69300.0,
    "high":     69700.0,
    "low":      69200.0,
    "close":    69600.0,
    "volume":   1000.0,
    "ema_20":   69600.0,
    "ema_50":   69500.0,    # EMA20 just crossed above
    "rsi_14":   55.0,       # neutral
    "bb_upper": 72000.0,
    "bb_mid":   70000.0,
    "bb_lower": 67000.0,    # far from price
    "adx_14":   30.0,       # above ADX_MIN=25
}

EMA_CROSS_LONG_PREV = {
    "open":     69200.0,
    "high":     73000.0,    # high enough for 20-bar TP (R:R needs this)
    "low":      69100.0,    # tight SL for good R:R
    "close":    69300.0,
    "volume":   1000.0,
    "ema_20":   69500.0,
    "ema_50":   69500.0,    # equal — not crossed yet
    "rsi_14":   50.0,
    "bb_upper": 72000.0,
    "bb_mid":   70000.0,
    "bb_lower": 67000.0,
    "adx_14":   30.0,
}


def _df_ema_cross(direction: str = "LONG", n: int = 60) -> pd.DataFrame:
    """Build DataFrame where EMA crossover fires."""
    if direction == "LONG":
        last_row = EMA_CROSS_LONG_ROW
        prev_row = EMA_CROSS_LONG_PREV
    else:
        # SHORT: mirror — EMA20 crosses below EMA50
        last_row = {
            "open": 69700.0, "high": 69800.0, "low": 69200.0, "close": 69300.0,
            "volume": 1000.0,
            "ema_20": 69400.0, "ema_50": 69500.0,  # EMA20 just crossed below
            "rsi_14": 45.0, "bb_upper": 72000.0, "bb_mid": 70000.0, "bb_lower": 67000.0,
            "adx_14": 30.0,
        }
        prev_row = {
            "open": 69600.0, "high": 69800.0, "low": 66000.0, "close": 69500.0,
            "volume": 1000.0,
            "ema_20": 69500.0, "ema_50": 69500.0,  # equal
            "rsi_14": 50.0, "bb_upper": 72000.0, "bb_mid": 70000.0, "bb_lower": 67000.0,
            "adx_14": 30.0,
        }
    rows = [{**prev_row} for _ in range(n)]
    rows[-1] = {**last_row}
    idx = pd.date_range("2026-01-01", periods=n, freq="15min")
    return pd.DataFrame(rows, index=idx)


# ---------------------------------------------------------------------------
# Test 18 — EMA Cross LONG fires
# ---------------------------------------------------------------------------

def test_ema_cross_long_fires():
    with _silent():
        result = get_signal(_df_ema_cross("LONG"))

    assert result["signal"] == "LONG", (
        f"Expected EMA Cross LONG, got {result['signal']}. Reason: {result['reason']}"
    )
    assert "EMA Cross" in result["reason"], (
        f"Reason should mention EMA Cross, got: {result['reason']}"
    )
    print("[PASS] test_ema_cross_long_fires")


# ---------------------------------------------------------------------------
# Test 19 — EMA Cross SHORT fires
# ---------------------------------------------------------------------------

def test_ema_cross_short_fires():
    with _silent():
        result = get_signal(_df_ema_cross("SHORT"))

    assert result["signal"] == "SHORT", (
        f"Expected EMA Cross SHORT, got {result['signal']}. Reason: {result['reason']}"
    )
    assert "EMA Cross" in result["reason"], (
        f"Reason should mention EMA Cross, got: {result['reason']}"
    )
    print("[PASS] test_ema_cross_short_fires")


# ---------------------------------------------------------------------------
# Synthetic DataFrame factory — RSI Reversal (S4)
# ---------------------------------------------------------------------------

# RSI Reversal LONG: prev RSI < 30, current RSI >= 30, bullish body
# No S1/S2/S3 should fire: no trend, no BB touch, no crossover

RSI_REV_LONG_ROW = {
    "open":     69100.0,
    "high":     69500.0,
    "low":      69000.0,
    "close":    69400.0,
    "volume":   1000.0,
    "ema_20":   69500.0,
    "ema_50":   69500.0,    # no trend
    "rsi_14":   32.0,       # just exited oversold (was < 30)
    "bb_upper": 72000.0,
    "bb_mid":   70500.0,    # TP target — above entry for LONG
    "bb_lower": 67000.0,    # far from price
    "adx_14":   30.0,
}

RSI_REV_LONG_PREV = {
    "open":     69200.0,
    "high":     69300.0,
    "low":      69000.0,    # tight SL for good R:R
    "close":    69100.0,
    "volume":   1000.0,
    "ema_20":   69500.0,
    "ema_50":   69500.0,
    "rsi_14":   28.0,       # was oversold
    "bb_upper": 72000.0,
    "bb_mid":   70500.0,    # TP = bb_mid = 70500 (above entry 69400)
    "bb_lower": 67000.0,
    "adx_14":   30.0,
}


def _df_rsi_rev(direction: str = "LONG", n: int = 60) -> pd.DataFrame:
    """Build DataFrame where RSI reversal fires."""
    if direction == "LONG":
        last_row = RSI_REV_LONG_ROW
        prev_row = RSI_REV_LONG_PREV
    else:
        # SHORT: prev RSI > 70, now <= 70, bearish body
        last_row = {
            "open": 69600.0, "high": 69700.0, "low": 69100.0, "close": 69200.0,
            "volume": 1000.0,
            "ema_20": 69500.0, "ema_50": 69500.0,
            "rsi_14": 68.0,  # just exited overbought
            "bb_upper": 72000.0, "bb_mid": 68000.0, "bb_lower": 67000.0,
            "adx_14": 30.0,
        }
        prev_row = {
            "open": 69500.0, "high": 69800.0, "low": 69400.0, "close": 69600.0,
            "volume": 1000.0,
            "ema_20": 69500.0, "ema_50": 69500.0,
            "rsi_14": 72.0,  # was overbought
            "bb_upper": 72000.0, "bb_mid": 68000.0, "bb_lower": 67000.0,
            "adx_14": 30.0,
        }
    rows = [{**prev_row} for _ in range(n)]
    rows[-1] = {**last_row}
    idx = pd.date_range("2026-01-01", periods=n, freq="15min")
    return pd.DataFrame(rows, index=idx)


# ---------------------------------------------------------------------------
# Test 20 — RSI Reversal LONG fires
# ---------------------------------------------------------------------------

def test_rsi_reversal_long_fires():
    with _silent():
        result = get_signal(_df_rsi_rev("LONG"))

    assert result["signal"] == "LONG", (
        f"Expected RSI Reversal LONG, got {result['signal']}. Reason: {result['reason']}"
    )
    assert "RSI Reversal" in result["reason"], (
        f"Reason should mention RSI Reversal, got: {result['reason']}"
    )
    print("[PASS] test_rsi_reversal_long_fires")


# ---------------------------------------------------------------------------
# Test 21 — RSI Reversal SHORT fires
# ---------------------------------------------------------------------------

def test_rsi_reversal_short_fires():
    with _silent():
        result = get_signal(_df_rsi_rev("SHORT"))

    assert result["signal"] == "SHORT", (
        f"Expected RSI Reversal SHORT, got {result['signal']}. Reason: {result['reason']}"
    )
    assert "RSI Reversal" in result["reason"], (
        f"Reason should mention RSI Reversal, got: {result['reason']}"
    )
    print("[PASS] test_rsi_reversal_short_fires")


# ---------------------------------------------------------------------------
# Bonus — helper unit tests (nearest_round_support/resistance)
# ---------------------------------------------------------------------------

def test_round_level_helpers():
    assert nearest_round_support(69600, 500)    == 69500.0
    assert nearest_round_support(69500, 500)    == 69500.0   # exact multiple
    assert nearest_round_support(69001, 500)    == 69000.0
    assert nearest_round_resistance(69600, 500) == 70000.0
    assert nearest_round_resistance(70000, 500) == 70000.0   # exact multiple
    assert nearest_round_resistance(70001, 500) == 70500.0
    print("[PASS] test_round_level_helpers")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_all():
    tests = [
        test_round_level_helpers,
        test_short_fires_on_all_conditions,
        test_no_trade_when_rr_below_minimum,
        test_no_trade_when_trend_too_young,
        test_no_trade_when_wick_too_small,
        test_sl_above_entry_for_short,
        test_tp_below_entry_for_short,
        test_long_fires_on_all_conditions,
        test_sl_below_entry_for_long,
        test_tp_above_entry_for_long,
        test_no_trade_when_long_trend_too_young,
        test_no_trade_when_long_wick_too_small,
        test_bb_long_fires,
        test_bb_short_fires,
        test_bb_long_no_trade_rsi_not_oversold,
        test_bb_short_no_trade_rsi_not_overbought,
        test_strategy_1_priority_over_bb,
        test_ema_cross_long_fires,
        test_ema_cross_short_fires,
        test_rsi_reversal_long_fires,
        test_rsi_reversal_short_fires,
    ]

    print(f"Running {len(tests)} signal tests...\n")
    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"[FAIL] {test.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"[ERROR] {test.__name__}: {type(e).__name__}: {e}")
            failed += 1

    print(f"\n{'-' * 50}")
    print(f"  Results: {passed} passed, {failed} failed out of {len(tests)} tests")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    run_all()
