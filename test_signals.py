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
