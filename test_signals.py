"""
test_signals.py — V2: Funding Rate Mean Reversion tests

Run:  python -m pytest test_signals.py -v
"""

import pandas as pd
import pytest

import signals as signals_mod
from signals import (
    get_signal, _in_settlement_window,
    FUNDING_THRESHOLD, FUNDING_24H_THRESH,
    VOLUME_SPIKE_MULT, SL_MIN_DIST_PCT, TARGET_RR,
    SETTLEMENT_WINDOW_H,
)


# ---------------------------------------------------------------------------
# Test data factory
# ---------------------------------------------------------------------------

def _make_df(n=30, base_close=67000.0, base_vol=1000.0,
             last_overrides=None, prev_overrides=None,
             funding_rate=None, funding_24h=None):
    """
    Build a synthetic 15m DataFrame with optional funding columns.
    Last two candles are customizable for trigger testing.
    """
    rows = []
    for i in range(n):
        rows.append({
            "open":   base_close,
            "high":   base_close + 50,
            "low":    base_close - 50,
            "close":  base_close,
            "volume": base_vol,
        })

    # Apply prev overrides (second-to-last candle)
    if prev_overrides:
        rows[-2].update(prev_overrides)

    # Apply last overrides
    if last_overrides:
        rows[-1].update(last_overrides)

    idx = pd.date_range("2026-01-01", periods=n, freq="15min", tz="UTC")
    df = pd.DataFrame(rows, index=idx)

    # Add funding columns if requested
    if funding_rate is not None:
        df["funding_rate"] = funding_rate
    if funding_24h is not None:
        df["funding_24h"] = funding_24h

    return df


# ---------------------------------------------------------------------------
# Test 1: No funding data → NO_TRADE
# ---------------------------------------------------------------------------

def test_no_funding_data():
    df = _make_df()  # no funding columns
    result = get_signal(df)
    assert result["signal"] == "NO_TRADE"
    assert "No funding data" in result["reason"]


# ---------------------------------------------------------------------------
# Test 2: Normal funding → NO_TRADE (no extreme)
# ---------------------------------------------------------------------------

def test_normal_funding_no_trade():
    df = _make_df(funding_rate=0.0001, funding_24h=0.0003)
    result = get_signal(df)
    assert result["signal"] == "NO_TRADE"
    assert "No funding extreme" in result["reason"]


# ---------------------------------------------------------------------------
# Test 3: Extreme positive funding + bearish trigger → SHORT
# ---------------------------------------------------------------------------

def test_extreme_positive_funding_short():
    df = _make_df(
        funding_rate=0.001,     # 0.1% — way above threshold
        funding_24h=0.003,
        base_vol=100.0,         # low base volume
        prev_overrides={
            "open": 67100, "high": 67200, "low": 67000, "close": 67100,
        },
        last_overrides={
            "open": 67050, "high": 67060, "low": 66800, "close": 66900,
            "volume": 300.0,    # 3x spike vs base 100
        },
    )
    result = get_signal(df)
    assert result["signal"] == "SHORT", f"Expected SHORT, got {result['signal']}: {result['reason']}"
    assert result["sl_price"] > result["entry_price"]
    assert result["tp_price"] < result["entry_price"]
    assert result["rr"] >= 1.0


# ---------------------------------------------------------------------------
# Test 4: Extreme negative funding + bullish trigger → LONG
# ---------------------------------------------------------------------------

def test_extreme_negative_funding_long():
    df = _make_df(
        funding_rate=-0.001,    # -0.1%
        funding_24h=-0.003,
        base_vol=100.0,
        prev_overrides={
            "open": 66900, "high": 67000, "low": 66800, "close": 66900,
        },
        last_overrides={
            "open": 66950, "high": 67100, "low": 66940, "close": 67050,
            "volume": 300.0,
        },
    )
    result = get_signal(df)
    assert result["signal"] == "LONG", f"Expected LONG, got {result['signal']}: {result['reason']}"
    assert result["sl_price"] < result["entry_price"]
    assert result["tp_price"] > result["entry_price"]


# ---------------------------------------------------------------------------
# Test 5: Extreme funding but no volume spike → NO_TRADE
# ---------------------------------------------------------------------------

def test_extreme_funding_no_volume():
    df = _make_df(
        funding_rate=0.001,
        funding_24h=0.003,
        base_vol=1000.0,        # high base
        prev_overrides={
            "open": 67100, "high": 67200, "low": 67000, "close": 67100,
        },
        last_overrides={
            "open": 67050, "high": 67060, "low": 66800, "close": 66900,
            "volume": 1000.0,   # same as base → no spike
        },
    )
    result = get_signal(df)
    assert result["signal"] == "NO_TRADE"
    assert "trigger" in result["reason"].lower()


# ---------------------------------------------------------------------------
# Test 6: Extreme funding but no price break → NO_TRADE
# ---------------------------------------------------------------------------

def test_extreme_funding_no_break():
    df = _make_df(
        funding_rate=0.001,
        funding_24h=0.003,
        base_vol=100.0,
        prev_overrides={
            "open": 67000, "high": 67100, "low": 66900, "close": 67000,
        },
        last_overrides={
            "open": 66980, "high": 67000, "low": 66910, "close": 66950,
            "volume": 300.0,    # 3x spike but close > prev_low (66900)
        },
    )
    result = get_signal(df)
    assert result["signal"] == "NO_TRADE"


# ---------------------------------------------------------------------------
# Test 7: Cumulative 24h threshold fires even if single rate is below threshold
# ---------------------------------------------------------------------------

def test_cumulative_24h_threshold():
    df = _make_df(
        funding_rate=0.0004,    # below single threshold of 0.05%
        funding_24h=0.0015,     # above cumulative threshold of 0.10%
        base_vol=100.0,
        prev_overrides={
            "open": 67100, "high": 67200, "low": 67000, "close": 67100,
        },
        last_overrides={
            "open": 67050, "high": 67060, "low": 66800, "close": 66900,
            "volume": 300.0,
        },
    )
    result = get_signal(df)
    assert result["signal"] == "SHORT", f"Expected SHORT via 24h cumulative, got {result['signal']}"


# ---------------------------------------------------------------------------
# Test 8: SL too tight → NO_TRADE
# ---------------------------------------------------------------------------

def test_sl_too_tight():
    # SL_MIN_DIST_PCT is 0.15% of 67000 ≈ 100.5 points minimum.
    # Set swing_high to 67050 → SL = 67050*1.001 = 67117 → dist = 67117-67060 = 57 (0.085%) → too tight
    df = _make_df(
        funding_rate=0.001,
        funding_24h=0.003,
        base_close=67060.0,
        base_vol=100.0,
        prev_overrides={
            "open": 66985, "high": 66985, "low": 66980, "close": 66982,
        },
        last_overrides={
            "open": 67050, "high": 67050, "low": 66920, "close": 66960,
            "volume": 300.0,  # body = 0.13%, break below 66980
        },
    )
    # Set lookback highs to barely above entry so SL distance is tiny
    for i in range(len(df) - 2):
        df.iloc[i, df.columns.get_loc("high")] = 66970.0
        df.iloc[i, df.columns.get_loc("low")]  = 66960.0
        df.iloc[i, df.columns.get_loc("close")] = 66965.0
        df.iloc[i, df.columns.get_loc("open")]  = 66965.0

    result = get_signal(df)
    assert result["signal"] == "NO_TRADE", f"Got {result['signal']}: {result['reason']}"
    assert "SL too tight" in result["reason"]


# ---------------------------------------------------------------------------
# Test 9: Insufficient data → NO_TRADE
# ---------------------------------------------------------------------------

def test_insufficient_data():
    df = _make_df(n=5, funding_rate=0.001, funding_24h=0.003)
    result = get_signal(df)
    assert result["signal"] == "NO_TRADE"
    assert "Insufficient" in result["reason"]


# ---------------------------------------------------------------------------
# Test 10: Settlement window function — unit tests
# ---------------------------------------------------------------------------

def test_settlement_window_inside():
    """Candles within 3h before settlement should pass."""
    # 07:15 is 45 min before 08:00 settlement → inside
    ts = pd.Timestamp("2026-01-01 07:15:00", tz="UTC")
    assert _in_settlement_window(ts) is True

    # 05:00 is exactly 3h before 08:00 → inside (start of window)
    ts = pd.Timestamp("2026-01-01 05:00:00", tz="UTC")
    assert _in_settlement_window(ts) is True

    # 13:00 is 3h before 16:00 → inside
    ts = pd.Timestamp("2026-01-01 13:00:00", tz="UTC")
    assert _in_settlement_window(ts) is True

    # 21:30 is 2.5h before 00:00 settlement → inside (midnight wrap)
    ts = pd.Timestamp("2026-01-01 21:30:00", tz="UTC")
    assert _in_settlement_window(ts) is True

    # 23:45 is 15 min before 00:00 → inside
    ts = pd.Timestamp("2026-01-01 23:45:00", tz="UTC")
    assert _in_settlement_window(ts) is True


def test_settlement_window_outside():
    """Candles outside all settlement windows should fail."""
    # 09:00 is 1h AFTER 08:00 settlement, 7h before 16:00 → outside
    ts = pd.Timestamp("2026-01-01 09:00:00", tz="UTC")
    assert _in_settlement_window(ts) is False

    # 12:00 is 4h before 16:00 → outside (window is only 3h)
    ts = pd.Timestamp("2026-01-01 12:00:00", tz="UTC")
    assert _in_settlement_window(ts) is False

    # 01:00 is 1h after 00:00 settlement, 7h before 08:00 → outside
    ts = pd.Timestamp("2026-01-01 01:00:00", tz="UTC")
    assert _in_settlement_window(ts) is False

    # 17:00 is 1h after 16:00, 4h before 21:00 window start → outside
    ts = pd.Timestamp("2026-01-01 17:00:00", tz="UTC")
    assert _in_settlement_window(ts) is False


# ---------------------------------------------------------------------------
# Test 11: Extreme funding outside settlement window → NO_TRADE
# ---------------------------------------------------------------------------

def test_extreme_funding_outside_window():
    """Perfect setup + trigger but outside settlement window → NO_TRADE (when filter enabled)."""
    rows = []
    n = 30
    base_close = 67000.0
    for i in range(n):
        rows.append({
            "open":   base_close,
            "high":   base_close + 50,
            "low":    base_close - 50,
            "close":  base_close,
            "volume": 100.0,
        })
    rows[-2].update({
        "open": 67100, "high": 67200, "low": 67000, "close": 67100,
    })
    rows[-1].update({
        "open": 67050, "high": 67060, "low": 66800, "close": 66900,
        "volume": 300.0,
    })

    # Start at 02:45 UTC → last candle at 10:00 UTC (outside all windows)
    idx = pd.date_range("2026-01-01 02:45", periods=n, freq="15min", tz="UTC")
    df = pd.DataFrame(rows, index=idx)
    df["funding_rate"] = 0.001
    df["funding_24h"] = 0.003

    # Enable the filter for this test
    old_val = signals_mod.USE_SETTLEMENT_FILTER
    signals_mod.USE_SETTLEMENT_FILTER = True
    try:
        result = get_signal(df)
        assert result["signal"] == "NO_TRADE", f"Got {result['signal']}: {result['reason']}"
        assert "settlement window" in result["reason"].lower()
    finally:
        signals_mod.USE_SETTLEMENT_FILTER = old_val


# ---------------------------------------------------------------------------
# Run all
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
