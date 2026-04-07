"""
signals.py — V2: Funding Rate Mean Reversion

Edge: When perpetual swap funding is extreme, the market is asymmetrically
positioned. High positive funding = over-leveraged longs paying shorts.
A small adverse move triggers cascading liquidations → violent mean reversion.

Strategy:
  SETUP  — Funding rate exceeds a threshold (structural leverage imbalance)
  TIME   — Candle falls within settlement proximity window (00/08/16 UTC)
  TRIGGER — 15m price action confirms the squeeze has started

Setup:
  funding_rate > +FUNDING_THRESHOLD  → SHORT setup (longs over-leveraged)
  funding_rate < -FUNDING_THRESHOLD  → LONG setup  (shorts over-leveraged)

Time filter:
  Trades only fire within SETTLEMENT_WINDOW_H hours before each 8h settlement.
  This is when over-leveraged traders close to avoid paying, and when
  liquidation cascades cluster. Outside this window, the structural mechanic
  that drives mean reversion is dormant.

Trigger (must occur while setup + time filter are active):
  SHORT trigger: 15m candle closes below previous candle's low + volume spike
  LONG trigger:  15m candle closes above previous candle's high + volume spike

Exit:
  Fixed R:R target (aggressive — mean reversion is violent and fast).
  SL placed at recent swing high/low.

This module has NO lagging indicators. It trades a structural market mechanic.
"""

import pandas as pd

# ---------------------------------------------------------------------------
# Strategy constants
# ---------------------------------------------------------------------------

# Funding threshold — 0.05% per 8h = 0.15%/day annualized ~55%.
# This is extreme. Normal funding oscillates around 0.01%.
FUNDING_THRESHOLD   = 0.0005   # 0.05% — setup activates above this

# Cumulative 24h funding threshold (sum of last 3 × 8h rates).
# Persistent extreme funding is a stronger signal than a single spike.
FUNDING_24H_THRESH  = 0.001    # 0.10% cumulative over 24h

# Volume spike: current candle volume must be >= this multiple of
# the rolling 20-candle average volume. Confirms participation.
VOLUME_SPIKE_MULT   = 1.5

# Price breakdown: candle must close beyond prev candle's range.
# No additional buffer — the close itself is the confirmation.

# Risk parameters
SL_LOOKBACK         = 5        # candles back for swing high/low SL
SL_BUFFER_PCT       = 0.001    # 0.1% buffer beyond swing point
SL_MIN_DIST_PCT     = 0.0015   # 0.15% minimum SL distance (noise filter)
TARGET_RR           = 1.5      # fixed reward:risk ratio for TP
MIN_RR              = 1.0      # minimum acceptable R:R

# Settlement time proximity filter
# BitMEX funding settles at 00:00, 08:00, 16:00 UTC.
# Over-leveraged traders close BEFORE settlement to avoid paying.
# Liquidation cascades cluster around settlement.
# Only trade within this window before each settlement.
SETTLEMENT_HOURS    = [0, 8, 16]       # UTC hours when funding settles
SETTLEMENT_WINDOW_H = 3                # hours before settlement to allow trades
USE_SETTLEMENT_FILTER = True           # structural rule — do not toggle based on small samples

# Volume lookback for the rolling average
VOLUME_LOOKBACK     = 20


# ---------------------------------------------------------------------------
# Settlement proximity check
# ---------------------------------------------------------------------------

def _in_settlement_window(timestamp: pd.Timestamp) -> bool:
    """
    Returns True if the candle timestamp falls within SETTLEMENT_WINDOW_H
    hours before any funding settlement time (00:00, 08:00, 16:00 UTC).

    Example with SETTLEMENT_WINDOW_H=3:
      08:00 settlement → window is 05:00–08:00
      16:00 settlement → window is 13:00–16:00
      00:00 settlement → window is 21:00–00:00
    """
    hour = timestamp.hour
    minute = timestamp.minute
    candle_minutes = hour * 60 + minute

    for settle_h in SETTLEMENT_HOURS:
        settle_minutes = settle_h * 60
        window_start = (settle_minutes - SETTLEMENT_WINDOW_H * 60) % (24 * 60)

        if window_start < settle_minutes:
            # Normal case: window doesn't wrap midnight
            if window_start <= candle_minutes < settle_minutes:
                return True
        else:
            # Wraps midnight (e.g., 21:00–00:00)
            if candle_minutes >= window_start or candle_minutes < settle_minutes:
                return True

    return False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_signal(df: pd.DataFrame, current_funding: dict = None) -> dict:
    """
    Evaluate funding rate mean reversion signal.

    Parameters
    ----------
    df : DataFrame
        15m OHLCV with columns: open, high, low, close, volume.
        If 'funding_rate' and 'funding_24h' columns exist (from merge_funding),
        they are used directly. Otherwise, current_funding dict is used.

    current_funding : dict, optional
        Live funding data: {"rate": float, "funding_24h": float}.
        Used when df doesn't contain merged funding columns.

    Returns
    -------
    dict with keys: signal, reason, entry_price, sl_price, tp_price, rr,
                    funding_rate, funding_24h
    """
    no_trade = lambda reason: {
        "signal": "NO_TRADE", "reason": reason,
        "entry_price": 0, "sl_price": 0, "tp_price": 0,
        "rr": 0, "funding_rate": None, "funding_24h": None,
    }

    if len(df) < max(SL_LOOKBACK, VOLUME_LOOKBACK) + 2:
        return no_trade(f"Insufficient data: {len(df)} candles (need {VOLUME_LOOKBACK + 2})")

    # ------------------------------------------------------------------
    # Extract latest candle and previous candle
    # ------------------------------------------------------------------
    curr = df.iloc[-1]
    prev = df.iloc[-2]

    close   = float(curr["close"])
    opn     = float(curr["open"])
    high    = float(curr["high"])
    low     = float(curr["low"])
    vol     = float(curr["volume"])

    prev_high = float(prev["high"])
    prev_low  = float(prev["low"])

    # ------------------------------------------------------------------
    # Get funding rate
    # ------------------------------------------------------------------
    if "funding_rate" in df.columns:
        funding_rate = float(curr["funding_rate"]) if pd.notna(curr.get("funding_rate")) else None
        funding_24h  = float(curr.get("funding_24h", 0)) if pd.notna(curr.get("funding_24h")) else None
    elif current_funding:
        funding_rate = current_funding.get("rate")
        funding_24h  = current_funding.get("funding_24h")
    else:
        return no_trade("No funding data available")

    if funding_rate is None:
        return no_trade("Funding rate is None")

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------
    print(f"\n  -- Funding Rate Signal Diagnostics --")
    print(f"  Candle  O={opn:.2f}  H={high:.2f}  L={low:.2f}  C={close:.2f}  V={vol:.0f}")
    print(f"  Prev    H={prev_high:.2f}  L={prev_low:.2f}")
    print(f"  Funding rate:  {funding_rate:+.6f}  ({funding_rate*100:+.4f}%)")
    if funding_24h is not None:
        print(f"  Funding 24h:   {funding_24h:+.6f}  ({funding_24h*100:+.4f}%)")

    # ------------------------------------------------------------------
    # SETUP — Is funding extreme?
    # ------------------------------------------------------------------
    # Primary: single-rate threshold
    setup_short = funding_rate > FUNDING_THRESHOLD
    setup_long  = funding_rate < -FUNDING_THRESHOLD

    # Secondary: cumulative 24h threshold (stronger signal)
    if funding_24h is not None:
        cum_short = funding_24h > FUNDING_24H_THRESH
        cum_long  = funding_24h < -FUNDING_24H_THRESH
    else:
        cum_short = False
        cum_long  = False

    # Either threshold triggers the setup
    short_setup = setup_short or cum_short
    long_setup  = setup_long or cum_long

    setup_reasons = []
    if setup_short:
        setup_reasons.append(f"rate {funding_rate*100:+.4f}% > +{FUNDING_THRESHOLD*100:.2f}%")
    if cum_short:
        setup_reasons.append(f"24h {funding_24h*100:+.4f}% > +{FUNDING_24H_THRESH*100:.2f}%")
    if setup_long:
        setup_reasons.append(f"rate {funding_rate*100:+.4f}% < -{FUNDING_THRESHOLD*100:.2f}%")
    if cum_long:
        setup_reasons.append(f"24h {funding_24h*100:+.4f}% < -{FUNDING_24H_THRESH*100:.2f}%")

    print(f"\n  Setup conditions:")
    print(f"  [{'PASS' if short_setup else 'FAIL'}] SHORT setup (longs over-leveraged): "
          f"rate={funding_rate*100:+.4f}% threshold=±{FUNDING_THRESHOLD*100:.2f}%")
    print(f"  [{'PASS' if long_setup else 'FAIL'}] LONG setup  (shorts over-leveraged): "
          f"rate={funding_rate*100:+.4f}% threshold=±{FUNDING_THRESHOLD*100:.2f}%")

    if not short_setup and not long_setup:
        return no_trade(
            f"No funding extreme: rate={funding_rate*100:+.4f}% "
            f"(threshold=±{FUNDING_THRESHOLD*100:.2f}%)"
        )

    # ------------------------------------------------------------------
    # TIME FILTER — Settlement proximity (optional)
    # ------------------------------------------------------------------
    if USE_SETTLEMENT_FILTER:
        candle_ts = df.index[-1]
        in_window = _in_settlement_window(candle_ts)
        next_settle = None
        for sh in sorted(SETTLEMENT_HOURS):
            if candle_ts.hour < sh:
                next_settle = sh
                break
        if next_settle is None:
            next_settle = SETTLEMENT_HOURS[0]  # wraps to 00:00

        print(f"\n  Time filter:")
        print(f"  [{'PASS' if in_window else 'FAIL'}] Settlement window: "
              f"candle {candle_ts.strftime('%H:%M')} UTC, "
              f"next settlement {next_settle:02d}:00 UTC, "
              f"window={SETTLEMENT_WINDOW_H}h before")

        if not in_window:
            setup_dir = "SHORT" if short_setup else "LONG"
            return no_trade(
                f"{setup_dir} setup active ({', '.join(setup_reasons)}) "
                f"but outside settlement window: "
                f"{candle_ts.strftime('%H:%M')} UTC "
                f"(next settle {next_settle:02d}:00, window={SETTLEMENT_WINDOW_H}h)"
            )

    # ------------------------------------------------------------------
    # TRIGGER — Price action confirms the squeeze
    # ------------------------------------------------------------------
    # Volume spike check
    vol_series = df["volume"].iloc[-(VOLUME_LOOKBACK + 1):-1].astype(float)
    avg_vol    = vol_series.mean()
    vol_ratio  = vol / avg_vol if avg_vol > 0 else 0
    vol_ok     = vol_ratio >= VOLUME_SPIKE_MULT

    # Price breakdown
    bearish_break = close < prev_low     # closes below previous candle's low
    bullish_break = close > prev_high    # closes above previous candle's high

    # Body confirmation (not a doji)
    body_pct = abs(close - opn) / close if close > 0 else 0
    has_body = body_pct > 0.0005  # > 0.05% body

    print(f"\n  Trigger conditions:")
    print(f"  [{'PASS' if vol_ok else 'FAIL'}] Volume spike: "
          f"{vol:.0f} / avg {avg_vol:.0f} = {vol_ratio:.2f}x (min {VOLUME_SPIKE_MULT}x)")
    print(f"  [{'PASS' if bearish_break else 'FAIL'}] Bearish break: "
          f"close {close:.2f} < prev_low {prev_low:.2f}")
    print(f"  [{'PASS' if bullish_break else 'FAIL'}] Bullish break: "
          f"close {close:.2f} > prev_high {prev_high:.2f}")
    print(f"  [{'PASS' if has_body else 'FAIL'}] Body: {body_pct*100:.3f}% (min 0.05%)")

    # SHORT trigger: funding extreme positive + bearish breakdown
    short_trigger = short_setup and bearish_break and vol_ok and has_body
    # LONG trigger: funding extreme negative + bullish breakdown
    long_trigger  = long_setup and bullish_break and vol_ok and has_body

    if not short_trigger and not long_trigger:
        if short_setup:
            reason = (f"SHORT setup active ({', '.join(setup_reasons)}) "
                      f"but no trigger: break={'Y' if bearish_break else 'N'} "
                      f"vol={vol_ratio:.2f}x body={body_pct*100:.3f}%")
        else:
            reason = (f"LONG setup active ({', '.join(setup_reasons)}) "
                      f"but no trigger: break={'Y' if bullish_break else 'N'} "
                      f"vol={vol_ratio:.2f}x body={body_pct*100:.3f}%")
        return no_trade(reason)

    # ------------------------------------------------------------------
    # Determine direction
    # ------------------------------------------------------------------
    if short_trigger:
        direction = "SHORT"
    else:
        direction = "LONG"

    # ------------------------------------------------------------------
    # SL + TP calculation
    # ------------------------------------------------------------------
    entry = close
    lookback = df.iloc[-(SL_LOOKBACK + 1):-1]

    if direction == "SHORT":
        swing_high = float(lookback["high"].max())
        sl_price   = swing_high * (1 + SL_BUFFER_PCT)
        sl_dist    = sl_price - entry

        # Min distance check
        if sl_dist / entry < SL_MIN_DIST_PCT:
            return no_trade(
                f"SHORT SL too tight: {sl_dist:.2f} ({sl_dist/entry*100:.3f}%) "
                f"< min {SL_MIN_DIST_PCT*100:.2f}%"
            )

        tp_price = entry - (sl_dist * TARGET_RR)
        rr = (entry - tp_price) / sl_dist

    else:  # LONG
        swing_low = float(lookback["low"].min())
        sl_price  = swing_low * (1 - SL_BUFFER_PCT)
        sl_dist   = entry - sl_price

        if sl_dist / entry < SL_MIN_DIST_PCT:
            return no_trade(
                f"LONG SL too tight: {sl_dist:.2f} ({sl_dist/entry*100:.3f}%) "
                f"< min {SL_MIN_DIST_PCT*100:.2f}%"
            )

        tp_price = entry + (sl_dist * TARGET_RR)
        rr = (tp_price - entry) / sl_dist

    if rr < MIN_RR:
        return no_trade(f"R:R {rr:.2f} < minimum {MIN_RR}")

    reason = (f"Funding {direction}: {', '.join(setup_reasons)} | "
              f"Trigger: {'bearish' if direction == 'SHORT' else 'bullish'} break "
              f"+ vol {vol_ratio:.1f}x")

    print(f"\n  [{direction}] entry={entry:.2f}  SL={sl_price:.2f}"
          f"  TP={tp_price:.2f}  R:R={rr:.2f}")

    return {
        "signal":       direction,
        "reason":       reason,
        "entry_price":  entry,
        "sl_price":     sl_price,
        "tp_price":     tp_price,
        "rr":           rr,
        "funding_rate": funding_rate,
        "funding_24h":  funding_24h,
    }
