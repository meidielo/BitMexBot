"""
Fold-3 Autopsy — H2 Vol Regime Losses (2025-01 → 2026-04)
============================================================
Pre-committed kill rule (READ BEFORE RUNNING):
  If no single filter captures >=6/9 losses without also removing
  >=3 winners from folds 1-2, H2 is dead. No further salvage attempts.

Hypotheses to test (pre-registered, do not add after seeing data):
  A. Vol re-spiked within 5 days of entry (chop hypothesis)
  B. 90-day ATR had expanded >50% vs its own 90-day mean (regime too wide)
  C. All losses clustered in a specific calendar sub-window
  D. Entry vol_z was below 0.5 (weak contraction, not clean reset)
  E. Loss happened within 3 days of entry (fast stop-out = wrong timing)
"""

import pandas as pd
import numpy as np
from pathlib import Path

FOLD3_START = "2025-01-02"
FOLD3_END   = "2026-04-03"

# ── Load trades and daily features ──────────────────────────────────────────
trades_path = Path("data/vol_regime_v2_H2_trades.csv")
daily_path  = Path("data/vol_regime_daily.csv")

if not trades_path.exists():
    trades_path = Path("data/vol_regime_trades.csv")

trades = pd.read_csv(trades_path, parse_dates=['entry_date', 'exit_date'])
daily  = pd.read_csv(daily_path, index_col=0, parse_dates=True)

# Ensure daily index is tz-aware UTC to match trades dates
if daily.index.tz is None:
    daily.index = daily.index.tz_localize('UTC')

# ── Pre-processing: join vol_z and atr onto trades (not a hypothesis change) ─
# The V2 backtest saved a minimal trades schema; daily CSV has the features.
if 'vol_z_entry' not in trades.columns:
    def _lookup(d, col):
        try:
            return daily.loc[d, col]
        except KeyError:
            return np.nan
    trades['vol_z_entry'] = trades['entry_date'].apply(lambda d: _lookup(d, 'vol_z'))
if 'atr' not in trades.columns:
    trades['atr'] = trades['entry_date'].apply(lambda d: _lookup(d, 'atr'))

# Isolate fold-3 window
fold3  = trades[(trades['entry_date'] >= FOLD3_START) &
                (trades['entry_date'] <= FOLD3_END)].copy()
f3_loss = fold3[fold3['outcome'] == 'LOSS'].copy()
f3_win  = fold3[fold3['outcome'] == 'WIN'].copy()

# Folds 1+2 winners (the ones we must NOT remove)
fold12_win = trades[(trades['entry_date'] < FOLD3_START) &
                    (trades['outcome'] == 'WIN')].copy()

print("=" * 60)
print("FOLD-3 AUTOPSY — Pre-committed kill rule applies")
print("=" * 60)
print(f"\nSource: {trades_path}")
print(f"Fold-3 total  : {len(fold3)} trades ({len(f3_loss)} L / {len(f3_win)} W)")
print(f"Folds 1+2 wins: {len(fold12_win)} (these must survive any filter)")

print("\n── RAW FOLD-3 LOSSES ──────────────────────────────────────")
cols = ['entry_date', 'exit_date', 'direction', 'entry_price',
        'exit_price', 'r_multiple', 'vol_z_entry', 'regime', 'atr']
print(f3_loss[cols].to_string(index=False))

# ── Hypothesis A: Vol re-spiked within 5 days of entry ──────────────────────
print("\n── HYPOTHESIS A: Vol re-spike within 5 days ───────────────")
def days_to_next_spike(entry_date, daily_df, window=5, threshold=2.0):
    try:
        future = daily_df.loc[entry_date:].iloc[1:window+1]
    except KeyError:
        return 0
    spikes = future[future['vol_z'] >= threshold]
    return len(spikes)

f3_loss['respike_5d'] = f3_loss['entry_date'].apply(
    lambda d: days_to_next_spike(d, daily))
fold12_win['respike_5d'] = fold12_win['entry_date'].apply(
    lambda d: days_to_next_spike(d, daily))

a_loss_caught  = (f3_loss['respike_5d'] > 0).sum()
a_wins_removed = (fold12_win['respike_5d'] > 0).sum()
print(f"  Losses with re-spike <=5d: {a_loss_caught}/{len(f3_loss)}")
print(f"  F1+2 wins removed        : {a_wins_removed}/{len(fold12_win)}")
print(f"  Filter useful?           : {a_loss_caught >= 6 and a_wins_removed < 3}")

# ── Hypothesis B: ATR expanded >50% above its own 90-day mean ───────────────
print("\n── HYPOTHESIS B: ATR regime expansion ─────────────────────")
daily['atr_mean90'] = daily['atr'].rolling(90).mean()
daily['atr_ratio']  = daily['atr'] / daily['atr_mean90']

def get_atr_ratio(entry_date, daily_df):
    try:
        return daily_df.loc[entry_date, 'atr_ratio']
    except KeyError:
        return np.nan

f3_loss['atr_ratio'] = f3_loss['entry_date'].apply(
    lambda d: get_atr_ratio(d, daily))
fold12_win['atr_ratio'] = fold12_win['entry_date'].apply(
    lambda d: get_atr_ratio(d, daily))

thresholds = [1.3, 1.5, 1.75, 2.0]
for t in thresholds:
    b_loss  = (f3_loss['atr_ratio'] > t).sum()
    b_wins  = (fold12_win['atr_ratio'] > t).sum()
    print(f"  ATR ratio > {t}: losses={b_loss}/{len(f3_loss)}, "
          f"wins removed={b_wins}/{len(fold12_win)} | "
          f"useful={b_loss >= 6 and b_wins < 3}")

# ── Hypothesis C: Calendar clustering ───────────────────────────────────────
print("\n── HYPOTHESIS C: Calendar clustering ──────────────────────")
f3_loss['month'] = f3_loss['entry_date'].dt.to_period('M')
print("  Loss distribution by month:")
print(f3_loss.groupby('month')['r_multiple'].agg(['count', 'sum']).to_string())

# ── Hypothesis D: Weak contraction (vol_z_entry above threshold) ───────────
print("\n── HYPOTHESIS D: Weak contraction signal ──────────────────")
thresholds_z = [-0.25, -0.50, -0.75, -1.0]
for z in thresholds_z:
    d_loss = (f3_loss['vol_z_entry'] > z).sum()
    d_wins = (fold12_win['vol_z_entry'] > z).sum()
    # vol_z lookup for fold12_win (only computed if not already present)
    if 'vol_z_entry' not in fold12_win.columns or fold12_win['vol_z_entry'].isna().all():
        fold12_win['vol_z_entry'] = fold12_win['entry_date'].apply(
            lambda d: daily.loc[d, 'vol_z'] if d in daily.index else np.nan)
        d_wins = (fold12_win['vol_z_entry'] > z).sum()
    print(f"  vol_z > {z}: losses={d_loss}/{len(f3_loss)}, "
          f"wins removed={d_wins}/{len(fold12_win)} | "
          f"useful={d_loss >= 6 and d_wins < 3}")

# ── Hypothesis E: Fast stop-out (exit within 3 days) ───────────────────────
print("\n── HYPOTHESIS E: Fast stop-outs ────────────────────────────")
f3_loss['hold_days'] = (f3_loss['exit_date'] - f3_loss['entry_date']).dt.days
fold12_win['hold_days'] = (fold12_win['exit_date'] - fold12_win['entry_date']).dt.days

for d in [2, 3, 5]:
    e_loss = (f3_loss['hold_days'] <= d).sum()
    print(f"  Stopped out <={d}d: {e_loss}/{len(f3_loss)} losses")

print(f"\n  Fold-3 loss hold times: {sorted(f3_loss['hold_days'].tolist())}")
print(f"  F1+2 win hold times  : {sorted(fold12_win['hold_days'].tolist())}")

# ── Summary and verdict ──────────────────────────────────────────────────────
print("\n── VERDICT ─────────────────────────────────────────────────")
print("Kill rule: filter must catch >=6/9 losses AND remove <3 folds 1+2 wins")
print()
print("Review the numbers above. If no hypothesis clears both bars:")
print("  -> H2 is dead. Vol-regime family is CLOSED. Add to graveyard.")
print()
print("If one hypothesis clears both bars:")
print("  -> Note it as salvage candidate. Run ONE more backtest with")
print("    that single filter on full data. Accept that result final.")
print("  -> Do not combine multiple partial filters to manufacture an edge.")
