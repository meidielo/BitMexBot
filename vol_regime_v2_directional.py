"""
Vol Regime V2 — Directional Separation

Tests three competing hypotheses from the V1 autopsy:

  H1 (Direction A, user-specified):
      Long only on fear spikes (price ↓ during spike) in bull regime
      Short only on blow-off spikes (price ↑ during spike) in bear regime
      Thesis: align spike type with regime — trade with the prevailing trend

  H2 (Salvage finding, contrarian):
      Always LONG on vol contraction in bear regime (regardless of spike direction)
      Based on N=12 from V1 where bear-regime longs had implied WR 91.7%
      Thesis: bear-regime vol spikes are capitulation lows, not blow-offs

  H3 (Baseline V1 reference):
      Long in bull, short in bear (original logic, PF 0.95 full-sample)

All three run on the same 5m cached data with identical pre-registered
parameters, identical slippage, identical walk-forward windows. Only the
signal-to-direction mapping differs.

Pre-registered parameters (DO NOT TUNE):
  Same as vol_regime_backtest.py. The only new parameter is:
  SPIKE_DIR_LOOKBACK = VOL_LOOKBACK (20 days) — window for measuring
  price direction during the spike.
"""

import pandas as pd
import numpy as np
from pathlib import Path

# ── Config (frozen) ────────────────────────────────────────────────────────
VOL_LOOKBACK       = 20
REGIME_LOOKBACK    = 90
SPIKE_THRESHOLD    = 2.0
EMA_PERIOD         = 200
ATR_MULT_SL        = 1.5
ATR_MULT_TP        = 3.0
SLIPPAGE           = 0.001
MIN_TRADE_GAP      = 3
ANNUALIZE          = np.sqrt(288)
SPIKE_DIR_LOOKBACK = VOL_LOOKBACK  # same window as vol calc

DATA_PATHS = [
    Path("data/xbtusd_raw_candles.csv"),
    Path("data/raw_candles.csv"),
]


# ── Load + features (copied from v1 — no changes) ─────────────────────────
def load_5m_candles():
    for p in DATA_PATHS:
        if p.exists():
            print(f"[DATA] Loading {p}")
            df = pd.read_csv(p)
            df.columns = [c.lower().strip() for c in df.columns]
            ts_col = next((c for c in df.columns if 'time' in c or 'date' in c), None)
            if ts_col is None:
                raise ValueError(f"No timestamp column in {p}")
            if pd.api.types.is_numeric_dtype(df[ts_col]):
                df[ts_col] = pd.to_datetime(df[ts_col].astype('int64'),
                                            unit='ms', utc=True)
            else:
                df[ts_col] = pd.to_datetime(df[ts_col], utc=True, errors='coerce')
            df = df.rename(columns={ts_col: 'timestamp'})
            return df.set_index('timestamp').sort_index()
    raise FileNotFoundError("No cached 5m data found")


def build_daily_features(df5m):
    df5m = df5m.copy()
    df5m['log_ret'] = np.log(df5m['close'] / df5m['close'].shift(1))

    daily = df5m['close'].resample('1D').ohlc()
    daily.columns = ['open', 'high', 'low', 'close']
    daily['realized_vol'] = df5m['log_ret'].resample('1D').std() * ANNUALIZE
    bar_count = df5m['close'].resample('1D').count()
    daily = daily[bar_count >= 200].dropna(subset=['close', 'realized_vol'])

    daily['vol_mean'] = daily['realized_vol'].rolling(REGIME_LOOKBACK).mean()
    daily['vol_std']  = daily['realized_vol'].rolling(REGIME_LOOKBACK).std()
    daily['vol_z']    = (daily['realized_vol'] - daily['vol_mean']) / daily['vol_std']

    daily['ema200'] = daily['close'].ewm(span=EMA_PERIOD, adjust=False).mean()
    tr = pd.concat([
        daily['high'] - daily['low'],
        (daily['high'] - daily['close'].shift(1)).abs(),
        (daily['low']  - daily['close'].shift(1)).abs(),
    ], axis=1).max(axis=1)
    daily['atr'] = tr.rolling(14).mean()

    # NEW: price direction during the spike lookback window
    # Positive = price rose during spike (blow-off), negative = price fell (fear)
    daily['spike_direction'] = np.sign(
        daily['close'] - daily['close'].shift(SPIKE_DIR_LOOKBACK))

    return daily


# ── Signal detection (generalized) ─────────────────────────────────────────
def detect_signals_generic(daily: pd.DataFrame, direction_fn) -> pd.DataFrame:
    """
    Same state machine as V1 but the direction is determined by a function
    of (row, spike_direction_at_spike).

    direction_fn(row, spike_dir_at_spike) returns:
      +1 → LONG
      -1 → SHORT
       0 → SKIP (no trade)
    """
    daily = daily.copy()
    daily['signal'] = 0
    daily['spike_day'] = pd.Series(pd.NaT, index=daily.index,
                                   dtype='datetime64[ns, UTC]')
    daily['spike_dir_at_spike'] = 0.0

    state = 'IDLE'
    spike_date = None
    spike_dir = 0
    last_trade = None

    for i in range(len(daily)):
        row = daily.iloc[i]
        date = daily.index[i]

        if pd.isna(row['vol_z']) or pd.isna(row['ema200']) or pd.isna(row['atr']):
            continue

        in_cooldown = (last_trade is not None and
                       (date - last_trade).days < MIN_TRADE_GAP)

        if state == 'IDLE':
            if row['vol_z'] >= SPIKE_THRESHOLD and not in_cooldown:
                state = 'ARMED'
                spike_date = date
                # Capture direction during spike formation
                spike_dir = row['spike_direction'] if not pd.isna(
                    row['spike_direction']) else 0

        elif state == 'ARMED':
            if row['vol_z'] < 0:
                direction = direction_fn(row, spike_dir)
                if direction != 0:
                    daily.at[date, 'signal'] = direction
                    daily.at[date, 'spike_day'] = spike_date
                    daily.at[date, 'spike_dir_at_spike'] = spike_dir
                state = 'IDLE'
                last_trade = date
            elif row['vol_z'] >= SPIKE_THRESHOLD:
                spike_date = date
                spike_dir = row['spike_direction'] if not pd.isna(
                    row['spike_direction']) else 0

    return daily


# ── Direction functions for the three hypotheses ──────────────────────────
def direction_h1(row, spike_dir):
    """
    H1 (Direction A): Long fear-spikes in bull, short blow-offs in bear.
      Bull regime (close > EMA200):
        If spike was DOWN (fear):    LONG
        If spike was UP (blow-off):  SKIP (against-trend)
      Bear regime (close < EMA200):
        If spike was UP (blow-off):  SHORT
        If spike was DOWN (fear):    SKIP (against-trend)
    """
    bull = row['close'] > row['ema200']
    if bull and spike_dir < 0:
        return 1  # LONG
    if (not bull) and spike_dir > 0:
        return -1  # SHORT
    return 0  # SKIP


def direction_h2(row, spike_dir):
    """
    H2 (Salvage): Always LONG on vol contraction.
    Tests if "mean-reversion bounce" is direction-agnostic.
    """
    return 1  # always long


def direction_h2b(row, spike_dir):
    """
    H2b (Salvage refined): LONG always, but only in bear regime.
    Tests if the 91.7% bear-long signal was regime-specific.
    """
    bull = row['close'] > row['ema200']
    if not bull:
        return 1  # long in bear only
    return 0


def direction_h3(row, spike_dir):
    """H3 (V1 baseline): Long in bull, short in bear. No spike-direction filter."""
    return 1 if row['close'] > row['ema200'] else -1


# ── Backtest engine (copied from v1) ───────────────────────────────────────
def run_backtest(daily: pd.DataFrame):
    signals = daily[daily['signal'] != 0].copy()
    trades = []

    for entry_date, sig_row in signals.iterrows():
        direction = sig_row['signal']
        entry_price = sig_row['close'] * (1 + SLIPPAGE * direction)
        atr = sig_row['atr']
        sl_dist = atr * ATR_MULT_SL
        tp_dist = atr * ATR_MULT_TP
        sl_price = entry_price - direction * sl_dist
        tp_price = entry_price + direction * tp_dist

        future = daily.loc[entry_date:].iloc[1:]
        outcome = 'OPEN'
        exit_price = None
        exit_date = None

        for fwd_date, fwd_row in future.iterrows():
            hi, lo = fwd_row['high'], fwd_row['low']
            if direction == 1:
                if lo <= sl_price:
                    outcome = 'LOSS'
                    exit_price = sl_price * (1 - SLIPPAGE)
                    exit_date = fwd_date
                    break
                elif hi >= tp_price:
                    outcome = 'WIN'
                    exit_price = tp_price * (1 - SLIPPAGE)
                    exit_date = fwd_date
                    break
            else:
                if hi >= sl_price:
                    outcome = 'LOSS'
                    exit_price = sl_price * (1 + SLIPPAGE)
                    exit_date = fwd_date
                    break
                elif lo <= tp_price:
                    outcome = 'WIN'
                    exit_price = tp_price * (1 + SLIPPAGE)
                    exit_date = fwd_date
                    break

        if outcome == 'OPEN':
            continue

        r_multiple = direction * (exit_price - entry_price) / sl_dist
        trades.append({
            'entry_date': entry_date, 'exit_date': exit_date,
            'direction': 'LONG' if direction == 1 else 'SHORT',
            'regime': 'BULL' if sig_row['close'] > sig_row['ema200'] else 'BEAR',
            'spike_dir': sig_row.get('spike_dir_at_spike', 0),
            'entry_price': round(entry_price, 2),
            'exit_price': round(exit_price, 2),
            'outcome': outcome,
            'r_multiple': round(r_multiple, 4),
        })

    return pd.DataFrame(trades)


def summarize(trades_df: pd.DataFrame, label: str):
    if trades_df.empty:
        print(f"\n  [{label}] NO TRADES")
        return

    wins = trades_df[trades_df['outcome'] == 'WIN']
    losses = trades_df[trades_df['outcome'] == 'LOSS']
    total_r = trades_df['r_multiple'].sum()
    win_r = wins['r_multiple'].sum() if not wins.empty else 0
    loss_r = abs(losses['r_multiple'].sum()) if not losses.empty else 0
    pf = win_r / loss_r if loss_r > 0 else float('inf')

    cum = np.cumsum(trades_df['r_multiple'].values)
    max_dd = float((cum - np.maximum.accumulate(cum)).min()) if len(cum) > 0 else 0

    # Annualized
    span_days = (trades_df['exit_date'].max() - trades_df['entry_date'].min()).days
    ann_r = total_r / span_days * 365 if span_days > 0 else 0

    print(f"\n  [{label}]")
    print(f"    N Trades      : {len(trades_df)} ({len(wins)}W / {len(losses)}L)")
    print(f"    Win Rate      : {len(wins)/len(trades_df)*100:.1f}%")
    print(f"    Profit Factor : {pf:.2f}")
    print(f"    Total R       : {total_r:+.2f}R")
    print(f"    Max DD        : {max_dd:.2f}R")
    print(f"    Ann. R/year   : {ann_r:+.2f}")

    # Breakdown by regime
    for regime in ['BULL', 'BEAR']:
        sub = trades_df[trades_df['regime'] == regime]
        if len(sub) > 0:
            sub_wins = (sub['outcome'] == 'WIN').sum()
            print(f"    {regime:>5}: N={len(sub):>3} WR={sub_wins/len(sub)*100:5.1f}% "
                  f"R={sub['r_multiple'].sum():+.2f}")


# ── Walk-forward (generalized) ─────────────────────────────────────────────
def walk_forward_generic(daily, direction_fn, n_folds=4, label=""):
    n = len(daily)
    fold_sz = n // (n_folds + 1)
    results = []

    for fold in range(n_folds):
        train_end = (fold + 1) * fold_sz + fold_sz
        test_end = min(train_end + fold_sz, n)
        test_slice = daily.iloc[:test_end]

        sig_df = detect_signals_generic(test_slice.copy(), direction_fn)
        trades_df = run_backtest(sig_df)

        if trades_df.empty:
            results.append({'fold': fold+1, 'n': 0, 'wr': None, 'pf': None, 'r': None})
            continue

        oos_start = daily.index[train_end]
        oos_end = daily.index[test_end - 1]
        oos = trades_df[(trades_df['entry_date'] >= oos_start) &
                        (trades_df['entry_date'] <= oos_end)]

        if oos.empty:
            results.append({'fold': fold+1, 'n': 0, 'wr': None, 'pf': None, 'r': None})
            continue

        oos_wins = (oos['outcome'] == 'WIN').sum()
        oos_loss_r = abs(oos[oos['outcome'] == 'LOSS']['r_multiple'].sum())
        oos_win_r = oos[oos['outcome'] == 'WIN']['r_multiple'].sum()
        pf = oos_win_r / oos_loss_r if oos_loss_r > 0 else None

        results.append({
            'fold': fold+1,
            'n': len(oos),
            'wr': round(oos_wins/len(oos)*100, 1),
            'pf': round(pf, 2) if pf else None,
            'r': round(oos['r_multiple'].sum(), 2),
        })

    return pd.DataFrame(results)


# ── Main ────────────────────────────────────────────────────────────────────
def main():
    print("=" * 64)
    print("VOL REGIME V2 — Directional Separation")
    print("=" * 64)

    df5m = load_5m_candles()
    print(f"[DATA] {len(df5m):,} 5m bars | "
          f"{df5m.index[0].date()} → {df5m.index[-1].date()}")

    daily = build_daily_features(df5m)
    print(f"[FEAT] {len(daily)} daily bars")

    # Distribution of spike directions
    valid_spikes = daily[daily['vol_z'] >= SPIKE_THRESHOLD]
    fear_spikes = (valid_spikes['spike_direction'] < 0).sum()
    fomo_spikes = (valid_spikes['spike_direction'] > 0).sum()
    flat_spikes = (valid_spikes['spike_direction'] == 0).sum()
    print(f"\n[SPIKES] Total vol spikes: {len(valid_spikes)}")
    print(f"         Fear  (price down during spike): {fear_spikes}")
    print(f"         Blow-off (price up during spike): {fomo_spikes}")
    print(f"         Flat                           : {flat_spikes}")

    # By regime x spike_direction quadrant
    print(f"\n[QUADRANTS at spike time]")
    bull_spikes = valid_spikes[valid_spikes['close'] > valid_spikes['ema200']]
    bear_spikes = valid_spikes[valid_spikes['close'] < valid_spikes['ema200']]
    print(f"  Bull + fear   : {(bull_spikes['spike_direction'] < 0).sum()}")
    print(f"  Bull + blow-off: {(bull_spikes['spike_direction'] > 0).sum()}")
    print(f"  Bear + fear   : {(bear_spikes['spike_direction'] < 0).sum()}")
    print(f"  Bear + blow-off: {(bear_spikes['spike_direction'] > 0).sum()}")

    # Run each hypothesis
    hypotheses = [
        ("H1", "Direction A: bull+fear→LONG, bear+blow-off→SHORT", direction_h1),
        ("H2", "Salvage: always LONG on contraction", direction_h2),
        ("H2b", "Salvage refined: LONG only in bear regime", direction_h2b),
        ("H3", "V1 baseline: long bull / short bear", direction_h3),
    ]

    print(f"\n{'=' * 64}")
    print(f"  FULL-SAMPLE BACKTEST — all hypotheses")
    print(f"{'=' * 64}")

    all_results = {}
    for name, desc, fn in hypotheses:
        sig_df = detect_signals_generic(daily.copy(), fn)
        trades_df = run_backtest(sig_df)
        summarize(trades_df, f"{name}: {desc}")
        all_results[name] = trades_df

    # Walk-forward for H1 and H2b (the interesting ones)
    print(f"\n{'=' * 64}")
    print(f"  WALK-FORWARD (4 folds, OOS only)")
    print(f"{'=' * 64}")

    for name, desc, fn in hypotheses:
        wf = walk_forward_generic(daily, fn, n_folds=4, label=name)
        print(f"\n  [{name}] {desc}")
        print("  " + wf.to_string(index=False).replace('\n', '\n  '))
        total_n = wf['n'].sum()
        total_r = wf['r'].sum() if wf['r'].notna().any() else 0
        print(f"  Aggregate OOS: N={total_n}, R={total_r:+.2f}")

    # Save outputs
    for name, trades in all_results.items():
        if not trades.empty:
            trades.to_csv(f"data/vol_regime_v2_{name}_trades.csv", index=False)
    print(f"\n[SAVE] Per-hypothesis trade CSVs in data/vol_regime_v2_*.csv")


if __name__ == '__main__':
    main()
