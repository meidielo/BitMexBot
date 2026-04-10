"""
Volatility Regime Mean-Reversion Backtest
==========================================
Thesis: Realized vol spikes 2σ above 90-day mean → contraction phase entry
        EMA200 (daily) determines direction filter
        Entry: vol crosses back below 90-day mean after spike
        Exit: fixed R-multiple or time-based

Data: 5m OHLCV candles (cached locally, ~658k rows)
      Resampled to daily for vol computation
      Daily realized vol = std of 5m log returns within each day × sqrt(288)

Pre-registered parameters (DO NOT CHANGE AFTER LOADING DATA):
  VOL_LOOKBACK     = 20    days  (rolling vol window)
  REGIME_LOOKBACK  = 90    days  (mean/std of vol)
  SPIKE_THRESHOLD  = 2.0   σ above regime mean
  EMA_PERIOD       = 200   days
  RISK_R           = 1.0   (1R per trade, SL = ATR-based)
  ATR_MULT_SL      = 1.5   ATR for stop loss
  ATR_MULT_TP      = 3.0   ATR for take profit (3R target)
  SLIPPAGE         = 0.001 (0.1% per side, 0.2% round trip)
  MIN_TRADE_GAP    = 3     days cooldown after any trade

Author: BitMexBot project
Date: 2026-04-09
"""

import pandas as pd
import numpy as np
import sqlite3
import os
from pathlib import Path

# ── Config (pre-registered, do not tune post-hoc) ──────────────────────────
VOL_LOOKBACK     = 20
REGIME_LOOKBACK  = 90
SPIKE_THRESHOLD  = 2.0
EMA_PERIOD       = 200
ATR_MULT_SL      = 1.5
ATR_MULT_TP      = 3.0
SLIPPAGE         = 0.001
MIN_TRADE_GAP    = 3
ANNUALIZE        = np.sqrt(288)   # 288 5m bars per day

DATA_PATHS = [
    Path("data/xbtusd_raw_candles.csv"),   # XBTUSD inverse cache (ms epoch format)
    Path("data/raw_candles.csv"),          # XBTUSDT linear cache (ms epoch format)
    Path("data/btc_5m.csv"),
    Path("data/XBTUSD_5m.csv"),
    Path("data/btc_5m_candles.csv"),
]

# ── Load Data ───────────────────────────────────────────────────────────────

def load_5m_candles():
    for p in DATA_PATHS:
        if p.exists():
            print(f"[DATA] Loading {p}")
            df = pd.read_csv(p)
            df.columns = [c.lower().strip() for c in df.columns]

            # Find timestamp column — handle both ms-epoch int and datetime strings
            ts_col = next((c for c in df.columns if 'time' in c or 'date' in c), None)
            if ts_col is None:
                raise ValueError(f"No timestamp column found in {p}: {list(df.columns)}")

            # Detect format: if the column contains large integers, assume ms epoch
            if pd.api.types.is_numeric_dtype(df[ts_col]):
                df[ts_col] = pd.to_datetime(df[ts_col].astype('int64'), unit='ms', utc=True)
            else:
                df[ts_col] = pd.to_datetime(df[ts_col], utc=True, errors='coerce')

            df = df.rename(columns={ts_col: 'timestamp'})
            df = df.set_index('timestamp').sort_index()
            return df
    raise FileNotFoundError(f"No 5m candle file found. Tried: {DATA_PATHS}")


# ── Feature Engineering ─────────────────────────────────────────────────────

def build_daily_features(df5m: pd.DataFrame) -> pd.DataFrame:
    """
    From 5m bars, produce a daily feature frame:
      - open, high, low, close (OHLC from 5m resampled)
      - realized_vol: annualized std of 5m log returns within each day
      - atr: 14-day ATR on daily bars
      - ema200: EMA(200) on daily close
    """
    if 'close' not in df5m.columns:
        raise ValueError(f"Expected 'close' column. Got: {list(df5m.columns)}")

    # Log returns at 5m resolution
    df5m = df5m.copy()
    df5m['log_ret'] = np.log(df5m['close'] / df5m['close'].shift(1))

    # Resample to daily
    daily = df5m['close'].resample('1D').ohlc()
    daily.columns = ['open', 'high', 'low', 'close']
    daily['volume'] = df5m['volume'].resample('1D').sum() if 'volume' in df5m.columns else np.nan

    # Realized vol per day = std of intraday 5m log returns × sqrt(288)
    daily['realized_vol'] = df5m['log_ret'].resample('1D').std() * ANNUALIZE

    # Drop days with < 200 bars (incomplete days / weekends if any gaps)
    bar_count = df5m['close'].resample('1D').count()
    daily = daily[bar_count >= 200]
    daily = daily.dropna(subset=['close', 'realized_vol'])

    # Regime: rolling mean + std of realized vol
    daily['vol_mean'] = daily['realized_vol'].rolling(REGIME_LOOKBACK).mean()
    daily['vol_std']  = daily['realized_vol'].rolling(REGIME_LOOKBACK).std()
    daily['vol_z']    = (daily['realized_vol'] - daily['vol_mean']) / daily['vol_std']

    # Vol MA (smoothed for trend)
    daily['vol_ma20'] = daily['realized_vol'].rolling(VOL_LOOKBACK).mean()

    # EMA200 on daily close
    daily['ema200'] = daily['close'].ewm(span=EMA_PERIOD, adjust=False).mean()

    # ATR (14-day)
    tr = pd.concat([
        daily['high'] - daily['low'],
        (daily['high'] - daily['close'].shift(1)).abs(),
        (daily['low']  - daily['close'].shift(1)).abs(),
    ], axis=1).max(axis=1)
    daily['atr'] = tr.rolling(14).mean()

    return daily


# ── Signal Logic ─────────────────────────────────────────────────────────────

def detect_signals(daily: pd.DataFrame) -> pd.DataFrame:
    """
    State machine per day:
      IDLE      → ARMED when vol_z >= SPIKE_THRESHOLD (spike detected)
      ARMED     → TRIGGERED when vol_z drops back below 0 (contraction)
      TRIGGERED → IDLE after trade or cooldown

    Direction:
      LONG if close > ema200 (bull regime)
      SHORT if close < ema200 (bear regime)
    """
    daily = daily.copy()
    daily['signal']    = 0      # +1 long, -1 short
    # Tz-aware NaT column matching the index tz
    daily['spike_day'] = pd.Series(pd.NaT, index=daily.index, dtype='datetime64[ns, UTC]')

    state       = 'IDLE'
    spike_date  = None
    last_trade  = None

    for i in range(len(daily)):
        row  = daily.iloc[i]
        date = daily.index[i]

        if pd.isna(row['vol_z']) or pd.isna(row['ema200']) or pd.isna(row['atr']):
            continue

        # Cooldown check
        in_cooldown = (last_trade is not None and
                       (date - last_trade).days < MIN_TRADE_GAP)

        if state == 'IDLE':
            if row['vol_z'] >= SPIKE_THRESHOLD and not in_cooldown:
                state      = 'ARMED'
                spike_date = date

        elif state == 'ARMED':
            # Wait for vol to contract back below mean (z < 0)
            if row['vol_z'] < 0:
                direction = 1 if row['close'] > row['ema200'] else -1
                daily.at[date, 'signal']    = direction
                daily.at[date, 'spike_day'] = spike_date
                state      = 'IDLE'
                last_trade = date
            elif row['vol_z'] >= SPIKE_THRESHOLD:
                # Spike still extending — reset spike date to latest
                spike_date = date

    return daily


# ── Backtest Engine ───────────────────────────────────────────────────────────

def run_backtest(daily: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    signals = daily[daily['signal'] != 0].copy()
    trades  = []

    for i, (entry_date, sig_row) in enumerate(signals.iterrows()):
        direction    = sig_row['signal']
        entry_price  = sig_row['close'] * (1 + SLIPPAGE * direction)
        atr          = sig_row['atr']

        sl_dist = atr * ATR_MULT_SL
        tp_dist = atr * ATR_MULT_TP

        sl_price = entry_price - direction * sl_dist
        tp_price = entry_price + direction * tp_dist

        # Walk forward to find exit
        future = daily.loc[entry_date:].iloc[1:]
        outcome = 'OPEN'
        exit_price = None
        exit_date  = None

        for fwd_date, fwd_row in future.iterrows():
            hi, lo = fwd_row['high'], fwd_row['low']
            if direction == 1:   # long
                if lo <= sl_price:
                    outcome    = 'LOSS'
                    exit_price = sl_price * (1 - SLIPPAGE)
                    exit_date  = fwd_date
                    break
                elif hi >= tp_price:
                    outcome    = 'WIN'
                    exit_price = tp_price * (1 - SLIPPAGE)
                    exit_date  = fwd_date
                    break
            else:                # short
                if hi >= sl_price:
                    outcome    = 'LOSS'
                    exit_price = sl_price * (1 + SLIPPAGE)
                    exit_date  = fwd_date
                    break
                elif lo <= tp_price:
                    outcome    = 'WIN'
                    exit_price = tp_price * (1 + SLIPPAGE)
                    exit_date  = fwd_date
                    break

        if outcome == 'OPEN':
            continue  # incomplete trade at end of data, skip

        r_multiple = direction * (exit_price - entry_price) / sl_dist

        trades.append({
            'entry_date'  : entry_date,
            'exit_date'   : exit_date,
            'spike_day'   : sig_row['spike_day'],
            'direction'   : 'LONG' if direction == 1 else 'SHORT',
            'entry_price' : round(entry_price, 2),
            'exit_price'  : round(exit_price, 2),
            'sl_price'    : round(sl_price, 2),
            'tp_price'    : round(tp_price, 2),
            'atr'         : round(atr, 2),
            'vol_z_entry' : round(sig_row['vol_z'], 2),
            'ema200'      : round(sig_row['ema200'], 2),
            'regime'      : 'BULL' if sig_row['close'] > sig_row['ema200'] else 'BEAR',
            'outcome'     : outcome,
            'r_multiple'  : round(r_multiple, 4),
        })

    trades_df = pd.DataFrame(trades)

    if trades_df.empty:
        return trades_df, {'error': 'No completed trades'}

    wins   = trades_df[trades_df['outcome'] == 'WIN']
    losses = trades_df[trades_df['outcome'] == 'LOSS']

    total_r     = trades_df['r_multiple'].sum()
    win_r       = wins['r_multiple'].sum() if not wins.empty else 0
    loss_r      = losses['r_multiple'].sum() if not losses.empty else 0
    pf          = win_r / abs(loss_r) if loss_r != 0 else float('inf')
    win_rate    = len(wins) / len(trades_df)
    avg_r       = trades_df['r_multiple'].mean()
    max_dd_r    = _max_drawdown_r(trades_df['r_multiple'].values)

    # By regime breakdown
    bull_trades = trades_df[trades_df['regime'] == 'BULL']
    bear_trades = trades_df[trades_df['regime'] == 'BEAR']

    stats = {
        'n_trades'       : len(trades_df),
        'n_wins'         : len(wins),
        'n_losses'       : len(losses),
        'win_rate'       : round(win_rate, 4),
        'profit_factor'  : round(pf, 4),
        'total_r'        : round(total_r, 4),
        'avg_r_per_trade': round(avg_r, 4),
        'max_drawdown_r' : round(max_dd_r, 4),
        'bull_n'         : len(bull_trades),
        'bull_wr'        : round(len(bull_trades[bull_trades['outcome']=='WIN']) / len(bull_trades), 4) if len(bull_trades) > 0 else None,
        'bear_n'         : len(bear_trades),
        'bear_wr'        : round(len(bear_trades[bear_trades['outcome']=='WIN']) / len(bear_trades), 4) if len(bear_trades) > 0 else None,
        'annualized_r'   : _annualize_r(trades_df),
        'data_start'     : str(daily.index[0].date()),
        'data_end'       : str(daily.index[-1].date()),
        'params': {
            'vol_lookback'    : VOL_LOOKBACK,
            'regime_lookback' : REGIME_LOOKBACK,
            'spike_threshold' : SPIKE_THRESHOLD,
            'ema_period'      : EMA_PERIOD,
            'atr_mult_sl'     : ATR_MULT_SL,
            'atr_mult_tp'     : ATR_MULT_TP,
            'slippage'        : SLIPPAGE,
            'min_trade_gap'   : MIN_TRADE_GAP,
        }
    }

    return trades_df, stats


def _max_drawdown_r(r_series: np.ndarray) -> float:
    cumulative = np.cumsum(r_series)
    running_max = np.maximum.accumulate(cumulative)
    drawdown = cumulative - running_max
    return float(drawdown.min())


def _annualize_r(trades_df: pd.DataFrame) -> float:
    if trades_df.empty:
        return 0.0
    span_days = (trades_df['exit_date'].max() - trades_df['entry_date'].min()).days
    if span_days <= 0:
        return 0.0
    total_r = trades_df['r_multiple'].sum()
    return round(total_r / span_days * 365, 4)


# ── Walk-Forward Validation ───────────────────────────────────────────────────

def walk_forward(daily: pd.DataFrame, n_folds: int = 4) -> pd.DataFrame:
    """
    Expanding window walk-forward:
      Fold 1: train on 50% of data, test on next 12.5%
      Fold 2: train on 62.5%, test on next 12.5%
      etc.
    Parameters fixed (same as global config). No re-fitting.
    """
    n       = len(daily)
    fold_sz = n // (n_folds + 1)   # size of each OOS window
    results = []

    for fold in range(n_folds):
        train_end = (fold + 1) * fold_sz + fold_sz   # expanding
        test_end  = train_end + fold_sz

        test_end  = min(test_end, n)
        # Use full history up to train_end for feature computation (need warmup)
        test_slice = daily.iloc[:test_end]

        # Re-compute signals on full slice (features need warmup data)
        featured = test_slice.copy()
        sig_df   = detect_signals(featured)

        # Only evaluate trades that entered in the OOS window
        oos_start = daily.index[train_end]
        oos_end   = daily.index[test_end - 1]

        trades_df, stats = run_backtest(sig_df)

        if trades_df.empty:
            results.append({'fold': fold+1, 'oos_start': oos_start,
                            'oos_end': oos_end, 'n_trades': 0,
                            'win_rate': None, 'profit_factor': None, 'total_r': None})
            continue

        oos_trades = trades_df[
            (trades_df['entry_date'] >= oos_start) &
            (trades_df['entry_date'] <= oos_end)
        ]

        if oos_trades.empty:
            results.append({'fold': fold+1, 'oos_start': oos_start,
                            'oos_end': oos_end, 'n_trades': 0,
                            'win_rate': None, 'profit_factor': None, 'total_r': None})
            continue

        oos_wins = oos_trades[oos_trades['outcome'] == 'WIN']
        oos_loss = oos_trades[oos_trades['outcome'] == 'LOSS']
        pf = oos_wins['r_multiple'].sum() / abs(oos_loss['r_multiple'].sum()) \
             if not oos_loss.empty and oos_loss['r_multiple'].sum() != 0 else None

        results.append({
            'fold'          : fold + 1,
            'oos_start'     : oos_start.date(),
            'oos_end'       : oos_end.date(),
            'n_trades'      : len(oos_trades),
            'win_rate'      : round(len(oos_wins) / len(oos_trades), 4),
            'profit_factor' : round(pf, 4) if pf else None,
            'total_r'       : round(oos_trades['r_multiple'].sum(), 4),
        })

    return pd.DataFrame(results)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("VOL REGIME BACKTEST — Parameters pre-registered, running once")
    print("=" * 60)

    # 1. Load
    df5m  = load_5m_candles()
    print(f"[DATA] Loaded {len(df5m):,} 5m bars | {df5m.index[0]} → {df5m.index[-1]}")

    # 2. Build features
    daily = build_daily_features(df5m)
    print(f"[FEAT] Daily bars after resampling: {len(daily)} | "
          f"Vol range: {daily['realized_vol'].min():.3f} – {daily['realized_vol'].max():.3f}")

    # 3. Signal detection
    daily = detect_signals(daily)
    n_signals = (daily['signal'] != 0).sum()
    print(f"[SIGS] Total signals detected: {n_signals}")

    if n_signals == 0:
        print("\n[WARN] No signals detected. Check data quality / date range.")
        print("       Vol spikes > 2σ in data:")
        spikes = daily[daily['vol_z'] >= SPIKE_THRESHOLD]
        print(f"       {len(spikes)} spike days found")
        print(spikes[['realized_vol', 'vol_mean', 'vol_std', 'vol_z']].head(10))
        return

    # 4. Full backtest
    print("\n── FULL BACKTEST ──────────────────────────────────────────")
    trades_df, stats = run_backtest(daily)

    if 'error' in stats:
        print(f"[ERROR] {stats['error']}")
        return

    print(f"  Period         : {stats['data_start']} → {stats['data_end']}")
    print(f"  N Trades       : {stats['n_trades']}  ({stats['n_wins']}W / {stats['n_losses']}L)")
    print(f"  Win Rate       : {stats['win_rate']:.1%}")
    print(f"  Profit Factor  : {stats['profit_factor']:.2f}")
    print(f"  Total R        : {stats['total_r']:+.2f}R")
    print(f"  Avg R/Trade    : {stats['avg_r_per_trade']:+.4f}R")
    print(f"  Max Drawdown   : {stats['max_drawdown_r']:.2f}R")
    print(f"  Annualized R   : {stats['annualized_r']:+.2f}R/year")
    print(f"\n  BULL regime    : N={stats['bull_n']}, WR={stats['bull_wr']:.1%}" if stats['bull_n'] else "  BULL regime    : N=0")
    print(f"  BEAR regime    : N={stats['bear_n']}, WR={stats['bear_wr']:.1%}" if stats['bear_n'] else "  BEAR regime    : N=0")

    # 5. Walk-forward
    print("\n── WALK-FORWARD (4 folds, OOS only) ──────────────────────")
    wf = walk_forward(daily, n_folds=4)
    print(wf.to_string(index=False))
    agg_n = wf['n_trades'].sum()
    agg_r = wf['total_r'].sum()
    print(f"\n  Aggregate OOS: N={agg_n}, Total R={agg_r:+.2f}")

    # 6. Condition frequency analysis
    print("\n── CONDITION FREQUENCY ────────────────────────────────────")
    valid = daily.dropna(subset=['vol_z', 'ema200'])
    spike_days = valid[valid['vol_z'] >= SPIKE_THRESHOLD]
    bull_days  = valid[valid['close'] > valid['ema200']]
    print(f"  Total valid days      : {len(valid)}")
    print(f"  Spike days (vol≥{SPIKE_THRESHOLD}σ): {len(spike_days)} ({len(spike_days)/len(valid):.1%})")
    print(f"  Bull regime days      : {len(bull_days)} ({len(bull_days)/len(valid):.1%})")
    print(f"  Spike rate/year       : {len(spike_days) / (len(valid)/365):.1f}")
    print(f"  Signal rate/year      : {n_signals / (len(valid)/365):.1f}")

    # 7. Save outputs
    trades_df.to_csv("data/vol_regime_trades.csv", index=False)
    wf.to_csv("data/vol_regime_walk_forward.csv", index=False)
    daily[['realized_vol', 'vol_mean', 'vol_std', 'vol_z',
           'ema200', 'atr', 'signal']].to_csv("data/vol_regime_daily.csv")

    print("\n[SAVE] Outputs written:")
    print("       data/vol_regime_trades.csv")
    print("       data/vol_regime_walk_forward.csv")
    print("       data/vol_regime_daily.csv")

    # 8. Red flags
    print("\n── HONESTY CHECKS ─────────────────────────────────────────")
    if stats['n_trades'] < 15:
        print(f"  ! N={stats['n_trades']} — still below 15 threshold for statistical meaning")
    if stats['profit_factor'] > 3.0:
        print(f"  ! PF={stats['profit_factor']:.2f} — suspiciously high, check for look-ahead bias")
    if abs(stats['max_drawdown_r']) < 1.0:
        print(f"  ! Max DD={stats['max_drawdown_r']:.2f}R — suspiciously low, check SL logic")
    if agg_n < 5:
        print(f"  ! OOS N={agg_n} — walk-forward has insufficient OOS trades to be meaningful")
    if stats['n_trades'] >= 15 and stats['profit_factor'] > 1.2:
        print(f"  + N and PF both look promising — treat as hypothesis, not edge")


if __name__ == '__main__':
    main()
