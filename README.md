# BitMexBot

A BitMEX testnet trading bot built as a learning project. Python 3.12, ccxt, pandas.

## What It Does

Runs a 15-minute loop: fetch candles from mainnet (public data) -> evaluate V2 Funding Rate Mean-Reversion signal -> validate risk -> execute orders on testnet -> log to SQLite + condition telemetry.

V4 Cascade Dip-Buy runs as a separate systemd service with adaptive 1-min/15-min polling.

## Current Strategy Status

| Strategy | File | Status |
|----------|------|--------|
| V2 Funding Rate Mean-Reversion | `signals.py` | Live on `main.py`, regime-silent since mid-2024 (L30) |
| V4 Cascade Dip-Buy | `v4_execution.py` | Separate systemd service, data-blocked at N=4 |

8 strategy families systematically tested and killed. See `tasks/lessons.md` L01-L30 and `CLAUDE.md` Strategy Graveyard for full history.

## Risk Controls

- 15x fixed leverage (verified after every `set_leverage` call)
- 2% of balance per trade, max 0.10 BTC
- Stop-loss on every trade, verified to fire before liquidation price
- Max 1 open position at a time
- $50 daily gross loss limit (bot halts for the day)
- Minimum 1.5:1 reward-to-risk ratio
- 10% minimum free margin after position open
- **Testnet only** — enforced in code

## Quick Start

```bash
cd BitMexBot
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure
cp .env.example .env
# Edit .env with your BitMEX testnet API keys + dashboard credentials

# Run
python main.py
```

## Architecture

```
main.py              V2 15-minute loop orchestrator
  |
  +-- fetch_data.py        5m OHLCV from mainnet, resampled to 15m
  +-- signals.py           V2: Funding Rate Mean Reversion (single strategy)
  +-- risk.py              6-rule risk filter (vetoes unsafe signals)
  +-- order_manager.py     Place orders with SL/TP on testnet
  +-- logger.py            SQLite trade logging
  +-- condition_logger.py  Per-condition telemetry every loop
  +-- monitor.py           Daily summary

v4_execution.py      V4 standalone service (separate systemd)
  +-- coinalyze_collector.py  15m OI + liquidation data collector
```

## Other Tools

| Script | Purpose |
|--------|---------|
| `dashboard.py` | Flask web dashboard (auth required, port 5000) |
| `backtest.py` | V2 funding rate backtest |
| `backtest_v3.py` | V4 daily backtest |
| `mainnet_monitor.py` | Read-only mainnet V4 condition monitor |

## Tests

```bash
python -m pytest test_risk.py test_signals.py test_v4_recovery.py -v   # 53 tests
```

## Exchange Details

- **Library**: ccxt
- **Data**: Mainnet public OHLCV (no API key needed)
- **Execution**: Testnet only (`testnet.bitmex.com`)
- **Instrument**: XBTUSDT linear perpetual (1 contract = $1 USDT)
- **Timeframe**: 15-minute candles

## Status

Learning project. Running on testnet. Not financial advice.
