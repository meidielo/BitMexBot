# BitMexBot

A BitMEX testnet trading bot built as a learning project. Python 3.12, ccxt, pandas, pandas-ta.

## What It Does

Runs a 15-minute loop: fetch candles from mainnet (public data) -> compute indicators -> evaluate 4 strategies -> validate risk -> execute orders on testnet -> log to SQLite.

## Strategies

| Priority | Strategy | Type | Entry Condition |
|----------|----------|------|-----------------|
| S1 | EMA Rejection | Trend-following | Established EMA20/50 trend + rejection candle with wick confirmation |
| S2 | BB Bounce | Mean-reversion | Price pierces Bollinger Band + RSI extreme + confirming candle body |
| S3 | EMA Crossover | Trend change | EMA20 crosses EMA50 + confirming candle body |
| S4 | RSI Reversal | Mean-reversion | RSI exits oversold (<30) or overbought (>70) + confirming body |

First strategy to match wins. All share the same SL/TP calculation and risk validation.

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
# Clone and setup
git clone https://github.com/meidielo/BitMexBot.git
cd BitMexBot
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure
cp .env.example .env
# Edit .env with your BitMEX testnet API keys

# Run
python main.py
```

## Architecture

```
main.py              15-minute loop orchestrator
  |
  +-- fetch_data.py        5m OHLCV from mainnet, resampled to 15m
  +-- indicators.py        EMA20, EMA50, RSI14, Bollinger Bands (20,2)
  +-- signals.py           4 strategies (S1 > S2 > S3 > S4 priority)
  +-- risk.py              6-rule risk filter (vetoes unsafe signals)
  +-- order_manager.py     Place orders with SL/TP on testnet
  +-- logger.py            SQLite trade logging
  +-- monitor.py           Daily summary
```

## Other Tools

| Script | Purpose |
|--------|---------|
| `dashboard.py` | Flask web dashboard (port 5000) |
| `backtest.py` | Replay strategies against historical data |
| `param_sweep.py` | Grid search over strategy parameters |
| `ml_filter.py` | Optional RandomForest signal filter |

## Tests

```bash
python -m pytest test_risk.py test_signals.py -v   # 55 tests
```

## Exchange Details

- **Library**: ccxt
- **Data**: Mainnet public OHLCV (no API key needed)
- **Execution**: Testnet only (`testnet.bitmex.com`)
- **Instrument**: XBTUSDT linear perpetual (1 contract = $1 USDT)
- **Timeframe**: 15-minute candles

## Status

Learning project. Running on testnet. Not financial advice.
