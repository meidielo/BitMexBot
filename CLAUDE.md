# BitMEX Trading Bot — Learning Project

## Project Goal
Learn trading bot engineering. Build in phases. Testnet only until confidence is high.

## Current Phase
**Phase 6 COMPLETE — All core systems operational. Multi-strategy signal engine, risk validation, order execution, logging, monitoring, and web dashboard all running on testnet.**

## Strategies (priority order — first match wins)

| # | Strategy | Type | Fires When |
|---|----------|------|------------|
| S1 | EMA Rejection | Trend-following | Established trend + rejection candle + wick confirmation |
| S2 | BB Bounce | Mean-reversion | Price pierces Bollinger Band + RSI extreme + confirming body |
| S3 | EMA Crossover | Trend change | EMA20 crosses EMA50 + confirming candle body |
| S4 | RSI Reversal | Mean-reversion | RSI exits oversold/overbought zone + confirming body |

All strategies share the same SL (swing high/low), R:R gate (≥1.5), and risk validation.

## Risk Rules (hardcoded, confirmed working)
- Leverage: 15x fixed (verified after set_leverage)
- Position size: 2% of balance, max 0.10 BTC
- SL fires before liquidation: verified mathematically
- Max 1 open position at a time
- Daily loss limit: $50 then halt (gross losses, not net)
- Minimum free margin: 10% of account balance
- R:R minimum: 1.5:1

## Exchange
- Library: ccxt (Python)
- Mode: TESTNET ONLY — URL: https://testnet.bitmex.com
- Data source: mainnet (public OHLCV, no API key) + testnet (order execution)
- Instrument: XBTUSDT linear perpetual (1 contract = $1 USDT notional)
- Timeframe: 15m candles (resampled from 5m)

## Hard Rules (hardcoded, never overridden by any signal or AI)
- Max leverage: 15x
- SL must fire before liquidation price — verified mathematically before every order
- Minimum free margin: 10% of account balance at all times
- Max daily loss: $50 — bot halts for the day if hit
- Withdraw permission: never enabled on any API key
- All dates/times use UTC consistently

## Stack
- Python 3.12
- ccxt, pandas, pandas-ta
- SQLite for trade logging
- python-dotenv for credentials
- Flask for web dashboard
- All API keys loaded from .env — never hardcoded

## File Structure
```
bitmex_client.py     # ccxt connection handler (testnet + mainnet data client)
fetch_data.py        # OHLCV fetching — 5m candles from mainnet, resampled to 15m
indicators.py        # EMA20, EMA50, RSI14, Bollinger Bands (20,2)
signals.py           # 4 strategies: EMA rejection, BB bounce, EMA cross, RSI reversal
risk.py              # 6-rule risk filter — vetoes any unsafe signal
order_manager.py     # order placement with SL + TP, leverage verification
logger.py            # SQLite trade logging (TOCTOU-safe)
monitor.py           # daily summary
main.py              # orchestrator — 15m loop
dashboard.py         # Flask web dashboard (Tailscale-bound)
ml_filter.py         # RandomForest signal filter (optional)
backtest.py          # historical replay
param_sweep.py       # grid search over strategy parameters
test_risk.py         # 34 risk tests
test_signals.py      # 21 signal tests
```

## Running
```bash
# Bot
cd ~/BitMexBot && source venv/bin/activate && python main.py

# Tests
python -m pytest test_risk.py test_signals.py -v

# Backtest
python backtest.py

# Dashboard
python dashboard.py

# Parameter sweep
python param_sweep.py
```

## Services (systemd)
```bash
sudo systemctl status bitmexbot.service   # trading bot
sudo systemctl status bitmexdash.service  # web dashboard
```

## Phase Checklist
- [x] Phase 1: Connect to testnet, fetch candles, print to terminal
- [x] Phase 2: Compute indicators on candle data
- [x] Phase 3: Signal logic returning SHORT/LONG/NO_TRADE
- [x] Phase 4: Risk filter
- [x] Phase 5: Order execution on testnet
- [x] Phase 6: Logging + monitoring + dashboard
- [x] Phase 7: Multi-strategy engine (S1-S4)
- [ ] Phase 8: First live testnet trade + review

## Gate Checklist (run before any order logic)
1. Does SL price fire before liquidation price?
2. Is free margin > 10% after position open?
3. Are all API calls wrapped in try/except?
4. Are credentials in .env, not hardcoded?
5. Does this do exactly what I asked — nothing more?
6. Can I explain every line?
