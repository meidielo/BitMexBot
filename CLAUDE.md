# BitMEX Trading Bot — Learning Project

## Project Goal
Learn trading bot engineering. Build in phases. Testnet only until Phase 6.

## Current Phase
**Phase 3 REWRITTEN — real strategy signal logic replacing placeholder EMA crossover**

## Risk Rules (hardcoded, confirmed working)
- Leverage: 15x fixed
- Position size: 2% of balance, max 0.10 BTC
- SL fires before liquidation: verified mathematically
- Max 1 open position at a time
- Daily loss limit: $50 then halt

## Exchange
- Library: ccxt (Python)
- Mode: TESTNET ONLY — URL: https://testnet.bitmex.com
- Instrument: XBTUSDT linear perpetual
- Timeframe: 15m candles

## Hard Rules (hardcoded, never overridden by any signal or AI)
- Max leverage: 15x
- SL must fire before liquidation price — verified mathematically before every order
- Minimum free margin: 10% of account balance at all times
- Max daily loss: $50 — bot halts for the day if hit
- No live trading until Phase 6 is complete and reviewed
- Withdraw permission: never enabled on any API key

## Stack
- Python 3.11
- ccxt, pandas, pandas-ta
- SQLite for trade logging
- python-dotenv for credentials
- All API keys loaded from .env — never hardcoded

## File Structure (building toward this)
bitmex_client.py     # ccxt connection handler
fetch_data.py        # OHLCV fetching
indicators.py        # EMA, RSI, Bollinger Bands
signals.py           # signal logic (SHORT / LONG / NO_TRADE)
risk.py              # risk filter — vetoes any unsafe signal
order_manager.py     # order placement with SL + TP
logger.py            # SQLite trade logging
monitor.py           # daily summary
main.py              # orchestrator

## Phase Checklist
- [ ] Phase 1: Connect to testnet, fetch candles, print to terminal
- [ ] Phase 2: Compute indicators on candle data
- [ ] Phase 3: Signal logic returning SHORT/LONG/NO_TRADE
- [ ] Phase 4: Risk filter
- [ ] Phase 5: Order execution on testnet
- [ ] Phase 6: Logging + monitoring

## Gate Checklist (run before any order logic)
1. Does SL price fire before liquidation price?
2. Is free margin > 10% after position open?
3. Are all API calls wrapped in try/except?
4. Are credentials in .env, not hardcoded?
5. Does this do exactly what I asked — nothing more?
6. Can I explain every line?
```