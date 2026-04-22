# BitMexBot

A BitMEX testnet trading bot built as a learning project. Python 3.12, ccxt, pandas.

## What It Does

Runs a 15-minute loop: fetch candles from mainnet (public data) -> evaluate V2 Funding Rate Mean-Reversion signal -> validate risk -> execute orders on testnet -> log to SQLite + condition telemetry.

## Current Strategy Status

| Strategy | File | Status |
|----------|------|--------|
| V2 Funding Rate Mean-Reversion | `signals.py` | Live on `main.py`. Last entry 2026-04-10. Funding gate has been below threshold for 14+ days; designed to fire ~0× / 6mo in current regime (see `tasks/lessons.md` L30). |

Nine strategy families systematically tested and killed (V1–V4 indicator/cascade variants, S1–S4 multi-strategy, pairs/stat-arb, cross-sectional momentum, funding-settlement arb, vol regime). See `tasks/lessons.md` L01–L30 and the Strategy Graveyard in [`DESIGN.md`](DESIGN.md) for full history.

## Risk Controls

- 15x fixed leverage (verified after every `set_leverage` call)
- 2% of balance per trade, max 0.10 BTC
- Stop-loss on every trade, verified to fire before liquidation price
- Max 1 open position at a time
- $50 daily gross loss limit (bot halts for the day)
- Minimum 1.5:1 reward-to-risk ratio
- 10% minimum free margin after position open
- **Testnet only** — enforced in code

## What This Project Demonstrates

Engineering for autonomous trading infrastructure on a real exchange API — not a profitable strategy. The interesting parts are the loop, the risk gate, the telemetry, and the deployment.

- **Decision loop on a real exchange API** — 15-minute scheduling, fetch / evaluate / risk / execute pipeline, ccxt for BitMEX testnet
- **Risk layer that can veto execution** — six independent rules; every signal that reaches `order_manager.py` has been approved. SL is verified to fire before liquidation price *before* every order
- **Per-condition telemetry** — every condition checked is logged to SQLite (`condition_log.db`) so post-hoc analysis can show *why* the bot did or didn't trade. Useful for reasoning about silent regimes
- **Honest documentation of dead branches** — nine strategy families tested and killed; see `tasks/lessons.md` and the Strategy Graveyard in [`DESIGN.md`](DESIGN.md). That file also contains the project's Hard Rules, Audit Protocol, Data Reality notes, and Gate Checklist — the project design doc
- **Deployment hygiene** — systemd-managed bot + dashboard services, .env-only secrets, testnet enforced in code, no withdraw permission on any API key

### Trade history

20 autonomous entries placed late March – early April 2026, all risk-approved. 8 closed (5 wins / 3 losses, **+1.41 USDT net** on a ~$700 testnet balance — directional edge was positive over the sample but N is far too small for an inference); the remaining 12 were order-placement failures the system logged and recovered from without crashing. Strategy is correctly silent right now — funding rate has been 5–10× below the entry threshold for 14+ days.

| # | Entry time (UTC) | Side  | Entry  | Exit   | Size (contracts) | PnL (USDT) | Closed by |
|---|------------------|-------|--------|--------|------------------|------------|-----------|
| 1 | 2026-04-05 22:33 | LONG  | 67,386 | 67,640 | 188              | +0.7095    | MANUAL    |
| 2 | 2026-04-06 16:03 | SHORT | 69,867 | 69,890 | 193              | −0.0633    | MANUAL    |
| 3 | 2026-04-07 15:03 | LONG  | 67,828 | 67,915 | 188              | +0.2395    | MANUAL    |
| 4 | 2026-04-07 21:18 | SHORT | 69,777 | 69,835 | 193              | −0.1596    | MANUAL    |
| 5 | 2026-04-08 13:48 | LONG  | 71,867 | 71,942 | 198              | +0.2069    | MANUAL    |
| 6 | 2026-04-08 17:33 | LONG  | 71,571 | 71,727 | 197              | +0.4299    | MANUAL    |
| 7 | 2026-04-09 02:03 | SHORT | 70,878 | 70,893 | 196              | −0.0420    | MANUAL    |
| 8 | 2026-04-10 08:03 | LONG  | 71,463 | 71,496 | 196              | +0.0883    | MANUAL    |
|   |                  |       |        |        | **Total**        | **+1.4092**|           |

PnL is computed in `logger.compute_pnl_usdt` as `contracts × (exit − entry) / entry` for longs (sign inverted for shorts) — the correct formula for XBTUSDT linear perpetual where 1 contract = 1 USDT notional. "Closed by: MANUAL" means manually closed (the autonomous SL/TP didn't fire); the *entries* are all autonomous and risk-approved.

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
```

## Other Tools

| Script | Purpose |
|--------|---------|
| `dashboard.py` | Flask web dashboard (HTTP Basic Auth, port 5000) |
| `backtest.py` | V2 funding-rate backtest |
| `audit.py` | Trade-log audit + summary statistics |
| `weekly_report.sh` | Weekly project status (cron, every Monday 09:00) |

## Tests

```bash
python -m pytest test_risk.py test_signals.py -v   # 46 tests
```

## Exchange Details

- **Library**: ccxt
- **Data**: Mainnet public OHLCV (no API key needed)
- **Execution**: Testnet only (`testnet.bitmex.com`)
- **Instrument**: XBTUSDT linear perpetual (1 contract = $1 USDT)
- **Timeframe**: 15-minute candles

## Status

Learning project. Running on testnet. Not financial advice.
