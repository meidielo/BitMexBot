# BitMexBot

A BitMEX testnet trading bot built as a learning project. Python 3.12, ccxt, pandas.

## What It Does

Runs a 15-minute loop: fetch candles from mainnet (public data) -> evaluate V2 Funding Rate Mean-Reversion signal -> validate risk -> execute orders on testnet -> log to SQLite + condition telemetry.

The live bot remains testnet-only. New strategy ideas should first go through read-only shadow tracking in `research_scanner.py`, which records watch-only candidate signals without importing the order manager, risk layer, API keys, or execution code.

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
# Edit .env with your BitMEX testnet API keys

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
| `dashboard.py` | Flask read-only web dashboard (port 5000) |
| `backtest.py` | V2 funding-rate backtest |
| `audit.py` | Trade-log audit + summary statistics |
| `research_scanner.py` | Read-only multi-symbol shadow scanner; writes watch-only candidate signals to `data/research_signals.db` |
| `live_readiness.py` | Read-only live-readiness and no-go gate evaluator; never enables live trading |
| `tools/dependency_audit_runner.py` | Controlled dependency audit runner; writes pip-audit, KEV, ledger, and diagnostics artifacts |
| `weekly_report.sh` | Weekly project status (cron, every Monday 09:00) |

## Research / Shadow Mode

Use testnet to validate plumbing and operational discipline, not to prove a trading edge. Candidate strategies are trained as shadow observations first:

```bash
python research_scanner.py --once
python research_scanner.py --scorecard-only
```

The scanner currently tracks BitMEX public-data candidates across BTC, ETH, SOL, and XRP USDT swaps:

- `btc_trend_breakout_vol_filter`: 20-bar breakout with EMA200, volatility, and volume filters.
- `vol_spike_reversion_proxy`: high-volume range shock proxy for possible mean reversion.
- `funding_extreme_watch`: lower-threshold funding watchlist for squeeze candidates.

Outputs are `WATCH_LONG`, `WATCH_SHORT`, or `NO_SIGNAL`. They are not trade orders, and the live testnet guards remain unchanged.

## Live Readiness / No-Go Gate

The project has a read-only readiness evaluator:

```bash
python live_readiness.py
```

It defaults to `NOT_READY` until hard evidence clears every gate: confirmed testnet environment, enough closed testnet trades, positive sample PnL, clean risk approvals, clean position-size audit, daily halt evidence, fresh shadow scanner data, and enough watch candidates. It also keeps no-go rules for deciding when a candidate should not be promoted, such as a negative 30+ trade sample or a long shadow period with almost no watch candidates.

This script does not switch the bot to live trading. It only reports whether the evidence is strong enough for manual review.

## Tests

```bash
python -m unittest test_logger test_risk test_research_scanner test_live_readiness test_dependency_audit_runner -v
python -m pytest test_signals.py -v
```

## Exchange Details

- **Library**: ccxt
- **Data**: Mainnet public OHLCV (no API key needed)
- **Execution**: Testnet only (`testnet.bitmex.com`)
- **Instrument**: XBTUSDT linear perpetual (1 contract = $1 USDT)
- **Timeframe**: 15-minute candles

## Status

Learning project. Running on testnet. Not financial advice.
