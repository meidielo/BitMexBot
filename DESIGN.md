# BitMEX Trading Bot — Learning Project

## Project Goal
Learn trading bot engineering. Build in phases. Testnet only until confidence is high.

## Current Phase
**Post-audit. One live strategy (V2 Funding Rate Mean-Reversion) structurally silent in 2025-2026 regime; bot still runs the loop but the funding gate hasn't fired since 2026-04-10. V4 Cascade Dip-Buy was the next strategy attempt — killed and deleted from the codebase 2026-04-22 after data-block at N=4 validation. Nine strategy families now killed in the graveyard.**

## Current Strategies

| Name | Type | Status |
|------|------|--------|
| V2 Funding Rate Mean-Reversion | `signals.py` | Live on main.py loop, fires ~0x/6mo in current regime (L30 — not broken, regime dead) |

Dead strategies (graveyard, see `tasks/lessons.md`): V1–V4 indicators/cascade variants, S1-S4 multi-strategy engine, pairs/stat-arb, cross-sectional momentum, funding settlement arb, vol regime (H1/H2/H2b/H3).

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
# Core trading loop
bitmex_client.py          # ccxt connection handler (testnet + mainnet data)
fetch_data.py             # OHLCV fetching — 5m candles, resampled to 15m
signals.py                # V2: Funding Rate Mean Reversion strategy
risk.py                   # 6-rule risk filter — vetoes any unsafe signal
order_manager.py          # order placement with SL + TP, leverage verification
logger.py                 # SQLite trade logging (TOCTOU-safe)
monitor.py                # daily summary
main.py                   # V2 orchestrator — 15m loop

# Telemetry + tracking
condition_logger.py       # per-condition pass/fail logging every loop
forward_tracker.py        # live trade vs backtest comparison tracking

# Historical data fetchers
bitmex_public_fetcher.py  # DEPRECATED — BitMEX dumps lack liquidation flags (L28)
binance_data_fetcher.py   # Binance OI + funding (free API)

# Backtesting
backtest.py               # V2 funding rate backtest
exec_simulator.py         # 1m micro-execution simulator

# Dashboard + utilities
dashboard.py              # Flask web dashboard (HTTP Basic Auth)
audit.py                  # trade log auditing
universe_builder.py       # survivorship-bias-free asset universe
weekly_report.sh          # weekly status report (cron, every Mon 09:00)

# Analysis scripts (one-shot, kept for reference)
v2_signal_audit.py        # V2 funding regime audit (L30)
vol_regime_backtest.py    # Vol regime V1 backtest (DEAD — L29)
vol_regime_v2_directional.py  # Vol regime V2 directional (DEAD — L29)
fold3_autopsy.py          # Vol regime fold-3 kill rule autopsy (L29)

# Dead studies (graveyard, kept for historical reference)
cointegration_study.py    # Pairs/stat-arb study (DEAD — L20-L21)
funding_study.py          # Funding settlement study (DEAD — L19)
momentum_backtest.py      # Cross-sectional momentum (DEAD — L24-L25)

# Tests (46 total)
test_risk.py              # 34 risk rule tests
test_signals.py           # 12 signal tests
```

## Data Sources
- **BitMEX mainnet** — public OHLCV (5m resampled to 15m) + funding rate history (free, no API key, cached to `data/xbtusd_raw_candles.csv` and `data/xbtusd_funding_history.csv`)
- **Coinalyze** — aggregated 15m OI + liquidations across 8 exchanges (paid, ~21-day retention on 15m endpoint, daily endpoint back to 2023-04)
- **BitMEX testnet** — order execution only (API key required, read+write, never withdraw)
- **BitMEX public dumps** — DOES NOT contain liquidation flags; `bitmex_public_fetcher.py` is deprecated (see L28)
- **Binance Futures API** — OI + funding history (geo-blocked from current region, `binance_data_fetcher.py` exists but returns 403)

## Running
```bash
# Main loop (V2)
cd ~/BitMexBot && source venv/bin/activate && python main.py

# Tests (46 total)
python -m pytest test_risk.py test_signals.py -v

# V2 funding rate backtest
python backtest.py

# Historical studies (mostly dead, kept for reference)
python vol_regime_backtest.py       # DEAD — see L29
python v2_signal_audit.py           # funding regime audit (L30)

# Dashboard (requires DASH_USER/DASH_PASS in .env)
python dashboard.py
```

## Services (systemd)
```bash
sudo systemctl status bitmexbot.service   # trading bot
sudo systemctl status bitmexdash.service  # web dashboard
```

## Phase Checklist
- [x] Phase 1: Connect to testnet, fetch candles, print to terminal
- [x] Phase 2: ~~Compute indicators on candle data~~ (removed in V2 rewrite — V2 has no separate indicators file)
- [x] Phase 3: Signal logic returning SHORT/LONG/NO_TRADE
- [x] Phase 4: Risk filter
- [x] Phase 5: Order execution on testnet
- [x] Phase 6: Logging + monitoring + dashboard
- [x] Phase 7: ~~Multi-strategy engine (S1-S4)~~ (killed in V2 rewrite — L26 graveyard)
- [x] Phase 8: V2 Funding Rate Mean-Reversion (live, currently regime-silent)
- [x] Phase 9: V4 Cascade Dip-Buy — killed at N=4 statistical validation, code deleted 2026-04-22 (graveyard L31)
- [x] Phase 10: 16-item audit remediation (all engineering complete, see `tasks/todo.md`)
- [ ] Phase 11: Next surviving strategy (TBD — funding exhaustion / other hypotheses)

## Audit Protocol
When asked to audit this codebase:
1. Read this file completely before touching anything
2. Run `python -m pytest test_risk.py test_signals.py -v` — all 46 must pass before and after any change
3. Check for ghost references: grep for any import or reference to files that don't exist
4. Read `tasks/lessons.md` before proposing any new strategy — do not propose strategies already in the graveyard
5. Pre-register parameters in a timestamped comment block before running any backtest
6. Never tune parameters after seeing results — document the run and accept it

## Kill Rules (non-negotiable)
- A filter that removes >50% of historical winners is a kill switch, not a filter
- N < 15 completed trades = hypothesis only, never an edge
- OOS walk-forward must be run on every strategy before any deployment decision
- If a regime condition makes signals impossible in current market, document it in `tasks/lessons.md` and kill the strategy — do not backtest to confirm what the data already shows
- Pre-commit a kill rule before any salvage attempt. If no hypothesis clears the bar, the family is CLOSED — no spawning next variants

## Backtest Protocol
Before running ANY backtest:
1. Write parameters as a comment block at top of script
2. Run `git hash-object <script>` or note the timestamp
3. Run the backtest ONCE
4. Accept the result — no parameter changes after seeing output
5. Write the lesson to `tasks/lessons.md` regardless of outcome

## Strategy Graveyard (do not re-test these)
| Family | Lesson | Why Dead |
|--------|--------|----------|
| V1-V3 indicators (EMA/RSI/BB) | L01, L26 | Lagging indicators on perpetual futures have no edge |
| V4 Cascade Dip-Buy (funding-extreme + liq spike + bull regime) | L31 | Data-blocked at N=4 — Coinalyze 15m retention too short to validate before regime shifts; code deleted 2026-04-22 |
| S1-S4 multi-strategy engine | L26 | Removed in V2 rewrite, ghost code cleaned 2026-04-09 |
| V2 Funding Rate Mean-Reversion | L19, L30 | Funding regime structurally died mid-2024 (0% of bars hit 0.05% in 2026) |
| V3 Triple Condition | L04, L05 | Logical paradox: mean-reversion trigger + trend-following filter |
| Funding Settlement Arb | L19 | Publicly scheduled timestamps fully arbitraged by HFT |
| Pairs / Stat-Arb | L20, L21 | Major crypto pairs correlated but not co-integrated |
| Cross-Sectional Momentum | L24, L25 | Noise not momentum; derivative universe too curated |
| Vol Regime (H1/H2/H2b/H3) | L29 | Vol contraction is non-directional; fold-3 autopsy clean kill |
| Funding Exhaustion (Path X) | L30 | Pre-killed by L30 — regime that makes it viable died mid-2024 |

## Data Reality
- **BitMEX public dumps** — no liquidation flag, DEPRECATED (`bitmex_public_fetcher.py`)
- **Binance Futures API** — geo-blocked from this region (HTTP 403)
- **Bybit API** — geo-blocked from this region (TLS handshake failure)
- **Coinalyze** — only goes back to 2023-04-07 (1096 days daily, ~29 days 15m)
- **Funding regime** — structurally died mid-2024. <0.1% of bars hit 0.05% threshold in 2025, 0% in 2026 YTD
- **OHLCV** — 6.3 years of 5m candles cached (658k bars, 2020-01-01 → 2026-04-05)
- **OKX API** — reachable, untapped

## Gate Checklist (run before any order logic)
1. Does SL price fire before liquidation price?
2. Is free margin > 10% after position open?
3. Are all API calls wrapped in try/except?
4. Are credentials in .env, not hardcoded?
5. Does this do exactly what I asked — nothing more?
6. Can I explain every line?
