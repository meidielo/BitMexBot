# BitMEX Trading Bot — Learning Project

## Project Goal
Build and verify fail-closed trading infrastructure without treating engineering
quality or Testnet results as proof of a profitable strategy.

## Current Phase
**Research/Testnet engineering only. Production execution is absent. The V2
Funding Rate Mean-Reversion hypothesis is structurally silent in the recent
regime and has not cleared the evidence gate. The old trade database is
quarantined because its quantity units were mixed.**

## Current Strategies

| Name | Type | Status |
|------|------|--------|
| V2 Funding Rate Mean-Reversion | `signals.py` | Experimental Testnet runner only; fires about 0x/6mo in the documented recent regime and is not promotion-eligible |

Dead strategies (graveyard, see `tasks/lessons.md`): V1–V4 indicators/cascade variants, S1-S4 multi-strategy engine, pairs/stat-arb, cross-sectional momentum, funding settlement arb, vol regime (H1/H2/H2b/H3).

## Risk Rules

The Testnet engineering profile remains 15x, 2% stop-risk, 0.10 BTC maximum,
$50 daily gross-loss limit, and 10% minimum free margin. It is not a proposed
production profile.

The separately defined future canary ceiling is 1x, 0.1% stop-risk, 0.001 BTC,
$25 notional, $5 daily gross loss, and 50% minimum free margin. No production
client can consume this profile.

Both profiles also require a dedicated account with at most one XBTUSDT
position, trusted current USDT balance, fresh daily-loss state, verified
XBTUSDT metadata, valid signal geometry, and a stop on the safe side of the
conservative liquidation estimate: above it for LONG and below it for SHORT.
Sizing includes entry and stop slippage plus round-trip taker-fee buffers, and
current realized daily loss plus the proposed buffered loss must remain within
the daily cap.

## Exchange
- Library: ccxt (Python)
- Mode: authenticated Testnet only. `set_sandbox_mode(True)` is the first CCXT call.
  Before private operations, both CCXT API origins must exactly equal
  `https://testnet.bitmex.com`.
- Data source: an unauthenticated mainnet client for public OHLCV/funding plus
  an authenticated Testnet client for order execution
- Instrument: XBTUSDT linear perpetual. Runtime metadata currently proves
  `contractSize=0.000001 BTC`, a 100-contract lot, and a 0.1 USDT tick.
- Timeframe: completed 15m candles built from three completed 5m candles

## Hard Rules (never overridden by a signal or AI)
- Authenticated mainnet construction is absent.
- The authenticated client must attest the exact Testnet endpoints immediately
  before private operations.
- The dedicated account position query must return exactly one XBTUSDT record;
  unified contracts/side must agree with native `currentQty`, `isOpen`, symbol,
  and OneWay mode.
- Before a new entry, an account-wide position inventory and open-order query
  must show no unexpected exposure or resting orders.
- Unknown instrument, balance, position, leverage, order, or daily-loss state vetoes execution.
- Every decision comes from the latest fully completed candle and has one durable identity.
- Every completed 15-minute sequence is contiguous and each parent has exactly
  three aligned, completed 5-minute children.
- One thread/OS execution lock and one-database-unresolved-intent constraint
  prevent concurrent entry authority.
- Ambiguous order submission is reconciled and never blindly retried.
- Every post-submit response is treated as ambiguous until it is a valid order
  mapping with the expected client ID. A returned anomalous order is retained
  for exchange-order-ID cancellation and manual halt.
- Every visible nonterminal IOC is canceled and proven terminal, and protection
  uses only proven filled contracts.
- An invisible expected entry never suppresses account monitoring. Each poll
  inventories and cleans unexpected orders, checks all positions before and
  after cancellation errors, and closes unattributed XBTUSDT exposure.
- The stop and target are anchored to actual average fill price.
- The stop and target use one deterministic native OCO link, and both exchange
  responses must prove that link and `OneCancelsTheOther` contingency.
- Untrusted protective legs are canceled and proven terminal. Target cleanup
  must be followed by fresh stop and position attestation.
- Missing stop proof triggers an emergency reduce-only close attempt and manual halt.
- Restart checks the single unresolved v2 intent before a new signal. Unknown
  entry exposure is reconciled or conservatively closed; existing protection
  is re-verified. A protected intent found flat closes only after sibling
  terminal proof, account-wide flat/no-order proof, and stable native execution
  history exactly reconcile entry and exit fills, fees, funding, identifiers,
  sides, timestamps, account, combined evidence hash, and native `realisedPnl`
  against independently calculated net PnL. Any mismatch enters manual halt.
- Unresolved and managed execution states use a five-second REST watchdog
  cadence; fatal execution states stop the process immediately.
- An independent authenticated Testnet WebSocket watchdog observes private
  state but has no order, cancellation, dead-man switch, or mainnet capability.
- The dashboard receives only a one-way sanitized snapshot. It has no exchange
  credentials, ledger, logs, trading code, control route, or Docker socket.
- Legacy `data/trades.db` remains a normal filesystem file for compatibility,
  but application trade/exit writes are disabled and its rows are excluded
  from readiness evidence.
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
bitmex_client.py          # authenticated Testnet + unauthenticated public data
instrument.py             # verified XBTUSDT unit and fee model
fetch_data.py             # completed 5m candles, resampled to completed 15m
signals.py                # V2: Funding Rate Mean Reversion strategy
risk.py                   # unit-aware fail-closed risk filter
execution_lock.py         # thread and OS singleton execution authority
execution_safety.py       # idempotency, IOC price, order classification
order_manager.py          # bounded entry and fill-anchored protection
trade_ledger.py           # versioned state plus append-only execution evidence
execution_reconciler.py   # stable native entry/exit/fee/funding/PnL attribution
daily_loss_state.py       # revalidated atomic loss state from reconciled v2 rows
logger.py                 # legacy reads/PnL compatibility; legacy writes disabled
monitor.py                # legacy dashboard summary only
main.py                   # completed-candle Testnet orchestrator
promotion.py              # evidence stages; never enables production
runtime_status.py         # atomic sanitized runner heartbeat
exchange_watchdog.py      # independent read-only private WebSocket state
operator_snapshot.py      # one-way dashboard snapshot exporter
healthcheck.py            # bounded container freshness probes

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
dashboard.py              # authenticated Flask read-only web dashboard
audit.py                  # v2 ledger and promotion no-go audit
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

# Tests
test_*.py                 # unit, lifecycle, audit, and signal coverage
```

## Data Sources
- **BitMEX mainnet** — public OHLCV (5m resampled to 15m) + funding rate history (free, no API key, cached to `data/xbtusd_raw_candles.csv` and `data/xbtusd_funding_history.csv`)
- **Coinalyze** — aggregated 15m OI + liquidations across 8 exchanges (paid, ~21-day retention on 15m endpoint, daily endpoint back to 2023-04)
- **BitMEX testnet** — order execution only (API key required, read+write, never withdraw)
- **BitMEX public dumps** — DOES NOT contain liquidation flags; `bitmex_public_fetcher.py` is deprecated (see L28)
- **Binance Futures API** — OI + funding history (geo-blocked from current region, `binance_data_fetcher.py` exists but returns 403)

## Running

Use the virtual environment's interpreter so commands cannot silently run
under another installed Python version:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
Copy-Item .env.example .env
.\.venv\Scripts\python.exe trade_ledger.py --init

# Testnet runner
.\.venv\Scripts\python.exe main.py

# Required verification
.\.venv\Scripts\python.exe -m unittest discover -v
.\.venv\Scripts\python.exe test_signals.py
.\.venv\Scripts\python.exe audit.py
```

`requirements-dev.txt` includes runtime requirements and pins pytest. Pytest is
therefore available after the development install, but unittest discovery and
the two direct scripts above remain the required repository checks.

## Remote services

The remote Coolify host uses the hardened Compose stack. The dashboard stays on
remote loopback and is published privately through Tailscale Serve HTTPS.

```bash
docker compose -f compose.remote.yml ps
tailscale serve status
```

See `docs/REMOTE_DEPLOYMENT.md`. No service in this stack adds authenticated
mainnet order authority.

## Phase Checklist
- [x] Phase 1: Connect to testnet, fetch candles, print to terminal
- [x] Phase 2: ~~Compute indicators on candle data~~ (removed in V2 rewrite — V2 has no separate indicators file)
- [x] Phase 3: Signal logic returning SHORT/LONG/NO_TRADE
- [x] Phase 4: Risk filter
- [x] Phase 5: Order execution on testnet
- [x] Phase 6: Logging + monitoring + dashboard
- [x] Phase 7: ~~Multi-strategy engine (S1-S4)~~ (killed in V2 rewrite — L26 graveyard)
- [x] Phase 8: V2 Funding Rate Mean-Reversion (Testnet runner, currently regime-silent)
- [x] Phase 9: V4 Cascade Dip-Buy — killed at N=4 statistical validation, code deleted 2026-04-22 (graveyard L31)
- [x] Phase 10: Historical 16-item audit remediation completed; current
  real-funds blockers remain tracked in `docs/REAL_FUNDS_READINESS.md`
- [ ] Phase 11: Next surviving strategy (TBD — funding exhaustion / other hypotheses)

## Audit Protocol
When asked to audit this codebase:
1. Read this file completely before touching anything
2. Run `.\.venv\Scripts\python.exe -m unittest discover -v` and
   `.\.venv\Scripts\python.exe test_signals.py`
   before and after any execution or risk change
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
1. Was the literal Testnet flag required before construction, and are both
   live CCXT API origins attested as exact BitMEX Testnet endpoints?
2. Does live market metadata prove the exact active XBTUSDT contract model?
3. Is the decision based on the latest fully completed candle?
4. Is the v2 ledger free of unresolved or manual-halt intents?
5. Are USDT equity, free margin, open positions, leverage, and daily loss current?
6. Is the risk payload consistent with verified contract units and signal geometry?
7. Can an ambiguous submission be reconciled by deterministic client order ID?
8. Will only the actually filled quantity receive fill-anchored protection?
9. Are credentials in `.env`, with no withdrawal permission?
10. Does `audit.py` still prevent any unsupported real-funds claim?

Promotion evidence also fails closed: all chronological OOS folds must be
acceptable; shadow and mainnet dry run require `order_authority_disabled=true`;
Testnet requires the `required_scenarios_passed` and
`zero_unreconciled_incidents` booleans; and dry run requires its alert,
credential-rotation, and operator-response booleans. The evaluator checks the
fields and thresholds only. It does not verify the provenance of those claims,
and it can never set `production_enabled=true`.
