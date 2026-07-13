# BitMexBot

BitMexBot is a Python 3.12 research and execution-safety project for the
BitMEX XBTUSDT perpetual. Authenticated order execution is hard-limited to
BitMEX Testnet.

> **Current verdict: DO NOT USE REAL FUNDS.** The repository now has a safer
> execution foundation, but it has no authenticated mainnet client and no
> evidence package that clears the promotion gates.

## What is implemented

- Completed-candle decisions only. A 15-minute signal is identified by its
  exact candle close and cannot be replayed on restart. Every accepted parent
  has exactly three aligned, completed 5-minute children, and the completed
  15-minute sequence must be contiguous.
- Causal live inputs. The runner excludes forming 5-minute/15-minute candles
  and uses only funding observations settled by the decision timestamp.
- Exact execution-endpoint attestation. Before private operations, the client
  rechecks `BITMEX_TESTNET=true`, exchange ID `bitmex`, and exact public and
  private API origins of `https://testnet.bitmex.com`. The separate mainnet
  data client is unauthenticated and has no order credentials.
- Runtime instrument verification. Startup rejects anything other than the
  active, linear, USDT-settled XBTUSDT swap and verifies contract size, lot
  size, tick size, fees, and margin metadata.
- Strict position verification. A private position query must return exactly
  one XBTUSDT record, and CCXT's contracts/side fields must agree with native
  BitMEX `currentQty`, `isOpen`, symbol, and OneWay mode. Before a new entry,
  an account-wide inventory must also show no other exposure or resting order.
  The operating boundary is a dedicated account with no manual trading.
- Correct XBTUSDT units. At the metadata observed on 2026-07-13, one contract
  represents `0.000001 BTC`, the order lot is 100 contracts, and the tick is
  `0.1 USDT`. The bot validates these values at runtime instead of trusting the
  documentation.
- Cost-buffered stop-distance sizing. Risk is expressed separately as
  contracts, BTC, notional USDT, raw stop loss, and expected loss including a
  10 bps entry-slippage buffer, 20 bps stop-slippage buffer, and round-trip
  taker fees. Current realized daily loss plus the proposed buffered loss may
  not exceed the daily cap. Missing state vetoes the order.
- Bounded entry execution. Entries are marketable IOC limit orders capped at
  10 basis points of top-of-book slippage.
- Idempotent order submission. Stable UUIDv5 client order IDs plus pre/post
  reconciliation prevent blind retries after network timeouts. Malformed
  create responses are reconciled; a returned order with a missing or wrong
  client ID is retained, canceled by exchange order ID, and manually halted.
- IOC terminal-state handling. Every visible nonterminal entry is canceled and
  proven terminal, including zero-fill and partial-fill responses. Only the
  proven filled quantity receives protective orders. If the expected client ID
  is invisible, every safety poll still inventories account-wide orders and
  positions; unattributed XBTUSDT exposure is closed and manually halted.
- Fill-anchored protection. Stop and target distances are recomputed from the
  actual average entry fill and rounded conservatively to the verified tick.
  Both orders share a deterministic native OCO link, and their returned
  `clOrdLinkID` and `OneCancelsTheOther` contingency are verified. An untrusted
  protective leg is canceled to a proven terminal state; after target cleanup,
  the stop and position are re-attested or the position is closed and halted.
- Durable lifecycle ledger. `data/trades_v2.db` records intent before
  submission and uses atomic state transitions. Unknown execution state stops
  automation and requires reconciliation.
- Single-intent authority. A non-blocking thread/OS file lock prevents two
  local runners from entering private execution together, while a SQLite
  uniqueness constraint allows only one unresolved durable intent.
- Restart reconciliation. Before evaluating a new signal, the runner repairs
  or re-verifies the one unresolved intent. It will not trade around unknown
  entry exposure or unproven protection. Unresolved and managed states are
  checked every five seconds rather than waiting for the next signal candle.
- Legacy evidence quarantine. `data/trades.db` mixed BTC and contract units,
  has application writes disabled, and is excluded from all readiness claims.
  The file itself is not made filesystem read-only.
- Fail-closed promotion gate. Automation can reach at most
  `CANARY_REVIEW`; it cannot enable production trading.

## What is not implemented

- No authenticated mainnet exchange constructor or live-order switch.
- No proven strategy edge. The funding strategy is historically
  regime-dependent and currently treated as an experimental hypothesis.
- No complete, automated exit, fee, and funding reconciliation suitable for
  unattended continuation after a protected position closes. If restart finds
  a protected intent flat, it enters manual halt because post-exit sibling
  cancellation verification, fill, fee, funding, and realized-PnL accounting
  are not automated end to end.
- No independent WebSocket watchdog, operator paging path, or tested host
  failover.
- No completed promotion evidence: costed out-of-sample research, shadow
  duration, lifecycle drills, and mainnet dry-run evidence are absent locally.

See [Real Funds Readiness](docs/REAL_FUNDS_READINESS.md) for the research,
control matrix, evidence thresholds, and remaining blockers. See
[Threat Model](docs/THREAT_MODEL.md) for security and operational failure
scenarios.

## Risk profiles

`TESTNET_LIMITS` preserves the engineering profile used for Testnet exercises:
15x configured leverage, 2% stop-loss budget, 0.10 BTC cap, $50 daily gross-loss
limit, and 10% minimum free margin.

`CANARY_REVIEW_LIMITS` is a separate, pre-committed ceiling for a possible
future human-reviewed canary:

- 1x leverage
- 0.1% account risk budget per trade
- 0.001 BTC and $25 notional caps
- $5 daily gross-loss limit
- 50% minimum free margin

The canary profile is not connected to a production exchange client.

## Setup

```powershell
cd "C:\path\to\BitMexBot"
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
Copy-Item .env.example .env
.\.venv\Scripts\python.exe trade_ledger.py --init
```

Create a dedicated BitMEX Testnet key with order permissions and no withdrawal
permission. Fill in `.env`, then run:

```powershell
.\.venv\Scripts\python.exe main.py
```

The authenticated client requires the literal value `BITMEX_TESTNET=true`
before CCXT is even constructed. Sandbox mode is its first exchange method
call.

## Verification

```powershell
.\.venv\Scripts\python.exe -m unittest discover -v
.\.venv\Scripts\python.exe test_signals.py
.\.venv\Scripts\python.exe audit.py
powershell -ExecutionPolicy Bypass -File C:\Users\meidi\.ai-agent-policy\scripts\Invoke-AgentQualityGate.ps1 -Path .
```

`requirements.txt` contains runtime dependencies. `requirements-dev.txt`
includes it and pins `pytest`, so pytest is available only after the development
requirements are installed. The required verification path is unittest
discovery plus `test_signals.py` and `audit.py` above.

`audit.py` is expected to return nonzero until every local safety, v2 history,
and promotion-evidence gate passes. Even a zero exit means only that a human
canary review may begin. It does not authorize live trading.

## Architecture

```text
main.py
  +-- bitmex_client.py       strict Testnet client and metadata verification
  +-- fetch_data.py          completed public candles and settled funding
  +-- signals.py             experimental funding mean-reversion hypothesis
  +-- risk.py                fail-closed unit-aware sizing and limits
  +-- order_manager.py       IOC entry, idempotency, partial fills, SL and TP
  +-- execution_lock.py      thread and OS singleton execution authority
  +-- execution_safety.py    pure decision/order reconciliation primitives
  +-- trade_ledger.py        durable v2 execution lifecycle
  +-- daily_loss_state.py    atomic loss state derived from reconciled v2 rows
  +-- promotion.py           evidence stages, never production enablement
  +-- audit.py               local safety and real-funds no-go audit
```

## Evidence policy

The old claims based on 2026 `data/trades.db` rows are withdrawn. Their mixed
quantity semantics make the historical PnL values unsuitable for statistical
or promotion evidence. Testnet also validates plumbing, not profitability.

The current promotion thresholds include 200 independent costed OOS clusters,
100 untouched clusters, at least three chronological folds with every fold
acceptable, Deflated Sharpe confidence of at least 0.95, Probability of
Backtest Overfitting no more than 0.20, 90 shadow days with order authority
disabled, 100 testnet lifecycle drills with required-scenarios and
zero-unreconciled-incident booleans true, and a 30-day mainnet dry run with
zero order authority plus all required dry-run booleans true. These are
evidence-field gates, not proof that the underlying exercises occurred.

## Dependency maintenance

The weekly dependency audit remains separate from strategy work. It combines
`pip-audit` findings with CISA KEV and EPSS/watchlist context. See
[Security Maintenance](SECURITY.md).

This is experimental software, not financial advice.
