# Real Funds Readiness

Research date: 2026-07-13

## Decision

**BitMexBot is not ready for real funds.**

The implementation now fails closed across the most dangerous execution
windows, but software controls cannot create a trading edge or operational
evidence. Authenticated production trading is intentionally unavailable. The
highest automated state is `CANARY_REVIEW`, which still requires a separate
human decision and a separately controlled deployment mechanism.

## Evidence reviewed

The review covered the repository, current BitMEX and CCXT documentation,
exchange metadata, Australian record-keeping guidance, secrets/logging
guidance, and statistical overfitting literature.

Current exchange metadata was checked for `XBTUSDT` through BitMEX/CCXT. At the
review timestamp it described an active linear USDT-settled swap with:

- `contractSize = 0.000001 BTC`
- `lotSize = 100 contracts`
- `tickSize = 0.1 USDT`
- maker and taker rates reported as `0.0005`
- initial margin `0.01`
- maintenance margin `0.005`

These values are observations, not permanent configuration. The client loads
and validates current metadata at startup and rejects contradictions.

The old claim that one XBTUSDT contract equals one USDT notional was wrong for
the current contract model. Correct gross linear PnL is:

```text
LONG  = contracts * contract_size_btc * (exit_price - entry_price)
SHORT = contracts * contract_size_btc * (entry_price - exit_price)
```

Because `data/trades.db` mixed BTC values and contract counts, its historical
PnL and sample statistics are excluded from readiness evidence.

## Controls implemented in this pass

| Risk | Control | State |
| --- | --- | --- |
| Wrong environment or credentials | Exact `BITMEX_TESTNET=true` gate before CCXT construction; sandbox mode first; exchange ID plus public/private origins re-attested as exact BitMEX Testnet before private operations; dedicated Testnet credential names | Implemented and tested |
| Contract drift or wrong symbol | Fail-closed runtime validation of active linear XBTUSDT metadata, fees, and margins | Implemented and tested |
| Ambiguous account state | Exactly one canonical XBTUSDT record plus an account-wide native position inventory; before entry, reject any unexpected exposure or resting order | Implemented and tested |
| Wrong units and oversized position | Stop-distance sizing with explicit contracts, BTC, USDT notional, margin, raw stop loss, and expected loss buffered for 10 bps entry slippage, 20 bps stop slippage, and round-trip taker fees | Implemented and tested |
| Daily loss overshoot | Current realized daily loss plus the proposed trade's buffered expected loss must remain within the configured daily cap | Implemented and tested |
| Stop beyond estimated liquidation | LONG stop must be above the conservative estimate; SHORT stop must be below it | Implemented and tested |
| Mutable/incomplete data | Exactly three aligned, completed 5m children per 15m candle; completed parent sequence must be contiguous; decision keyed to completed close | Implemented and tested |
| Live/backtest funding look-ahead | Live signal sees only funding settled by the candle decision time | Implemented and tested |
| Market-order slippage | Marketable IOC limit bounded to 10 bps from current top of book | Implemented and tested |
| Duplicate or orphan order after timeout/malformed response | Deterministic 36-character UUIDv5 `clOrdID`, pre/post reconciliation, no automatic retry, and native-order-ID cancellation when the returned `clOrdID` is missing or wrong | Implemented and tested |
| Nonterminal or partial entry | Cancel every visible nonterminal IOC, prove terminal, and protect only proven cumulative fill | Implemented and tested |
| Invisible expected entry ID | Every reconciliation pass inventories/cancels unexpected account orders and checks account-wide exposure before and after cancellation failure; unattributed XBTUSDT is closed and halted | Implemented and tested |
| Stop/target based on stale signal price or independent sibling orders | Re-anchor percentage distances to actual average fill, round conservatively, submit one native OCO pair, and verify both returned link/contingency fields | Implemented and tested |
| Untrusted protective response | Cancel the untrusted leg to proven terminal; after target cleanup, re-attest stop and position or emergency-close and halt | Implemented and tested |
| Stop placement failure | Attempt idempotent reduce-only emergency Close, verify flat when possible, then halt for reconciliation | Implemented and tested |
| Concurrent runners or intents | Non-blocking thread/OS file lock plus a SQLite uniqueness constraint for one unresolved durable intent | Implemented and tested |
| Crash between decisions and orders | Register intent before submission; atomic versioned lifecycle transitions; reconcile the unresolved intent before a new signal | Implemented and tested |
| Stale or corrupt daily loss | Rebuild atomically only after revalidating the ledger schema, terminal provenance, event accounting, and combined evidence hash; refuse publication while any intent is unresolved | Implemented and tested |
| Flat position with incomplete exit evidence | Prove protective siblings terminal, re-prove flat/no open orders, read paginated native Testnet execution history twice without change, attribute exact entry/exit fills, fees, funding, and native `realisedPnl`, cross-check independently calculated net PnL, then append events and close atomically; any ambiguity halts | Implemented and tested |
| REST process blind spot | Independent authenticated Testnet WebSocket watchdog for order, position, execution, and margin state; no order/cancel/mainnet capability; stale state is explicit | Implemented and tested |
| WebSocket credential forwarding | Reject every authenticated WebSocket redirect before aiohttp can reuse custom BitMEX auth headers at another origin | Implemented and tested |
| Dashboard credential or data overexposure | One-way sanitized snapshot, Basic auth behind private Tailscale HTTPS, no control routes, and dashboard image/volume isolation from keys, ledger, logs, source, and Docker socket | Implemented and tested |
| False readiness from legacy history | Disable application writes to the legacy database and exclude it from audit evidence; the file itself is not filesystem read-only | Implemented and tested |
| Accidental automated live switch | Promotion evaluator always returns `production_enabled=false`; no authenticated mainnet client exists, while the separate mainnet client is public-data-only | Implemented and tested |

## Testnet setup and account boundary

Use a dedicated BitMEX Testnet account with no manual trades. The code requires
one canonical XBTUSDT record, inventories account-wide native positions, and
rejects every open order before a fresh entry. A manual action can still race a
completed preflight, so operational account isolation remains mandatory.

Initialize the v2 ledger explicitly before first startup:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
Copy-Item .env.example .env
.\.venv\Scripts\python.exe trade_ledger.py --init
```

`requirements-dev.txt` includes `requirements.txt` and the pinned pytest test
tool. A runtime-only environment may install `requirements.txt`, but the full
local verification environment uses `requirements-dev.txt`. Subsequent Python
commands should use `.\.venv\Scripts\python.exe` explicitly.

## Important design choices

### No dead-man switch yet

BitMEX documents `cancelAllAfter` as a dead-man switch that cancels outstanding
orders. That can be useful for stale entry orders, but canceling all orders can
also remove protective stops while leaving a position open. It is therefore
not wired into this bot as a blanket safety control. A future implementation
needs a position-aware independent watchdog and must prove that protection is
restored or the position is flattened before any timer can cancel a stop.

### No blind retries

CCXT exposes `clientOrderId`, and BitMEX returns `clOrdID` on orders and
executions. A timeout is not proof that submission failed. The bot queries by
the deterministic ID before creation and again after ambiguous network errors.
If existence still cannot be proven, automation halts.

### Restart reconciliation requires stable native exit accounting

Before a new decision, the runner reads the v2 ledger. It allows no more than
one unresolved intent and holds a process/OS execution lock while repairing or
re-verifying it. Registered or pending entry states are reconciled rather than
replaying the signal. Known entry exposure without durable protection triggers
an idempotent reduce-only Close attempt. Protected states must still have an
open, native-verified stop and the expected position; otherwise the bot tries
to flatten and halts.

Unresolved entry, paused, stop-only, and managed-position states are checked on
a five-second REST safety cadence. A `failed` or `manual_halt` result stops the
runner immediately. The independent WebSocket watchdog narrows the monitoring
gap further, but its alert delivery, operator response, and host-failover paths
still require recorded drills before any real-funds review.

When a protected intent is found flat, the runner proves both durable
protective legs terminal, rechecks account-wide flat and open-order state, and
queries paginated native Testnet execution history. The exact attributable
entry, exit, fee, and funding evidence must remain unchanged across two reads.
It is then normalized into an append-only event table and committed atomically
with the terminal ledger transition. Mixed accounts, missing or contradictory
IDs, wrong sides, quantity mismatch, an unattributed XBTUSDT trade during the
position lifetime, incomplete funding evidence, or unstable history causes a
manual halt. This closes an engineering gap but does not prove mainnet fill
quality, latency, liquidity, or strategy profitability.

### Testnet is engineering evidence only

Testnet can validate authentication, order schemas, partial fills, restarts,
and lifecycle behavior. It cannot prove liquidity, slippage, fills, latency,
fees, funding behavior, or strategy profitability on mainnet.

## Promotion requirements

The thresholds are deliberately demanding because a low-frequency,
regime-dependent strategy creates weak statistical evidence.

### Research to shadow

- At least 200 independent, costed out-of-sample clusters
- At least 100 untouched lockbox clusters
- At least three chronological OOS folds, all acceptable, with the latest
  positive
- Bootstrap lower bound for net expectancy above zero
- Profit-factor lower bound above one
- Deflated Sharpe confidence at least 0.95
- Probability of Backtest Overfitting no more than 0.20
- No single cluster above 20% of PnL; top five no more than 50%
- Positive net expectancy under a 2x cost stress

### Shadow to Testnet engineering

- 90 consecutive shadow days
- At least 50 matured forward clusters
- `order_authority_disabled` is exactly `true`

### Testnet engineering to mainnet dry run

- At least 100 passed lifecycle drills
- `required_scenarios_passed` is exactly `true`
- `zero_unreconciled_incidents` is exactly `true`

### Mainnet dry run to canary review

- At least 30 consecutive days using public mainnet state
- Zero reconciliation differences
- Zero incomplete-candle decisions
- `order_authority_disabled` is exactly `true`
- `alert_drills_passed` is exactly `true`
- `credential_rotation_drill_passed` is exactly `true`
- `operator_response_drills_passed` is exactly `true`

These are the fields actually enforced by `promotion.py`. The evaluator checks
types, thresholds, consistency, and booleans. It does not inspect drill names,
alert logs, credential systems, operator response records, or evidence
provenance. Those artifacts still need independent human review.

### Pre-committed canary ceiling

If every automated gate passes and a human separately authorizes a canary, the
repository defines this maximum profile:

- 1x leverage
- 0.1% account risk budget per trade
- 0.001 BTC and $25 notional maximum
- $5 daily gross-loss limit
- 50% minimum free margin
- One position maximum

This is a ceiling, not permission. It is not wired to an authenticated mainnet
client.

## Remaining blockers

1. **No admissible edge evidence.** The current funding hypothesis is
   regime-dependent, and the legacy sample is invalid for inference.
2. **No verified paging or failover evidence.** The independent WebSocket
   watchdog and optional redacted HTTPS alert exist, but no delivery, operator
   response, credential rotation, backup restoration, or second-host failover
   drill has been proven.
3. **No operational evidence.** There is no completed 30-day dry run, 100-drill
   lifecycle record, failover proof, or zero-difference reconciliation record.
4. **No verified account hardening.** Code cannot prove MFA, IP allowlisting,
   least-privilege key scope, host encryption, secret-manager use, or key
   rotation.
5. **No production deployment boundary.** This is intentional. A future
   production adapter must be a separately reviewed component with an explicit
   human-controlled release boundary.
6. **Promotion evidence provenance is not authenticated.** The evaluator
   validates fields and thresholds but does not cryptographically prove that
   the reported research runs and drills occurred.
7. **Tax and regulatory handling remains an operator duty.** Australian users
   should retain exchange records, timestamps, purpose, AUD values, costs, and
   other records required for their circumstances and obtain professional
   advice where needed.

## Operator go/no-go

Run:

```powershell
.\.venv\Scripts\python.exe audit.py
```

- Any nonzero result means **DO NOT USE REAL FUNDS**.
- A zero result means **eligible for human canary review only**.
- Never treat a Testnet fill, a profitable backtest, or a green unit-test run
  as authorization to deploy money.

## Primary sources

- [BitMEX API and account security](https://support.bitmex.com/hc/en-gb/articles/6133237456413-BitMEX-API-Account-Security)
- [BitMEX REST API](https://docs.bitmex.com/api-explorer)
- [BitMEX create-order semantics](https://docs.bitmex.com/api-explorer/new-order-1.html)
- [BitMEX instrument metadata](https://docs.bitmex.com/api-explorer/get-instruments.html)
- [BitMEX cancel-all-after documentation](https://docs.bitmex.com/api-explorer/cancel-all-after-order-1)
- [CCXT manual: sandbox, order state, client IDs, IOC](https://github.com/ccxt/ccxt/wiki/manual)
- [Bailey and Lopez de Prado, Deflated Sharpe Ratio](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551)
- [Bailey et al., Probability of Backtest Overfitting](https://doi.org/10.21314/JCF.2016.322)
- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [OWASP Logging Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html)
- [Australian Taxation Office: keeping crypto records](https://www.ato.gov.au/individuals-and-families/investments-and-assets/crypto-asset-investments/keeping-crypto-records)
