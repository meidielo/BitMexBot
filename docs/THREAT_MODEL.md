# BitMexBot Threat Model

## Overview

BitMexBot is a local Python research runner that reads mainnet market data
through an unauthenticated public BitMEX client and can place authenticated
orders only on BitMEX Testnet. Its primary
runtime surfaces are `main.py`, `bitmex_client.py`, `fetch_data.py`, `risk.py`,
`order_manager.py`, `execution_lock.py`, and the versioned SQLite ledger in
`trade_ledger.py`.
Backtests, scanners, dashboards, tests, documentation, and legacy monitoring
are secondary surfaces and must not silently gain order authority.

The highest-value assets are exchange credentials, account funds/exposure,
protective-order integrity, risk limits, strategy and promotion evidence,
execution history, and the operator's ability to distinguish known state from
unknown state.

## Threat Model, Trust Boundaries, and Assumptions

### Trust boundaries

1. **Operator to local host.** The operator controls `.env`, process startup,
   dependencies, host permissions, clock, and network configuration. A
   compromised host can read keys and alter code or evidence.
2. **Local process to BitMEX public APIs.** OHLCV, funding, instruments, and
   order books are remote, mutable, and potentially stale or incomplete.
3. **Local process to authenticated BitMEX Testnet APIs.** Orders, positions,
   balances, leverage, and fills cross a network where timeouts create
   ambiguous outcomes.
4. **Decision logic to execution logic.** A signal is untrusted until risk,
   unit, geometry, completed-candle, and ledger checks bind it to one intent.
5. **Process memory to local durable state.** SQLite and JSON files can be
   stale, corrupted, rolled back, concurrently changed, or manually edited.
6. **Research evidence to promotion decision.** Backtests and legacy logs can
   be biased, overfit, unit-inconsistent, or incomplete. Promotion evidence is
   separate from code correctness.
7. **Automation to human authority.** Automated checks may recommend
   `CANARY_REVIEW`; they never authorize production or create a production
   client.

### Input control

- Attacker-controlled or externally influenced: network responses, public
  market data, exchange errors, timing, rate limits, and any compromised
  dependency or API endpoint.
- Operator-controlled: environment variables, credentials, local data files,
  process clock, deployment host, account settings, and manual exchange action.
- Developer-controlled: strategy code, risk constants, dependency pins,
  migrations, test fixtures, audit thresholds, and deployment scripts.

### Security invariants

- Authenticated mainnet execution must remain unavailable.
- Sandbox mode must be the authenticated execution client's first exchange
  method call.
- Immediately before private operations, the exchange ID and exact public and
  private API origins must attest BitMEX Testnet.
- One completed candle and side map to one durable decision and deterministic
  order IDs.
- Each accepted 15-minute sequence must be contiguous, and each parent must
  contain exactly three aligned, completed 5-minute children.
- A thread/OS lock and the v2 database must permit only one local private
  execution owner and one unresolved intent.
- Unknown order outcome must never be interpreted as no order or a full fill.
- Position sizing must use current verified contract metadata and explicit
  units.
- Unified position contracts/side must agree with native BitMEX `currentQty`,
  `isOpen`, symbol, and OneWay mode for exactly one XBTUSDT record.
- Account-wide native position inventory and open-order preflight must reject
  unexpected exposure or resting orders before entry.
- Current realized daily loss plus cost-buffered expected loss for the proposed
  trade must remain within the daily cap.
- Actual filled exposure must be protected or flattened; canceled orders are
  not equivalent to closed positions.
- A stale, corrupt, or missing balance, position, leverage, daily-loss, or
  ledger state must stop execution.
- Secrets and signed/authenticated request material must not enter logs,
  source control, reports, or promotion evidence.
- Legacy mixed-unit history must never support a real-funds claim.

### Assumptions

- The local Python interpreter, operating system, CA trust store, CCXT package,
  and host clock are trustworthy. If these are compromised, application-level
  controls cannot protect credentials or orders.
- BitMEX enforces API permissions and `Close`/`ReduceOnly` semantics as
  documented.
- Operators use dedicated keys with no withdrawal permission. MFA, IP
  allowlisting, host encryption, patching, backup, and key rotation are outside
  this repository and must be verified separately.
- Operators use a dedicated account with no manual trades. Account-wide
  preflight narrows this risk but cannot eliminate a manual action racing a
  completed check.
- Testnet behavior is not assumed to predict mainnet liquidity or performance.

## Attack Surface, Mitigations, and Attacker Stories

| Surface or story | Impact | Existing mitigations | Residual risk |
| --- | --- | --- | --- |
| Production key is placed in `.env` and code connects to mainnet | Real orders and loss | Literal Testnet gate before construction, sandbox first, exact endpoint re-attestation before private calls, no authenticated production constructor | Operator could alter source or dependency; host controls remain essential |
| API key is stolen from disk, logs, crash reports, or source | Unauthorized orders or account reconnaissance | Environment-only loading, no key output, hardcoded-key audit, dedicated Testnet names | `.env` is still plaintext unless operator uses strict ACLs or a secret manager |
| Instrument contract changes or wrong symbol is loaded | Severe sizing/PnL error | Runtime validation of symbol, ID, settlement, linearity, active flag, sizes, fees, margins | Exchange and CCXT could disagree in an undetected field not yet modeled |
| Forming, gapped candle or future funding leaks into a decision | False signal and research/live divergence | Aligned three-child completion rule, contiguous completed-parent check, latest-close decision key, settled-funding cutoff | Public endpoint delays or timestamp errors still require monitoring |
| Unified position data contradicts native BitMEX state | Duplicate or wrongly directed exposure | Exactly one XBTUSDT record; contracts/side cross-checked with native `currentQty`, `isOpen`, symbol, and OneWay mode; account-wide native exposure inventory | Exchange state can change after preflight; a dedicated account is required |
| Unexpected manual or stale resting order exists before entry | Exposure appears after a flat check | Account-wide open-order query must be empty before leverage or submission | A manual action can race the completed preflight |
| Stop lies beyond the conservative liquidation estimate | Liquidation before intended stop | LONG stop must be above the estimate and SHORT stop below it | The estimate is pre-fill and conservative, not the exchange's final liquidation price |
| Network timeout occurs after order acceptance | Duplicate position after retry | Deterministic `clOrdID`, lookup before create, never retry unresolved creation, five-second reconciliation cadence | REST visibility can lag; unresolved state requires operator action |
| Create response is malformed or returns a wrong/missing `clOrdID` | An orphan order survives reconciliation and later creates exposure | Reconcile every malformed response by deterministic ID; when an order mapping exists, retain it, cancel by native order ID, prove terminal, and close/halt known exposure | If both the response and deterministic lookup omit the order, monitoring and operator reconciliation remain necessary |
| Expected entry ID stays invisible while an order fills | Unprotected exposure is hidden behind repeated ID lookups | Every poll inventories account orders and positions; position exposure is rechecked after cancellation errors; unexpected orders are canceled and unattributed XBTUSDT is closed/halted | REST can still lag or fail simultaneously, so an independent state stream remains required |
| IOC entry is still open or partially fills | Unprotected or oversized residual exposure | Cancel every visible nonterminal IOC, prove terminal, protect only cumulative fill | Cancel/fill race can still force manual halt |
| Protective stop is rejected, ambiguous, or semantically invalid | Open unprotected exposure or stale trigger | Untrusted returned order is canceled to proven terminal; emergency reduce-only Close attempt; manual halt | Simultaneous network/exchange outage can prevent cancellation and close |
| Stop or target fills but its sibling remains open | A later stale order creates incorrect exit state | Both protective orders share a deterministic native OCO link and each response must prove the link plus `OneCancelsTheOther` contingency | Post-exit sibling-cancellation verification, exact fills, fees, and realized PnL still require reconciliation |
| Blanket dead-man switch cancels the stop but not the position | Unprotected live position | `cancelAllAfter` intentionally not enabled as a blanket control | A safe future implementation requires a position-aware independent watchdog |
| Concurrent runner or multiple unresolved intents | Duplicate entry authority | Non-blocking thread/OS file lock plus database uniqueness for unresolved state | A compromised host or manually altered database can still defeat local controls |
| SQLite/JSON is corrupted, stale, or rolled back | Duplicate action or disabled loss limit | WAL, immediate transactions, compare-and-set transitions, schema/integrity checks, atomic daily-loss write | Host compromise or backup rollback can still invalidate state |
| Realized loss plus proposed trade loss exceeds the day cap | Daily loss overshoot | Expected loss includes entry/stop slippage and round-trip taker fees; current plus expected must remain inside the cap | Actual gap/slippage, fee, or funding outcomes can exceed the model |
| Developer tampers with promotion JSON or legacy results | Premature real-funds claim | Explicit fail-closed thresholds, invariant checks, legacy exclusion, production always false | Evidence provenance/signing is not implemented |
| Dashboard or legacy monitor is exposed | Information disclosure | Read-only intent, no order imports in current architecture | Flask deployment/auth posture must be reviewed before any network exposure |
| Dependency compromise | Credential theft or order manipulation | Exact pins, weekly pip-audit/KEV/EPSS triage, narrow dependency updates | Pinning does not prevent a malicious but correctly versioned package |
| Clock drift | Stale/future decisions and broken auth signatures | UTC timestamps and candle alignment checks | No independent NTP health gate is implemented |
| Restart sees a protected intent after the position closed | Incorrect exit history or unsafe continuation | Runner enters manual halt instead of inferring the exit | Post-exit sibling-cancellation verification, exact fills, fees, funding, exit reason, and realized PnL remain manual |
| Legacy database is mistaken for trusted history | False readiness evidence | Application trade/exit writes are disabled and audit excludes its rows | The normal filesystem file can still be modified by other tools or the operator |
| Operator manually trades in the same account | Ledger/account divergence | Dedicated-account requirement, strict XBTUSDT cross-check, account-wide position/open-order preflight, unresolved-intent pause | Manual actions can race preflight; operational isolation is still required |

The promotion evaluator requires all reported OOS folds to be acceptable and
explicit `true` values for shadow/dry-run order-authority disablement, required
Testnet scenarios, zero unreconciled incidents, and the three dry-run drill
booleans. It validates only fields and consistency. Evidence provenance and the
contents of underlying drill records remain outside the automated trust
boundary.

Out of scope attacker stories include exchange matching-engine compromise,
BitMEX insolvency, internet-wide denial of service, and physical coercion. They
remain real operational risks but cannot be fixed in this repository.

## Severity Calibration (Critical, High, Medium, Low)

### Critical

- Any reachable path that bypasses the Testnet boundary and can place
  authenticated mainnet orders.
- Credential exfiltration that exposes a production-capable key or withdrawal
  authority.
- A unit-conversion defect capable of multiplying real exposure by orders of
  magnitude.

### High

- Duplicate entry after ambiguous timeout.
- A filled position left without an affirmatively open stop or verified close.
- Ledger corruption or replay that permits a second position despite unknown
  exposure.
- Risk checks that fail open on missing balance, position, leverage, metadata,
  or daily loss.

### Medium

- Incomplete-candle or funding-timestamp mismatch that changes experimental
  signals but cannot bypass the Testnet boundary.
- Tampered promotion evidence that creates a misleading report while
  production enablement remains impossible.
- Sensitive account metadata in logs without keys or order authority.

### Low

- Incorrect dashboard labels, stale research summaries, or unavailable
  telemetry that do not affect execution, secrets, durable state, or promotion.
- Denial of service against local reporting tools when order execution is
  already halted safely.

Repository: and Version: identifiers for the reusable security-scan cache are
maintained in the external scan artifact generated for this workspace.
