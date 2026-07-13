# Security Maintenance

The repository-wide security and operational threat model is documented in
[`docs/THREAT_MODEL.md`](docs/THREAT_MODEL.md). Real-funds readiness and
promotion controls are documented in
[`docs/REAL_FUNDS_READINESS.md`](docs/REAL_FUNDS_READINESS.md).

## Trading Security Invariants

- Authenticated CCXT construction must fail unless `BITMEX_TESTNET` is exactly
  `true`; this repository has no authenticated mainnet constructor. It does
  have a separate unauthenticated mainnet client for public market data.
- Sandbox mode must be the authenticated execution client's first exchange
  method call. Immediately before private operations, the exchange ID must be
  `bitmex` and both CCXT public and private API origins must exactly equal
  `https://testnet.bitmex.com`.
- API credentials must be dedicated Testnet keys, stored only outside source
  control, without withdrawal permission. IP allowlisting and key rotation are
  operator responsibilities.
- Exchange instrument metadata, account state, leverage, decision time, order
  state, and durable ledger state must be proven before an order proceeds.
- A dedicated execution account is required. The scoped position query must
  return exactly one XBTUSDT record, and unified contracts/side must agree with
  native BitMEX `currentQty`, `isOpen`, symbol, and OneWay mode. A separate
  account-wide position inventory and open-order query must show no unexpected
  exposure or resting orders before a new entry.
- Cost-buffered expected loss includes entry and stop slippage plus round-trip
  taker fees. Current realized daily loss plus the proposed expected loss must
  stay within the configured daily cap.
- A non-blocking in-process/OS file lock and a database uniqueness constraint
  permit only one local private-execution owner and one unresolved durable
  intent at a time.
- A network timeout after submission must never cause an automatic resubmit.
- Every malformed create response must be reconciled by deterministic ID. When
  a returned order mapping has a wrong/missing `clOrdID`, that mapping must be
  retained for cancellation by native order ID. Known exposure after such an
  anomaly must be closed and manually halted.
- While an expected entry ID is invisible, each bounded poll must still inspect
  account-wide open orders and positions. Position exposure is rechecked after
  cancellation errors. Unattributed XBTUSDT exposure must trigger emergency
  Close and manual halt.
- Logs must not contain keys, secrets, signed headers, or full exchange
  responses that may expose account-private data.
- `data/trades.db` is legacy. Application trade/exit writes are disabled and
  its rows are excluded from readiness evidence, but the file is not made
  filesystem read-only.
- A promotion result can never enable production. `CANARY_REVIEW` requires a
  separate human decision and separately controlled deployment mechanism.
- A protected flat intent may close automatically only after both durable
  protective legs are proven terminal, the account is re-proven flat with no
  open orders, and stable paginated native Testnet execution history exactly
  reconciles quantities, identifiers, account, sides, timestamps, fees,
  funding, the combined evidence hash, and native `realisedPnl` against
  independently calculated net PnL. Otherwise it enters a manual halt.
- The independent WebSocket watchdog has no order, cancellation, dead-man
  switch, or mainnet capability. Its authenticated upgrade refuses redirects
  before custom auth headers can reach a second origin. Its output never
  authorizes execution.
- The dashboard image receives only the sanitized snapshot volume. It must not
  receive exchange credentials, the ledger, logs, repository, or Docker socket.

## Restart and Reconciliation Boundary

On startup and before each new decision, the runner examines the v2 ledger for
one unresolved intent. Ambiguous entry states are reconciled by deterministic
client order ID, entry exposure without proven protection is conservatively
closed when possible, and existing stop/target protection is re-verified. A
protected intent that is now flat is closed only from stable native execution
history after sibling terminal proof and account-wide flat/open-order checks.
Normalized events are append-only and the close transition is atomic. Missing
rows, mixed accounts, wrong IDs or sides, quantity mismatches, unattributed
lifetime trades, inconsistent fees or funding, unstable history, or an
unproven sibling cause `HALTED_MANUAL`. While an intent is unresolved or a
position is managed, REST reconciliation runs on a five-second safety cadence
instead of waiting for the next 15-minute candle. The separate WebSocket
  watchdog improves detection but does not replace these REST execution proofs.
  Filled terminal rows require complete exchange provenance, and both the daily
  loss publisher and readiness evaluator recompute immutable event accounting
  and evidence hashes before trusting a row.

Promotion evidence is also only a typed, thresholded input. The evaluator
requires all OOS folds to be acceptable and explicit booleans for disabled
order authority, required Testnet scenarios, zero unreconciled incidents, and
dry-run alert/credential/operator drills. It does not authenticate the
provenance behind those booleans and never enables production.

This repo keeps dependency/security work separate from trading strategy work.
Security maintenance must not change order execution, risk limits, testnet
guards, backtest parameters, or signal logic.

The remote deployment adds a strict application-local DNS-over-TLS resolver
because the current host's ISP DNS path returned a hostname-mismatched
interception certificate for BitMEX. Plaintext DNS fallback and disabled TLS
verification are forbidden. The dashboard binds only to the host's Tailscale
address, never to public or LAN interfaces, and its Docker bridge disables IP
masquerading. Tailscale Serve HTTPS is preferred when the tailnet capability is
enabled. See
[`docs/REMOTE_DEPLOYMENT.md`](docs/REMOTE_DEPLOYMENT.md).

## Dependency Triage

The `Dependency vulnerability triage` workflow runs weekly and on dependency
changes. It:

1. Runs `pip-audit` against `requirements.txt`.
2. Downloads the CISA Known Exploited Vulnerabilities catalog.
3. Downloads FIRST EPSS data for CVE aliases reported by `pip-audit`.
4. Builds a security triage ledger artifact with one required triage note per
   finding.
5. Fails the workflow when a finding maps to CISA KEV, because that is the
   active exploitation signal.
6. Fails scheduled/dependency workflows on high-risk non-KEV watchlist findings
   so they receive explicit maintainer triage before being accepted.

Priority mapping:

- P1: dependency advisory maps to CISA KEV. Patch before routine dependency
  work and block release until fixed.
- P2: dependency advisory has no KEV match, but crosses the local watchlist
  threshold from EPSS or CVSS-style data. This does not prove active
  exploitation, but it requires explicit triage.
- P3: dependency advisory has no KEV match or watchlist signal, or has no CVE
  alias to match. Handle through the weekly dependency update flow.

Current watchlist thresholds are EPSS score `>= 0.05`, EPSS percentile
`>= 0.90`, or CVSS score `>= 9.0`. KEV remains the hard exploited-in-the-wild
gate; EPSS and CVSS are prioritization signals.

The ledger artifact is the record of why a finding was treated as urgent or
routine for that run.

## Audit Runner Reliability

The weekly workflow uses `tools/dependency_audit_runner.py` instead of ad-hoc
shell steps. The runner creates a temporary audit virtual environment, installs
`pip-audit` with retries, runs `pip-audit`, downloads the CISA KEV catalog,
downloads FIRST EPSS data when CVE aliases are present, builds the triage
ledger, and keeps the raw evidence as CI artifacts.

Artifacts retained by the workflow include:

- `audit-runner-diagnostics.json` and `.md`: Python, platform, requirements
  hash, and relevant environment hints.
- `pip-audit-install-attempt-*`: install stdout, stderr, exit code, and timing.
- `pip-audit.json`, `pip-audit.stderr.txt`, and `pip-audit.status.json`.
- `known_exploited_vulnerabilities.json` and `kev-download.log`.
- `epss.json` and `epss-download.log`.
- `security-triage-ledger.md` and `security-triage-summary.json`.
- `audit-runner-summary.json`: pass/fail status plus actionable diagnostics.

If package index access, KEV download, EPSS lookup, or `pip-audit` JSON
generation fails, the job fails with the artifact set above. The expected
response is to inspect the diagnostic files and rerun in a network-permitted
environment, not to change trading strategy, order execution, risk limits, or
testnet guards.
