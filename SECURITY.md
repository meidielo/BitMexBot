# Security Maintenance

This repo keeps dependency/security work separate from trading strategy work.
Security maintenance must not change order execution, risk limits, testnet
guards, backtest parameters, or signal logic.

## Dependency Triage

The `Dependency vulnerability triage` workflow runs weekly and on dependency
changes. It:

1. Runs `pip-audit` against `requirements.txt`.
2. Downloads the CISA Known Exploited Vulnerabilities catalog.
3. Builds a security triage ledger artifact with one required triage note per
   finding.
4. Fails the workflow only when a finding maps to CISA KEV, because that is the
   active exploitation signal.

Priority mapping:

- P1: dependency advisory maps to CISA KEV. Patch before routine dependency
  work and block release until fixed.
- P3: dependency advisory has no KEV match, or has no CVE alias to match.
  Handle through the weekly dependency update flow.

The ledger artifact is the record of why a finding was treated as urgent or
routine for that run.
