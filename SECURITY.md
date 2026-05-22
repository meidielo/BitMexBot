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

## Audit Runner Reliability

The weekly workflow uses `tools/dependency_audit_runner.py` instead of ad-hoc
shell steps. The runner creates a temporary audit virtual environment, installs
`pip-audit` with retries, runs `pip-audit`, downloads the CISA KEV catalog,
builds the triage ledger, and keeps the raw evidence as CI artifacts.

Artifacts retained by the workflow include:

- `audit-runner-diagnostics.json` and `.md`: Python, platform, requirements
  hash, and relevant environment hints.
- `pip-audit-install-attempt-*`: install stdout, stderr, exit code, and timing.
- `pip-audit.json`, `pip-audit.stderr.txt`, and `pip-audit.status.json`.
- `known_exploited_vulnerabilities.json` and `kev-download.log`.
- `security-triage-ledger.md` and `security-triage-summary.json`.
- `audit-runner-summary.json`: pass/fail status plus actionable diagnostics.

If package index access, KEV download, or `pip-audit` JSON generation fails, the
job fails with the artifact set above. The expected response is to inspect the
diagnostic files and rerun in a network-permitted environment, not to change
trading strategy, order execution, risk limits, or testnet guards.
