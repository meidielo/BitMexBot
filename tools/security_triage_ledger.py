"""Build a KEV-aware dependency security triage ledger.

Inputs are deliberately plain JSON files so CI can keep the raw evidence as
artifacts beside the generated markdown ledger.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8-sig") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON in {path}: {exc}") from exc

    if isinstance(data, dict):
        return data
    raise SystemExit(f"Expected a JSON object in {path}")


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _as_text_list(value: Any) -> list[str]:
    items: list[str] = []
    for item in _as_list(value):
        if isinstance(item, dict):
            for key in ("version", "id", "name"):
                if item.get(key):
                    items.append(str(item[key]))
                    break
        elif item:
            items.append(str(item))
    return items


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        key = value.upper()
        if key not in seen:
            seen.add(key)
            result.append(value)
    return result


def _markdown_escape(value: Any) -> str:
    text = str(value) if value is not None else ""
    return text.replace("|", "\\|").replace("\r", " ").replace("\n", "<br>")


def _kev_index(kev_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for item in _as_list(kev_payload.get("vulnerabilities")):
        if not isinstance(item, dict):
            continue
        cve = str(item.get("cveID") or item.get("cveId") or "").upper()
        if cve.startswith("CVE-"):
            result[cve] = item
    return result


def _iter_pip_audit_findings(payload: dict[str, Any]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for dependency in _as_list(payload.get("dependencies")):
        if not isinstance(dependency, dict):
            continue

        name = dependency.get("name") or dependency.get("package_name") or "unknown"
        version = dependency.get("version") or dependency.get("installed_version") or "unknown"

        for vuln in _as_list(dependency.get("vulns") or dependency.get("vulnerabilities")):
            if not isinstance(vuln, dict):
                continue

            advisory_id = (
                vuln.get("id")
                or vuln.get("vulnerability_id")
                or vuln.get("name")
                or "unknown-advisory"
            )
            aliases = _as_text_list(vuln.get("aliases"))
            all_ids = _dedupe([str(advisory_id), *aliases])
            cves = [item.upper() for item in all_ids if item.upper().startswith("CVE-")]
            fix_versions = _as_text_list(
                vuln.get("fix_versions")
                or vuln.get("fixed_versions")
                or vuln.get("fixes")
            )

            findings.append(
                {
                    "package": str(name),
                    "installed_version": str(version),
                    "advisory_id": str(advisory_id),
                    "aliases": aliases,
                    "cves": _dedupe(cves),
                    "fix_versions": _dedupe(fix_versions),
                    "description": str(vuln.get("description") or ""),
                }
            )

    return findings


def _triage_finding(finding: dict[str, Any], kev_by_cve: dict[str, dict[str, Any]]) -> dict[str, Any]:
    kev_matches = [cve for cve in finding["cves"] if cve in kev_by_cve]

    if kev_matches:
        priority = "P1"
        kev_status = "yes"
        kev_names = [
            str(kev_by_cve[cve].get("vulnerabilityName") or kev_by_cve[cve].get("product") or cve)
            for cve in kev_matches
        ]
        triage_note = (
            "CISA KEV match indicates exploited-in-the-wild risk. "
            "Patch before routine dependency work and block release until fixed. "
            f"KEV: {', '.join(kev_matches)} ({'; '.join(kev_names)})."
        )
    elif finding["cves"]:
        priority = "P3"
        kev_status = "no"
        triage_note = (
            "No CISA KEV match for the CVE aliases in this catalog snapshot. "
            "Handle through the weekly dependency update flow and re-check KEV on the next run."
        )
    else:
        priority = "P3"
        kev_status = "not applicable"
        triage_note = (
            "No CVE alias was present, so CISA KEV matching is not possible. "
            "Review the advisory and update during normal security maintenance."
        )

    return {
        **finding,
        "kev_matches": kev_matches,
        "kev_status": kev_status,
        "priority": priority,
        "triage_note": triage_note,
    }


def _render_markdown(
    findings: list[dict[str, Any]],
    kev_payload: dict[str, Any],
    pip_audit_path: Path,
    kev_path: Path,
) -> str:
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    kev_date = kev_payload.get("dateReleased") or kev_payload.get("catalogVersion") or "unknown"
    kev_count = sum(1 for finding in findings if finding["kev_matches"])

    lines = [
        "# Security Triage Ledger",
        "",
        f"Generated: {generated}",
        f"pip-audit input: `{pip_audit_path.as_posix()}`",
        f"CISA KEV input: `{kev_path.as_posix()}`",
        f"CISA KEV catalog date/version: `{kev_date}`",
        "",
        "## Triage Policy",
        "",
        "- P1: dependency advisory maps to CISA Known Exploited Vulnerabilities (active exploitation signal).",
        "- P3: dependency advisory has no KEV match in this catalog snapshot, or has no CVE alias to match.",
        "- Every finding must include a triage note in this ledger.",
        "- This maintenance lane must not change trading strategy, order execution, risk limits, or testnet guards.",
        "",
        "## Summary",
        "",
        "| Metric | Count |",
        "| --- | ---: |",
        f"| Total pip-audit findings | {len(findings)} |",
        f"| KEV matched findings | {kev_count} |",
        f"| Non-KEV findings | {len(findings) - kev_count} |",
        "",
        "## Findings",
        "",
    ]

    if not findings:
        lines.append("No pip-audit findings were reported.")
        lines.append("")
        return "\n".join(lines)

    lines.extend(
        [
            "| Package | Installed | Advisory | CVEs | KEV | Priority | Fixed Versions | Required Triage Note |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )

    for finding in findings:
        advisory_ids = _dedupe([finding["advisory_id"], *finding["aliases"]])
        lines.append(
            "| "
            + " | ".join(
                [
                    _markdown_escape(finding["package"]),
                    _markdown_escape(finding["installed_version"]),
                    _markdown_escape(", ".join(advisory_ids) or "unknown"),
                    _markdown_escape(", ".join(finding["cves"]) or "none"),
                    _markdown_escape(", ".join(finding["kev_matches"]) or finding["kev_status"]),
                    _markdown_escape(finding["priority"]),
                    _markdown_escape(", ".join(finding["fix_versions"]) or "not listed"),
                    _markdown_escape(finding["triage_note"]),
                ]
            )
            + " |"
        )

    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pip-audit-json", required=True, type=Path)
    parser.add_argument("--kev-json", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--summary-json", type=Path)
    parser.add_argument("--fail-on-kev", action="store_true")
    args = parser.parse_args()

    pip_audit_payload = _load_json(args.pip_audit_json)
    kev_payload = _load_json(args.kev_json)
    kev_by_cve = _kev_index(kev_payload)

    findings = [
        _triage_finding(finding, kev_by_cve)
        for finding in _iter_pip_audit_findings(pip_audit_payload)
    ]
    missing_notes = [finding for finding in findings if not finding["triage_note"].strip()]
    kev_count = sum(1 for finding in findings if finding["kev_matches"])

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        _render_markdown(findings, kev_payload, args.pip_audit_json, args.kev_json),
        encoding="utf-8",
    )

    summary = {
        "total_findings": len(findings),
        "kev_findings": kev_count,
        "non_kev_findings": len(findings) - kev_count,
        "missing_triage_notes": len(missing_notes),
        "ledger": str(args.output),
    }
    if args.summary_json:
        args.summary_json.parent.mkdir(parents=True, exist_ok=True)
        args.summary_json.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    if missing_notes:
        print(f"Missing triage notes for {len(missing_notes)} finding(s).", file=sys.stderr)
        return 3

    if args.fail_on_kev and kev_count:
        print(f"{kev_count} KEV-matched dependency finding(s) require immediate triage.", file=sys.stderr)
        return 2

    print(
        f"Wrote {args.output} with {len(findings)} finding(s), "
        f"{kev_count} KEV match(es)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
