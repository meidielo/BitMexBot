"""Build a KEV-aware dependency security triage ledger.

Inputs are deliberately plain JSON files so CI can keep the raw evidence as
artifacts beside the generated markdown ledger.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


WATCHLIST_EPSS_SCORE = 0.05
WATCHLIST_EPSS_PERCENTILE = 0.90
WATCHLIST_CVSS_SCORE = 9.0


def _load_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8-sig") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        raise SystemExit(f"Required JSON file is missing: {path}") from None
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


def _parse_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", text):
        return None
    return float(text)


def _cvss_scores(value: Any) -> list[float]:
    scores: list[float] = []
    for item in _as_list(value):
        if isinstance(item, dict):
            for key in ("score", "base_score", "cvss_score", "baseScore"):
                score = _parse_float(item.get(key))
                if score is not None:
                    scores.append(score)
                    break
        elif isinstance(item, (list, tuple)):
            scores.extend(_cvss_scores(list(item)))
        else:
            score = _parse_float(item)
            if score is not None:
                scores.append(score)
    return scores


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


def _epss_index(epss_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for item in _as_list(epss_payload.get("data")):
        if not isinstance(item, dict):
            continue
        cve = str(item.get("cve") or item.get("cveID") or "").upper()
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
            cvss_scores = _cvss_scores(
                vuln.get("cvss")
                or vuln.get("cvss_score")
                or vuln.get("cvss_scores")
                or vuln.get("base_score")
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
                    "cvss_scores": cvss_scores,
                }
            )

    return findings


def _triage_finding(
    finding: dict[str, Any],
    kev_by_cve: dict[str, dict[str, Any]],
    epss_by_cve: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    kev_matches = [cve for cve in finding["cves"] if cve in kev_by_cve]
    epss_matches = [
        (cve, epss_by_cve[cve])
        for cve in finding["cves"]
        if cve in epss_by_cve
    ]
    epss_scores = [
        score
        for _, item in epss_matches
        for score in [_parse_float(item.get("epss"))]
        if score is not None
    ]
    epss_percentiles = [
        percentile
        for _, item in epss_matches
        for percentile in [_parse_float(item.get("percentile"))]
        if percentile is not None
    ]
    epss_dates = _dedupe(
        [
            str(item.get("date"))
            for _, item in epss_matches
            if item.get("date")
        ]
    )
    max_epss_score = max(epss_scores) if epss_scores else None
    max_epss_percentile = max(epss_percentiles) if epss_percentiles else None
    max_cvss_score = max(finding.get("cvss_scores") or []) if finding.get("cvss_scores") else None
    watchlist = (
        (max_epss_score is not None and max_epss_score >= WATCHLIST_EPSS_SCORE)
        or (max_epss_percentile is not None and max_epss_percentile >= WATCHLIST_EPSS_PERCENTILE)
        or (max_cvss_score is not None and max_cvss_score >= WATCHLIST_CVSS_SCORE)
    )

    if kev_matches:
        priority = "P1"
        kev_status = "yes"
        watchlist = False
        kev_names = [
            str(kev_by_cve[cve].get("vulnerabilityName") or kev_by_cve[cve].get("product") or cve)
            for cve in kev_matches
        ]
        triage_note = (
            "CISA KEV match indicates exploited-in-the-wild risk. "
            "Patch before routine dependency work and block release until fixed. "
            f"KEV: {', '.join(kev_matches)} ({'; '.join(kev_names)})."
        )
    elif watchlist:
        priority = "P2"
        kev_status = "no"
        reasons: list[str] = []
        if max_epss_score is not None and max_epss_score >= WATCHLIST_EPSS_SCORE:
            reasons.append(f"EPSS {max_epss_score:.5f}")
        if max_epss_percentile is not None and max_epss_percentile >= WATCHLIST_EPSS_PERCENTILE:
            reasons.append(f"EPSS percentile {max_epss_percentile:.5f}")
        if max_cvss_score is not None and max_cvss_score >= WATCHLIST_CVSS_SCORE:
            reasons.append(f"CVSS {max_cvss_score:.1f}")
        triage_note = (
            "High-risk non-KEV dependency watchlist finding. "
            "This does not prove active exploitation, but scheduled dependency "
            "runs require explicit maintainer triage. "
            f"Signal: {', '.join(reasons)}."
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
        "epss_score": max_epss_score,
        "epss_percentile": max_epss_percentile,
        "epss_date": ", ".join(epss_dates) or "",
        "max_cvss_score": max_cvss_score,
        "watchlist": watchlist,
        "priority": priority,
        "triage_note": triage_note,
    }


def _render_markdown(
    findings: list[dict[str, Any]],
    kev_payload: dict[str, Any],
    epss_payload: dict[str, Any],
    pip_audit_path: Path,
    kev_path: Path,
    epss_path: Path,
) -> str:
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    kev_date = kev_payload.get("dateReleased") or kev_payload.get("catalogVersion") or "unknown"
    epss_status = epss_payload.get("status") or "downloaded"
    kev_count = sum(1 for finding in findings if finding["kev_matches"])
    watchlist_count = sum(1 for finding in findings if finding["watchlist"])

    lines = [
        "# Security Triage Ledger",
        "",
        f"Generated: {generated}",
        f"pip-audit input: `{pip_audit_path.as_posix()}`",
        f"CISA KEV input: `{kev_path.as_posix()}`",
        f"FIRST EPSS input: `{epss_path.as_posix()}`",
        f"CISA KEV catalog date/version: `{kev_date}`",
        f"FIRST EPSS status: `{epss_status}`",
        "",
        "## Triage Policy",
        "",
        "- P1: dependency advisory maps to CISA Known Exploited Vulnerabilities (active exploitation signal).",
        "- P2: non-KEV dependency advisory meets the EPSS or CVSS watchlist threshold and requires explicit triage.",
        "- P3: dependency advisory has no KEV match or watchlist signal in this snapshot, or has no CVE alias to match.",
        "- KEV remains the only exploited-in-the-wild hard gate; EPSS/CVSS watchlist findings are prioritization signals.",
        "- Every finding must include a triage note in this ledger.",
        "- This maintenance lane must not change trading strategy, order execution, risk limits, or testnet guards.",
        "",
        "## Summary",
        "",
        "| Metric | Count |",
        "| --- | ---: |",
        f"| Total pip-audit findings | {len(findings)} |",
        f"| KEV matched findings | {kev_count} |",
        f"| Non-KEV watchlist findings | {watchlist_count} |",
        f"| Routine non-KEV findings | {len(findings) - kev_count - watchlist_count} |",
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
            "| Package | Installed | Advisory | CVEs | KEV | EPSS | EPSS Percentile | CVSS | Watchlist | Priority | Fixed Versions | Required Triage Note |",
            "| --- | --- | --- | --- | --- | ---: | ---: | ---: | --- | --- | --- | --- |",
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
                    _markdown_escape(
                        f"{finding['epss_score']:.5f}" if finding["epss_score"] is not None else "not listed"
                    ),
                    _markdown_escape(
                        f"{finding['epss_percentile']:.5f}"
                        if finding["epss_percentile"] is not None
                        else "not listed"
                    ),
                    _markdown_escape(
                        f"{finding['max_cvss_score']:.1f}" if finding["max_cvss_score"] is not None else "not listed"
                    ),
                    _markdown_escape("yes" if finding["watchlist"] else "no"),
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
    parser.add_argument("--epss-json", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--summary-json", type=Path)
    parser.add_argument("--fail-on-kev", action="store_true")
    parser.add_argument("--fail-on-watchlist", action="store_true")
    args = parser.parse_args()

    pip_audit_payload = _load_json(args.pip_audit_json)
    kev_payload = _load_json(args.kev_json)
    epss_payload = _load_json(args.epss_json)
    kev_by_cve = _kev_index(kev_payload)
    epss_by_cve = _epss_index(epss_payload)

    findings = [
        _triage_finding(finding, kev_by_cve, epss_by_cve)
        for finding in _iter_pip_audit_findings(pip_audit_payload)
    ]
    missing_notes = [finding for finding in findings if not finding["triage_note"].strip()]
    kev_count = sum(1 for finding in findings if finding["kev_matches"])
    watchlist_count = sum(1 for finding in findings if finding["watchlist"])

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        _render_markdown(
            findings,
            kev_payload,
            epss_payload,
            args.pip_audit_json,
            args.kev_json,
            args.epss_json,
        ),
        encoding="utf-8",
    )

    summary = {
        "total_findings": len(findings),
        "kev_findings": kev_count,
        "watchlist_findings": watchlist_count,
        "non_kev_findings": len(findings) - kev_count,
        "routine_non_kev_findings": len(findings) - kev_count - watchlist_count,
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

    if args.fail_on_watchlist and watchlist_count:
        print(f"{watchlist_count} non-KEV watchlist dependency finding(s) require explicit triage.", file=sys.stderr)
        return 4

    print(
        f"Wrote {args.output} with {len(findings)} finding(s), "
        f"{kev_count} KEV match(es), {watchlist_count} watchlist finding(s)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
