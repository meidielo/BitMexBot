"""Container health probes for sanitized BitMexBot status artifacts."""

from __future__ import annotations

import argparse
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


PROBES: dict[str, dict[str, Any]] = {
    "runner": {
        "path": "data/runner_status.json",
        "schema_version": 1,
        "max_age_seconds": 1_100,
        "status_field": "status",
        "allowed_statuses": {"STARTING", "RUNNING", "WAITING", "PAUSED"},
    },
    "watchdog": {
        "path": "data/watchdog_status.json",
        "schema_version": 1,
        "max_age_seconds": 45,
        "status_field": "state",
        "allowed_statuses": {"starting", "synchronizing", "healthy"},
    },
    "snapshot": {
        "path": "data/operator_status.json",
        "schema_version": 1,
        "max_age_seconds": 30,
        "status_field": None,
        "allowed_statuses": set(),
        "rejected_values": {"data_state": {"UNAVAILABLE"}},
    },
}


class HealthcheckError(RuntimeError):
    """Raised when a status artifact cannot prove process freshness."""


def _parse_utc(value: Any) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise HealthcheckError("generated_at_utc is missing")
    text = value.strip().replace("Z", "+00:00")
    try:
        timestamp = datetime.fromisoformat(text)
    except ValueError as exc:
        raise HealthcheckError("generated_at_utc is invalid") from exc
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise HealthcheckError("generated_at_utc must be timezone-aware")
    return timestamp.astimezone(timezone.utc)


def check_status_file(
    path: str | os.PathLike[str],
    *,
    schema_version: int,
    max_age_seconds: float,
    status_field: str | None = None,
    allowed_statuses: set[str] | frozenset[str] = frozenset(),
    rejected_values: Mapping[str, set[Any] | frozenset[Any]] | None = None,
    now: datetime | None = None,
) -> Mapping[str, Any]:
    """Validate a bounded, current JSON status record without mutating it."""

    if (
        isinstance(max_age_seconds, bool)
        or not isinstance(max_age_seconds, (int, float))
        or not math.isfinite(float(max_age_seconds))
        or max_age_seconds <= 0
    ):
        raise HealthcheckError("max_age_seconds must be positive and finite")
    status_path = Path(path)
    try:
        size = status_path.stat().st_size
        if size <= 0 or size > 1_000_000:
            raise HealthcheckError("status artifact has an unsafe size")
        payload = json.loads(status_path.read_text(encoding="utf-8"))
    except HealthcheckError:
        raise
    except (OSError, json.JSONDecodeError) as exc:
        raise HealthcheckError(
            f"status artifact is unavailable: {type(exc).__name__}"
        ) from exc
    if not isinstance(payload, Mapping):
        raise HealthcheckError("status artifact must be a JSON object")
    if payload.get("schema_version") != schema_version:
        raise HealthcheckError("status artifact schema is not supported")
    generated = _parse_utc(payload.get("generated_at_utc"))
    observed = datetime.now(timezone.utc) if now is None else now
    if observed.tzinfo is None or observed.utcoffset() is None:
        raise HealthcheckError("healthcheck now must be timezone-aware")
    age = (observed.astimezone(timezone.utc) - generated).total_seconds()
    if age < -5:
        raise HealthcheckError("status artifact timestamp is in the future")
    if age > float(max_age_seconds):
        raise HealthcheckError("status artifact is stale")
    if status_field is not None:
        value = payload.get(status_field)
        if value not in allowed_statuses:
            raise HealthcheckError(f"status artifact reports unsafe {status_field}")
    for field_name, forbidden in (rejected_values or {}).items():
        if payload.get(field_name) in forbidden:
            raise HealthcheckError(
                f"status artifact reports unsafe {field_name}"
            )
    return payload


def run_probe(name: str, *, base_dir: str | os.PathLike[str] = ".") -> None:
    try:
        config = PROBES[name]
    except KeyError as exc:
        raise HealthcheckError(f"unknown health probe {name!r}") from exc
    configured_path = os.getenv(f"BITMEX_{name.upper()}_STATUS_PATH")
    path = Path(configured_path) if configured_path else Path(base_dir) / config["path"]
    check_status_file(
        path,
        schema_version=config["schema_version"],
        max_age_seconds=config["max_age_seconds"],
        status_field=config["status_field"],
        allowed_statuses=config["allowed_statuses"],
        rejected_values=config.get("rejected_values"),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("probe", choices=sorted(PROBES))
    parser.add_argument("--base-dir", default=".")
    args = parser.parse_args()
    try:
        run_probe(args.probe, base_dir=args.base_dir)
    except HealthcheckError as exc:
        print(f"UNHEALTHY: {exc}")
        return 1
    print("HEALTHY")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["HealthcheckError", "PROBES", "check_status_file", "run_probe"]
