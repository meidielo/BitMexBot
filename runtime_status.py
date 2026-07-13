"""Atomic, sanitized runtime heartbeat for the operator snapshot exporter.

The heartbeat deliberately contains no exchange payloads, order identifiers,
credentials, balances, prices, or free-form exception text. It is safe to
mount read-only into the snapshot exporter, but it is not itself an
authorization or readiness signal.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from typing import Any


STATUS_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "runner_status.json"
)
SCHEMA_VERSION = 1
ALLOWED_STATUSES = frozenset(
    {
        "STARTING",
        "RUNNING",
        "WAITING",
        "PAUSED",
        "MANUAL_HALT",
        "FAILED",
        "STOPPED",
    }
)


class RuntimeStatusError(RuntimeError):
    """Raised when a sanitized heartbeat cannot be validated or persisted."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_timestamp(value: datetime | None) -> str:
    timestamp = _utc_now() if value is None else value
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise RuntimeStatusError("runtime heartbeat timestamps must be timezone-aware")
    return timestamp.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def build_runner_status(
    status: str,
    *,
    detail_code: str,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    """Build one allowlisted heartbeat record.

    ``detail_code`` is restricted to a short machine token. Callers must not
    pass exception messages or exchange responses.
    """

    normalized_status = str(status).strip().upper()
    if normalized_status not in ALLOWED_STATUSES:
        raise RuntimeStatusError(f"unsupported runtime status {status!r}")
    normalized_detail = str(detail_code).strip().lower()
    if (
        not normalized_detail
        or len(normalized_detail) > 64
        or any(
            character not in "abcdefghijklmnopqrstuvwxyz0123456789_-"
            for character in normalized_detail
        )
    ):
        raise RuntimeStatusError("detail_code must be a 1-64 character machine token")
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": _normalize_timestamp(observed_at),
        "environment": "testnet",
        "status": normalized_status,
        "detail_code": normalized_detail,
    }


def write_runner_status(
    status: str,
    *,
    detail_code: str,
    path: str = STATUS_PATH,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    """Atomically replace the heartbeat file and return its sanitized record."""

    record = build_runner_status(
        status,
        detail_code=detail_code,
        observed_at=observed_at,
    )
    absolute_path = os.path.abspath(path)
    directory = os.path.dirname(absolute_path)
    descriptor: int | None = None
    temporary_path: str | None = None
    try:
        os.makedirs(directory, exist_ok=True)
        descriptor, temporary_path = tempfile.mkstemp(
            prefix=".runner_status-",
            suffix=".json",
            dir=directory,
            text=True,
        )
        handle = os.fdopen(descriptor, "w", encoding="utf-8", newline="\n")
        descriptor = None
        with handle:
            json.dump(record, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.chmod(temporary_path, 0o644)
        except OSError:  # agent-quality: allow: chmod is best-effort on Windows where POSIX modes are unavailable
            # Windows does not provide meaningful POSIX file modes.
            pass
        os.replace(temporary_path, absolute_path)
    except Exception as exc:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:  # agent-quality: allow: preserve the original explicit persistence failure if descriptor cleanup also fails
                pass
        try:
            if temporary_path is not None:
                os.unlink(temporary_path)
        except OSError:  # agent-quality: allow: preserve the original explicit persistence failure if cleanup also fails
            pass
        raise RuntimeStatusError(
            f"could not persist sanitized runtime heartbeat: {type(exc).__name__}"
        ) from exc
    return record


__all__ = [
    "ALLOWED_STATUSES",
    "RuntimeStatusError",
    "SCHEMA_VERSION",
    "STATUS_PATH",
    "build_runner_status",
    "write_runner_status",
]
