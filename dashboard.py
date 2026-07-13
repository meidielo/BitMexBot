"""Read-only WSGI operator dashboard for a sanitized JSON snapshot.

The public web process deliberately has no database, log, exchange, trading,
or dotenv imports.  Gunicorn imports ``app`` from this module in production.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import os
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any, TypeVar

from flask import Flask, Response, current_app, jsonify, render_template_string, request
from werkzeug.security import check_password_hash


SNAPSHOT_SCHEMA_VERSION = 1
DEFAULT_SNAPSHOT_PATH = "data/operator_dashboard/operator_status.json"
DEFAULT_SNAPSHOT_MAX_AGE_SECONDS = 120
DEFAULT_SNAPSHOT_MAX_BYTES = 256 * 1024

_ENVIRONMENTS = frozenset(
    {"TESTNET", "DRY_RUN", "MIXED_NON_PRODUCTION", "UNKNOWN"}
)
_DATA_STATES = frozenset({"OK", "DEGRADED", "UNAVAILABLE"})
_RUNNER_STATES = frozenset(
    {
        "RUNNING",
        "WAITING",
        "PAUSED",
        "MANUAL_HALT",
        "FAILED",
        "STARTING",
        "STOPPED",
        "HALTED",
        "ERROR",
        "STALE",
        "UNKNOWN",
    }
)
_WATCHDOG_STATES = frozenset(
    {
        "STARTING",
        "SYNCHRONIZING",
        "HEALTHY",
        "DEGRADED",
        "ALERT",
        "DISABLED",
        "FAILED",
        "STALE",
        "UNKNOWN",
    }
)
_PROTECTION_STATES = frozenset(
    {"PROTECTED", "STOP_ONLY", "UNPROTECTED", "MANUAL_HALT", "FLAT", "UNKNOWN"}
)
_RISK_STATES = frozenset({"CURRENT", "STALE", "UNAVAILABLE"})
_EVIDENCE_STATES = frozenset({"VALID", "INVALID", "UNAVAILABLE"})
_READINESS_VERDICTS = frozenset({"NOT_READY", "CANARY_REVIEW_ONLY"})
_PROMOTION_STAGES = frozenset(
    {"RESEARCH", "SHADOW", "TESTNET_ENGINEERING", "MAINNET_DRY_RUN", "CANARY_REVIEW"}
)
_INTENT_STATUSES = frozenset(
    {
        "REGISTERED",
        "ENTRY_PENDING",
        "ENTRY_PARTIAL",
        "ENTRY_FILLED",
        "PROTECTED",
        "PROTECTED_NO_TP",
        "CLOSED",
        "FAILED_FLAT",
        "HALTED_MANUAL",
    }
)
_WARNINGS = frozenset(
    {
        "LEDGER_UNAVAILABLE",
        "DAILY_LOSS_UNAVAILABLE",
        "DAILY_LOSS_STALE",
        "HEARTBEAT_UNAVAILABLE",
        "HEARTBEAT_STALE",
        "WATCHDOG_UNAVAILABLE",
        "WATCHDOG_STALE",
        "WATCHDOG_ATTENTION",
        "PROMOTION_EVIDENCE_UNAVAILABLE",
        "PROMOTION_EVIDENCE_INVALID",
        "MANUAL_HALT_PRESENT",
        "UNPROTECTED_INTENT_PRESENT",
        "STOP_ONLY_PROTECTION",
        "RUNNER_ATTENTION",
    }
)
_SYMBOL_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_./:-]{0,31}\Z")
_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}\Z")

_STYLE = """
:root{color-scheme:dark;--bg:#071016;--panel:#0e1a22;--line:#203441;--text:#e8f1f5;--muted:#91a7b4;--cyan:#45d8d0;--amber:#f4be5b;--red:#ff6d72;--green:#65d68b}
*{box-sizing:border-box}
body{margin:0;background:radial-gradient(circle at 80% 0,#12303b 0,transparent 36%),var(--bg);color:var(--text);font-family:ui-monospace,SFMono-Regular,Consolas,"Liberation Mono",monospace;line-height:1.45}
main{width:min(1180px,calc(100% - 32px));margin:0 auto;padding:28px 0 52px}
header{display:flex;gap:20px;align-items:flex-start;justify-content:space-between;margin-bottom:22px}
h1{font-family:system-ui,sans-serif;font-size:clamp(1.7rem,4vw,2.7rem);letter-spacing:-.04em;margin:.25rem 0}
h2{font-family:system-ui,sans-serif;font-size:1rem;margin:0 0 14px}
p{margin:.35rem 0;color:var(--muted)}
.eyebrow{color:var(--cyan);font-size:.78rem;letter-spacing:.16em;text-transform:uppercase}
.notice{border:1px solid #795c22;background:#2b210e;color:#ffe0a0;border-radius:12px;padding:11px 14px;max-width:390px;font-size:.8rem}
.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:12px}
.card{background:linear-gradient(145deg,rgba(18,34,44,.96),rgba(10,22,29,.96));border:1px solid var(--line);border-radius:14px;padding:16px;min-width:0;box-shadow:0 14px 35px rgba(0,0,0,.16)}
.label{color:var(--muted);font-size:.72rem;letter-spacing:.11em;text-transform:uppercase}
.value{font-family:system-ui,sans-serif;font-weight:700;font-size:1.15rem;margin-top:7px;overflow-wrap:anywhere}
.meta{color:var(--muted);font-size:.72rem;margin-top:6px;overflow-wrap:anywhere}
.pill{display:inline-flex;align-items:center;border:1px solid var(--line);border-radius:999px;padding:4px 8px;font-size:.7rem;color:var(--cyan);background:#0b2027}
.warning{color:var(--amber);border-color:#6b5425;background:#281f0e}
.danger{color:var(--red);border-color:#653039;background:#281318}
.section{margin-top:12px}
.section-head{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:12px}
.warnings{display:flex;flex-wrap:wrap;gap:7px}
.table-wrap{overflow-x:auto;border:1px solid var(--line);border-radius:10px}
table{border-collapse:collapse;width:100%;min-width:760px;font-size:.78rem}
th,td{text-align:left;padding:10px 12px;border-bottom:1px solid var(--line);white-space:nowrap}
th{color:var(--muted);font-weight:500;letter-spacing:.06em;text-transform:uppercase;font-size:.68rem;background:#0a171e}
tr:last-child td{border-bottom:0}
.empty{color:var(--muted);padding:24px;text-align:center}
footer{margin-top:18px;color:var(--muted);font-size:.72rem;text-align:center}
@media(max-width:920px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}header{flex-direction:column}.notice{max-width:none;width:100%}}
@media(max-width:560px){main{width:min(100% - 20px,1180px);padding-top:18px}.grid{grid-template-columns:1fr}.card{padding:14px}}
"""
_STYLE_HASH = base64.b64encode(hashlib.sha256(_STYLE.encode("utf-8")).digest()).decode(
    "ascii"
)
_CSP = (
    "default-src 'none'; "
    f"style-src 'sha256-{_STYLE_HASH}'; "
    "script-src 'none'; img-src 'self'; connect-src 'self'; font-src 'none'; "
    "object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'"
)

_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta name="robots" content="noindex,nofollow,noarchive">
  <meta http-equiv="refresh" content="30">
  <title>BitMexBot Operator View</title>
  <style>{{ style|safe }}</style>
</head>
<body>
<main>
  <header>
    <div>
      <div class="eyebrow">Read-only operator view</div>
      <h1>BitMexBot safety telemetry</h1>
      <p>Sanitized status snapshot. Refreshes every 30 seconds.</p>
    </div>
    <div class="notice"><strong>TESTNET RESEARCH ONLY</strong><br>No trading controls are exposed. Real-funds execution remains disabled.</div>
  </header>

  <section class="grid" aria-label="Primary status">
    <article class="card"><div class="label">Snapshot</div><div class="value">{{ "FRESH" if snapshot_fresh else "STALE" }}</div><div class="meta">{{ snapshot.generated_at_utc }}</div></article>
    <article class="card"><div class="label">Data state</div><div class="value">{{ snapshot.data_state }}</div><div class="meta">Schema {{ snapshot.schema_version }}</div></article>
    <article class="card"><div class="label">Environment</div><div class="value">{{ snapshot.environment }}</div><div class="meta">Production enabled: NO</div></article>
    <article class="card"><div class="label">Protection</div><div class="value">{{ snapshot.ledger.protection_state }}</div><div class="meta">{{ snapshot.ledger.open_intents }} active, {{ snapshot.ledger.manual_halts }} manual halt</div></article>
  </section>

  <section class="grid" aria-label="Operational status">
    <article class="card"><div class="label">Runner</div><div class="value">{{ snapshot.runner.state }}</div><div class="meta">{{ snapshot.runner.heartbeat_at_utc or "No trusted heartbeat" }}</div></article>
    <article class="card"><div class="label">Watchdog</div><div class="value">{{ snapshot.watchdog.state }}</div><div class="meta">{{ snapshot.watchdog.checked_at_utc or "No trusted watchdog state" }}</div></article>
    <article class="card"><div class="label">Daily loss</div><div class="value">{% if snapshot.risk.daily_loss_usdt is not none %}{{ "%.4f"|format(snapshot.risk.daily_loss_usdt) }} USDT{% else %}UNAVAILABLE{% endif %}</div><div class="meta">{{ snapshot.risk.state }}{% if snapshot.risk.date %} · {{ snapshot.risk.date }}{% endif %}</div></article>
    <article class="card"><div class="label">Promotion</div><div class="value">{{ snapshot.readiness.stage }}</div><div class="meta">{{ snapshot.readiness.verdict }} · {{ snapshot.readiness.blocker_count }} blocker(s)</div></article>
  </section>

  <section class="card section" aria-labelledby="warning-heading">
    <div class="section-head"><h2 id="warning-heading">Attention signals</h2><span class="pill">{{ snapshot.warnings|length }} warning(s)</span></div>
    <div class="warnings">
      {% for warning in snapshot.warnings %}<span class="pill warning">{{ warning }}</span>{% else %}<span class="pill">NO_TELEMETRY_WARNINGS</span>{% endfor %}
    </div>
  </section>

  <section class="card section" aria-labelledby="intent-heading">
    <div class="section-head"><h2 id="intent-heading">Recent sanitized intents</h2><span class="pill">{{ snapshot.ledger.total_intents }} total</span></div>
    {% if snapshot.ledger.recent_intents %}
    <div class="table-wrap"><table>
      <thead><tr><th>Environment</th><th>Symbol</th><th>Side</th><th>Status</th><th>Requested</th><th>Filled</th><th>Closed</th><th>Net PnL</th><th>Updated UTC</th></tr></thead>
      <tbody>{% for intent in snapshot.ledger.recent_intents %}<tr><td>{{ intent.environment }}</td><td>{{ intent.symbol }}</td><td>{{ intent.side }}</td><td>{{ intent.status }}</td><td>{{ intent.requested_contracts }}</td><td>{{ intent.filled_contracts }}</td><td>{{ intent.closed_contracts }}</td><td>{% if intent.net_pnl_usdt is none %}--{% else %}{{ "%.4f"|format(intent.net_pnl_usdt) }}{% endif %}</td><td>{{ intent.updated_at_utc }}</td></tr>{% endfor %}</tbody>
    </table></div>
    {% else %}<div class="empty">No sanitized execution intents are available.</div>{% endif %}
  </section>
  <footer>Observation only. Use the private runbook and exchange console for operator actions.</footer>
</main>
</body>
</html>"""


class SnapshotValidationError(RuntimeError):
    """Raised when the public snapshot cannot be trusted."""


@dataclass(frozen=True)
class SnapshotView:
    payload: dict[str, Any]
    fresh: bool


F = TypeVar("F", bound=Callable[..., Response | str])


def _positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):  # agent-quality: allow: invalid public configuration is returned as unavailable
        return None
    return parsed if parsed > 0 else None


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SnapshotValidationError("object expected")
    return value


def _enum(mapping: Mapping[str, Any], key: str, allowed: frozenset[str]) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or value not in allowed:
        raise SnapshotValidationError("enum value rejected")
    return value


def _boolean(mapping: Mapping[str, Any], key: str) -> bool:
    value = mapping.get(key)
    if not isinstance(value, bool):
        raise SnapshotValidationError("boolean expected")
    return value


def _bounded_int(mapping: Mapping[str, Any], key: str, maximum: int = 1_000_000_000) -> int:
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise SnapshotValidationError("bounded integer expected")
    return value


def _number(mapping: Mapping[str, Any], key: str, *, nullable: bool = False) -> float | None:
    value = mapping.get(key)
    if value is None and nullable:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SnapshotValidationError("number expected")
    result = float(value)
    if not math.isfinite(result) or abs(result) > 1_000_000_000_000:
        raise SnapshotValidationError("finite bounded number expected")
    return result


def _parse_timestamp(value: Any, *, nullable: bool = False) -> tuple[str | None, datetime | None]:
    if value is None and nullable:
        return None, None
    if not isinstance(value, str) or not value or len(value) > 64:
        raise SnapshotValidationError("timestamp rejected")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SnapshotValidationError("timestamp rejected") from exc
    if parsed.tzinfo is None:
        raise SnapshotValidationError("timezone required")
    normalized = parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return normalized, parsed.astimezone(timezone.utc)


def _sanitize_intent(value: Any) -> dict[str, Any]:
    intent = _mapping(value)
    environment = _enum(intent, "environment", frozenset({"TESTNET", "DRY_RUN"}))
    symbol = intent.get("symbol")
    if not isinstance(symbol, str) or not _SYMBOL_PATTERN.fullmatch(symbol):
        raise SnapshotValidationError("symbol rejected")
    requested = _bounded_int(intent, "requested_contracts")
    filled = _bounded_int(intent, "filled_contracts")
    closed = _bounded_int(intent, "closed_contracts")
    if requested <= 0 or filled > requested or closed > filled:
        raise SnapshotValidationError("contract invariant rejected")
    expected_loss = _number(intent, "expected_max_loss_usdt")
    if expected_loss is None or expected_loss <= 0:
        raise SnapshotValidationError("loss evidence rejected")
    created, _ = _parse_timestamp(intent.get("created_at_utc"))
    updated, _ = _parse_timestamp(intent.get("updated_at_utc"))
    return {
        "environment": environment,
        "symbol": symbol,
        "side": _enum(intent, "side", frozenset({"LONG", "SHORT"})),
        "status": _enum(intent, "status", _INTENT_STATUSES),
        "requested_contracts": requested,
        "filled_contracts": filled,
        "closed_contracts": closed,
        "expected_max_loss_usdt": expected_loss,
        "net_pnl_usdt": _number(intent, "net_pnl_usdt", nullable=True),
        "created_at_utc": created,
        "updated_at_utc": updated,
    }


def _sanitize_timed_state(
    value: Any,
    *,
    allowed_states: frozenset[str],
    timestamp_key: str,
) -> dict[str, Any]:
    state = _mapping(value)
    normalized_timestamp, _ = _parse_timestamp(
        state.get(timestamp_key), nullable=True
    )
    clean = {
        "state": _enum(state, "state", allowed_states),
        timestamp_key: normalized_timestamp,
        "fresh": _boolean(state, "fresh"),
    }
    if clean["fresh"] and (
        normalized_timestamp is None or clean["state"] in {"STALE", "UNKNOWN"}
    ):
        raise SnapshotValidationError("fresh timed state is inconsistent")
    return clean


def _sanitize_snapshot(value: Any) -> tuple[dict[str, Any], datetime]:
    source = _mapping(value)
    if source.get("schema_version") != SNAPSHOT_SCHEMA_VERSION:
        raise SnapshotValidationError("snapshot schema rejected")
    generated, generated_dt = _parse_timestamp(source.get("generated_at_utc"))
    if generated_dt is None:
        raise SnapshotValidationError("snapshot timestamp missing")
    if source.get("production_enabled") is not False or source.get("actions_enabled") is not False:
        raise SnapshotValidationError("unsafe enablement flag rejected")

    runner = _sanitize_timed_state(
        source.get("runner"),
        allowed_states=_RUNNER_STATES,
        timestamp_key="heartbeat_at_utc",
    )
    watchdog = _sanitize_timed_state(
        source.get("watchdog"),
        allowed_states=_WATCHDOG_STATES,
        timestamp_key="checked_at_utc",
    )

    ledger_source = _mapping(source.get("ledger"))
    recent_source = ledger_source.get("recent_intents")
    if not isinstance(recent_source, list) or len(recent_source) > 20:
        raise SnapshotValidationError("recent intent list rejected")
    recent = [_sanitize_intent(item) for item in recent_source]
    latest_source = ledger_source.get("latest_intent")
    latest = None if latest_source is None else _sanitize_intent(latest_source)
    if recent and latest != recent[0]:
        raise SnapshotValidationError("latest intent is inconsistent")
    if not recent and latest is not None:
        raise SnapshotValidationError("latest intent is inconsistent")
    ledger = {
        "available": _boolean(ledger_source, "available"),
        "total_intents": _bounded_int(ledger_source, "total_intents"),
        "open_intents": _bounded_int(ledger_source, "open_intents"),
        "manual_halts": _bounded_int(ledger_source, "manual_halts"),
        "protection_state": _enum(ledger_source, "protection_state", _PROTECTION_STATES),
        "latest_intent": latest,
        "recent_intents": recent,
    }
    if not ledger["available"] and (
        ledger["total_intents"] != 0
        or ledger["open_intents"] != 0
        or ledger["manual_halts"] != 0
        or ledger["protection_state"] != "UNKNOWN"
        or recent
    ):
        raise SnapshotValidationError("unavailable ledger is inconsistent")

    risk_source = _mapping(source.get("risk"))
    risk_state = _enum(risk_source, "state", _RISK_STATES)
    risk_date = risk_source.get("date")
    if risk_date is not None and (
        not isinstance(risk_date, str) or not _DATE_PATTERN.fullmatch(risk_date)
    ):
        raise SnapshotValidationError("risk date rejected")
    daily_loss = _number(risk_source, "daily_loss_usdt", nullable=True)
    risk_fresh = _boolean(risk_source, "fresh")
    if daily_loss is not None and daily_loss < 0:
        raise SnapshotValidationError("negative loss rejected")
    if risk_state == "UNAVAILABLE":
        if risk_date is not None or daily_loss is not None or risk_fresh:
            raise SnapshotValidationError("unavailable risk state is inconsistent")
    elif risk_date is None or daily_loss is None or risk_fresh != (risk_state == "CURRENT"):
        raise SnapshotValidationError("risk state is inconsistent")
    risk = {
        "state": risk_state,
        "date": risk_date,
        "daily_loss_usdt": daily_loss,
        "fresh": risk_fresh,
    }

    readiness_source = _mapping(source.get("readiness"))
    readiness = {
        "evidence_state": _enum(readiness_source, "evidence_state", _EVIDENCE_STATES),
        "verdict": _enum(readiness_source, "verdict", _READINESS_VERDICTS),
        "stage": _enum(readiness_source, "stage", _PROMOTION_STAGES),
        "blocker_count": _bounded_int(readiness_source, "blocker_count", maximum=999),
        "human_approval_required": _boolean(readiness_source, "human_approval_required"),
        "production_enabled": _boolean(readiness_source, "production_enabled"),
    }
    if not readiness["human_approval_required"] or readiness["production_enabled"]:
        raise SnapshotValidationError("readiness safety invariant rejected")

    warnings_source = source.get("warnings")
    if not isinstance(warnings_source, list) or len(warnings_source) > 20:
        raise SnapshotValidationError("warning list rejected")
    if any(not isinstance(item, str) or item not in _WARNINGS for item in warnings_source):
        raise SnapshotValidationError("warning code rejected")
    warnings = list(dict.fromkeys(warnings_source))
    if len(warnings) != len(warnings_source):
        raise SnapshotValidationError("duplicate warning rejected")

    data_state = _enum(source, "data_state", _DATA_STATES)
    if not ledger["available"] and data_state != "UNAVAILABLE":
        raise SnapshotValidationError("data state is inconsistent")
    if warnings and data_state == "OK":
        raise SnapshotValidationError("warning state is inconsistent")

    return (
        {
            "schema_version": SNAPSHOT_SCHEMA_VERSION,
            "generated_at_utc": generated,
            "data_state": data_state,
            "environment": _enum(source, "environment", _ENVIRONMENTS),
            "production_enabled": False,
            "actions_enabled": False,
            "runner": runner,
            "watchdog": watchdog,
            "ledger": ledger,
            "risk": risk,
            "readiness": readiness,
            "warnings": warnings,
        },
        generated_dt,
    )


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON constant rejected: {value}")


def _load_snapshot(
    path: str | os.PathLike[str],
    *,
    max_bytes: int,
    max_age_seconds: int,
    now: datetime | None = None,
) -> SnapshotView:
    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            raw = handle.read(max_bytes + 1)
    except (OSError, UnicodeError) as exc:
        raise SnapshotValidationError("snapshot unavailable") from exc
    if len(raw.encode("utf-8")) > max_bytes:
        raise SnapshotValidationError("snapshot too large")
    try:
        parsed = json.loads(raw, parse_constant=_reject_json_constant)
    except (json.JSONDecodeError, ValueError) as exc:
        raise SnapshotValidationError("snapshot JSON rejected") from exc
    payload, generated = _sanitize_snapshot(parsed)
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    age = (current - generated).total_seconds()
    fresh = -30.0 <= age <= max_age_seconds
    payload["snapshot_fresh"] = fresh
    return SnapshotView(payload=payload, fresh=fresh)


def _auth_configuration_valid(app: Flask) -> bool:
    user = app.config.get("DASH_USER")
    password_hash = app.config.get("DASH_PASSWORD_HASH")
    if not isinstance(user, str) or not 1 <= len(user) <= 128:
        return False
    if not isinstance(password_hash, str) or not 1 <= len(password_hash) <= 512:
        return False
    parts = password_hash.split("$")
    return len(parts) == 3 and (
        parts[0].startswith("scrypt:") or parts[0].startswith("pbkdf2:")
    )


def _unauthorized() -> Response:
    response = jsonify(status="authentication_required")
    response.status_code = 401
    response.headers["WWW-Authenticate"] = 'Basic realm="BitMexBot Operator", charset="UTF-8"'
    return response


def _service_unavailable() -> tuple[Response, int]:
    return jsonify(status="unavailable"), 503


def _require_basic_auth(view: F) -> F:
    @wraps(view)
    def wrapped(*args: Any, **kwargs: Any) -> Response | str:
        if not _auth_configuration_valid(current_app):
            return _service_unavailable()
        auth = request.authorization
        if auth is None or str(auth.type).lower() != "basic":
            return _unauthorized()
        supplied_user = auth.username or ""
        supplied_password = auth.password or ""
        if len(supplied_user) > 128 or len(supplied_password) > 1024:
            return _unauthorized()
        expected_user = current_app.config["DASH_USER"]
        username_ok = hmac.compare_digest(
            supplied_user.encode("utf-8"), expected_user.encode("utf-8")
        )
        try:
            password_ok = check_password_hash(
                current_app.config["DASH_PASSWORD_HASH"], supplied_password
            )
        except (TypeError, ValueError):  # agent-quality: allow: malformed hashes fail closed as service unavailable
            return _service_unavailable()
        if not (username_ok and password_ok):
            return _unauthorized()
        return view(*args, **kwargs)

    return wrapped  # type: ignore[return-value]


def _snapshot_from_app(app: Flask) -> SnapshotView:
    max_bytes = _positive_int(app.config.get("SNAPSHOT_MAX_BYTES"))
    max_age = _positive_int(app.config.get("SNAPSHOT_MAX_AGE_SECONDS"))
    path = app.config.get("SNAPSHOT_PATH")
    if max_bytes is None or max_age is None or not isinstance(path, (str, os.PathLike)):
        raise SnapshotValidationError("dashboard configuration rejected")
    return _load_snapshot(path, max_bytes=max_bytes, max_age_seconds=max_age)


def create_app(config: Mapping[str, Any] | None = None) -> Flask:
    app = Flask(__name__, static_folder=None)
    app.config.from_mapping(
        DASH_USER=os.environ.get("DASH_USER", ""),
        DASH_PASSWORD_HASH=os.environ.get("DASH_PASSWORD_HASH", ""),
        SNAPSHOT_PATH=os.environ.get("DASH_SNAPSHOT_PATH", DEFAULT_SNAPSHOT_PATH),
        SNAPSHOT_MAX_AGE_SECONDS=os.environ.get(
            "DASH_SNAPSHOT_MAX_AGE_SECONDS", str(DEFAULT_SNAPSHOT_MAX_AGE_SECONDS)
        ),
        SNAPSHOT_MAX_BYTES=os.environ.get(
            "DASH_SNAPSHOT_MAX_BYTES", str(DEFAULT_SNAPSHOT_MAX_BYTES)
        ),
        JSON_SORT_KEYS=True,
    )
    if config:
        app.config.update(config)

    @app.before_request
    def get_only() -> tuple[Response, int] | None:
        if request.method != "GET":
            return jsonify(status="method_not_allowed"), 405
        return None

    @app.after_request
    def security_headers(response: Response) -> Response:
        response.headers["Cache-Control"] = "no-store, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        response.headers["Content-Security-Policy"] = _CSP
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Permissions-Policy"] = (
            "accelerometer=(), camera=(), geolocation=(), gyroscope=(), "
            "microphone=(), payment=(), usb=()"
        )
        response.headers["Cross-Origin-Opener-Policy"] = "same-origin"
        response.headers["Cross-Origin-Resource-Policy"] = "same-origin"
        response.headers["Vary"] = "Authorization"
        response.headers.pop("Access-Control-Allow-Origin", None)
        return response

    @app.errorhandler(404)
    def not_found(_error: Any) -> tuple[Response, int]:
        return jsonify(status="not_found"), 404

    @app.errorhandler(405)
    def method_not_allowed(_error: Any) -> tuple[Response, int]:
        return jsonify(status="method_not_allowed"), 405

    @app.get("/healthz", provide_automatic_options=False)
    def healthz() -> Response:
        return jsonify(status="ok")

    @app.get("/readyz", provide_automatic_options=False)
    def readyz() -> tuple[Response, int] | Response:
        if not _auth_configuration_valid(app):
            return jsonify(status="unready"), 503
        try:
            snapshot = _snapshot_from_app(app)
        except SnapshotValidationError:  # agent-quality: allow: health endpoint intentionally exposes only generic unready state
            return jsonify(status="unready"), 503
        if not snapshot.fresh or snapshot.payload["data_state"] == "UNAVAILABLE":
            return jsonify(status="unready"), 503
        return jsonify(status="ready")

    @app.get("/api/v1/status", provide_automatic_options=False)
    @_require_basic_auth
    def status_api() -> tuple[Response, int] | Response:
        try:
            snapshot = _snapshot_from_app(app)
        except SnapshotValidationError:  # agent-quality: allow: public API intentionally exposes only generic unavailable state
            return _service_unavailable()
        return jsonify(snapshot.payload)

    @app.get("/", provide_automatic_options=False)
    @_require_basic_auth
    def index() -> tuple[Response, int] | str:
        try:
            snapshot = _snapshot_from_app(app)
        except SnapshotValidationError:  # agent-quality: allow: public page intentionally exposes only generic unavailable state
            return _service_unavailable()
        return render_template_string(
            _HTML,
            style=_STYLE,
            snapshot=snapshot.payload,
            snapshot_fresh=snapshot.fresh,
        )

    return app


app = create_app()
