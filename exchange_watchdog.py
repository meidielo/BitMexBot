"""Independent, read-only BitMEX Testnet WebSocket watchdog.

This process observes authenticated account tables and publishes a deliberately
small local status document.  It has no order, cancellation, or dead-man-switch
commands and supports only the literal BitMEX Testnet WebSocket endpoint.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import aiohttp
from dotenv import load_dotenv


_LOGGER = logging.getLogger(__name__)

TESTNET_WSS_URL = "wss://ws.testnet.bitmex.com/realtime"
WATCHED_SYMBOL = "XBTUSDT"
SUBSCRIPTIONS = ("order", "position", "execution", "margin")
SCHEMA_VERSION = 1
STATUS_PATH = Path(__file__).resolve().parent / "data" / "watchdog_status.json"

AUTH_TTL_SECONDS = 60
CONNECT_TIMEOUT_SECONDS = 10
SYNC_TIMEOUT_SECONDS = 30
IDLE_PING_SECONDS = 15
PONG_TIMEOUT_SECONDS = 5
STALE_AFTER_SECONDS = 30
STATUS_WRITE_INTERVAL_SECONDS = 2
ALERT_TIMEOUT_SECONDS = 5
ALERT_COOLDOWN_SECONDS = 300
BACKOFF_INITIAL_SECONDS = 1
BACKOFF_MAX_SECONDS = 30
MAX_MESSAGE_BYTES = 1_048_576
MAX_BATCH_ROWS = 5_000
MAX_TABLE_ROWS = 20_000

_TERMINAL_ORDER_STATUSES = frozenset(
    {"Filled", "Canceled", "Rejected", "Expired", "Stopped"}
)
_RETAINED_FIELDS = {
    "order": frozenset({"symbol", "ordStatus", "leavesQty"}),
    "position": frozenset({"symbol", "currentQty"}),
    "execution": frozenset(),
    "margin": frozenset(),
}
_STATUS_STATES = frozenset(
    {"starting", "synchronizing", "healthy", "stale", "failed"}
)
_EVENT_TYPES = frozenset(
    {
        "startup",
        "connected",
        "synchronized",
        "update",
        "stale",
        "auth_error",
        "subscription_error",
        "protocol_error",
        "transport_error",
    }
)
_ALERT_EVENT_TYPES = frozenset(
    {
        "stale",
        "auth_error",
        "subscription_error",
        "protocol_error",
        "transport_error",
    }
)
_STATUS_FIELDS = frozenset(
    {
        "schema_version",
        "environment",
        "state",
        "generated_at_utc",
        "last_message_at_utc",
        "freshness_seconds",
        "fresh",
        "subscriptions",
        "position_contracts",
        "open_order_count",
        "last_event_type",
    }
)


class WatchdogError(RuntimeError):
    """Base class for sanitized watchdog failures."""


class WatchdogProtocolError(WatchdogError):
    """The server sent a malformed or internally inconsistent message."""


class WatchdogSubscriptionError(WatchdogError):
    """An expected private table did not subscribe and synchronize."""


class WatchdogStaleError(WatchdogError):
    """The connection stopped proving freshness."""


class WatchdogTransportError(WatchdogError):
    """The WebSocket transport closed or returned an unsupported frame."""


class WatchdogPublishError(WatchdogError):
    """The sanitized status document could not be published atomically."""


class WatchdogAlertError(WatchdogError):
    """The optional alert could not be delivered safely."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise WatchdogProtocolError("watchdog timestamp must be timezone-aware")
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def require_testnet_credentials(
    env: Mapping[str, str] | None = None,
) -> tuple[str, str]:
    """Return only dedicated Testnet credentials after the literal gate."""

    values = os.environ if env is None else env
    if values.get("BITMEX_TESTNET") != "true":
        raise EnvironmentError(
            "BITMEX_TESTNET must be exactly 'true' for the Testnet watchdog"
        )
    api_key = values.get("BITMEX_TESTNET_API_KEY", "")
    api_secret = values.get("BITMEX_TESTNET_API_SECRET", "")
    if not _credential_is_valid(api_key) or not _credential_is_valid(api_secret):
        raise EnvironmentError(
            "dedicated BITMEX_TESTNET_API_KEY and BITMEX_TESTNET_API_SECRET "
            "are required"
        )
    return api_key, api_secret


def _credential_is_valid(value: Any) -> bool:
    return (
        isinstance(value, str)
        and bool(value)
        and value == value.strip()
        and all(32 < ord(character) < 127 for character in value)
    )


def build_auth_headers(
    api_key: str,
    api_secret: str,
    expires: int,
) -> dict[str, str]:
    """Build BitMEX upgrade headers signed as ``GET /realtime``."""

    if not _credential_is_valid(api_key):
        raise ValueError("api_key must be a non-empty string")
    if not _credential_is_valid(api_secret):
        raise ValueError("api_secret must be a non-empty string")
    if isinstance(expires, bool) or not isinstance(expires, int) or expires <= 0:
        raise ValueError("expires must be a positive integer Unix timestamp")
    signature_input = f"GET/realtime{expires}".encode("utf-8")
    signature = hmac.new(
        api_secret.encode("utf-8"),
        signature_input,
        hashlib.sha256,
    ).hexdigest()
    return {
        "api-key": api_key,
        "api-expires": str(expires),
        "api-signature": signature,
    }


def _whole_number(value: Any, field_name: str, *, signed: bool = False) -> int:
    if value is None or isinstance(value, bool):
        raise WatchdogProtocolError(f"{field_name} must be a finite whole number")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise WatchdogProtocolError(
            f"{field_name} must be a finite whole number"
        ) from exc
    if not parsed.is_finite() or parsed != parsed.to_integral_value():
        raise WatchdogProtocolError(f"{field_name} must be a finite whole number")
    if not signed and parsed < 0:
        raise WatchdogProtocolError(f"{field_name} cannot be negative")
    return int(parsed)


def _validate_keys(value: Any) -> tuple[str, ...]:
    if (
        isinstance(value, (str, bytes, Mapping))
        or not isinstance(value, Sequence)
        or not value
        or len(value) > 8
    ):
        raise WatchdogProtocolError("partial table message has invalid keys")
    keys: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item or len(item) > 64:
            raise WatchdogProtocolError("partial table key is invalid")
        keys.append(item)
    if len(keys) != len(set(keys)):
        raise WatchdogProtocolError("partial table keys are duplicated")
    return tuple(keys)


def _validate_data(value: Any) -> list[Mapping[str, Any]]:
    if (
        isinstance(value, (str, bytes, Mapping))
        or not isinstance(value, Sequence)
        or len(value) > MAX_BATCH_ROWS
    ):
        raise WatchdogProtocolError("table data must be a bounded row array")
    rows: list[Mapping[str, Any]] = []
    for row in value:
        if not isinstance(row, Mapping):
            raise WatchdogProtocolError("table row must be an object")
        rows.append(row)
    return rows


def _row_key(row: Mapping[str, Any], keys: tuple[str, ...]) -> tuple[str | int, ...]:
    values: list[str | int] = []
    for key in keys:
        value = row.get(key)
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise WatchdogProtocolError("table row is missing a valid key value")
        if isinstance(value, str) and (not value or len(value) > 256):
            raise WatchdogProtocolError("table row has an invalid key value")
        values.append(value)
    return tuple(values)


@dataclass(slots=True)
class DeltaTable:
    """Conservative BitMEX table-diff state with atomic in-memory updates."""

    name: str
    keys: tuple[str, ...] = ()
    rows: dict[tuple[str | int, ...], dict[str, Any]] = field(default_factory=dict)
    initialized: bool = False

    def _project(
        self,
        row: Mapping[str, Any],
        keys: tuple[str, ...],
    ) -> dict[str, Any]:
        retained = _RETAINED_FIELDS[self.name]
        return {
            key: row[key]
            for key in (*keys, *sorted(retained))
            if key in row
        }

    def _validate_full_row(self, row: Mapping[str, Any]) -> None:
        required = _RETAINED_FIELDS[self.name]
        if not required.issubset(row):
            raise WatchdogProtocolError(
                f"{self.name} snapshot row omitted required fields"
            )

    def _validate_projected_state(
        self,
        rows: Mapping[tuple[str | int, ...], Mapping[str, Any]],
    ) -> None:
        watched_positions = 0
        for row in rows.values():
            if self.name == "order":
                symbol = row.get("symbol")
                status = row.get("ordStatus")
                if not isinstance(symbol, str) or not symbol:
                    raise WatchdogProtocolError("order symbol is invalid")
                if not isinstance(status, str) or not status:
                    raise WatchdogProtocolError("order status is invalid")
                leaves = _whole_number(row.get("leavesQty"), "order leavesQty")
                if status in _TERMINAL_ORDER_STATUSES and leaves != 0:
                    raise WatchdogProtocolError(
                        "terminal order has a non-zero remaining quantity"
                    )
            elif self.name == "position":
                symbol = row.get("symbol")
                if not isinstance(symbol, str) or not symbol:
                    raise WatchdogProtocolError("position symbol is invalid")
                _whole_number(
                    row.get("currentQty"),
                    "position contracts",
                    signed=True,
                )
                if symbol == WATCHED_SYMBOL:
                    watched_positions += 1
        if self.name == "position" and watched_positions > 1:
            raise WatchdogProtocolError(
                "multiple watched-symbol positions are ambiguous"
            )

    def apply(self, action: str, data: Any, message_keys: Any = None) -> None:
        if action not in {"partial", "insert", "update", "delete"}:
            raise WatchdogProtocolError("unsupported table action")
        rows = _validate_data(data)

        if action == "partial":
            keys = _validate_keys(message_keys)
            if self.initialized:
                raise WatchdogProtocolError("duplicate partial snapshot is invalid")
            replacement: dict[tuple[str | int, ...], dict[str, Any]] = {}
            for row in rows:
                self._validate_full_row(row)
                key = _row_key(row, keys)
                if key in replacement:
                    raise WatchdogProtocolError("partial table contains duplicate rows")
                replacement[key] = self._project(row, keys)
            if len(replacement) > MAX_TABLE_ROWS:
                raise WatchdogProtocolError("partial table exceeds the row limit")
            self._validate_projected_state(replacement)
            self.keys = keys
            self.rows = replacement
            self.initialized = True
            return

        if not self.initialized or not self.keys:
            raise WatchdogProtocolError("table delta arrived before partial snapshot")
        updated = dict(self.rows)
        for row in rows:
            key = _row_key(row, self.keys)
            if action == "insert":
                self._validate_full_row(row)
                if key in updated:
                    raise WatchdogProtocolError("insert duplicated an existing table row")
                updated[key] = self._project(row, self.keys)
            elif action == "update":
                existing = updated.get(key)
                if existing is None:
                    raise WatchdogProtocolError("update referenced an unknown table row")
                merged = dict(existing)
                merged.update(self._project(row, self.keys))
                updated[key] = merged
            else:
                if key not in updated:
                    raise WatchdogProtocolError("delete referenced an unknown table row")
                del updated[key]
        if len(updated) > MAX_TABLE_ROWS:
            raise WatchdogProtocolError("table exceeds the row limit")
        self._validate_projected_state(updated)
        self.rows = updated


@dataclass(slots=True)
class WatchdogState:
    """Connection-local account state that only emits a sanitized summary."""

    tables: dict[str, DeltaTable] = field(
        default_factory=lambda: {name: DeltaTable(name) for name in SUBSCRIPTIONS}
    )
    subscriptions: dict[str, bool] = field(
        default_factory=lambda: {name: False for name in SUBSCRIPTIONS}
    )
    last_message_monotonic: float | None = None
    last_message_at: datetime | None = None
    ever_synchronized: bool = False

    @property
    def synchronized(self) -> bool:
        return all(self.subscriptions.values()) and all(
            table.initialized for table in self.tables.values()
        )

    def _mark_message(self, now_monotonic: float, now_utc: datetime) -> None:
        if (
            isinstance(now_monotonic, bool)
            or not isinstance(now_monotonic, (int, float))
            or now_monotonic < 0
        ):
            raise WatchdogProtocolError("monotonic arrival time is invalid")
        if now_utc.tzinfo is None or now_utc.utcoffset() is None:
            raise WatchdogProtocolError("arrival timestamp must be timezone-aware")
        self.last_message_monotonic = float(now_monotonic)
        self.last_message_at = now_utc.astimezone(timezone.utc)

    def handle(
        self,
        payload: Any,
        *,
        now_monotonic: float,
        now_utc: datetime,
    ) -> str:
        """Apply one decoded message and return a sanitized message category."""

        if payload == "pong":
            self._mark_message(now_monotonic, now_utc)
            return "pong"
        if not isinstance(payload, Mapping):
            raise WatchdogProtocolError("WebSocket payload must be an object")
        if "error" in payload or payload.get("success") is False:
            raise WatchdogSubscriptionError("WebSocket request was rejected")

        if "table" in payload or "action" in payload or "data" in payload:
            if any(
                field_name in payload
                for field_name in ("success", "subscribe", "info", "error")
            ):
                raise WatchdogProtocolError("table and control fields cannot be mixed")
            table_name = payload.get("table")
            action = payload.get("action")
            if table_name not in self.tables or not isinstance(action, str):
                raise WatchdogProtocolError("table message is not allowlisted")
            if not self.subscriptions[table_name]:
                raise WatchdogSubscriptionError(
                    "table data arrived before subscription acknowledgement"
                )
            self.tables[table_name].apply(
                action,
                payload.get("data"),
                payload.get("keys"),
            )
            self._mark_message(now_monotonic, now_utc)
            if self.synchronized:
                self.ever_synchronized = True
            return "table"

        if "success" in payload or "subscribe" in payload:
            if payload.get("success") is not True:
                raise WatchdogSubscriptionError("subscription acknowledgement failed")
            topic = payload.get("subscribe")
            if topic not in self.subscriptions:
                raise WatchdogSubscriptionError("unexpected subscription acknowledgement")
            if self.subscriptions[topic]:
                raise WatchdogSubscriptionError("duplicate subscription acknowledgement")
            self.subscriptions[topic] = True
            self._mark_message(now_monotonic, now_utc)
            if self.synchronized:
                self.ever_synchronized = True
            return "subscription"

        if isinstance(payload.get("info"), str):
            self._mark_message(now_monotonic, now_utc)
            return "info"
        raise WatchdogProtocolError("unrecognized WebSocket control message")

    def _position_contracts(self) -> int | None:
        table = self.tables["position"]
        if not table.initialized:
            return None
        matches = [
            row for row in table.rows.values() if row.get("symbol") == WATCHED_SYMBOL
        ]
        if len(matches) > 1:
            raise WatchdogProtocolError("multiple watched-symbol positions are ambiguous")
        if not matches:
            return 0
        return _whole_number(
            matches[0].get("currentQty"),
            "position contracts",
            signed=True,
        )

    def _open_order_count(self) -> int | None:
        table = self.tables["order"]
        if not table.initialized:
            return None
        count = 0
        for row in table.rows.values():
            if row.get("symbol") != WATCHED_SYMBOL:
                continue
            status = row.get("ordStatus")
            if not isinstance(status, str) or not status:
                raise WatchdogProtocolError("order status is invalid")
            leaves = _whole_number(row.get("leavesQty"), "order leavesQty")
            if status in _TERMINAL_ORDER_STATUSES:
                if leaves != 0:
                    raise WatchdogProtocolError(
                        "terminal order has a non-zero remaining quantity"
                    )
            else:
                count += 1
        return count

    def snapshot(
        self,
        *,
        now_monotonic: float,
        now_utc: datetime,
        state_override: str | None = None,
        event_type: str = "update",
    ) -> dict[str, Any]:
        if event_type not in _EVENT_TYPES:
            raise WatchdogProtocolError("status event type is not allowlisted")
        if (
            isinstance(now_monotonic, bool)
            or not isinstance(now_monotonic, (int, float))
            or now_monotonic < 0
        ):
            raise WatchdogProtocolError("snapshot monotonic time is invalid")
        freshness: float | None = None
        if self.last_message_monotonic is not None:
            if now_monotonic < self.last_message_monotonic:
                raise WatchdogProtocolError("monotonic clock moved backwards")
            freshness = max(0.0, now_monotonic - self.last_message_monotonic)
        fresh = freshness is not None and freshness <= STALE_AFTER_SECONDS
        if state_override is not None:
            state = state_override
        elif freshness is not None and not fresh:
            state = "stale"
        elif self.synchronized:
            state = "healthy"
        elif self.last_message_at is not None:
            state = "synchronizing"
        else:
            state = "starting"
        if state not in _STATUS_STATES:
            raise WatchdogProtocolError("status state is not allowlisted")
        return {
            "schema_version": SCHEMA_VERSION,
            "environment": "testnet",
            "state": state,
            "generated_at_utc": _utc_text(now_utc),
            "last_message_at_utc": (
                _utc_text(self.last_message_at)
                if self.last_message_at is not None
                else None
            ),
            "freshness_seconds": (
                round(freshness, 3) if freshness is not None else None
            ),
            "fresh": fresh,
            "subscriptions": dict(self.subscriptions),
            "position_contracts": self._position_contracts(),
            "open_order_count": self._open_order_count(),
            "last_event_type": event_type,
        }


def decode_ws_text(value: str) -> Mapping[str, Any] | str:
    """Decode one bounded text frame without retaining raw invalid content."""

    if not isinstance(value, str) or len(value.encode("utf-8")) > MAX_MESSAGE_BYTES:
        raise WatchdogProtocolError("WebSocket text frame is invalid or oversized")
    if value == "pong":
        return "pong"
    try:
        payload = json.loads(value)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise WatchdogProtocolError("WebSocket text frame is not valid JSON") from exc
    if not isinstance(payload, Mapping):
        raise WatchdogProtocolError("WebSocket JSON payload must be an object")
    return payload


def _validate_status_payload(payload: Mapping[str, Any]) -> None:
    if set(payload) != _STATUS_FIELDS:
        raise WatchdogPublishError("status payload has unexpected fields")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise WatchdogPublishError("status schema version is invalid")
    if payload.get("environment") != "testnet":
        raise WatchdogPublishError("status environment is invalid")
    if payload.get("state") not in _STATUS_STATES:
        raise WatchdogPublishError("status state is invalid")
    if payload.get("last_event_type") not in _EVENT_TYPES:
        raise WatchdogPublishError("status event type is invalid")
    subscriptions = payload.get("subscriptions")
    if not isinstance(subscriptions, Mapping) or set(subscriptions) != set(SUBSCRIPTIONS):
        raise WatchdogPublishError("status subscriptions are invalid")
    if any(type(value) is not bool for value in subscriptions.values()):
        raise WatchdogPublishError("status subscription flag is invalid")
    if type(payload.get("fresh")) is not bool:
        raise WatchdogPublishError("status freshness flag is invalid")
    for field_name in ("generated_at_utc", "last_message_at_utc"):
        value = payload.get(field_name)
        if field_name == "last_message_at_utc" and value is None:
            continue
        if not isinstance(value, str) or len(value) > 64:
            raise WatchdogPublishError(f"status {field_name} is invalid")
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise WatchdogPublishError(f"status {field_name} is invalid") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise WatchdogPublishError(f"status {field_name} is invalid")
    freshness = payload.get("freshness_seconds")
    if freshness is not None and (
        isinstance(freshness, bool)
        or not isinstance(freshness, (int, float))
        or freshness < 0
        or freshness != freshness
        or freshness == float("inf")
    ):
        raise WatchdogPublishError("status freshness value is invalid")
    if (payload.get("last_message_at_utc") is None) != (freshness is None):
        raise WatchdogPublishError("status message freshness is inconsistent")
    expected_fresh = freshness is not None and freshness <= STALE_AFTER_SECONDS
    if payload.get("fresh") is not expected_fresh:
        raise WatchdogPublishError("status freshness fields are inconsistent")
    for field_name in ("position_contracts", "open_order_count"):
        value = payload.get(field_name)
        if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
            raise WatchdogPublishError(f"status {field_name} is invalid")
    open_order_count = payload.get("open_order_count")
    if open_order_count is not None and open_order_count < 0:
        raise WatchdogPublishError("status open_order_count is invalid")
    if payload.get("state") == "healthy" and (
        payload.get("fresh") is not True
        or not all(subscriptions.values())
        or payload.get("position_contracts") is None
        or open_order_count is None
    ):
        raise WatchdogPublishError("healthy status is internally inconsistent")


def write_status_atomic(
    payload: Mapping[str, Any],
    destination: str | os.PathLike[str] = STATUS_PATH,
) -> None:
    """Atomically publish the exact allowlisted status schema."""

    _validate_status_payload(payload)
    path = Path(destination)
    temporary_name: str | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_name = handle.name
            if os.name != "nt":
                os.chmod(temporary_name, 0o600)
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
        temporary_name = None
    except (OSError, TypeError, ValueError) as exc:
        raise WatchdogPublishError("could not publish watchdog status") from exc
    finally:
        if temporary_name is not None:
            try:
                Path(temporary_name).unlink(missing_ok=True)
            except OSError as exc:
                raise WatchdogPublishError(
                    "could not clean failed watchdog status publication"
                ) from exc


def build_alert_payload(event_type: str, now_utc: datetime) -> dict[str, Any]:
    """Return the only fields permitted to leave through the alert webhook."""

    if event_type not in _ALERT_EVENT_TYPES:
        raise WatchdogAlertError("alert event type is not allowlisted")
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "bitmex-testnet-watchdog",
        "environment": "testnet",
        "event_type": event_type,
        "generated_at_utc": _utc_text(now_utc),
    }


def validate_alert_webhook_url(value: str | None) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str) or len(value) > 2_048 or value != value.strip():
        raise EnvironmentError("WATCHDOG_ALERT_WEBHOOK_URL is invalid")
    parsed = urlsplit(value)
    try:
        parsed.port
    except ValueError as exc:
        raise EnvironmentError("WATCHDOG_ALERT_WEBHOOK_URL has an invalid port") from exc
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise EnvironmentError("WATCHDOG_ALERT_WEBHOOK_URL must be a safe HTTPS URL")
    return value


async def send_alert(webhook_url: str, event_type: str) -> None:
    payload = build_alert_payload(event_type, _utc_now())
    timeout = aiohttp.ClientTimeout(
        total=ALERT_TIMEOUT_SECONDS,
        connect=min(2, ALERT_TIMEOUT_SECONDS),
        sock_connect=min(2, ALERT_TIMEOUT_SECONDS),
        sock_read=min(3, ALERT_TIMEOUT_SECONDS),
    )
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                webhook_url,
                json=payload,
                allow_redirects=False,
            ) as response:
                if response.status < 200 or response.status >= 300:
                    raise WatchdogAlertError("alert endpoint rejected the event")
    except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
        raise WatchdogAlertError("alert delivery failed") from exc


async def _receive_payload(
    websocket: aiohttp.ClientWebSocketResponse,
    timeout_seconds: float,
) -> Mapping[str, Any] | str:
    try:
        message = await asyncio.wait_for(
            websocket.receive(),
            timeout=timeout_seconds,
        )
    except asyncio.TimeoutError:
        raise
    if message.type is aiohttp.WSMsgType.TEXT:
        return decode_ws_text(message.data)
    if message.type in {
        aiohttp.WSMsgType.CLOSE,
        aiohttp.WSMsgType.CLOSED,
        aiohttp.WSMsgType.CLOSING,
        aiohttp.WSMsgType.ERROR,
    }:
        raise WatchdogTransportError("WebSocket connection closed")
    raise WatchdogProtocolError("WebSocket returned an unsupported frame type")


async def _prove_idle_connection(
    websocket: aiohttp.ClientWebSocketResponse,
    state: WatchdogState,
) -> None:
    await websocket.send_str("ping")
    deadline = time.monotonic() + PONG_TIMEOUT_SECONDS
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise WatchdogStaleError("WebSocket pong deadline expired")
        try:
            payload = await _receive_payload(websocket, remaining)
        except asyncio.TimeoutError as exc:
            raise WatchdogStaleError("WebSocket pong deadline expired") from exc
        category = state.handle(
            payload,
            now_monotonic=time.monotonic(),
            now_utc=_utc_now(),
        )
        if category == "pong":
            return


async def _reject_websocket_redirect(
    _session: aiohttp.ClientSession,
    _trace_config_ctx: Any,
    _params: aiohttp.TraceRequestRedirectParams,
) -> None:
    """Refuse every redirect before custom BitMEX auth headers can be reused."""

    raise WatchdogTransportError("authenticated WebSocket redirects are forbidden")


def _no_redirect_trace_config() -> aiohttp.TraceConfig:
    trace_config = aiohttp.TraceConfig()
    trace_config.on_request_redirect.append(_reject_websocket_redirect)
    return trace_config


async def _run_connection(
    api_key: str,
    api_secret: str,
    state: WatchdogState,
    destination: str | os.PathLike[str],
) -> None:
    expires = int(time.time()) + AUTH_TTL_SECONDS
    headers = build_auth_headers(api_key, api_secret, expires)
    timeout = aiohttp.ClientTimeout(
        total=None,
        connect=CONNECT_TIMEOUT_SECONDS,
        sock_connect=CONNECT_TIMEOUT_SECONDS,
    )
    websocket_timeout = aiohttp.ClientWSTimeout(ws_receive=None, ws_close=5)
    connected_at = time.monotonic()
    last_publish = connected_at
    was_synchronized = False

    async with aiohttp.ClientSession(
        timeout=timeout,
        trace_configs=[_no_redirect_trace_config()],
    ) as session:
        async with session.ws_connect(
            TESTNET_WSS_URL,
            headers=headers,
            timeout=websocket_timeout,
            autoping=True,
            autoclose=True,
            heartbeat=None,
            compress=0,
            max_msg_size=MAX_MESSAGE_BYTES,
        ) as websocket:
            await websocket.send_json(
                {"op": "subscribe", "args": list(SUBSCRIPTIONS)}
            )
            write_status_atomic(
                state.snapshot(
                    now_monotonic=time.monotonic(),
                    now_utc=_utc_now(),
                    event_type="connected",
                ),
                destination,
            )

            while True:
                now_monotonic = time.monotonic()
                if not state.synchronized:
                    remaining_sync = SYNC_TIMEOUT_SECONDS - (
                        now_monotonic - connected_at
                    )
                    if remaining_sync <= 0:
                        raise WatchdogSubscriptionError(
                            "private table synchronization timed out"
                        )
                    receive_timeout = min(IDLE_PING_SECONDS, remaining_sync)
                else:
                    receive_timeout = IDLE_PING_SECONDS
                try:
                    payload = await _receive_payload(websocket, receive_timeout)
                except asyncio.TimeoutError:
                    if (
                        not state.synchronized
                        and time.monotonic() - connected_at >= SYNC_TIMEOUT_SECONDS
                    ):
                        raise WatchdogSubscriptionError(
                            "private table synchronization timed out"
                        )
                    await _prove_idle_connection(websocket, state)
                else:
                    state.handle(
                        payload,
                        now_monotonic=time.monotonic(),
                        now_utc=_utc_now(),
                    )

                now_monotonic = time.monotonic()
                became_synchronized = state.synchronized and not was_synchronized
                if (
                    became_synchronized
                    or now_monotonic - last_publish
                    >= STATUS_WRITE_INTERVAL_SECONDS
                ):
                    write_status_atomic(
                        state.snapshot(
                            now_monotonic=now_monotonic,
                            now_utc=_utc_now(),
                            event_type=(
                                "synchronized" if became_synchronized else "update"
                            ),
                        ),
                        destination,
                    )
                    last_publish = now_monotonic
                was_synchronized = state.synchronized


def _failure_event(exc: BaseException) -> str:
    if isinstance(exc, WatchdogStaleError):
        return "stale"
    if isinstance(exc, WatchdogSubscriptionError):
        return "subscription_error"
    if isinstance(exc, WatchdogProtocolError):
        return "protocol_error"
    if isinstance(exc, aiohttp.WSServerHandshakeError) and exc.status in {401, 403}:
        return "auth_error"
    return "transport_error"


async def run_watchdog(
    *,
    destination: str | os.PathLike[str] = STATUS_PATH,
    env: Mapping[str, str] | None = None,
) -> None:
    """Run forever, reconnecting with a bounded exponential delay."""

    values = os.environ if env is None else env
    api_key, api_secret = require_testnet_credentials(values)
    webhook_url = validate_alert_webhook_url(
        values.get("WATCHDOG_ALERT_WEBHOOK_URL")
    )
    initial = WatchdogState()
    write_status_atomic(
        initial.snapshot(
            now_monotonic=time.monotonic(),
            now_utc=_utc_now(),
            event_type="startup",
        ),
        destination,
    )

    backoff = BACKOFF_INITIAL_SECONDS
    last_alert_at: dict[str, float] = {}
    while True:
        state = WatchdogState()
        try:
            await _run_connection(api_key, api_secret, state, destination)
            raise WatchdogTransportError("WebSocket connection ended unexpectedly")
        except asyncio.CancelledError:
            raise
        except (
            WatchdogProtocolError,
            WatchdogSubscriptionError,
            WatchdogStaleError,
            WatchdogTransportError,
            aiohttp.ClientError,
            asyncio.TimeoutError,
            OSError,
        ) as exc:
            event_type = _failure_event(exc)
            failure_state = "stale" if event_type == "stale" else "failed"
            write_status_atomic(
                state.snapshot(
                    now_monotonic=time.monotonic(),
                    now_utc=_utc_now(),
                    state_override=failure_state,
                    event_type=event_type,
                ),
                destination,
            )
            now_monotonic = time.monotonic()
            should_alert = webhook_url is not None and (
                now_monotonic - last_alert_at.get(event_type, float("-inf"))
                >= ALERT_COOLDOWN_SECONDS
            )
            if should_alert:
                try:
                    await send_alert(webhook_url, event_type)
                except WatchdogAlertError:  # agent-quality: allow: sanitized alert failure is logged while local monitoring continues
                    _LOGGER.error("watchdog alert delivery failed")
                last_alert_at[event_type] = now_monotonic
            if state.ever_synchronized:
                backoff = BACKOFF_INITIAL_SECONDS
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)


def main() -> int:
    load_dotenv()
    try:
        asyncio.run(run_watchdog())
    except KeyboardInterrupt:  # agent-quality: allow: operator interrupt maps to the standard explicit exit code
        return 130
    except EnvironmentError:
        print("watchdog refused to start: Testnet environment gate failed", file=sys.stderr)
        return 2
    except WatchdogPublishError:
        print("watchdog stopped: sanitized status publication failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "BACKOFF_MAX_SECONDS",
    "DeltaTable",
    "SCHEMA_VERSION",
    "STATUS_PATH",
    "SUBSCRIPTIONS",
    "TESTNET_WSS_URL",
    "WATCHED_SYMBOL",
    "WatchdogAlertError",
    "WatchdogProtocolError",
    "WatchdogPublishError",
    "WatchdogStaleError",
    "WatchdogState",
    "WatchdogSubscriptionError",
    "build_alert_payload",
    "build_auth_headers",
    "decode_ws_text",
    "require_testnet_credentials",
    "run_watchdog",
    "send_alert",
    "validate_alert_webhook_url",
    "write_status_atomic",
]
