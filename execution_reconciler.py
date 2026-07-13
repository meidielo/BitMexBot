"""Fail-closed BitMEX XBTUSDT execution-history reconciliation.

Only authenticated Testnet clients accepted by :mod:`bitmex_client` may be
queried. Raw BitMEX ``execution/tradeHistory`` rows are normalized, attributed
to one durable intent, observed twice without change, then committed through
the ledger's atomic append-and-close operation.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from bitmex_client import attest_testnet_exchange
from trade_ledger import close_intent_from_events


NATIVE_SYMBOL = "XBTUSDT"
PAGE_SIZE = 500
MAX_PAGES = 100
USDT_MINOR_SCALE = Decimal("1000000")
USDT_CURRENCIES = frozenset({"USDT"})


class ExitReconciliationError(RuntimeError):
    """Base error for exchange evidence that cannot safely close an intent."""


class ExitEvidencePending(ExitReconciliationError):
    """Raised while required exchange history has not become visible."""


class ExitEvidenceAmbiguous(ExitReconciliationError):
    """Raised for contradictory or unattributable exchange evidence."""


@dataclass(frozen=True, slots=True)
class NativeExecution:
    exec_id: str
    account_id: str
    native_symbol: str
    event_type: str
    order_id: str | None
    client_order_id: str | None
    link_id: str | None
    side: str | None
    last_qty: int
    last_price_usdt: Decimal | None
    commission_usdt: Decimal
    funding_usdt: Decimal
    realised_pnl_usdt: Decimal
    transact_time_utc: str
    source_hash: str


@dataclass(frozen=True, slots=True)
class AttributedExit:
    events: tuple[dict[str, Any], ...]
    evidence_source_hash: str
    exit_reason: str


def _text(value: Any, field: str, *, required: bool = True) -> str | None:
    if value is None:
        if required:
            raise ExitEvidenceAmbiguous(f"execution omitted {field}")
        return None
    normalized = str(value).strip()
    if not normalized and required:
        raise ExitEvidenceAmbiguous(f"execution omitted {field}")
    return normalized or None


def _decimal(value: Any, field: str) -> Decimal:
    if isinstance(value, bool):
        raise ExitEvidenceAmbiguous(f"execution {field} is not numeric")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ExitEvidenceAmbiguous(f"execution {field} is not numeric") from exc
    if not parsed.is_finite():
        raise ExitEvidenceAmbiguous(f"execution {field} is not finite")
    return parsed


def _whole(value: Any, field: str, *, non_negative: bool = True) -> int:
    parsed = _decimal(value, field)
    integral = int(parsed)
    if parsed != integral or (non_negative and integral < 0):
        raise ExitEvidenceAmbiguous(f"execution {field} is not a whole number")
    return integral


def _timestamp(value: Any) -> str:
    text = _text(value, "transactTime")
    try:
        parsed = datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ExitEvidenceAmbiguous("execution timestamp is not ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ExitEvidenceAmbiguous("execution timestamp is not timezone-aware")
    return parsed.astimezone(timezone.utc).isoformat()


def _source_hash(row: Mapping[str, Any]) -> str:
    try:
        encoded = json.dumps(
            dict(row),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            default=str,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ExitEvidenceAmbiguous("execution row cannot be canonically hashed") from exc
    return hashlib.sha256(encoded).hexdigest()


def _minor_usdt(value: Any, currency: Any, field: str) -> Decimal:
    normalized_currency = str(currency or "").replace("₮", "T").upper()
    if normalized_currency not in USDT_CURRENCIES:
        raise ExitEvidenceAmbiguous(
            f"execution {field} has unsupported currency {currency!r}"
        )
    return _decimal(value, field) / USDT_MINOR_SCALE


def parse_native_execution(row: Mapping[str, Any]) -> NativeExecution | None:
    """Normalize one relevant native trade-history row.

    Non-balance-affecting lifecycle rows are ignored. Trade and funding rows
    require exact native XBTUSDT fields and settlement-unit evidence.
    """

    if not isinstance(row, Mapping):
        raise ExitEvidenceAmbiguous("trade-history row is not a mapping")
    event_type = str(row.get("execType", "")).strip().upper()
    if event_type not in {"TRADE", "FUNDING"}:
        return None
    exec_id = str(_text(row.get("execID"), "execID"))
    account_id = str(_text(row.get("account"), "account"))
    native_symbol = str(_text(row.get("symbol"), "symbol"))
    if native_symbol != NATIVE_SYMBOL:
        raise ExitEvidenceAmbiguous(
            f"trade-history query returned unexpected symbol {native_symbol!r}"
        )
    timestamp = _timestamp(row.get("transactTime", row.get("timestamp")))
    if row.get("realisedPnl") is None:
        raise ExitEvidenceAmbiguous("execution omitted native realisedPnl")
    realised = _minor_usdt(
        row["realisedPnl"],
        row.get("settlCurrency")
        or row.get("execCommCcy")
        or row.get("currency"),
        "realisedPnl",
    )

    if event_type == "TRADE":
        order_id = str(_text(row.get("orderID"), "orderID"))
        side = str(_text(row.get("side"), "side")).upper()
        if side not in {"BUY", "SELL"}:
            raise ExitEvidenceAmbiguous("trade execution side is invalid")
        quantity = _whole(row.get("lastQty"), "lastQty")
        price = _decimal(row.get("lastPx"), "lastPx")
        if quantity <= 0 or price <= 0:
            raise ExitEvidenceAmbiguous(
                "trade execution requires positive lastQty and lastPx"
            )
        commission = _minor_usdt(
            row.get("execComm"), row.get("execCommCcy"), "execComm"
        )
        funding = Decimal(0)
    else:
        order_id = _text(row.get("orderID"), "orderID", required=False)
        side_value = _text(row.get("side"), "side", required=False)
        side = side_value.upper() if side_value else None
        quantity = abs(_whole(row.get("lastQty"), "lastQty", non_negative=False))
        price = None
        commission = Decimal(0)
        native_funding_commission = _minor_usdt(
            row.get("execComm"),
            row.get("execCommCcy") or row.get("currency"),
            "funding execComm",
        )
        # BitMEX reports an account credit as a negative funding execComm.
        funding = -native_funding_commission

    return NativeExecution(
        exec_id=exec_id,
        account_id=account_id,
        native_symbol=native_symbol,
        event_type=event_type,
        order_id=order_id,
        client_order_id=_text(row.get("clOrdID"), "clOrdID", required=False),
        link_id=_text(row.get("clOrdLinkID"), "clOrdLinkID", required=False),
        side=side,
        last_qty=quantity,
        last_price_usdt=price,
        commission_usdt=commission,
        funding_usdt=funding,
        realised_pnl_usdt=realised,
        transact_time_utc=timestamp,
        source_hash=_source_hash(row),
    )


def fetch_execution_history(
    exchange: Any,
    *,
    start_time_utc: str,
    page_size: int = PAGE_SIZE,
    max_pages: int = MAX_PAGES,
) -> tuple[NativeExecution, ...]:
    """Fetch all native XBTUSDT trade/funding history with strict pagination."""

    if not 1 <= page_size <= PAGE_SIZE or max_pages <= 0:
        raise ValueError("invalid execution-history pagination limits")
    endpoint = getattr(exchange, "private_get_execution_tradehistory", None)
    if not callable(endpoint):
        raise ExitEvidencePending(
            "exchange client does not expose native execution/tradeHistory"
        )
    start = 0
    seen: dict[str, NativeExecution] = {}
    for page_number in range(max_pages):
        attest_testnet_exchange(exchange)
        response = endpoint(
            {
                "symbol": NATIVE_SYMBOL,
                "startTime": start_time_utc,
                "count": page_size,
                "start": start,
                "reverse": False,
            }
        )
        if (
            response is None
            or isinstance(response, (str, bytes, Mapping))
            or not isinstance(response, Sequence)
        ):
            raise ExitEvidenceAmbiguous("trade-history response is not a list")
        if len(response) > page_size:
            raise ExitEvidenceAmbiguous("trade-history page exceeds requested count")
        for raw in response:
            parsed = parse_native_execution(raw)
            if parsed is None:
                continue
            previous = seen.get(parsed.exec_id)
            if previous is not None and previous.source_hash != parsed.source_hash:
                raise ExitEvidenceAmbiguous(
                    "duplicate execID has contradictory native evidence"
                )
            seen[parsed.exec_id] = parsed
        if len(response) < page_size:
            return tuple(
                sorted(
                    seen.values(),
                    key=lambda item: (item.transact_time_utc, item.exec_id),
                )
            )
        start += len(response)
    raise ExitEvidenceAmbiguous(
        f"trade-history pagination exceeded {max_pages} pages"
    )


def _matches(
    execution: NativeExecution,
    *,
    order_id: Any,
    client_order_id: Any,
) -> bool:
    comparisons = []
    if order_id is not None:
        comparisons.append(execution.order_id == str(order_id))
    if client_order_id is not None:
        comparisons.append(execution.client_order_id == str(client_order_id))
    return bool(comparisons) and all(comparisons)


def _assert_reference_consistency(
    execution: NativeExecution,
    *,
    label: str,
    order_id: Any,
    client_order_id: Any,
) -> None:
    comparisons = []
    if order_id is not None:
        comparisons.append(execution.order_id == str(order_id))
    if client_order_id is not None:
        comparisons.append(execution.client_order_id == str(client_order_id))
    if comparisons and any(comparisons) and not all(comparisons):
        raise ExitEvidenceAmbiguous(
            f"{label} execution identifiers contradict durable references"
        )


def _role_event(execution: NativeExecution, role: str) -> dict[str, Any]:
    return {
        "exec_id": execution.exec_id,
        "account_id": execution.account_id,
        "native_symbol": execution.native_symbol,
        "event_type": execution.event_type,
        "event_role": role,
        "order_id": execution.order_id,
        "client_order_id": execution.client_order_id,
        "link_id": execution.link_id,
        "side": execution.side,
        "last_qty": execution.last_qty,
        "last_price_usdt": (
            None
            if execution.last_price_usdt is None
            else float(execution.last_price_usdt)
        ),
        "commission_usdt": float(execution.commission_usdt),
        "funding_usdt": float(execution.funding_usdt),
        "realised_pnl_usdt": float(execution.realised_pnl_usdt),
        "transact_time_utc": execution.transact_time_utc,
        "source_hash": execution.source_hash,
    }


def attribute_exit(
    intent: Mapping[str, Any], executions: Sequence[NativeExecution]
) -> AttributedExit:
    """Attribute a complete entry, exit, fees and funding to one intent."""

    if not isinstance(intent, Mapping):
        raise ExitEvidenceAmbiguous("durable intent is not a mapping")
    try:
        filled = _whole(intent["filled_contracts"], "filled_contracts")
        side = str(intent["side"]).upper()
    except KeyError as exc:
        raise ExitEvidenceAmbiguous("durable intent is incomplete") from exc
    if filled <= 0 or side not in {"LONG", "SHORT"}:
        raise ExitEvidenceAmbiguous("durable intent has invalid filled exposure")
    expected_entry_side = "BUY" if side == "LONG" else "SELL"
    expected_exit_side = "SELL" if side == "LONG" else "BUY"

    entry_trades = [
        item
        for item in executions
        if item.event_type == "TRADE"
        and _matches(
            item,
            order_id=intent.get("entry_order_id"),
            client_order_id=intent.get("entry_client_order_id"),
        )
    ]
    exit_legs = (
        ("stop_loss", intent.get("stop_order_id"), intent.get("stop_client_order_id")),
        (
            "take_profit",
            intent.get("target_order_id"),
            intent.get("target_client_order_id"),
        ),
        (
            "emergency_close",
            intent.get("exit_order_id"),
            intent.get("exit_client_order_id"),
        ),
    )
    for item in executions:
        if item.event_type != "TRADE":
            continue
        _assert_reference_consistency(
            item,
            label="entry",
            order_id=intent.get("entry_order_id"),
            client_order_id=intent.get("entry_client_order_id"),
        )
        for label, order_id, client_id in exit_legs:
            _assert_reference_consistency(
                item,
                label=label,
                order_id=order_id,
                client_order_id=client_id,
            )
    exit_by_reason: dict[str, list[NativeExecution]] = {}
    for reason, order_id, client_id in exit_legs:
        if order_id is None and client_id is None:
            continue
        matches = [
            item
            for item in executions
            if item.event_type == "TRADE"
            and _matches(item, order_id=order_id, client_order_id=client_id)
        ]
        if matches:
            exit_by_reason[reason] = matches

    entry_qty = sum(item.last_qty for item in entry_trades)
    if entry_qty < filled:
        raise ExitEvidencePending(
            f"entry execution history exposes {entry_qty} of {filled} contracts"
        )
    if entry_qty > filled:
        raise ExitEvidenceAmbiguous("entry executions exceed the durable fill")
    if not exit_by_reason:
        raise ExitEvidencePending("no attributable exit execution is visible")
    if len(exit_by_reason) != 1:
        raise ExitEvidenceAmbiguous("multiple exit order legs contain fills")
    exit_reason, exit_trades = next(iter(exit_by_reason.items()))
    exit_qty = sum(item.last_qty for item in exit_trades)
    if exit_qty < filled:
        raise ExitEvidencePending(
            f"exit execution history exposes {exit_qty} of {filled} contracts"
        )
    if exit_qty > filled:
        raise ExitEvidenceAmbiguous("exit executions exceed the durable fill")
    if any(item.side != expected_entry_side for item in entry_trades):
        raise ExitEvidenceAmbiguous("entry execution side contradicts the intent")
    if any(item.side != expected_exit_side for item in exit_trades):
        raise ExitEvidenceAmbiguous("exit execution side contradicts the intent")

    entry_time = min(
        datetime.fromisoformat(item.transact_time_utc) for item in entry_trades
    )
    exit_time = max(
        datetime.fromisoformat(item.transact_time_utc) for item in exit_trades
    )
    if entry_time > exit_time:
        raise ExitEvidenceAmbiguous("exit execution precedes entry execution")
    account_ids = {item.account_id for item in [*entry_trades, *exit_trades]}
    if len(account_ids) != 1:
        raise ExitEvidenceAmbiguous("attributable trades span multiple accounts")
    account_id = next(iter(account_ids))

    known_exec_ids = {
        item.exec_id for item in [*entry_trades, *exit_trades]
    }
    for item in executions:
        if item.event_type != "TRADE" or item.exec_id in known_exec_ids:
            continue
        timestamp = datetime.fromisoformat(item.transact_time_utc)
        if entry_time <= timestamp <= exit_time:
            raise ExitEvidenceAmbiguous(
                "unattributed XBTUSDT trade occurred during the position lifetime"
            )

    funding_events = []
    for item in executions:
        if item.event_type != "FUNDING":
            continue
        timestamp = datetime.fromisoformat(item.transact_time_utc)
        if not entry_time <= timestamp <= exit_time:
            continue
        if item.account_id != account_id:
            raise ExitEvidenceAmbiguous(
                "funding evidence belongs to a different exchange account"
            )
        if item.last_qty > filled:
            raise ExitEvidenceAmbiguous(
                "funding execution quantity exceeds the managed position"
            )
        funding_events.append(item)

    contract_size = _decimal(intent.get("contract_size_btc"), "contract_size_btc")
    if contract_size <= 0:
        raise ExitEvidenceAmbiguous("durable contract size is invalid")
    entry_notional = sum(
        item.last_price_usdt * item.last_qty for item in entry_trades
    )
    exit_notional = sum(item.last_price_usdt * item.last_qty for item in exit_trades)
    entry_average = entry_notional / entry_qty
    exit_average = exit_notional / exit_qty
    direction = Decimal(1) if side == "LONG" else Decimal(-1)
    gross = Decimal(filled) * contract_size * (exit_average - entry_average) * direction
    fees = sum(
        (item.commission_usdt for item in [*entry_trades, *exit_trades]),
        Decimal(0),
    )
    funding = sum(
        (item.funding_usdt for item in funding_events),
        Decimal(0),
    )
    calculated_net = gross - fees + funding
    native_net = sum(
        (
            item.realised_pnl_usdt
            for item in [*entry_trades, *exit_trades, *funding_events]
        ),
        Decimal(0),
    )
    tolerance = max(Decimal("0.000001"), abs(calculated_net) * Decimal("1e-9"))
    if abs(native_net - calculated_net) > tolerance:
        raise ExitEvidenceAmbiguous(
            "native realised PnL disagrees with price, fee, and funding accounting"
        )

    attributed = [
        *(_role_event(item, "ENTRY") for item in entry_trades),
        *(_role_event(item, "EXIT") for item in exit_trades),
        *(_role_event(item, "FUNDING") for item in funding_events),
    ]
    attributed.sort(key=lambda item: (item["transact_time_utc"], item["exec_id"]))
    evidence_hash = hashlib.sha256(
        "\n".join(
            sorted(
                f"{item['exec_id']}:{item['source_hash']}:{item['event_role']}"
                for item in attributed
            )
        ).encode("utf-8")
    ).hexdigest()
    return AttributedExit(tuple(attributed), evidence_hash, exit_reason)


def reconcile_exit_from_exchange(
    exchange: Any,
    intent: Mapping[str, Any],
    *,
    attempts: int = 5,
    confirmations: int = 2,
    delay_seconds: float = 1.0,
) -> AttributedExit:
    """Require a complete attributable history to remain stable before use."""

    if attempts < confirmations or confirmations < 2 or delay_seconds < 0:
        raise ValueError("invalid exit-reconciliation retry policy")
    start_time = str(intent.get("decision_time_utc", "")).strip()
    if not start_time:
        raise ExitEvidenceAmbiguous("durable intent omitted decision_time_utc")
    previous_hash: str | None = None
    stable = 0
    last_pending: ExitEvidencePending | None = None
    for attempt in range(attempts):
        history = fetch_execution_history(exchange, start_time_utc=start_time)
        try:
            attributed = attribute_exit(intent, history)
        except ExitEvidencePending as exc:  # agent-quality: allow: bounded retries end in an explicit failure containing the last pending state
            previous_hash = None
            stable = 0
            last_pending = exc
        else:
            if attributed.evidence_source_hash == previous_hash:
                stable += 1
            else:
                previous_hash = attributed.evidence_source_hash
                stable = 1
            if stable >= confirmations:
                return attributed
        if attempt + 1 < attempts and delay_seconds:
            time.sleep(delay_seconds)
    detail = str(last_pending) if last_pending is not None else "history never stabilized"
    raise ExitEvidencePending(
        f"complete exit evidence was not stable after {attempts} attempts: {detail}"
    )


def reconcile_and_close(
    exchange: Any,
    intent: Mapping[str, Any],
    *,
    ledger_path: str,
    attempts: int = 5,
    delay_seconds: float = 1.0,
    final_status: str = "CLOSED",
    halt_reason: str | None = None,
) -> dict[str, Any]:
    """Stabilize exchange evidence and atomically close its durable intent."""

    attributed = reconcile_exit_from_exchange(
        exchange,
        intent,
        attempts=attempts,
        confirmations=2,
        delay_seconds=delay_seconds,
    )
    return close_intent_from_events(
        str(intent["decision_key"]),
        expected_statuses={str(intent["status"])},
        events=attributed.events,
        final_status=final_status,
        halt_reason=halt_reason,
        db_path=ledger_path,
    )


__all__ = [
    "AttributedExit",
    "ExitEvidenceAmbiguous",
    "ExitEvidencePending",
    "ExitReconciliationError",
    "NativeExecution",
    "attribute_exit",
    "fetch_execution_history",
    "parse_native_execution",
    "reconcile_and_close",
    "reconcile_exit_from_exchange",
]
