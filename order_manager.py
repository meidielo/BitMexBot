"""Restart-safe BitMEX Testnet order execution.

The module has no authenticated mainnet path. Every entry is a bounded IOC
limit order with a deterministic ``clOrdID`` and a durable ledger intent that
is written before submission. Missing, partial, ambiguous, or conflicting
exchange state fails closed.
"""

from __future__ import annotations

import os
import time
from collections.abc import Sequence
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any, Mapping

import ccxt
from dotenv import load_dotenv

from bitmex_client import (
    ExchangeAttestationError,
    attest_testnet_exchange,
    fetch_xbtusdt_position,
    get_client,
)
from execution_lock import ExecutionLockError, exclusive_execution_lock
from execution_safety import (
    AmbiguousCreateUnresolved,
    bounded_ioc_limit_price,
    classify_order,
    client_order_ids,
    create_order_once,
    ExecutionSafetyError,
    find_order_by_client_id,
    reconciliation_client_order_id,
    validate_completed_candle_decision_key,
)
from instrument import EXPECTED_SYMBOL, XBTUSDTInstrument
from risk import MIN_ENTRY_SLIPPAGE_BPS, MIN_STOP_SLIPPAGE_BPS
from trade_ledger import (
    DB_PATH,
    UNIT_MODEL,
    LedgerError,
    record_execution_evidence,
    register_intent,
    transition_intent,
)


load_dotenv()

SYMBOL = EXPECTED_SYMBOL
STRATEGY_VERSION = "funding-mean-reversion-v2"
MAX_ENTRY_SLIPPAGE_BPS = Decimal("10")
RECONCILE_ATTEMPTS = 5
RECONCILE_DELAY_SECONDS = 1.0
FLAT_CONFIRMATIONS = 3


class OrderExecutionError(RuntimeError):
    """Raised when safe order lifecycle state cannot be proven."""


class EntryOrderUnresolved(OrderExecutionError):
    """Raised while an entry may still fill and must remain under monitoring."""


class ProtectiveOrderUnresolved(OrderExecutionError):
    """Raised when a malformed protective order cannot be proven terminal."""


class UnattributedEntryExposure(OrderExecutionError):
    """Raised when ENTRY_PENDING has position exposure but no attributable order."""


class UnexpectedPendingAccountState(OrderExecutionError):
    """Raised after unexpected pending-state orders/exposure are made fail-closed."""


def _assert_testnet() -> None:
    """Refuse all orders unless Testnet is explicitly enabled."""

    value = os.getenv("BITMEX_TESTNET", "")
    if value != "true":
        raise EnvironmentError(
            "BITMEX_TESTNET is not set to 'true' in .env. Refusing to place "
            "orders. Authenticated mainnet execution is not implemented."
        )


def _result(
    status: str,
    *,
    decision_key: str | None = None,
    entry_order: Mapping[str, Any] | None = None,
    sl_order: Mapping[str, Any] | None = None,
    tp_order: Mapping[str, Any] | None = None,
    filled_contracts: int = 0,
    fill_price: float | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    return {
        "order_id": _order_id(entry_order, required=False),
        "decision_key": decision_key,
        "entry_order": _order_summary(entry_order),
        "sl_order": _order_summary(sl_order),
        "tp_order": _order_summary(tp_order),
        "filled_contracts": filled_contracts,
        "fill_price": fill_price,
        "status": status,
        "error": error,
    }


def _order_summary(order: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Return only identifiers and normalized status, never a raw API response."""

    if order is None:
        return None
    client_id = order.get("clientOrderId")
    if client_id is None and isinstance(order.get("info"), Mapping):
        client_id = order["info"].get("clOrdID")
    return {
        "id": _order_id(order, required=False),
        "client_order_id": str(client_id) if client_id is not None else None,
        "status": classify_order(order).state,
    }


def _safe_error(exc: BaseException) -> str:
    """Keep internal validation detail while redacting external API payloads."""

    internal = (
        OrderExecutionError,
        AmbiguousCreateUnresolved,
        ExecutionSafetyError,
        LedgerError,
        ExchangeAttestationError,
        ExecutionLockError,
    )
    if isinstance(exc, internal):
        return str(exc)
    return f"{type(exc).__name__}: external exchange operation failed"


def _order_id(order: Mapping[str, Any] | None, *, required: bool = True) -> str | None:
    if order is not None:
        value = order.get("id")
        if value is None and isinstance(order.get("info"), Mapping):
            value = order["info"].get("orderID")
        if value is not None and str(value).strip():
            return str(value).strip()
    if required:
        raise OrderExecutionError("exchange order response omitted its order ID")
    return None


def _average_price(order: Mapping[str, Any]) -> Decimal:
    value = order.get("average")
    if value is None and isinstance(order.get("info"), Mapping):
        value = order["info"].get("avgPx")
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise OrderExecutionError("filled order omitted its average price") from exc
    if not result.is_finite() or result <= 0:
        raise OrderExecutionError("filled order average price is invalid")
    return result


def _as_decimal(value: Any, field: str) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise OrderExecutionError(f"{field} must be numeric")
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise OrderExecutionError(f"{field} must be numeric") from exc
    if not result.is_finite():
        raise OrderExecutionError(f"{field} must be finite")
    return result


def _close_enough(actual: Any, expected: Decimal) -> bool:
    try:
        actual_value = Decimal(str(actual))
    except Exception:  # agent-quality: allow: False explicitly fails the consistency check
        return False
    tolerance = max(Decimal("0.00000001"), abs(expected) * Decimal("0.00000001"))
    return actual_value.is_finite() and abs(actual_value - expected) <= tolerance


def _validate_inputs(
    signal: Mapping[str, Any],
    validated_risk: Mapping[str, Any],
    instrument: XBTUSDTInstrument,
) -> tuple[str, Decimal, Decimal, Decimal, int, int, Decimal]:
    if validated_risk.get("approved") is not True:
        raise OrderExecutionError(
            f"risk filter did not approve signal: {validated_risk.get('reason')}"
        )
    side = str(signal.get("signal", "")).upper()
    if side not in {"LONG", "SHORT"}:
        raise OrderExecutionError("signal must be LONG or SHORT")
    entry = _as_decimal(signal.get("entry_price"), "entry_price")
    stop = _as_decimal(signal.get("sl_price"), "sl_price")
    target = _as_decimal(signal.get("tp_price"), "tp_price")
    if min(entry, stop, target) <= 0:
        raise OrderExecutionError("signal prices must be positive")
    if side == "LONG" and not stop < entry < target:
        raise OrderExecutionError("LONG requires stop < entry < target")
    if side == "SHORT" and not target < entry < stop:
        raise OrderExecutionError("SHORT requires target < entry < stop")

    try:
        contracts = int(validated_risk["position_size_contracts"])
        leverage = int(validated_risk["leverage"])
    except (KeyError, TypeError, ValueError) as exc:
        raise OrderExecutionError(
            "risk result omitted whole contract or leverage values"
        ) from exc
    if contracts <= 0 or instrument.round_down_contracts(contracts) != contracts:
        raise OrderExecutionError("risk contract quantity is not a positive exchange lot")
    if leverage < 1 or leverage > 100:
        raise OrderExecutionError("risk leverage is outside the supported range")

    base_btc = instrument.contracts_to_btc(contracts)
    notional = instrument.notional_usdt(contracts, entry)
    entry_slippage_bps = _as_decimal(
        validated_risk.get("entry_slippage_bps"),
        "entry_slippage_bps",
    )
    stop_slippage_bps = _as_decimal(
        validated_risk.get("stop_slippage_bps"),
        "stop_slippage_bps",
    )
    taker_fee_rate = _as_decimal(
        validated_risk.get("taker_fee_rate"),
        "taker_fee_rate",
    )
    if entry_slippage_bps < MIN_ENTRY_SLIPPAGE_BPS:
        raise OrderExecutionError("entry slippage buffer is below the safety floor")
    if stop_slippage_bps < MIN_STOP_SLIPPAGE_BPS:
        raise OrderExecutionError("stop slippage buffer is below the safety floor")
    if taker_fee_rate != instrument.taker_fee_rate:
        raise OrderExecutionError("risk fee rate does not match instrument metadata")
    raw_stop_loss = base_btc * abs(entry - stop)
    entry_slippage = base_btc * entry * entry_slippage_bps / Decimal(10_000)
    stop_slippage = base_btc * stop * stop_slippage_bps / Decimal(10_000)
    round_trip_fees = base_btc * (entry + stop) * taker_fee_rate
    estimated_costs = entry_slippage + stop_slippage + round_trip_fees
    expected_loss = raw_stop_loss + estimated_costs
    expected_fields = {
        "position_size_btc": base_btc,
        "notional_usdt": notional,
        "expected_max_loss_usdt": expected_loss,
        "raw_stop_loss_usdt": raw_stop_loss,
        "estimated_costs_usdt": estimated_costs,
    }
    for field, expected in expected_fields.items():
        if not _close_enough(validated_risk.get(field), expected):
            raise OrderExecutionError(
                f"risk result {field} does not match verified instrument units"
            )
    return side, entry, stop, target, contracts, leverage, expected_loss


def _ambiguous_error_types() -> tuple[type[BaseException], ...]:
    candidates: list[type[BaseException]] = [TimeoutError, ConnectionError]
    for name in ("NetworkError", "RequestTimeout", "ExchangeNotAvailable"):
        candidate = getattr(ccxt, name, None)
        if isinstance(candidate, type) and candidate not in candidates:
            candidates.append(candidate)
    return tuple(candidates)


def _best_prices(exchange: Any) -> tuple[Any, Any]:
    book = exchange.fetch_order_book(SYMBOL, limit=1)
    bids = book.get("bids") if isinstance(book, Mapping) else None
    asks = book.get("asks") if isinstance(book, Mapping) else None
    if not bids or not asks or not bids[0] or not asks[0]:
        raise OrderExecutionError("top-of-book bid and ask are required")
    return bids[0][0], asks[0][0]


def _bounded_entry_limit(
    *,
    side: str,
    signal_entry: Decimal,
    best_bid: Any,
    best_ask: Any,
    tick_size: Decimal,
) -> Decimal:
    """Bound the IOC to both the current book and the risk-approved signal."""

    bid = _as_decimal(best_bid, "best bid")
    ask = _as_decimal(best_ask, "best ask")
    reference = ask if side == "buy" else bid
    fraction = MAX_ENTRY_SLIPPAGE_BPS / Decimal(10_000)
    if abs(reference - signal_entry) > signal_entry * fraction:
        raise OrderExecutionError(
            "top of book moved beyond the risk-approved entry slippage window"
        )
    book_limit = bounded_ioc_limit_price(
        side=side,
        best_bid=bid,
        best_ask=ask,
        max_slippage_bps=MAX_ENTRY_SLIPPAGE_BPS,
        tick_size=tick_size,
    )
    if side == "buy":
        signal_cap = _round_tick(
            signal_entry * (Decimal(1) + fraction),
            tick_size,
            ROUND_FLOOR,
        )
        if ask > signal_cap:
            raise OrderExecutionError("best ask exceeds the approved signal cap")
        return min(book_limit, signal_cap)
    signal_floor = _round_tick(
        signal_entry * (Decimal(1) - fraction),
        tick_size,
        ROUND_CEILING,
    )
    if bid < signal_floor:
        raise OrderExecutionError("best bid is below the approved signal floor")
    return max(book_limit, signal_floor)


def _account_open_positions(exchange: Any) -> list[tuple[str, int]]:
    """Return strictly validated account-wide open positions."""

    attest_testnet_exchange(exchange)
    positions = exchange.fetch_positions()
    if (
        positions is None
        or isinstance(positions, (str, bytes, Mapping))
        or not isinstance(positions, Sequence)
    ):
        raise OrderExecutionError(
            "account-wide position query returned an invalid response"
        )
    open_positions: list[tuple[str, int]] = []
    for position in positions:
        if not isinstance(position, Mapping):
            raise OrderExecutionError("account-wide position record is invalid")
        info = position.get("info")
        if not isinstance(info, Mapping):
            raise OrderExecutionError(
                "account-wide position omitted native BitMEX fields"
            )
        native_symbol = info.get("symbol")
        if not isinstance(native_symbol, str) or not native_symbol.strip():
            raise OrderExecutionError(
                "account-wide position omitted its native symbol"
            )
        current_qty = _as_decimal(
            info.get("currentQty"), "account-wide currentQty"
        )
        if current_qty != current_qty.to_integral_value():
            raise OrderExecutionError(
                "account-wide currentQty must be a whole contract count"
            )
        contracts = _as_decimal(
            position.get("contracts"), "account-wide contracts"
        )
        if (
            contracts < 0
            or contracts != contracts.to_integral_value()
            or contracts != abs(current_qty)
        ):
            raise OrderExecutionError(
                "account-wide contracts disagree with native currentQty"
            )
        is_open = info.get("isOpen")
        if not isinstance(is_open, bool) or is_open != (current_qty != 0):
            raise OrderExecutionError(
                "account-wide isOpen disagrees with native currentQty"
            )
        side = position.get("side")
        expected_side = (
            "long" if current_qty > 0 else "short" if current_qty < 0 else None
        )
        normalized_side = str(side).lower() if side is not None else None
        if normalized_side != expected_side:
            raise OrderExecutionError(
                "account-wide side disagrees with native currentQty"
            )
        if info.get("strategy") not in (None, "OneWay"):
            raise OrderExecutionError(
                "account-wide position is not in OneWay mode"
            )
        if current_qty:
            open_positions.append((native_symbol, int(current_qty)))
    return open_positions


def _account_open_orders(exchange: Any) -> list[Mapping[str, Any]]:
    """Return a strictly shaped account-wide open-order inventory."""

    attest_testnet_exchange(exchange)
    orders = exchange.fetch_open_orders()
    if (
        orders is None
        or isinstance(orders, (str, bytes, Mapping))
        or not isinstance(orders, Sequence)
    ):
        raise OrderExecutionError("account-wide open-order query is invalid")
    if any(not isinstance(order, Mapping) for order in orders):
        raise OrderExecutionError("account-wide open-order record is invalid")
    return list(orders)


def _assert_no_open_orders(exchange: Any) -> None:
    """Reject a new entry while any account order is already resting."""

    orders = _account_open_orders(exchange)
    if orders:
        raise OrderExecutionError(
            "account has an unexpected resting order; refusing a new entry"
        )


def _assert_flat(exchange: Any) -> None:
    position = fetch_xbtusdt_position(exchange)
    if position.contracts != 0:
        raise OrderExecutionError(
            "account has existing XBTUSDT exposure; refusing a new entry"
        )
    if _account_open_positions(exchange):
        raise OrderExecutionError(
            "account has exposure outside the expected flat XBTUSDT state"
        )


def _assert_position(
    exchange: Any,
    *,
    contracts: int,
    side: str,
) -> None:
    position = fetch_xbtusdt_position(exchange)
    expected_side = "long" if side == "LONG" else "short"
    if position.contracts != contracts or position.side != expected_side:
        raise OrderExecutionError(
            "exchange position does not match the filled quantity and side"
        )
    expected_native_qty = contracts if side == "LONG" else -contracts
    if _account_open_positions(exchange) != [("XBTUSDT", expected_native_qty)]:
        raise OrderExecutionError(
            "account-wide exposure differs from the managed XBTUSDT position"
        )


def _set_and_verify_leverage(exchange: Any, leverage: int) -> None:
    attest_testnet_exchange(exchange)
    exchange.set_leverage(leverage, SYMBOL)
    attest_testnet_exchange(exchange)
    state = exchange.fetch_leverage(SYMBOL)
    if not isinstance(state, Mapping):
        raise OrderExecutionError("leverage query returned an invalid response")
    long_value = state.get("longLeverage")
    short_value = state.get("shortLeverage")
    if long_value is None or short_value is None:
        raise OrderExecutionError("exchange did not report both leverage values")
    long_dec = _as_decimal(long_value, "long leverage")
    short_dec = _as_decimal(short_value, "short leverage")
    requested = Decimal(leverage)
    if long_dec != requested or short_dec != requested:
        raise OrderExecutionError(
            f"leverage mismatch: requested {leverage}x, exchange reports "
            f"long={long_value!r}, short={short_value!r}"
        )


def _native_order(order: Mapping[str, Any]) -> Mapping[str, Any]:
    info = order.get("info")
    if not isinstance(info, Mapping):
        raise OrderExecutionError("order response omitted native BitMEX fields")
    return info


def _verify_order_common(
    order: Mapping[str, Any],
    *,
    side: str,
    contracts: int,
    native_type: str,
    client_id: str,
) -> Mapping[str, Any]:
    if order.get("symbol") != SYMBOL:
        raise OrderExecutionError("order response has the wrong symbol")
    if str(order.get("side", "")).lower() != side:
        raise OrderExecutionError("order response has the wrong side")
    amount = _as_decimal(order.get("amount"), "order amount")
    if amount != Decimal(contracts):
        raise OrderExecutionError("order response has the wrong quantity")

    info = _native_order(order)
    if order.get("clientOrderId") != client_id or info.get("clOrdID") != client_id:
        raise OrderExecutionError(
            "unified/native order response has the wrong client order ID"
        )
    if info.get("symbol") != "XBTUSDT":
        raise OrderExecutionError("native order response has the wrong symbol")
    if str(info.get("side", "")).lower() != side:
        raise OrderExecutionError("native order response has the wrong side")
    if _as_decimal(info.get("orderQty"), "native order quantity") != Decimal(
        contracts
    ):
        raise OrderExecutionError("native order response has the wrong quantity")
    if str(info.get("ordType", "")).lower() != native_type.lower():
        raise OrderExecutionError("native order response has the wrong order type")
    return info


def _verify_entry_order(
    order: Mapping[str, Any],
    *,
    side: str,
    contracts: int,
    limit_price: Decimal,
    client_id: str,
) -> None:
    info = _verify_order_common(
        order,
        side=side,
        contracts=contracts,
        native_type="Limit",
        client_id=client_id,
    )
    if str(info.get("timeInForce", "")).lower() != "immediateorcancel":
        raise OrderExecutionError("entry response is not ImmediateOrCancel")
    if _as_decimal(info.get("price"), "native entry price") != limit_price:
        raise OrderExecutionError("entry response price exceeds the approved bound")


def _verify_stop_order(
    order: Mapping[str, Any],
    *,
    side: str,
    contracts: int,
    stop_price: Decimal,
    link_id: str,
    client_id: str,
) -> None:
    info = _verify_order_common(
        order,
        side=side,
        contracts=contracts,
        native_type="Stop",
        client_id=client_id,
    )
    if _as_decimal(info.get("stopPx"), "native stop price") != stop_price:
        raise OrderExecutionError("stop response has the wrong trigger price")
    instructions = {
        item.strip().lower()
        for item in str(info.get("execInst", "")).split(",")
        if item.strip()
    }
    if not {"close", "markprice"} <= instructions:
        raise OrderExecutionError(
            "stop response lacks Close and MarkPrice execution instructions"
        )
    if order.get("reduceOnly") is not True:
        raise OrderExecutionError("stop response is not affirmatively reduce-only")
    if info.get("clOrdLinkID") != link_id:
        raise OrderExecutionError("stop response has the wrong OCO link ID")
    if str(info.get("contingencyType", "")).lower() != "onecancelstheother":
        raise OrderExecutionError(
            "stop response lacks OneCancelsTheOther contingency"
        )


def _verify_target_order(
    order: Mapping[str, Any],
    *,
    side: str,
    contracts: int,
    target_price: Decimal,
    link_id: str,
    client_id: str,
) -> None:
    info = _verify_order_common(
        order,
        side=side,
        contracts=contracts,
        native_type="Limit",
        client_id=client_id,
    )
    if _as_decimal(info.get("price"), "native target price") != target_price:
        raise OrderExecutionError("target response has the wrong limit price")
    instructions = {
        item.strip().lower()
        for item in str(info.get("execInst", "")).split(",")
        if item.strip()
    }
    if "reduceonly" not in instructions or order.get("reduceOnly") is not True:
        raise OrderExecutionError("target response is not affirmatively reduce-only")
    if str(info.get("timeInForce", "")).lower() != "goodtillcancel":
        raise OrderExecutionError("target response is not GoodTillCancel")
    if info.get("clOrdLinkID") != link_id:
        raise OrderExecutionError("target response has the wrong OCO link ID")
    if str(info.get("contingencyType", "")).lower() != "onecancelstheother":
        raise OrderExecutionError(
            "target response lacks OneCancelsTheOther contingency"
        )


def _create_private_order(exchange: Any, *args: Any, **kwargs: Any) -> Mapping[str, Any]:
    attest_testnet_exchange(exchange)
    return exchange.create_order(*args, **kwargs)


def _confirm_flat(exchange: Any) -> None:
    for confirmation in range(FLAT_CONFIRMATIONS):
        _assert_flat(exchange)
        if confirmation + 1 < FLAT_CONFIRMATIONS and RECONCILE_DELAY_SECONDS:
            time.sleep(RECONCILE_DELAY_SECONDS)


def _verify_emergency_order(
    order: Mapping[str, Any],
    *,
    side: str,
    contracts: int,
    client_id: str,
) -> None:
    info = _verify_order_common(
        order,
        side=side,
        contracts=contracts,
        native_type="Market",
        client_id=client_id,
    )
    instructions = {
        item.strip().lower()
        for item in str(info.get("execInst", "")).split(",")
        if item.strip()
    }
    if "close" not in instructions or order.get("reduceOnly") is not True:
        raise OrderExecutionError("emergency response is not affirmatively Close")


def _refresh_order(
    exchange: Any,
    order: Mapping[str, Any],
    client_id: str,
    *,
    stop_when_open: bool,
) -> tuple[Mapping[str, Any], Any]:
    current: Mapping[str, Any] = order
    for attempt in range(RECONCILE_ATTEMPTS + 1):
        classification = classify_order(current)
        if classification.is_filled:
            return current, classification
        if classification.is_partial and classification.filled > 0:
            return current, classification
        if classification.state in {"canceled", "rejected", "expired"}:
            return current, classification
        if stop_when_open and classification.state == "open":
            return current, classification
        raw = (classification.raw_status or "").strip().lower()
        if raw == "closed" and classification.filled == 0:
            return current, classification
        if attempt >= RECONCILE_ATTEMPTS:
            return current, classification
        if RECONCILE_DELAY_SECONDS:
            time.sleep(RECONCILE_DELAY_SECONDS)
        order_id = _order_id(current, required=False)
        if order_id is not None:
            try:
                attest_testnet_exchange(exchange)
                current = exchange.fetch_order(order_id, SYMBOL)
                continue
            except Exception:  # agent-quality: allow: deterministic client-ID lookup is the explicit fallback
                pass
        attest_testnet_exchange(exchange)
        reconciled = find_order_by_client_id(exchange, client_id, symbol=SYMBOL)
        if reconciled is not None:
            current = reconciled
    return current, classify_order(current)


def _cancel_order_to_terminal(
    exchange: Any,
    order: Mapping[str, Any],
    client_id: str,
    *,
    label: str,
) -> tuple[Mapping[str, Any], Any]:
    current = order
    classification = classify_order(current)
    if classification.terminal:
        return current, classification
    order_id = _order_id(current)
    cancel_error: BaseException | None = None
    try:
        attest_testnet_exchange(exchange)
        exchange.cancel_order(order_id, SYMBOL)
    except Exception as exc:  # agent-quality: allow: poll after an ambiguous cancel response
        cancel_error = exc

    for attempt in range(RECONCILE_ATTEMPTS + 1):
        try:
            attest_testnet_exchange(exchange)
            current = exchange.fetch_order(order_id, SYMBOL)
        except Exception:  # agent-quality: allow: deterministic client-ID lookup is the explicit fallback
            attest_testnet_exchange(exchange)
            reconciled = find_order_by_client_id(
                exchange,
                client_id,
                symbol=SYMBOL,
            )
            if reconciled is not None:
                current = reconciled
        classification = classify_order(current)
        if classification.terminal:
            return current, classification
        if attempt < RECONCILE_ATTEMPTS and RECONCILE_DELAY_SECONDS:
            time.sleep(RECONCILE_DELAY_SECONDS)

    suffix = (
        f"; cancel response was {_safe_error(cancel_error)}"
        if cancel_error is not None
        else ""
    )
    raise ProtectiveOrderUnresolved(
        f"{label} cancellation could not be proven terminal{suffix}"
    )


def _cancel_entry_remainder(
    exchange: Any,
    order: Mapping[str, Any],
    client_id: str,
) -> tuple[Mapping[str, Any], Any]:
    try:
        return _cancel_order_to_terminal(
            exchange,
            order,
            client_id,
            label="entry remainder",
        )
    except ProtectiveOrderUnresolved as exc:
        raise EntryOrderUnresolved(str(exc)) from exc


def _find_and_cancel_untrusted_order(
    exchange: Any,
    order: Mapping[str, Any] | None,
    client_id: str,
    *,
    label: str,
) -> tuple[Mapping[str, Any], Any]:
    """Find an untrusted order by durable ID and prove it terminal."""

    current = order
    if current is None:
        for attempt in range(RECONCILE_ATTEMPTS + 1):
            attest_testnet_exchange(exchange)
            current = find_order_by_client_id(
                exchange,
                client_id,
                symbol=SYMBOL,
            )
            if current is not None:
                break
            if attempt < RECONCILE_ATTEMPTS and RECONCILE_DELAY_SECONDS:
                time.sleep(RECONCILE_DELAY_SECONDS)
    if current is None:
        raise ProtectiveOrderUnresolved(
            f"{label} may exist but was not visible for deterministic cancellation"
        )
    return _cancel_order_to_terminal(
        exchange,
        current,
        client_id,
        label=label,
    )


def _reverify_active_stop(
    exchange: Any,
    *,
    client_id: str,
    close_side: str,
    contracts: int,
    stop_price: Decimal,
    link_id: str,
    position_side: str,
) -> Mapping[str, Any]:
    """Prove the stop and account exposure after target cleanup."""

    attest_testnet_exchange(exchange)
    stop_order = find_order_by_client_id(exchange, client_id, symbol=SYMBOL)
    if stop_order is None:
        raise OrderExecutionError("protective stop disappeared during target cleanup")
    stop_order, stop_state = _refresh_order(
        exchange,
        stop_order,
        client_id,
        stop_when_open=True,
    )
    if stop_state.state != "open":
        raise OrderExecutionError("protective stop is no longer open")
    _verify_stop_order(
        stop_order,
        side=close_side,
        contracts=contracts,
        stop_price=stop_price,
        link_id=link_id,
        client_id=client_id,
    )
    _assert_position(exchange, contracts=contracts, side=position_side)
    return stop_order


def _stable_position(exchange: Any) -> Any:
    snapshots = []
    for attempt in range(FLAT_CONFIRMATIONS):
        snapshots.append(fetch_xbtusdt_position(exchange))
        if attempt + 1 < FLAT_CONFIRMATIONS and RECONCILE_DELAY_SECONDS:
            time.sleep(RECONCILE_DELAY_SECONDS)
    first = snapshots[0]
    if any(
        item.contracts != first.contracts or item.side != first.side
        for item in snapshots[1:]
    ):
        raise OrderExecutionError("position changed during the stabilization window")
    return first


def _inspect_unattributed_pending_state(
    exchange: Any,
    expected_client_id: str,
) -> None:
    """Inspect/cancel account state while the expected entry is invisible."""

    def inspect_exposure() -> list[tuple[str, int]]:
        position = fetch_xbtusdt_position(exchange)
        account_positions = _account_open_positions(exchange)
        has_xbtusdt = any(
            symbol == "XBTUSDT" for symbol, _ in account_positions
        )
        if position.contracts or has_xbtusdt:
            raise UnattributedEntryExposure(
                "ENTRY_PENDING has XBTUSDT exposure without a visible expected order"
            )
        return account_positions

    unexpected_orders = _account_open_orders(exchange)
    if unexpected_orders:
        cancellation_error: BaseException | None = None
        try:
            for order in unexpected_orders:
                inspect_exposure()
                _cancel_order_to_terminal(
                    exchange,
                    order,
                    expected_client_id,
                    label="unexpected ENTRY_PENDING order",
                )
        except Exception as exc:  # agent-quality: allow: exposure is rechecked before unresolved cancellation is returned
            cancellation_error = exc
        post_cancel_positions = inspect_exposure()
        if post_cancel_positions:
            raise UnexpectedPendingAccountState(
                "ENTRY_PENDING has unexpected non-XBTUSDT account exposure"
            )
        if cancellation_error is not None:
            raise EntryOrderUnresolved(
                f"unexpected ENTRY_PENDING order cleanup is unresolved: "
                f"{_safe_error(cancellation_error)}"
            ) from cancellation_error

    account_positions: list[tuple[str, int]] = []
    for confirmation in range(FLAT_CONFIRMATIONS):
        account_positions = inspect_exposure()
        if confirmation + 1 < FLAT_CONFIRMATIONS and RECONCILE_DELAY_SECONDS:
            time.sleep(RECONCILE_DELAY_SECONDS)
    if account_positions:
        raise UnexpectedPendingAccountState(
            "ENTRY_PENDING has unexpected non-XBTUSDT account exposure"
        )
    if unexpected_orders:
        raise UnexpectedPendingAccountState(
            "unexpected ENTRY_PENDING order was canceled and proven terminal; "
            "manual exchange reconciliation is required"
        )


def _reconcile_entry_to_terminal(
    exchange: Any,
    client_id: str,
) -> tuple[Mapping[str, Any], Any, Any]:
    """Poll, cancel when visible, and prove an ambiguous IOC terminal."""

    last_order: Mapping[str, Any] | None = None
    for attempt in range(RECONCILE_ATTEMPTS + 1):
        attest_testnet_exchange(exchange)
        current = find_order_by_client_id(exchange, client_id, symbol=SYMBOL)
        if current is not None:
            last_order = current
            classification = classify_order(current)
            if not classification.terminal:
                try:
                    current, classification = _cancel_entry_remainder(
                        exchange,
                        current,
                        client_id,
                    )
                except EntryOrderUnresolved:
                    _inspect_unattributed_pending_state(exchange, client_id)
                    raise
                last_order = current
            if classification.terminal:
                position = _stable_position(exchange)
                if position.contracts != int(classification.filled):
                    raise OrderExecutionError(
                        "terminal entry fill disagrees with the stable position"
                    )
                return current, classification, position
        else:
            _inspect_unattributed_pending_state(exchange, client_id)
        if attempt < RECONCILE_ATTEMPTS and RECONCILE_DELAY_SECONDS:
            time.sleep(RECONCILE_DELAY_SECONDS)
    detail = "not visible" if last_order is None else "not terminal"
    raise OrderExecutionError(
        f"entry remains {detail} after bounded reconciliation; keep monitoring"
    )


def _round_tick(value: Decimal, tick: Decimal, rounding: str) -> Decimal:
    return (value / tick).to_integral_value(rounding=rounding) * tick


def _anchored_exit_prices(
    side: str,
    signal_entry: Decimal,
    signal_stop: Decimal,
    signal_target: Decimal,
    actual_entry: Decimal,
    tick: Decimal,
) -> tuple[Decimal, Decimal]:
    if side == "LONG":
        stop_fraction = (signal_entry - signal_stop) / signal_entry
        target_fraction = (signal_target - signal_entry) / signal_entry
        stop = _round_tick(
            actual_entry * (Decimal(1) - stop_fraction), tick, ROUND_CEILING
        )
        target = _round_tick(
            actual_entry * (Decimal(1) + target_fraction), tick, ROUND_FLOOR
        )
        valid = stop < actual_entry < target
    else:
        stop_fraction = (signal_stop - signal_entry) / signal_entry
        target_fraction = (signal_entry - signal_target) / signal_entry
        stop = _round_tick(
            actual_entry * (Decimal(1) + stop_fraction), tick, ROUND_FLOOR
        )
        target = _round_tick(
            actual_entry * (Decimal(1) - target_fraction), tick, ROUND_CEILING
        )
        valid = target < actual_entry < stop
    if not valid:
        raise OrderExecutionError("tick-rounded exits are not on safe sides of fill")
    return stop, target


def _halt(
    decision_key: str,
    current_status: str,
    reason: str,
    ledger_path: str,
) -> None:
    transition_intent(
        decision_key,
        "HALTED_MANUAL",
        expected_statuses={current_status},
        updates={"halt_reason": reason},
        db_path=ledger_path,
    )


def _emergency_close(
    exchange: Any,
    *,
    decision_key: str,
    now_ms: int,
    close_side: str,
    contracts: int,
    current_status: str,
    ledger_path: str,
    reason: str,
) -> str:
    del now_ms
    emergency_id = reconciliation_client_order_id(decision_key, "emergency")
    try:
        position = fetch_xbtusdt_position(exchange)
        if position.contracts == 0:
            _confirm_flat(exchange)
            halt_reason = f"{reason}; account was repeatedly verified flat"
            _halt(decision_key, current_status, halt_reason, ledger_path)
            return halt_reason
        actual_close_side = "sell" if position.side == "long" else "buy"
        actual_contracts = position.contracts
        if actual_close_side != close_side or actual_contracts > contracts:
            reason = (
                f"{reason}; canonical position differed from expected exposure and "
                "the actual position was used for Close"
            )
        attest_testnet_exchange(exchange)
        result = create_order_once(
            exchange,
            client_order_id=emergency_id,
            symbol=SYMBOL,
            ambiguous_error_types=_ambiguous_error_types(),
            create=lambda cid: _create_private_order(
                exchange,
                SYMBOL,
                "market",
                actual_close_side,
                actual_contracts,
                params={"execInst": "Close", "clientOrderId": cid},
            ),
        )
        close_order, close_state = _refresh_order(
            exchange,
            result.order,
            emergency_id,
            stop_when_open=False,
        )
        _verify_emergency_order(
            close_order,
            side=actual_close_side,
            contracts=actual_contracts,
            client_id=emergency_id,
        )
        if not close_state.is_filled:
            raise OrderExecutionError("emergency Close did not prove a full fill")
        close_order_id = _order_id(close_order)
        record_execution_evidence(
            decision_key,
            expected_statuses={current_status},
            updates={
                "exit_client_order_id": emergency_id,
                "exit_order_id": close_order_id,
            },
            db_path=ledger_path,
        )
        _confirm_flat(exchange)
        halt_reason = f"{reason}; emergency Close repeatedly verified account flat"
    except Exception as exc:  # agent-quality: allow: failure is persisted as HALTED_MANUAL below
        try:
            _confirm_flat(exchange)
        except Exception as flat_exc:  # agent-quality: allow: both failures are persisted in HALTED_MANUAL
            halt_reason = (
                f"{reason}; emergency Close could not be proven: {_safe_error(exc)}; "
                "flat account state also could not be proven: "
                f"{_safe_error(flat_exc)}. "
                "Immediate manual exchange reconciliation required"
            )
        else:
            halt_reason = (
                f"{reason}; emergency Close submission failed "
                f"({_safe_error(exc)}), but the account was repeatedly verified flat"
            )
    _halt(decision_key, current_status, halt_reason, ledger_path)
    return halt_reason


def _register(
    *,
    decision_key: str,
    now_ms: int,
    side: str,
    entry: Decimal,
    stop: Decimal,
    target: Decimal,
    contracts: int,
    expected_max_loss: Decimal,
    instrument: XBTUSDTInstrument,
    ids: Mapping[str, str],
    ledger_path: str,
) -> dict[str, Any]:
    decision = validate_completed_candle_decision_key(decision_key, now_ms=now_ms)
    return register_intent(
        {
            "decision_key": decision_key,
            "strategy_version": STRATEGY_VERSION,
            "environment": "testnet",
            "symbol": SYMBOL,
            "side": side,
            "decision_time_utc": datetime.fromtimestamp(
                decision.candle_close_ms / 1000,
                tz=timezone.utc,
            ).isoformat(),
            "signal_entry_price_usdt": float(entry),
            "signal_stop_price_usdt": float(stop),
            "signal_target_price_usdt": float(target),
            "requested_contracts": contracts,
            "contract_size_btc": float(instrument.contract_size_btc),
            "requested_base_btc": float(instrument.contracts_to_btc(contracts)),
            "requested_notional_usdt": float(
                instrument.notional_usdt(contracts, entry)
            ),
            "expected_max_loss_usdt": float(expected_max_loss),
            "unit_model": UNIT_MODEL,
            "entry_client_order_id": ids["entry"],
            "stop_client_order_id": ids["sl"],
            "target_client_order_id": ids["tp"],
        },
        db_path=ledger_path,
    )


def _verify_existing_protection(
    exchange: Any,
    row: Mapping[str, Any],
    instrument: XBTUSDTInstrument,
) -> None:
    side = str(row["side"])
    filled = int(row["filled_contracts"])
    close_side = "sell" if side == "LONG" else "buy"
    actual_entry = _as_decimal(row["actual_entry_price_usdt"], "actual entry")
    stop_price, target_price = _anchored_exit_prices(
        side,
        _as_decimal(row["signal_entry_price_usdt"], "signal entry"),
        _as_decimal(row["signal_stop_price_usdt"], "signal stop"),
        _as_decimal(row["signal_target_price_usdt"], "signal target"),
        actual_entry,
        instrument.tick_size_usdt,
    )
    link_id = reconciliation_client_order_id(
        str(row["decision_key"]),
        "oco",
    )
    stop_client_id = str(row["stop_client_order_id"])
    attest_testnet_exchange(exchange)
    stop_order = find_order_by_client_id(
        exchange,
        stop_client_id,
        symbol=SYMBOL,
    )
    if stop_order is None:
        raise OrderExecutionError("durable protective stop is missing")
    stop_order, stop_state = _refresh_order(
        exchange,
        stop_order,
        stop_client_id,
        stop_when_open=True,
    )
    if stop_state.state != "open":
        raise OrderExecutionError("durable protective stop is not open")
    _verify_stop_order(
        stop_order,
        side=close_side,
        contracts=filled,
        stop_price=stop_price,
        link_id=link_id,
        client_id=stop_client_id,
    )

    if str(row["status"]) == "PROTECTED":
        target_client_id = str(row["target_client_order_id"])
        attest_testnet_exchange(exchange)
        target_order = find_order_by_client_id(
            exchange,
            target_client_id,
            symbol=SYMBOL,
        )
        if target_order is None:
            raise OrderExecutionError("durable take-profit order is missing")
        target_order, target_state = _refresh_order(
            exchange,
            target_order,
            target_client_id,
            stop_when_open=True,
        )
        if target_state.state != "open":
            raise OrderExecutionError("durable take-profit order is not open")
        _verify_target_order(
            target_order,
            side=close_side,
            contracts=filled,
            target_price=target_price,
            link_id=link_id,
            client_id=target_client_id,
        )
    _assert_position(exchange, contracts=filled, side=side)


def _reconcile_open_intent_locked(
    exchange: Any,
    row: Mapping[str, Any],
    *,
    ledger_path: str,
) -> dict[str, Any]:
    decision_key = str(row["decision_key"])
    status = str(row["status"])
    side = str(row["side"])
    close_side = "sell" if side == "LONG" else "buy"
    requested = int(row["requested_contracts"])
    instrument = getattr(exchange, "xbtusdt_instrument", None)
    if not isinstance(instrument, XBTUSDTInstrument):
        raise OrderExecutionError("exchange lacks verified XBTUSDT metadata")

    if status == "REGISTERED":
        _confirm_flat(exchange)
        _assert_no_open_orders(exchange)
        transition_intent(
            decision_key,
            "FAILED_FLAT",
            expected_statuses={"REGISTERED"},
            db_path=ledger_path,
        )
        return {
            "status": "reconciled_flat",
            "reason": "pre-submission intent was flat and retired",
        }

    if status in {"ENTRY_PENDING", "ENTRY_PARTIAL", "ENTRY_FILLED"}:
        try:
            entry_order, entry_state, _ = _reconcile_entry_to_terminal(
                exchange,
                str(row["entry_client_order_id"]),
            )
        except UnattributedEntryExposure as exc:  # agent-quality: allow: detected exposure is closed and returned as manual_halt
            reason = _emergency_close(
                exchange,
                decision_key=decision_key,
                now_ms=int(time.time() * 1000),
                close_side=close_side,
                contracts=requested,
                current_status=status,
                ledger_path=ledger_path,
                reason=_safe_error(exc),
            )
            return {"status": "manual_halt", "reason": reason}
        except UnexpectedPendingAccountState as exc:  # agent-quality: allow: unexpected state is persisted and returned as manual_halt
            reason = _safe_error(exc)
            _halt(decision_key, status, reason, ledger_path)
            return {"status": "manual_halt", "reason": reason}
        except Exception as exc:  # agent-quality: allow: unresolved state remains durable for the next monitor pass
            return {
                "status": "reconciling",
                "reason": _safe_error(exc),
            }
        filled = int(entry_state.filled)
        if Decimal(filled) != entry_state.filled:
            raise OrderExecutionError("exchange reported fractional contracts")
        if filled == 0:
            if status != "ENTRY_PENDING":
                raise OrderExecutionError("ledger fill disagrees with terminal entry")
            _assert_no_open_orders(exchange)
            transition_intent(
                decision_key,
                "FAILED_FLAT",
                expected_statuses={"ENTRY_PENDING"},
                db_path=ledger_path,
            )
            return {
                "status": "reconciled_flat",
                "reason": "entry was terminal with zero fill",
            }
        if filled > requested:
            raise OrderExecutionError("entry fill exceeds requested contracts")

        current_status = status
        if status == "ENTRY_PENDING":
            average = _average_price(entry_order)
            current_status = "ENTRY_FILLED" if filled == requested else "ENTRY_PARTIAL"
            transition_intent(
                decision_key,
                current_status,
                expected_statuses={"ENTRY_PENDING"},
                updates={
                    "entry_order_id": _order_id(entry_order),
                    "filled_contracts": filled,
                    "actual_entry_price_usdt": float(average),
                },
                db_path=ledger_path,
            )
        elif int(row["filled_contracts"]) != filled:
            raise OrderExecutionError("ledger fill disagrees with terminal entry")

        reason = _emergency_close(
            exchange,
            decision_key=decision_key,
            now_ms=int(time.time() * 1000),
            close_side=close_side,
            contracts=filled,
            current_status=current_status,
            ledger_path=ledger_path,
            reason="restart found entry exposure before durable protection",
        )
        return {"status": "manual_halt", "reason": reason}

    if status in {"PROTECTED", "PROTECTED_NO_TP"}:
        position = _stable_position(exchange)
        if position.contracts == 0:
            reason = (
                "protected position is now flat; fill, fee, funding, sibling-order "
                "cancellation, and exit accounting require reconciliation"
            )
            _halt(decision_key, status, reason, ledger_path)
            return {"status": "manual_halt", "reason": reason}
        try:
            _verify_existing_protection(exchange, row, instrument)
        except Exception as exc:  # agent-quality: allow: broken protection triggers conservative flattening
            reason = _emergency_close(
                exchange,
                decision_key=decision_key,
                now_ms=int(time.time() * 1000),
                close_side=close_side,
                contracts=int(row["filled_contracts"]),
                current_status=status,
                ledger_path=ledger_path,
                reason=f"restart could not prove protection: {_safe_error(exc)}",
            )
            return {"status": "manual_halt", "reason": reason}
        return {
            "status": "managed_position",
            "reason": f"{status} exchange protection was re-verified",
        }

    if status == "HALTED_MANUAL":
        position = _stable_position(exchange)
        exposure = f"{position.contracts} contracts" if position.contracts else "flat"
        return {
            "status": "manual_halt",
            "reason": f"manual reconciliation remains required; account is {exposure}",
        }
    raise OrderExecutionError(f"unsupported durable intent status {status}")


def reconcile_open_intent(
    exchange: Any,
    row: Mapping[str, Any],
    *,
    ledger_path: str = DB_PATH,
) -> dict[str, Any]:
    """Repair or re-verify one durable intent without replaying its signal."""

    _assert_testnet()
    try:
        attest_testnet_exchange(exchange)
        with exclusive_execution_lock(ledger_path):
            return _reconcile_open_intent_locked(
                exchange,
                row,
                ledger_path=ledger_path,
            )
    except Exception as exc:  # agent-quality: allow: failed reconciliation remains a visible no-trade state
        return {"status": "reconciling", "reason": _safe_error(exc)}


def _execute_signal_locked(
    signal: Mapping[str, Any],
    validated_risk: Mapping[str, Any],
    *,
    decision_key: str,
    now_ms: int | None = None,
    exchange: Any | None = None,
    ledger_path: str = DB_PATH,
) -> dict[str, Any]:
    """Execute while the caller holds exclusive execution authority."""

    _assert_testnet()
    current_ms = int(time.time() * 1000) if now_ms is None else now_ms
    try:
        if exchange is None:
            exchange = get_client()
        attest_testnet_exchange(exchange)
        instrument = getattr(exchange, "xbtusdt_instrument", None)
        if not isinstance(instrument, XBTUSDTInstrument):
            raise OrderExecutionError("exchange lacks verified XBTUSDT metadata")
        side, entry, stop, target, contracts, leverage, expected_max_loss = _validate_inputs(
            signal,
            validated_risk,
            instrument,
        )
        ids = client_order_ids(decision_key, now_ms=current_ms)
        oco_id = reconciliation_client_order_id(decision_key, "oco")
        row = _register(
            decision_key=decision_key,
            now_ms=current_ms,
            side=side,
            entry=entry,
            stop=stop,
            target=target,
            contracts=contracts,
            expected_max_loss=expected_max_loss,
            instrument=instrument,
            ids=ids,
            ledger_path=ledger_path,
        )
    except Exception as exc:  # agent-quality: allow: preflight failure is returned with status=failed
        return _result("failed", decision_key=decision_key, error=_safe_error(exc))

    status = str(row["status"])
    if status == "PROTECTED":
        filled = int(row["filled_contracts"])
        close_side = "sell" if side == "LONG" else "buy"
        try:
            actual_entry = _as_decimal(
                row["actual_entry_price_usdt"], "actual entry"
            )
            stop_price, target_price = _anchored_exit_prices(
                side,
                entry,
                stop,
                target,
                actual_entry,
                instrument.tick_size_usdt,
            )
            attest_testnet_exchange(exchange)
            existing_stop = find_order_by_client_id(
                exchange, ids["sl"], symbol=SYMBOL
            )
            attest_testnet_exchange(exchange)
            existing_target = find_order_by_client_id(
                exchange, ids["tp"], symbol=SYMBOL
            )
            if existing_stop is None or existing_target is None:
                raise OrderExecutionError("protected order is missing from the exchange")
            existing_stop, stop_state = _refresh_order(
                exchange, existing_stop, ids["sl"], stop_when_open=True
            )
            existing_target, target_state = _refresh_order(
                exchange, existing_target, ids["tp"], stop_when_open=True
            )
            if stop_state.state != "open" or target_state.state != "open":
                raise OrderExecutionError("protected order is no longer open")
            _verify_stop_order(
                existing_stop,
                side=close_side,
                contracts=filled,
                stop_price=stop_price,
                link_id=oco_id,
                client_id=ids["sl"],
            )
            _verify_target_order(
                existing_target,
                side=close_side,
                contracts=filled,
                target_price=target_price,
                link_id=oco_id,
                client_id=ids["tp"],
            )
            _assert_position(exchange, contracts=filled, side=side)
        except Exception as exc:  # agent-quality: allow: invalid protection triggers conservative flattening
            halt_reason = _emergency_close(
                exchange,
                decision_key=decision_key,
                now_ms=current_ms,
                close_side=close_side,
                contracts=filled,
                current_status="PROTECTED",
                ledger_path=ledger_path,
                reason=f"existing protection could not be proven: {_safe_error(exc)}",
            )
            return _result(
                "manual_halt",
                decision_key=decision_key,
                filled_contracts=filled,
                fill_price=float(actual_entry) if "actual_entry" in locals() else None,
                error=halt_reason,
            )
        return _result(
            "already_protected",
            decision_key=decision_key,
            sl_order=existing_stop,
            tp_order=existing_target,
            filled_contracts=filled,
            fill_price=float(actual_entry),
        )
    if status in {"CLOSED", "FAILED_FLAT"}:
        return _result(
            "duplicate_blocked",
            decision_key=decision_key,
            error=f"decision is already terminal with status {status}",
        )
    if status == "HALTED_MANUAL":
        return _result(
            "manual_halt",
            decision_key=decision_key,
            error=str(row["halt_reason"]),
        )

    entry_order: Mapping[str, Any] | None = None
    close_side = "sell" if side == "LONG" else "buy"
    entry_submission_started = False
    potential_exposure_contracts = int(row["filled_contracts"])
    protection_proven = status == "PROTECTED_NO_TP"
    try:
        if status == "REGISTERED":
            _assert_flat(exchange)
            _assert_no_open_orders(exchange)
            _set_and_verify_leverage(exchange, leverage)
            row = transition_intent(
                decision_key,
                "ENTRY_PENDING",
                expected_statuses={"REGISTERED"},
                db_path=ledger_path,
            )
            status = "ENTRY_PENDING"

        if status == "ENTRY_PENDING":
            best_bid, best_ask = _best_prices(exchange)
            entry_side = "buy" if side == "LONG" else "sell"
            limit_price = _bounded_entry_limit(
                side=entry_side,
                signal_entry=entry,
                best_bid=best_bid,
                best_ask=best_ask,
                tick_size=instrument.tick_size_usdt,
            )
            entry_submission_started = True
            attest_testnet_exchange(exchange)
            safe_create = create_order_once(
                exchange,
                client_order_id=ids["entry"],
                symbol=SYMBOL,
                ambiguous_error_types=_ambiguous_error_types(),
                create=lambda cid: _create_private_order(
                    exchange,
                    SYMBOL,
                    "limit",
                    entry_side,
                    contracts,
                    float(limit_price),
                    {
                        "timeInForce": "ImmediateOrCancel",
                        "clientOrderId": cid,
                    },
                ),
            )
            entry_order = safe_create.order
            entry_order_id = _order_id(entry_order)
            row = record_execution_evidence(
                decision_key,
                expected_statuses={"ENTRY_PENDING"},
                updates={"entry_order_id": entry_order_id},
                db_path=ledger_path,
            )
            entry_order, entry_state = _refresh_order(
                exchange,
                entry_order,
                ids["entry"],
                stop_when_open=False,
            )
            if not entry_state.terminal:
                entry_order, entry_state = _cancel_entry_remainder(
                    exchange,
                    entry_order,
                    ids["entry"],
                )
            _verify_entry_order(
                entry_order,
                side=entry_side,
                contracts=contracts,
                limit_price=limit_price,
                client_id=ids["entry"],
            )
            if entry_state.filled <= 0:
                raw = (entry_state.raw_status or "").strip().lower()
                if entry_state.state in {"canceled", "rejected", "expired"} or raw == "closed":
                    _confirm_flat(exchange)
                    _assert_no_open_orders(exchange)
                    transition_intent(
                        decision_key,
                        "FAILED_FLAT",
                        expected_statuses={"ENTRY_PENDING"},
                        db_path=ledger_path,
                    )
                    return _result(
                        "no_fill",
                        decision_key=decision_key,
                        entry_order=entry_order,
                        error="bounded IOC entry completed with zero fill",
                    )
                raise OrderExecutionError(
                    "entry fill state remains ambiguous; order will not be resubmitted"
                )
            if not (entry_state.is_filled or entry_state.is_partial):
                raise OrderExecutionError("entry response did not prove an executed quantity")
            filled = int(entry_state.filled)
            potential_exposure_contracts = filled
            if Decimal(filled) != entry_state.filled:
                raise OrderExecutionError("exchange reported fractional contracts")
            average = _average_price(entry_order)
            target_state = "ENTRY_FILLED" if filled == contracts else "ENTRY_PARTIAL"
            row = transition_intent(
                decision_key,
                target_state,
                expected_statuses={"ENTRY_PENDING"},
                updates={
                    "entry_order_id": entry_order_id,
                    "filled_contracts": filled,
                    "actual_entry_price_usdt": float(average),
                },
                db_path=ledger_path,
            )
            status = target_state

        if status not in {"ENTRY_PARTIAL", "ENTRY_FILLED", "PROTECTED_NO_TP"}:
            raise OrderExecutionError(f"unsupported resume status {status}")

        filled = int(row["filled_contracts"])
        actual_entry = _as_decimal(row["actual_entry_price_usdt"], "actual entry")
        stop_price, target_price = _anchored_exit_prices(
            side,
            entry,
            stop,
            target,
            actual_entry,
            instrument.tick_size_usdt,
        )

        sl_order: Mapping[str, Any] | None = None
        if status in {"ENTRY_PARTIAL", "ENTRY_FILLED"}:
            trigger_direction = "below" if side == "LONG" else "above"
            try:
                attest_testnet_exchange(exchange)
                stop_create = create_order_once(
                    exchange,
                    client_order_id=ids["sl"],
                    symbol=SYMBOL,
                    ambiguous_error_types=_ambiguous_error_types(),
                    create=lambda cid: _create_private_order(
                        exchange,
                        SYMBOL,
                        "market",
                        close_side,
                        filled,
                        params={
                            "triggerPrice": float(stop_price),
                            "triggerDirection": trigger_direction,
                            "execInst": "Close,MarkPrice",
                            "clOrdLinkID": oco_id,
                            "contingencyType": "OneCancelsTheOther",
                            "clientOrderId": cid,
                        },
                    ),
                )
                sl_order = stop_create.order
                sl_order, stop_state = _refresh_order(
                    exchange,
                    sl_order,
                    ids["sl"],
                    stop_when_open=True,
                )
                if stop_state.state != "open":
                    raise OrderExecutionError(
                        f"stop order is not affirmatively open ({stop_state.state})"
                    )
                _verify_stop_order(
                    sl_order,
                    side=close_side,
                    contracts=filled,
                    stop_price=stop_price,
                    link_id=oco_id,
                    client_id=ids["sl"],
                )
                _assert_position(exchange, contracts=filled, side=side)
                row = transition_intent(
                    decision_key,
                    "PROTECTED_NO_TP",
                    expected_statuses={status},
                    updates={"stop_order_id": _order_id(sl_order)},
                    db_path=ledger_path,
                )
                status = "PROTECTED_NO_TP"
                protection_proven = True
            except Exception as exc:  # agent-quality: allow: stop failure returns manual_halt after emergency close
                if (
                    sl_order is None
                    and isinstance(exc, AmbiguousCreateUnresolved)
                    and isinstance(exc.returned_order, Mapping)
                ):
                    sl_order = exc.returned_order
                cleanup_detail = ""
                if sl_order is not None or isinstance(
                    exc, AmbiguousCreateUnresolved
                ):
                    try:
                        sl_order, _ = _find_and_cancel_untrusted_order(
                            exchange,
                            sl_order,
                            ids["sl"],
                            label="untrusted protective stop",
                        )
                        cleanup_detail = "; untrusted stop was proven terminal"
                    except Exception as cleanup_exc:  # agent-quality: allow: unresolved cleanup is included in the manual-halt reason
                        cleanup_detail = (
                            "; untrusted stop cleanup is unresolved: "
                            f"{_safe_error(cleanup_exc)}"
                        )
                halt_reason = _emergency_close(
                    exchange,
                    decision_key=decision_key,
                    now_ms=current_ms,
                    close_side=close_side,
                    contracts=filled,
                    current_status=status,
                    ledger_path=ledger_path,
                    reason=(
                        "protective stop could not be proven: "
                        f"{_safe_error(exc)}{cleanup_detail}"
                    ),
                )
                return _result(
                    "manual_halt",
                    decision_key=decision_key,
                    entry_order=entry_order,
                    filled_contracts=filled,
                    fill_price=float(actual_entry),
                    error=halt_reason,
                )

        tp_order: Mapping[str, Any] | None = None
        try:
            attest_testnet_exchange(exchange)
            target_create = create_order_once(
                exchange,
                client_order_id=ids["tp"],
                symbol=SYMBOL,
                ambiguous_error_types=_ambiguous_error_types(),
                create=lambda cid: _create_private_order(
                    exchange,
                    SYMBOL,
                    "limit",
                    close_side,
                    filled,
                    float(target_price),
                    {
                        "reduceOnly": True,
                        "timeInForce": "GoodTillCancel",
                        "clientOrderId": cid,
                        "clOrdLinkID": oco_id,
                        "contingencyType": "OneCancelsTheOther",
                    },
                ),
            )
            tp_order = target_create.order
            tp_order, target_state = _refresh_order(
                exchange,
                tp_order,
                ids["tp"],
                stop_when_open=True,
            )
            if target_state.state != "open":
                raise OrderExecutionError(
                    f"target order is not affirmatively open ({target_state.state})"
                )
            _verify_target_order(
                tp_order,
                side=close_side,
                contracts=filled,
                target_price=target_price,
                link_id=oco_id,
                client_id=ids["tp"],
            )
            _assert_position(exchange, contracts=filled, side=side)
            transition_intent(
                decision_key,
                "PROTECTED",
                expected_statuses={"PROTECTED_NO_TP"},
                updates={"target_order_id": _order_id(tp_order)},
                db_path=ledger_path,
            )
            return _result(
                "placed",
                decision_key=decision_key,
                entry_order=entry_order,
                sl_order=sl_order,
                tp_order=tp_order,
                filled_contracts=filled,
                fill_price=float(actual_entry),
            )
        except Exception as exc:  # agent-quality: allow: target failure is cleaned up and the stop is re-attested before any stop-only result
            if (
                tp_order is None
                and isinstance(exc, AmbiguousCreateUnresolved)
                and isinstance(exc.returned_order, Mapping)
            ):
                tp_order = exc.returned_order
            cleanup_error: BaseException | None = None
            target_state = classify_order(tp_order) if tp_order is not None else None
            if (
                isinstance(exc, AmbiguousCreateUnresolved)
                or (target_state is not None and not target_state.terminal)
            ):
                try:
                    tp_order, _ = _find_and_cancel_untrusted_order(
                        exchange,
                        tp_order,
                        ids["tp"],
                        label="untrusted take-profit order",
                    )
                except Exception as cleanup_exc:  # agent-quality: allow: failure forces flatten-and-halt below
                    cleanup_error = cleanup_exc

            if cleanup_error is None:
                try:
                    sl_order = _reverify_active_stop(
                        exchange,
                        client_id=ids["sl"],
                        close_side=close_side,
                        contracts=filled,
                        stop_price=stop_price,
                        link_id=oco_id,
                        position_side=side,
                    )
                except Exception as stop_exc:  # agent-quality: allow: failure forces flatten-and-halt below
                    cleanup_error = stop_exc

            if cleanup_error is None:
                return _result(
                    "protected_no_tp",
                    decision_key=decision_key,
                    entry_order=entry_order,
                    sl_order=sl_order,
                    tp_order=tp_order,
                    filled_contracts=filled,
                    fill_price=float(actual_entry),
                    error=(
                        "target placement was not usable; its absence/terminal "
                        "state and the remaining stop were re-verified: "
                        f"{_safe_error(exc)}"
                    ),
                )

            halt_reason = _emergency_close(
                exchange,
                decision_key=decision_key,
                now_ms=current_ms,
                close_side=close_side,
                contracts=filled,
                current_status="PROTECTED_NO_TP",
                ledger_path=ledger_path,
                reason=(
                    "take-profit failure left protection untrusted: "
                    f"{_safe_error(exc)}; cleanup/re-attestation failed: "
                    f"{_safe_error(cleanup_error)}"
                ),
            )
            return _result(
                "manual_halt",
                decision_key=decision_key,
                entry_order=entry_order,
                sl_order=sl_order,
                tp_order=tp_order,
                filled_contracts=filled,
                fill_price=float(actual_entry),
                error=halt_reason,
            )
    except (AmbiguousCreateUnresolved, EntryOrderUnresolved) as exc:  # agent-quality: allow: unresolved IOC is polled to terminal before any Close
        reason = f"entry submission or cancellation is unresolved: {_safe_error(exc)}"
        returned_id_anomaly = (
            isinstance(exc, AmbiguousCreateUnresolved)
            and isinstance(exc.returned_order, Mapping)
        )
        returned_order_terminal = False
        try:
            if returned_id_anomaly:
                entry_order = exc.returned_order
                entry_order, entry_state = _cancel_entry_remainder(
                    exchange,
                    entry_order,
                    ids["entry"],
                )
                returned_order_terminal = True
                position = _stable_position(exchange)
                if position.contracts != int(entry_state.filled):
                    raise OrderExecutionError(
                        "returned anomalous entry disagrees with the stable position"
                    )
                reason = (
                    f"{reason}; exchange returned an order with a missing or "
                    "different client ID"
                )
            else:
                entry_order, entry_state, _ = _reconcile_entry_to_terminal(
                    exchange,
                    ids["entry"],
                )
            _verify_entry_order(
                entry_order,
                side="buy" if side == "LONG" else "sell",
                contracts=contracts,
                limit_price=limit_price,
                client_id=ids["entry"],
            )
            record_execution_evidence(
                decision_key,
                expected_statuses={"ENTRY_PENDING"},
                updates={"entry_order_id": _order_id(entry_order)},
                db_path=ledger_path,
            )
            if entry_state.filled <= 0:
                _assert_no_open_orders(exchange)
                if returned_id_anomaly:
                    halt_reason = (
                        f"{reason}; anomalous entry was proven terminal and the "
                        "account was stable-flat; manual review is required"
                    )
                    _halt(
                        decision_key,
                        "ENTRY_PENDING",
                        halt_reason,
                        ledger_path,
                    )
                    return _result(
                        "manual_halt",
                        decision_key=decision_key,
                        entry_order=entry_order,
                        error=halt_reason,
                    )
                transition_intent(
                    decision_key,
                    "FAILED_FLAT",
                    expected_statuses={"ENTRY_PENDING"},
                    db_path=ledger_path,
                )
                return _result(
                    "no_fill",
                    decision_key=decision_key,
                    entry_order=entry_order,
                    error="ambiguous IOC was reconciled terminal with zero fill",
                )
            filled = int(entry_state.filled)
            if Decimal(filled) != entry_state.filled:
                raise OrderExecutionError("exchange reported fractional contracts")
            average = _average_price(entry_order)
            target_state = "ENTRY_FILLED" if filled == contracts else "ENTRY_PARTIAL"
            transition_intent(
                decision_key,
                target_state,
                expected_statuses={"ENTRY_PENDING"},
                updates={
                    "filled_contracts": filled,
                    "actual_entry_price_usdt": float(average),
                },
                db_path=ledger_path,
            )
            reason = _emergency_close(
                exchange,
                decision_key=decision_key,
                now_ms=current_ms,
                close_side=close_side,
                contracts=filled,
                current_status=target_state,
                ledger_path=ledger_path,
                reason=reason,
            )
        except Exception as reconcile_exc:  # agent-quality: allow: unresolved entry remains durable and is never resubmitted
            if isinstance(reconcile_exc, UnattributedEntryExposure):
                close_reason = f"{reason}; {_safe_error(reconcile_exc)}"
                try:
                    close_reason = _emergency_close(
                        exchange,
                        decision_key=decision_key,
                        now_ms=current_ms,
                        close_side=close_side,
                        contracts=contracts,
                        current_status="ENTRY_PENDING",
                        ledger_path=ledger_path,
                        reason=close_reason,
                    )
                except Exception as close_exc:  # agent-quality: allow: fatal status stops normal runner sleep
                    return _result(
                        "failed",
                        decision_key=decision_key,
                        entry_order=entry_order,
                        error=(
                            f"{close_reason}; emergency handling also failed: "
                            f"{_safe_error(close_exc)}"
                        ),
                    )
                return _result(
                    "manual_halt",
                    decision_key=decision_key,
                    entry_order=entry_order,
                    error=close_reason,
                )
            if isinstance(reconcile_exc, UnexpectedPendingAccountState):
                halt_reason = f"{reason}; {_safe_error(reconcile_exc)}"
                try:
                    _halt(
                        decision_key,
                        "ENTRY_PENDING",
                        halt_reason,
                        ledger_path,
                    )
                except Exception as halt_exc:  # agent-quality: allow: fatal status stops normal runner sleep
                    return _result(
                        "failed",
                        decision_key=decision_key,
                        entry_order=entry_order,
                        error=(
                            f"{halt_reason}; ledger halt also failed: "
                            f"{_safe_error(halt_exc)}"
                        ),
                    )
                return _result(
                    "manual_halt",
                    decision_key=decision_key,
                    entry_order=entry_order,
                    error=halt_reason,
                )
            if returned_id_anomaly and returned_order_terminal:
                close_reason = (
                    f"{reason}; returned anomalous entry was terminal but its "
                    "semantics/accounting could not be trusted: "
                    f"{_safe_error(reconcile_exc)}"
                )
                try:
                    close_reason = _emergency_close(
                        exchange,
                        decision_key=decision_key,
                        now_ms=current_ms,
                        close_side=close_side,
                        contracts=contracts,
                        current_status="ENTRY_PENDING",
                        ledger_path=ledger_path,
                        reason=close_reason,
                    )
                except Exception as close_exc:  # agent-quality: allow: fatal result stops the runner when close/ledger proof fails
                    return _result(
                        "failed",
                        decision_key=decision_key,
                        entry_order=entry_order,
                        error=(
                            f"{close_reason}; emergency handling also failed: "
                            f"{_safe_error(close_exc)}"
                        ),
                    )
                return _result(
                    "manual_halt",
                    decision_key=decision_key,
                    entry_order=entry_order,
                    error=close_reason,
                )
            return _result(
                "reconciling",
                decision_key=decision_key,
                entry_order=entry_order,
                error=(
                    f"{reason}; {_safe_error(reconcile_exc)}. New entries remain blocked"
                ),
            )
        return _result(
            "manual_halt",
            decision_key=decision_key,
            entry_order=entry_order,
            error=reason,
        )
    except Exception as exc:  # agent-quality: allow: execution failure is persisted and returned explicitly
        reason = _safe_error(exc)
        try:
            if (
                not protection_proven
                and (entry_submission_started or potential_exposure_contracts > 0)
            ):
                reason = _emergency_close(
                    exchange,
                    decision_key=decision_key,
                    now_ms=current_ms,
                    close_side=close_side,
                    contracts=max(contracts, potential_exposure_contracts),
                    current_status=status,
                    ledger_path=ledger_path,
                    reason=reason,
                )
            else:
                _halt(decision_key, status, reason, ledger_path)
            result_status = "manual_halt"
        except Exception as ledger_exc:  # agent-quality: allow: ledger failure changes result status to failed
            reason = f"{reason}; ledger halt also failed: {_safe_error(ledger_exc)}"
            result_status = "failed"
        return _result(
            result_status,
            decision_key=decision_key,
            entry_order=entry_order,
            error=reason,
        )


def execute_signal(
    signal: Mapping[str, Any],
    validated_risk: Mapping[str, Any],
    *,
    decision_key: str,
    now_ms: int | None = None,
    exchange: Any | None = None,
    ledger_path: str = DB_PATH,
) -> dict[str, Any]:
    """Execute one completed-candle signal under a singleton Testnet lock."""

    _assert_testnet()
    try:
        execution_exchange = get_client() if exchange is None else exchange
        attest_testnet_exchange(execution_exchange)
        with exclusive_execution_lock(ledger_path):
            return _execute_signal_locked(
                signal,
                validated_risk,
                decision_key=decision_key,
                now_ms=now_ms,
                exchange=execution_exchange,
                ledger_path=ledger_path,
            )
    except Exception as exc:  # agent-quality: allow: preflight/lease failure is an explicit failed result
        return _result(
            "failed",
            decision_key=decision_key,
            error=_safe_error(exc),
        )


__all__ = [
    "MAX_ENTRY_SLIPPAGE_BPS",
    "OrderExecutionError",
    "STRATEGY_VERSION",
    "SYMBOL",
    "_assert_testnet",
    "execute_signal",
]
