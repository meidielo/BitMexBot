"""Pure execution-safety primitives for BitMEX order handling.

This module deliberately contains no exchange construction, credentials, endpoint
selection, or production-mode switch.  Callers provide an exchange object and an
order creation callback, which keeps every function deterministic or mockable.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
import re
from typing import Any, Callable, Mapping, Sequence
from uuid import NAMESPACE_URL, uuid5


_DECISION_KEY_PATTERN = re.compile(
    r"^(?P<symbol>[A-Z0-9][A-Z0-9:/._-]{0,39})"
    r"\|(?P<timeframe>[1-9][0-9]*[smhd])"
    r"\|(?P<candle_close_ms>[1-9][0-9]*)"
    r"\|(?P<side>LONG|SHORT)$"
)
_TIMEFRAME_MULTIPLIERS_MS = {
    "s": 1_000,
    "m": 60_000,
    "h": 3_600_000,
    "d": 86_400_000,
}
_ORDER_LEGS = {"entry", "sl", "tp", "emergency", "oco"}
_CLIENT_ORDER_NAMESPACE_PREFIX = "urn:mdpstudio:bitmexbot:order:v1"

_FILLED_STATUSES = {"filled", "closed"}
_PARTIAL_STATUSES = {"partial", "partiallyfilled"}
_OPEN_STATUSES = {
    "acceptedforbidding",
    "new",
    "open",
    "pendingcancel",
    "pendingnew",
    "pendingreplace",
    "triggered",
    "untriggered",
}
_CANCELED_STATUSES = {"canceled", "cancelled", "doneforday", "stopped"}
_REJECTED_STATUSES = {"rejected", "amendreject", "cancelreject"}
_EXPIRED_STATUSES = {"expired"}


class ExecutionSafetyError(ValueError):
    """Base error for invalid execution-safety inputs."""


class ReconciliationError(RuntimeError):
    """Raised when exchange order state cannot be reconciled safely."""


class AmbiguousCreateUnresolved(ReconciliationError):
    """Raised when an order may exist and therefore must not be resubmitted."""

    def __init__(
        self,
        client_order_id: str,
        message: str,
        *,
        returned_order: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.client_order_id = client_order_id
        self.returned_order = returned_order


@dataclass(frozen=True)
class CompletedCandleDecision:
    """Canonical identity for one signal on one completed candle."""

    symbol: str
    timeframe: str
    candle_close_ms: int
    side: str

    @property
    def key(self) -> str:
        """Return the canonical serialized decision key."""
        return (
            f"{self.symbol}|{self.timeframe}|{self.candle_close_ms}|{self.side}"
        )


@dataclass(frozen=True)
class OrderClassification:
    """Conservative normalized order state."""

    state: str
    raw_status: str | None
    filled: Decimal
    leaves: Decimal | None
    amount: Decimal | None
    terminal: bool

    @property
    def is_filled(self) -> bool:
        """Only an affirmatively classified full fill returns true."""
        return self.state == "filled"

    @property
    def is_partial(self) -> bool:
        """Return whether any, but not all, quantity has executed."""
        return self.state == "partial"


@dataclass(frozen=True)
class SafeCreateResult:
    """Result of a create-once operation and its reconciliation source."""

    order: Mapping[str, Any]
    source: str
    submitted: bool


def _timeframe_ms(timeframe: str) -> int:
    match = re.fullmatch(r"([1-9][0-9]*)([smhd])", timeframe)
    if match is None:
        raise ExecutionSafetyError(
            "timeframe must be a positive integer followed by s, m, h, or d"
        )
    return int(match.group(1)) * _TIMEFRAME_MULTIPLIERS_MS[match.group(2)]


def build_completed_candle_decision_key(
    *,
    symbol: str,
    timeframe: str,
    candle_close_ms: int,
    side: str,
    now_ms: int,
) -> str:
    """Build and validate a canonical key for the latest completed candle."""
    if not isinstance(symbol, str) or not symbol.strip():
        raise ExecutionSafetyError("symbol must be a non-empty string")
    if not isinstance(timeframe, str):
        raise ExecutionSafetyError("timeframe must be a string")
    if not isinstance(side, str):
        raise ExecutionSafetyError("side must be LONG or SHORT")

    decision_key = (
        f"{symbol.strip().upper()}|{timeframe.strip().lower()}|"
        f"{candle_close_ms}|{side.strip().upper()}"
    )
    return validate_completed_candle_decision_key(
        decision_key,
        now_ms=now_ms,
    ).key


def validate_completed_candle_decision_key(
    decision_key: str,
    *,
    now_ms: int,
) -> CompletedCandleDecision:
    """Validate a canonical key for the latest fully completed candle.

    A valid key is ``SYMBOL|TIMEFRAME|CLOSE_MS|SIDE``.  The candle close must
    align to the timeframe, must not be in the future, and must be the most
    recent completed candle.  Rejecting stale keys prevents a restart from
    replaying an old trading decision after exchange order history is pruned.
    """
    if not isinstance(decision_key, str):
        raise ExecutionSafetyError("decision_key must be a string")
    if not isinstance(now_ms, int) or isinstance(now_ms, bool) or now_ms < 0:
        raise ExecutionSafetyError("now_ms must be a non-negative integer")

    match = _DECISION_KEY_PATTERN.fullmatch(decision_key)
    if match is None:
        raise ExecutionSafetyError(
            "decision_key must use canonical SYMBOL|TIMEFRAME|CLOSE_MS|SIDE format"
        )

    timeframe = match.group("timeframe")
    timeframe_ms = _timeframe_ms(timeframe)
    candle_close_ms = int(match.group("candle_close_ms"))
    if candle_close_ms % timeframe_ms != 0:
        raise ExecutionSafetyError("candle close is not aligned to its timeframe")
    if candle_close_ms > now_ms:
        raise ExecutionSafetyError("decision candle has not completed")
    if now_ms - candle_close_ms >= timeframe_ms:
        raise ExecutionSafetyError("decision candle is stale")

    return CompletedCandleDecision(
        symbol=match.group("symbol"),
        timeframe=timeframe,
        candle_close_ms=candle_close_ms,
        side=match.group("side"),
    )


def client_order_id(
    decision_key: str,
    leg: str,
    *,
    now_ms: int,
) -> str:
    """Return a stable, namespaced UUIDv5 ``clOrdID`` for one order leg.

    UUID text is exactly 36 characters, BitMEX's documented maximum.  The
    completed-candle decision and leg are both in the UUID name, making entry,
    stop-loss, and take-profit IDs distinct while remaining deterministic.
    """
    decision = validate_completed_candle_decision_key(
        decision_key,
        now_ms=now_ms,
    )
    normalized_leg = leg.strip().lower() if isinstance(leg, str) else ""
    if normalized_leg not in _ORDER_LEGS:
        raise ExecutionSafetyError("leg must be one of: entry, sl, tp, emergency")

    name = f"{_CLIENT_ORDER_NAMESPACE_PREFIX}:{decision.key}:{normalized_leg}"
    value = str(uuid5(NAMESPACE_URL, name))
    if len(value) > 36:  # Defensive assertion for the exchange contract.
        raise AssertionError("generated client order ID exceeds BitMEX limit")
    return value


def client_order_ids(decision_key: str, *, now_ms: int) -> dict[str, str]:
    """Return deterministic client IDs for entry, stop-loss, and take-profit."""
    return {
        leg: client_order_id(decision_key, leg, now_ms=now_ms)
        for leg in ("entry", "sl", "tp")
    }


def reconciliation_client_order_id(decision_key: str, leg: str) -> str:
    """Derive an ID for a durable historical intent without replay authority.

    This validates the same canonical key and alignment by evaluating it at its
    own candle close.  It must only be used to reconcile or reduce an existing
    durable intent, never to authorize a new entry.
    """

    if not isinstance(decision_key, str):
        raise ExecutionSafetyError("decision_key must be a string")
    match = _DECISION_KEY_PATTERN.fullmatch(decision_key)
    if match is None:
        raise ExecutionSafetyError(
            "decision_key must use canonical SYMBOL|TIMEFRAME|CLOSE_MS|SIDE format"
        )
    return client_order_id(
        decision_key,
        leg,
        now_ms=int(match.group("candle_close_ms")),
    )


def _decimal(value: Any, field: str) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ExecutionSafetyError(f"{field} must be a finite number")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ExecutionSafetyError(f"{field} must be a finite number") from exc
    if not result.is_finite():
        raise ExecutionSafetyError(f"{field} must be a finite number")
    return result


def bounded_ioc_limit_price(
    *,
    side: str,
    best_bid: Any,
    best_ask: Any,
    max_slippage_bps: Any,
    tick_size: Any,
) -> Decimal:
    """Calculate a marketable IOC limit without exceeding the slippage bound.

    Buy limits use the best ask and round the worst acceptable price down.
    Sell limits use the best bid and round the worst acceptable price up.
    These directions ensure tick alignment never makes the permitted execution
    price less conservative.
    """
    normalized_side = side.strip().lower() if isinstance(side, str) else ""
    if normalized_side not in {"buy", "sell"}:
        raise ExecutionSafetyError("side must be buy or sell")

    bid = _decimal(best_bid, "best_bid")
    ask = _decimal(best_ask, "best_ask")
    slippage = _decimal(max_slippage_bps, "max_slippage_bps")
    tick = _decimal(tick_size, "tick_size")
    if bid <= 0 or ask <= 0 or tick <= 0:
        raise ExecutionSafetyError("prices and tick_size must be positive")
    if bid > ask:
        raise ExecutionSafetyError("best_bid cannot exceed best_ask")
    if slippage < 0 or slippage >= 10_000:
        raise ExecutionSafetyError(
            "max_slippage_bps must be at least 0 and less than 10000"
        )
    if (bid / tick) % 1 != 0 or (ask / tick) % 1 != 0:
        raise ExecutionSafetyError("best bid and ask must be tick-aligned")

    fraction = slippage / Decimal(10_000)
    if normalized_side == "buy":
        worst_price = ask * (Decimal(1) + fraction)
        ticks = (worst_price / tick).to_integral_value(rounding=ROUND_FLOOR)
        price = ticks * tick
        if price < ask:
            raise ExecutionSafetyError("slippage bound cannot cross the best ask")
    else:
        worst_price = bid * (Decimal(1) - fraction)
        ticks = (worst_price / tick).to_integral_value(rounding=ROUND_CEILING)
        price = ticks * tick
        if price > bid:
            raise ExecutionSafetyError("slippage bound cannot cross the best bid")

    return price


def _normalized_status(value: Any) -> tuple[str | None, str | None]:
    if value is None:
        return None, None
    if not isinstance(value, str) or not value.strip():
        return str(value), None
    raw = value.strip()
    return raw, re.sub(r"[^a-z]", "", raw.lower())


def _first_present(order: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in order and order[key] is not None:
            return order[key]
    info = order.get("info")
    if isinstance(info, Mapping):
        for key in keys:
            if key in info and info[key] is not None:
                return info[key]
    return None


def _optional_quantity(value: Any, field: str) -> Decimal | None:
    if value is None:
        return None
    result = _decimal(value, field)
    if result < 0:
        raise ExecutionSafetyError(f"{field} cannot be negative")
    return result


def classify_order(order: Mapping[str, Any] | None) -> OrderClassification:
    """Classify exchange order state without inferring fills from missing data."""
    if order is None:
        return OrderClassification(
            state="missing",
            raw_status=None,
            filled=Decimal(0),
            leaves=None,
            amount=None,
            terminal=False,
        )
    if not isinstance(order, Mapping):
        raise ExecutionSafetyError("order must be a mapping or None")

    raw_status, status = _normalized_status(
        _first_present(order, "status", "ordStatus")
    )
    amount = _optional_quantity(
        _first_present(order, "amount", "orderQty"),
        "amount",
    )
    filled = _optional_quantity(
        _first_present(order, "filled", "cumQty"),
        "filled",
    )
    leaves = _optional_quantity(
        _first_present(order, "remaining", "leavesQty"),
        "leaves",
    )
    filled = filled if filled is not None else Decimal(0)
    if leaves is None and amount is not None:
        if filled > amount:
            raise ExecutionSafetyError("filled quantity exceeds order amount")
        leaves = amount - filled
    if amount is None and leaves is not None:
        amount = filled + leaves

    terminal = status in (
        _FILLED_STATUSES
        | _CANCELED_STATUSES
        | _REJECTED_STATUSES
        | _EXPIRED_STATUSES
    )

    # A partial execution remains economically important even when the order's
    # remainder has been canceled, rejected, or expired.
    if status in _PARTIAL_STATUSES or (
        filled > 0
        and (
            leaves is None
            or leaves > 0
            or status in (_CANCELED_STATUSES | _REJECTED_STATUSES | _EXPIRED_STATUSES)
        )
    ):
        return OrderClassification(
            "partial", raw_status, filled, leaves, amount, terminal
        )

    if status in _FILLED_STATUSES:
        # A unified "closed" response without quantities is not sufficient
        # evidence for real-funds accounting.  Native "Filled" is affirmative,
        # while "closed" also requires zero leaves or filled == amount.
        native_filled = status == "filled"
        quantity_proves_fill = (
            leaves == 0
            or (amount is not None and amount > 0 and filled == amount)
        )
        if native_filled or quantity_proves_fill:
            return OrderClassification(
                "filled", raw_status, filled, leaves, amount, True
            )
        return OrderClassification(
            "unknown", raw_status, filled, leaves, amount, False
        )

    if status in _CANCELED_STATUSES:
        state = "canceled"
    elif status in _REJECTED_STATUSES:
        state = "rejected"
    elif status in _EXPIRED_STATUSES:
        state = "expired"
    elif status in _OPEN_STATUSES:
        state = "open"
    else:
        state = "unknown"

    return OrderClassification(state, raw_status, filled, leaves, amount, terminal)


def _validate_client_order_id(value: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 36:
        raise ExecutionSafetyError(
            "client_order_id must be a non-empty string of at most 36 characters"
        )
    return value


def _extract_client_order_id(order: Mapping[str, Any]) -> str | None:
    for key in ("clientOrderId", "clOrdID"):
        value = order.get(key)
        if isinstance(value, str) and value:
            return value
    info = order.get("info")
    if isinstance(info, Mapping):
        value = info.get("clOrdID")
        if isinstance(value, str) and value:
            return value
    return None


def find_order_by_client_id(
    exchange: Any,
    client_order_id: str,
    *,
    symbol: str | None = None,
) -> Mapping[str, Any] | None:
    """Fetch and find exactly one order using BitMEX's ``clOrdID`` filter."""
    validated_id = _validate_client_order_id(client_order_id)
    params = {"filter": {"clOrdID": validated_id}}
    if symbol is None:
        orders = exchange.fetch_orders(params=params)
    else:
        orders = exchange.fetch_orders(symbol, params=params)

    if orders is None:
        raise ReconciliationError("fetch_orders returned no response")
    if isinstance(orders, (str, bytes, Mapping)) or not isinstance(orders, Sequence):
        raise ReconciliationError("fetch_orders returned an invalid response")

    matches: list[Mapping[str, Any]] = []
    for order in orders:
        if not isinstance(order, Mapping):
            raise ReconciliationError("fetch_orders returned a non-mapping order")
        if _extract_client_order_id(order) == validated_id:
            matches.append(order)

    if len(matches) > 1:
        raise ReconciliationError(
            f"multiple orders returned for unique clOrdID {validated_id}"
        )
    return matches[0] if matches else None


def reconcile_order_by_client_id(
    exchange: Any,
    client_order_id: str,
    *,
    symbol: str | None = None,
) -> OrderClassification:
    """Return conservative normalized state for an order lookup."""
    order = find_order_by_client_id(
        exchange,
        client_order_id,
        symbol=symbol,
    )
    return classify_order(order)


def create_order_once(
    exchange: Any,
    *,
    client_order_id: str,
    create: Callable[[str], Mapping[str, Any] | None],
    symbol: str | None = None,
    ambiguous_error_types: tuple[type[BaseException], ...] = (
        TimeoutError,
        ConnectionError,
    ),
) -> SafeCreateResult:
    """Create at most once, reconciling ambiguous outcomes by ``clOrdID``.

    The exchange is queried before submission.  Timeout and network errors are
    queried again, but the create callback is never retried.  If the second
    lookup cannot prove whether the order exists, an explicit unresolved error
    is raised so the caller must halt and reconcile rather than duplicate.
    """
    validated_id = _validate_client_order_id(client_order_id)
    existing = find_order_by_client_id(
        exchange,
        validated_id,
        symbol=symbol,
    )
    if existing is not None:
        return SafeCreateResult(existing, "existing", False)

    try:
        created = create(validated_id)
    except ambiguous_error_types as exc:
        try:
            reconciled = find_order_by_client_id(
                exchange,
                validated_id,
                symbol=symbol,
            )
        except Exception as reconcile_exc:
            raise AmbiguousCreateUnresolved(
                validated_id,
                "order submission failed ambiguously and reconciliation also failed; "
                "do not resubmit",
            ) from reconcile_exc
        if reconciled is not None:
            return SafeCreateResult(reconciled, "reconciled", True)
        raise AmbiguousCreateUnresolved(
            validated_id,
            "order submission failed ambiguously and no matching order is visible; "
            "do not resubmit until independently reconciled",
        ) from exc

    if created is None:
        reconciled = find_order_by_client_id(
            exchange,
            validated_id,
            symbol=symbol,
        )
        if reconciled is not None:
            return SafeCreateResult(reconciled, "reconciled", True)
        raise AmbiguousCreateUnresolved(
            validated_id,
            "order creation returned no response and no matching order is visible; "
            "do not resubmit until independently reconciled",
        )
    if not isinstance(created, Mapping):
        reconciled = find_order_by_client_id(
            exchange,
            validated_id,
            symbol=symbol,
        )
        if reconciled is not None:
            return SafeCreateResult(reconciled, "reconciled", True)
        raise AmbiguousCreateUnresolved(
            validated_id,
            "order creation returned a malformed response and no matching order "
            "is visible; do not resubmit until independently reconciled",
        )

    response_client_id = _extract_client_order_id(created)
    if response_client_id is None:
        reconciled = find_order_by_client_id(
            exchange,
            validated_id,
            symbol=symbol,
        )
        if reconciled is not None:
            return SafeCreateResult(reconciled, "reconciled", True)
        raise AmbiguousCreateUnresolved(
            validated_id,
            "order creation response omitted clOrdID and no matching order is visible; "
            "do not resubmit until independently reconciled",
            returned_order=created,
        )
    if response_client_id != validated_id:
        raise AmbiguousCreateUnresolved(
            validated_id,
            "created order response contains a different client order ID; "
            "cancel the returned exchange order and do not resubmit",
            returned_order=created,
        )
    return SafeCreateResult(created, "submitted", True)
