"""Canonical, fail-closed metadata model for the BitMEX XBTUSDT swap.

The model consumes an already-loaded CCXT market dictionary.  It performs no
network or exchange calls.  Financial calculations use :class:`Decimal` so
contract quantities are never silently changed by binary floating-point math.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from typing import Any


EXPECTED_SYMBOL = "BTC/USDT:USDT"
EXPECTED_MARKET_ID = "XBTUSDT"
EXPECTED_SETTLEMENT = "USDT"


class InstrumentMetadataError(ValueError):
    """Raised when CCXT metadata cannot prove the expected instrument shape."""


def _decimal(value: Any, field: str, *, allow_zero: bool = False) -> Decimal:
    if value is None or isinstance(value, bool):
        raise InstrumentMetadataError(f"{field} is missing or non-numeric")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise InstrumentMetadataError(f"{field} is not a valid number") from exc
    if not parsed.is_finite():
        raise InstrumentMetadataError(f"{field} must be finite")
    if parsed < 0 or (parsed == 0 and not allow_zero):
        qualifier = "non-negative" if allow_zero else "positive"
        raise InstrumentMetadataError(f"{field} must be {qualifier}")
    return parsed


def _non_negative(value: Any, field: str) -> Decimal:
    return _decimal(value, field, allow_zero=True)


@dataclass(frozen=True, slots=True)
class XBTUSDTInstrument:
    """Verified contract specification for ``BTC/USDT:USDT``.

    ``contract_size_btc`` is the base-currency quantity represented by one
    contract.  ``lot_size_contracts`` is the minimum order increment.
    """

    symbol: str
    market_id: str
    settlement_currency: str
    contract_size_btc: Decimal
    lot_size_contracts: int
    tick_size_usdt: Decimal
    maker_fee_rate: Decimal
    taker_fee_rate: Decimal
    initial_margin_rate: Decimal
    maintenance_margin_rate: Decimal

    @classmethod
    def from_ccxt_market(cls, market: Mapping[str, Any]) -> "XBTUSDTInstrument":
        """Build from CCXT metadata, rejecting missing or contradictory data."""

        if not isinstance(market, Mapping):
            raise InstrumentMetadataError("market metadata must be a mapping")

        expected_values = {
            "symbol": EXPECTED_SYMBOL,
            "id": EXPECTED_MARKET_ID,
            "type": "swap",
            "settle": EXPECTED_SETTLEMENT,
        }
        for field, expected in expected_values.items():
            actual = market.get(field)
            if actual != expected:
                raise InstrumentMetadataError(
                    f"expected {field}={expected!r}, received {actual!r}"
                )

        if market.get("contract") is not True:
            raise InstrumentMetadataError("market must be a derivatives contract")
        if market.get("linear") is not True:
            raise InstrumentMetadataError("market must be linear")
        if market.get("inverse") is not False:
            raise InstrumentMetadataError("market must explicitly be non-inverse")
        if market.get("active") is not True:
            raise InstrumentMetadataError("market must explicitly be active")

        contract_size = _decimal(market.get("contractSize"), "contractSize")

        precision = market.get("precision")
        if not isinstance(precision, Mapping):
            raise InstrumentMetadataError("precision metadata is missing")
        lot_size = _decimal(precision.get("amount"), "precision.amount")
        tick_size = _decimal(precision.get("price"), "precision.price")

        if lot_size != lot_size.to_integral_value():
            raise InstrumentMetadataError("precision.amount must be whole contracts")

        info = market.get("info")
        if info is not None and not isinstance(info, Mapping):
            raise InstrumentMetadataError("info metadata must be a mapping")
        if isinstance(info, Mapping):
            raw_lot = info.get("lotSize")
            if raw_lot is not None and _decimal(raw_lot, "info.lotSize") != lot_size:
                raise InstrumentMetadataError(
                    "precision.amount and info.lotSize disagree"
                )
            raw_tick = info.get("tickSize")
            if raw_tick is not None and _decimal(raw_tick, "info.tickSize") != tick_size:
                raise InstrumentMetadataError(
                    "precision.price and info.tickSize disagree"
                )

        maker_fee = cls._resolve_rate(
            market,
            info,
            normalized_fields=("maker", "makerFee"),
            raw_field="makerFee",
            label="maker fee",
            allow_zero=True,
        )
        taker_fee = cls._resolve_rate(
            market,
            info,
            normalized_fields=("taker", "takerFee"),
            raw_field="takerFee",
            label="taker fee",
            allow_zero=True,
        )
        initial_margin = cls._resolve_rate(
            market,
            info,
            normalized_fields=("initialMargin", "initialMarginRate"),
            raw_field="initMargin",
            label="initial margin",
            allow_zero=False,
        )
        maintenance_margin = cls._resolve_rate(
            market,
            info,
            normalized_fields=("maintenanceMargin", "maintenanceMarginRate"),
            raw_field="maintMargin",
            label="maintenance margin",
            allow_zero=False,
        )
        if maintenance_margin >= initial_margin:
            raise InstrumentMetadataError(
                "maintenance margin must be lower than initial margin"
            )

        return cls(
            symbol=EXPECTED_SYMBOL,
            market_id=EXPECTED_MARKET_ID,
            settlement_currency=EXPECTED_SETTLEMENT,
            contract_size_btc=contract_size,
            lot_size_contracts=int(lot_size),
            tick_size_usdt=tick_size,
            maker_fee_rate=maker_fee,
            taker_fee_rate=taker_fee,
            initial_margin_rate=initial_margin,
            maintenance_margin_rate=maintenance_margin,
        )

    @staticmethod
    def _resolve_rate(
        market: Mapping[str, Any],
        info: Mapping[str, Any] | None,
        *,
        normalized_fields: tuple[str, ...],
        raw_field: str,
        label: str,
        allow_zero: bool,
    ) -> Decimal:
        """Resolve a rate and reject disagreement between duplicate metadata."""

        candidates: list[tuple[str, Decimal]] = []
        for field in normalized_fields:
            value = market.get(field)
            if value is not None:
                candidates.append(
                    (field, _decimal(value, field, allow_zero=allow_zero))
                )

        if info is not None:
            raw_value = info.get(raw_field)
            if raw_value is not None:
                candidates.append(
                    (
                        f"info.{raw_field}",
                        _decimal(
                            raw_value,
                            f"info.{raw_field}",
                            allow_zero=allow_zero,
                        ),
                    )
                )

        if not candidates:
            raise InstrumentMetadataError(f"{label} metadata is missing")

        expected = candidates[0][1]
        if any(value != expected for _, value in candidates[1:]):
            fields = ", ".join(field for field, _ in candidates)
            raise InstrumentMetadataError(f"{label} metadata disagrees across {fields}")
        return expected

    def round_down_contracts(self, contracts: Any) -> int:
        """Round a proposed quantity down to the nearest exchange lot."""

        quantity = _non_negative(contracts, "contracts")
        lot = Decimal(self.lot_size_contracts)
        lot_count = (quantity / lot).to_integral_value(rounding=ROUND_FLOOR)
        return int(lot_count * lot)

    def contracts_to_btc(self, contracts: Any) -> Decimal:
        """Convert a non-negative contract quantity to base BTC quantity."""

        quantity = _non_negative(contracts, "contracts")
        return quantity * self.contract_size_btc

    def notional_usdt(self, contracts: Any, price_usdt: Any) -> Decimal:
        """Return linear position notional in USDT at ``price_usdt``."""

        price = _decimal(price_usdt, "price_usdt")
        return self.contracts_to_btc(contracts) * price

    def gross_pnl_usdt(
        self,
        side: str,
        contracts: Any,
        entry_price_usdt: Any,
        exit_price_usdt: Any,
    ) -> Decimal:
        """Return signed gross PnL for a linear LONG or SHORT position."""

        direction = str(side).strip().upper()
        if direction not in {"LONG", "SHORT"}:
            raise ValueError("side must be LONG or SHORT")

        entry = _decimal(entry_price_usdt, "entry_price_usdt")
        exit_ = _decimal(exit_price_usdt, "exit_price_usdt")
        price_change = exit_ - entry
        if direction == "SHORT":
            price_change = -price_change
        return self.contracts_to_btc(contracts) * price_change

    def estimate_fee_usdt(
        self, contracts: Any, price_usdt: Any, fee_rate: Any
    ) -> Decimal:
        """Estimate a non-negative execution fee from notional and fee rate."""

        rate = _non_negative(fee_rate, "fee_rate")
        return self.notional_usdt(contracts, price_usdt) * rate


__all__ = [
    "EXPECTED_MARKET_ID",
    "EXPECTED_SETTLEMENT",
    "EXPECTED_SYMBOL",
    "InstrumentMetadataError",
    "XBTUSDTInstrument",
]
