"""Fail-closed pre-trade risk validation for the XBTUSDT testnet engine.

Sizing is derived from current exchange instrument metadata and the distance to
the stop.  Contract counts, BTC exposure, USDT notional, and expected stop loss
are kept as separate values throughout the decision.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_FLOOR

from instrument import XBTUSDTInstrument


LEVERAGE = 15
MAX_POSITION_BTC = 0.10
RISK_PER_TRADE_PCT = 0.02
MAX_DAILY_LOSS_USD = 50.0
MIN_FREE_MARGIN_PCT = 0.10
LIQ_BUFFER = 0.9
MIN_ENTRY_SLIPPAGE_BPS = Decimal("10")
MIN_STOP_SLIPPAGE_BPS = Decimal("20")
DAILY_LOSS_FILE = os.path.join("data", "daily_loss.json")


@dataclass(frozen=True, slots=True)
class RiskLimits:
    leverage: int = LEVERAGE
    risk_per_trade_fraction: Decimal = Decimal("0.02")
    max_position_btc: Decimal = Decimal("0.10")
    max_notional_usdt: Decimal | None = None
    max_daily_loss_usdt: Decimal = Decimal("50")
    minimum_free_margin_fraction: Decimal = Decimal("0.10")
    entry_slippage_bps: Decimal = MIN_ENTRY_SLIPPAGE_BPS
    stop_slippage_bps: Decimal = MIN_STOP_SLIPPAGE_BPS

    def __post_init__(self) -> None:
        if self.leverage < 1:
            raise ValueError("leverage must be at least 1")
        for name in (
            "risk_per_trade_fraction",
            "max_position_btc",
            "max_daily_loss_usdt",
            "minimum_free_margin_fraction",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.risk_per_trade_fraction >= 1:
            raise ValueError("risk_per_trade_fraction must be below 1")
        if self.minimum_free_margin_fraction >= 1:
            raise ValueError("minimum_free_margin_fraction must be below 1")
        if self.max_notional_usdt is not None and self.max_notional_usdt <= 0:
            raise ValueError("max_notional_usdt must be positive when set")
        if not 0 <= self.entry_slippage_bps < 10_000:
            raise ValueError("entry_slippage_bps must be in [0, 10000)")
        if not 0 <= self.stop_slippage_bps < 10_000:
            raise ValueError("stop_slippage_bps must be in [0, 10000)")


TESTNET_LIMITS = RiskLimits()

# Pre-committed ceiling for a future human-reviewed canary. This profile is
# deliberately not wired to an authenticated production client, because that
# client does not exist in this repository.
CANARY_REVIEW_LIMITS = RiskLimits(
    leverage=1,
    risk_per_trade_fraction=Decimal("0.001"),
    max_position_btc=Decimal("0.001"),
    max_notional_usdt=Decimal("25"),
    max_daily_loss_usdt=Decimal("5"),
    minimum_free_margin_fraction=Decimal("0.50"),
)


def _decimal(value: float | int | Decimal, field: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except Exception as exc:
        raise ValueError(f"{field} must be numeric") from exc
    if not parsed.is_finite():
        raise ValueError(f"{field} must be finite")
    return parsed


def _load_daily_loss(path: str = DAILY_LOSS_FILE) -> float | None:
    """Return today's gross realised loss, or ``None`` when state is untrusted."""

    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if data.get("date") != today:
            return None
        if data.get("source") != "trades_v2.db":
            return None
        loss = float(data["loss_usd"])
        if loss < 0:
            return None
        return loss
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):  # agent-quality: allow: None is an explicit fail-closed state
        return None


def _calc_liq_price(entry: float, signal: str, leverage: int = LEVERAGE) -> float:
    """Conservative pre-fill estimate; exchange liquidation is verified later."""

    margin_fraction = (1 / leverage) * LIQ_BUFFER
    if signal == "LONG":
        return entry * (1 - margin_fraction)
    return entry * (1 + margin_fraction)


def _calc_position_size(
    account_balance_usdt: float,
    entry_price_usdt: float,
    stop_price_usdt: float,
    instrument: XBTUSDTInstrument,
    limits: RiskLimits = TESTNET_LIMITS,
) -> int:
    """Size from maximum stop loss, then apply BTC/notional caps and lot floor."""

    equity = _decimal(account_balance_usdt, "account_balance_usdt")
    entry = _decimal(entry_price_usdt, "entry_price_usdt")
    stop = _decimal(stop_price_usdt, "stop_price_usdt")
    if equity <= 0 or entry <= 0 or stop <= 0 or entry == stop:
        return 0

    risk_budget = equity * limits.risk_per_trade_fraction
    loss_per_contract = _estimated_loss_per_contract(
        entry,
        stop,
        instrument,
        limits,
    )
    if loss_per_contract <= 0:
        return 0
    by_stop = (risk_budget / loss_per_contract).to_integral_value(rounding=ROUND_FLOOR)
    by_btc = (
        limits.max_position_btc / instrument.contract_size_btc
    ).to_integral_value(rounding=ROUND_FLOOR)
    caps = [by_stop, by_btc]
    if limits.max_notional_usdt is not None:
        per_contract_notional = instrument.contract_size_btc * entry
        caps.append(
            (limits.max_notional_usdt / per_contract_notional).to_integral_value(
                rounding=ROUND_FLOOR
            )
        )
    return instrument.round_down_contracts(min(caps))


def _estimated_loss_per_contract(
    entry_price_usdt: Decimal,
    stop_price_usdt: Decimal,
    instrument: XBTUSDTInstrument,
    limits: RiskLimits,
) -> Decimal:
    """Return stop loss plus conservative entry/exit slippage and taker fees."""

    entry_fraction = limits.entry_slippage_bps / Decimal(10_000)
    stop_fraction = limits.stop_slippage_bps / Decimal(10_000)
    price_loss = abs(entry_price_usdt - stop_price_usdt)
    slippage_loss = (
        entry_price_usdt * entry_fraction
        + stop_price_usdt * stop_fraction
    )
    round_trip_fees = (
        entry_price_usdt + stop_price_usdt
    ) * instrument.taker_fee_rate
    return instrument.contract_size_btc * (
        price_loss + slippage_loss + round_trip_fees
    )


def _veto(reason: str) -> dict:
    return {
        "approved": False,
        "reason": reason,
        "position_size_contracts": None,
        "position_size_btc": None,
        "notional_usdt": None,
        "expected_max_loss_usdt": None,
        "raw_stop_loss_usdt": None,
        "estimated_costs_usdt": None,
        "entry_slippage_bps": None,
        "stop_slippage_bps": None,
        "taker_fee_rate": None,
        "leverage": None,
    }


def validate_signal(
    signal: dict,
    account_balance: float,
    open_positions: list,
    *,
    instrument: XBTUSDTInstrument,
    free_balance_usdt: float | None = None,
    limits: RiskLimits = TESTNET_LIMITS,
    daily_loss_usdt: float | None = None,
) -> dict:
    """Apply every risk rule and return an approval bound to explicit units."""

    sig = signal.get("signal")
    if sig not in ("LONG", "SHORT"):
        return _veto(
            f"Rule 1 FAILED: signal is '{sig}'. Only LONG or SHORT may proceed."
        )
    try:
        entry = float(signal["entry_price"])
        stop = float(signal["sl_price"])
        target = float(signal["tp_price"])
    except (KeyError, TypeError, ValueError):  # agent-quality: allow: malformed signal is returned as an explicit veto
        return _veto("Rule 1 FAILED: entry, stop, and target must be numeric.")
    if min(entry, stop, target) <= 0:
        return _veto("Rule 1 FAILED: entry, stop, and target must be positive.")
    if sig == "LONG" and not (stop < entry < target):
        return _veto("Rule 1 FAILED: LONG requires stop < entry < target.")
    if sig == "SHORT" and not (target < entry < stop):
        return _veto("Rule 1 FAILED: SHORT requires target < entry < stop.")

    if open_positions:
        return _veto(
            f"Rule 2 FAILED: {len(open_positions)} open position(s) already exist."
        )

    loss_today = _load_daily_loss() if daily_loss_usdt is None else daily_loss_usdt
    if loss_today is None:
        return _veto("Rule 3 FAILED: daily loss state is missing, stale, or corrupt.")
    try:
        loss_today_dec = _decimal(loss_today, "daily_loss_usdt")
    except ValueError as exc:  # agent-quality: allow: conversion failure is returned as an explicit veto
        return _veto(f"Rule 3 FAILED: {exc}.")
    if loss_today_dec < 0:
        return _veto("Rule 3 FAILED: daily loss cannot be negative.")
    if loss_today_dec >= limits.max_daily_loss_usdt:
        return _veto(
            f"Rule 3 FAILED: daily loss ${loss_today_dec:.2f} reached the "
            f"${limits.max_daily_loss_usdt:.2f} limit."
        )

    liq_price = _calc_liq_price(entry, sig, limits.leverage)
    if sig == "LONG" and stop <= liq_price:
        return _veto(
            f"Rule 4 FAILED: LONG stop {stop:.2f} is not above estimated "
            f"liquidation {liq_price:.2f}."
        )
    if sig == "SHORT" and stop >= liq_price:
        return _veto(
            f"Rule 4 FAILED: SHORT stop {stop:.2f} is not below estimated "
            f"liquidation {liq_price:.2f}."
        )

    contracts = _calc_position_size(
        account_balance, entry, stop, instrument, limits
    )
    if contracts < instrument.lot_size_contracts:
        return _veto(
            "Rule 5 FAILED: risk budget is below the exchange minimum lot size."
        )

    base_btc = instrument.contracts_to_btc(contracts)
    notional = instrument.notional_usdt(contracts, entry)
    raw_stop_loss = base_btc * Decimal(str(abs(entry - stop)))
    expected_loss = Decimal(contracts) * _estimated_loss_per_contract(
        Decimal(str(entry)),
        Decimal(str(stop)),
        instrument,
        limits,
    )
    estimated_costs = expected_loss - raw_stop_loss
    risk_budget = Decimal(str(account_balance)) * limits.risk_per_trade_fraction
    if loss_today_dec + expected_loss > limits.max_daily_loss_usdt:
        remaining_daily_budget = limits.max_daily_loss_usdt - loss_today_dec
        return _veto(
            "Rule 3 FAILED: buffered expected loss "
            f"${expected_loss:.2f} exceeds the remaining daily loss budget "
            f"${remaining_daily_budget:.2f}."
        )
    if expected_loss > risk_budget:
        return _veto("Rule 5 FAILED: rounded position exceeds the stop-loss budget.")
    if base_btc > limits.max_position_btc:
        return _veto("Rule 5 FAILED: rounded position exceeds the BTC exposure cap.")
    if limits.max_notional_usdt is not None and notional > limits.max_notional_usdt:
        return _veto("Rule 5 FAILED: rounded position exceeds the USDT notional cap.")

    free_balance = account_balance if free_balance_usdt is None else free_balance_usdt
    try:
        free = _decimal(free_balance, "free_balance_usdt")
        equity = _decimal(account_balance, "account_balance")
    except ValueError as exc:  # agent-quality: allow: conversion failure is returned as an explicit veto
        return _veto(f"Rule 6 FAILED: {exc}.")
    if equity <= 0 or free < 0:
        return _veto("Rule 6 FAILED: account equity/free balance is invalid.")
    margin_required = notional / Decimal(limits.leverage)
    free_after = free - margin_required
    free_fraction = free_after / equity
    if free_fraction < limits.minimum_free_margin_fraction:
        return _veto(
            f"Rule 6 FAILED: free margin after trade would be ${free_after:.2f} "
            f"({free_fraction:.1%}); minimum is "
            f"{limits.minimum_free_margin_fraction:.0%}."
        )

    return {
        "approved": True,
        "reason": (
            f"All risk rules passed for {sig}; {contracts} contracts, "
            f"{base_btc:.6f} BTC, ${notional:.2f} notional, "
            f"${expected_loss:.2f} expected loss including cost buffers."
        ),
        "position_size_contracts": contracts,
        "position_size_btc": float(base_btc),
        "notional_usdt": float(notional),
        "expected_max_loss_usdt": float(expected_loss),
        "raw_stop_loss_usdt": float(raw_stop_loss),
        "estimated_costs_usdt": float(estimated_costs),
        "entry_slippage_bps": float(limits.entry_slippage_bps),
        "stop_slippage_bps": float(limits.stop_slippage_bps),
        "taker_fee_rate": float(instrument.taker_fee_rate),
        "leverage": limits.leverage,
    }


__all__ = [
    "DAILY_LOSS_FILE",
    "CANARY_REVIEW_LIMITS",
    "LEVERAGE",
    "LIQ_BUFFER",
    "MAX_DAILY_LOSS_USD",
    "MAX_POSITION_BTC",
    "MIN_FREE_MARGIN_PCT",
    "MIN_ENTRY_SLIPPAGE_BPS",
    "MIN_STOP_SLIPPAGE_BPS",
    "RISK_PER_TRADE_PCT",
    "RiskLimits",
    "TESTNET_LIMITS",
    "_calc_liq_price",
    "_calc_position_size",
    "_estimated_loss_per_contract",
    "_load_daily_loss",
    "validate_signal",
]
