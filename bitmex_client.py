"""Fail-closed CCXT client construction for BitMEX.

Authenticated clients are restricted to BitMEX Testnet.  There is deliberately
no authenticated mainnet constructor in this module.
"""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import ccxt
from dotenv import load_dotenv

from instrument import EXPECTED_SYMBOL, XBTUSDTInstrument


load_dotenv()

CLIENT_TIMEOUT_MS = 10_000
TESTNET_API_ORIGIN = "https://testnet.bitmex.com"


class ExchangeAttestationError(RuntimeError):
    """Raised when an execution client cannot prove its Testnet identity."""


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    """Canonical, cross-checked XBTUSDT position state."""

    contracts: int
    side: str | None
    raw: Mapping[str, Any]


def _whole_number(value: Any, field: str, *, signed: bool = False) -> int:
    if isinstance(value, bool) or value is None:
        raise ExchangeAttestationError(f"{field} must be a finite whole number")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ExchangeAttestationError(
            f"{field} must be a finite whole number"
        ) from exc
    if not parsed.is_finite() or parsed != parsed.to_integral_value():
        raise ExchangeAttestationError(f"{field} must be a finite whole number")
    if not signed and parsed < 0:
        raise ExchangeAttestationError(f"{field} cannot be negative")
    return int(parsed)


def attest_testnet_exchange(exchange: Any) -> None:
    """Prove that private requests are routed only to BitMEX Testnet.

    The environment flag is necessary but not sufficient.  The live exchange
    object is checked immediately before private operations so an accidentally
    injected mainnet client cannot inherit Testnet authority from the process.
    """

    _require_testnet_enabled()
    if getattr(exchange, "id", None) != "bitmex":
        raise ExchangeAttestationError("execution client is not BitMEX")
    urls = getattr(exchange, "urls", None)
    api = urls.get("api") if isinstance(urls, Mapping) else None
    if not isinstance(api, Mapping):
        raise ExchangeAttestationError("execution client omitted API endpoints")
    public = api.get("public")
    private = api.get("private")
    if public != TESTNET_API_ORIGIN or private != TESTNET_API_ORIGIN:
        raise ExchangeAttestationError(
            "execution client endpoints are not exactly BitMEX Testnet"
        )


def fetch_xbtusdt_position(exchange: Any) -> PositionSnapshot:
    """Fetch one canonical XBTUSDT position and reject ambiguous responses."""

    attest_testnet_exchange(exchange)
    positions = exchange.fetch_positions([EXPECTED_SYMBOL])
    if (
        positions is None
        or isinstance(positions, (str, bytes, Mapping))
        or not isinstance(positions, Sequence)
    ):
        raise ExchangeAttestationError("position query returned an invalid response")
    if len(positions) != 1:
        raise ExchangeAttestationError(
            "position query must return exactly one XBTUSDT record"
        )
    position = positions[0]
    if not isinstance(position, Mapping):
        raise ExchangeAttestationError("position record is not a mapping")
    if position.get("symbol") != EXPECTED_SYMBOL:
        raise ExchangeAttestationError("position record has the wrong symbol")

    contracts = _whole_number(position.get("contracts"), "position contracts")
    info = position.get("info")
    if not isinstance(info, Mapping):
        raise ExchangeAttestationError("position record omitted native BitMEX data")
    if info.get("symbol") != "XBTUSDT":
        raise ExchangeAttestationError("native position record has the wrong symbol")
    current_qty = _whole_number(
        info.get("currentQty"), "native currentQty", signed=True
    )
    if abs(current_qty) != contracts:
        raise ExchangeAttestationError(
            "unified position contracts disagree with native currentQty"
        )

    side_value = position.get("side")
    side = str(side_value).lower() if side_value is not None else None
    if contracts > 0:
        expected_side = "long" if current_qty > 0 else "short"
        if side != expected_side:
            raise ExchangeAttestationError(
                "unified position side disagrees with native currentQty"
            )
    elif current_qty != 0:
        raise ExchangeAttestationError("flat position has non-zero native quantity")
    if side not in {None, "long", "short"}:
        raise ExchangeAttestationError("position side is invalid")

    is_open = info.get("isOpen")
    if not isinstance(is_open, bool) or is_open != (contracts > 0):
        raise ExchangeAttestationError("native isOpen disagrees with position quantity")
    strategy = info.get("strategy")
    if strategy not in (None, "OneWay"):
        raise ExchangeAttestationError("only BitMEX OneWay position mode is supported")
    return PositionSnapshot(contracts=contracts, side=side, raw=position)


def _require_testnet_enabled() -> None:
    """Reject authenticated client creation unless testnet is explicit."""

    if os.getenv("BITMEX_TESTNET") != "true":
        raise EnvironmentError(
            "BITMEX_TESTNET must be exactly 'true'. Authenticated mainnet "
            "client creation is not supported."
        )


def _testnet_credentials() -> tuple[str, str]:
    """Return credentials from dedicated Testnet-only environment names."""

    api_key = os.getenv("BITMEX_TESTNET_API_KEY", "")
    api_secret = os.getenv("BITMEX_TESTNET_API_SECRET", "")
    if not api_key.strip() or not api_secret.strip():
        raise EnvironmentError(
            "BITMEX_TESTNET_API_KEY and BITMEX_TESTNET_API_SECRET must be set. "
            "Generic or production credential names are not accepted."
        )
    return api_key, api_secret


def get_client():
    """Create an authenticated, metadata-verified BitMEX Testnet client."""

    # Both checks intentionally happen before CCXT constructs an exchange.
    _require_testnet_enabled()
    api_key, api_secret = _testnet_credentials()

    exchange = ccxt.bitmex(
        {
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "timeout": CLIENT_TIMEOUT_MS,
            "options": {"defaultType": "swap"},
        }
    )

    # CCXT requires sandbox mode to be its first exchange method call.
    exchange.set_sandbox_mode(True)
    attest_testnet_exchange(exchange)
    markets = exchange.load_markets()
    market = markets.get(EXPECTED_SYMBOL) if isinstance(markets, dict) else None
    exchange.xbtusdt_instrument = XBTUSDTInstrument.from_ccxt_market(market)
    return exchange


def get_data_client():
    """Create an unauthenticated client for public BitMEX market data."""

    exchange = ccxt.bitmex(
        {
            "enableRateLimit": True,
            "timeout": CLIENT_TIMEOUT_MS,
            "options": {"defaultType": "swap"},
        }
    )
    exchange.load_markets()
    return exchange


get_exchange = get_client


__all__ = [
    "ExchangeAttestationError",
    "PositionSnapshot",
    "TESTNET_API_ORIGIN",
    "attest_testnet_exchange",
    "fetch_xbtusdt_position",
    "get_client",
    "get_data_client",
    "get_exchange",
]
