import os
import ccxt
from dotenv import load_dotenv

load_dotenv()

TESTNET_URL = "https://testnet.bitmex.com"


def get_client():
    """Testnet client — used for orders, balance, positions."""
    api_key = os.getenv("BITMEX_API_KEY", "")
    api_secret = os.getenv("BITMEX_API_SECRET", "")

    if not api_key or not api_secret:
        raise EnvironmentError(
            "BITMEX_API_KEY and BITMEX_API_SECRET must be set in .env. "
            "Cannot create authenticated testnet client."
        )

    exchange = ccxt.bitmex({
        "apiKey": api_key,
        "secret": api_secret,
        "urls": {
            "api": {
                "public": TESTNET_URL,
                "private": TESTNET_URL,
            }
        },
        "options": {
            "defaultType": "swap",
        },
    })

    exchange.set_sandbox_mode(True)
    exchange.load_markets()

    return exchange


def get_data_client():
    """Mainnet client — public only, no API key needed. Used for OHLCV data."""
    exchange = ccxt.bitmex({
        "options": {
            "defaultType": "swap",
        },
    })
    exchange.load_markets()
    return exchange


get_exchange = get_client  # alias for callers that import get_exchange
