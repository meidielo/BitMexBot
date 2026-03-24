import os
import ccxt
from dotenv import load_dotenv

load_dotenv()

TESTNET_URL = "https://testnet.bitmex.com"


def get_client():
    api_key = os.getenv("BITMEX_API_KEY", "")
    api_secret = os.getenv("BITMEX_API_SECRET", "")

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


get_exchange = get_client  # alias for callers that import get_exchange
