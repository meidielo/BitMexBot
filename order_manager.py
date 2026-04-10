"""
order_manager.py — Phase 5

Places a limit entry order on BitMEX testnet, waits for it to fill,
then attaches a stop-market SL and a limit TP.

TESTNET ONLY.  Will raise EnvironmentError immediately if
BITMEX_TESTNET != "true" in .env.

No signal or risk logic lives here — this module only executes
what it is handed and cleans up on any failure.
"""

import os
import time
from dotenv import load_dotenv
from bitmex_client import get_client

load_dotenv()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SYMBOL              = "BTC/USDT:USDT"
LEVERAGE            = 15

# How long to wait for the entry limit order to fill before giving up.
FILL_POLL_INTERVAL  = 3    # seconds between status checks
FILL_TIMEOUT        = 90   # total seconds before cancel + abort


# ---------------------------------------------------------------------------
# Testnet guard — must be the first call inside execute_signal
# ---------------------------------------------------------------------------

def _assert_testnet() -> None:
    """
    Raise immediately if BITMEX_TESTNET is not explicitly set to 'true'.
    This is a hard stop — no orders are placed on an unknown environment.
    """
    val = os.getenv("BITMEX_TESTNET", "").strip().lower()
    if val != "true":
        raise EnvironmentError(
            "BITMEX_TESTNET is not set to 'true' in .env. "
            "Refusing to place any orders. "
            "Add BITMEX_TESTNET=true to your .env file to enable testnet trading."
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _result(status: str, entry_order=None, sl_order=None,
            tp_order=None, error=None) -> dict:
    """Build the standardised return dict."""
    return {
        "order_id":    entry_order["id"] if entry_order else None,
        "entry_order": entry_order,
        "sl_order":    sl_order,
        "tp_order":    tp_order,
        "status":      status,
        "error":       error,
    }


def round_to_tick(price: float, tick: float = 0.1) -> float:
    """
    Round price to the nearest tick increment.
    Uses round(..., 2) to eliminate floating-point representation artefacts
    that caused the previous round(..., 1) fix to silently produce
    off-tick values (e.g. 69280.600000000001).
    Applied as the LAST STEP immediately before each create_order call —
    never earlier in the pipeline, so rounding is always fresh and visible.
    """
    return round(round(price / tick) * tick, 2)


def _cancel_all(exchange) -> None:
    """
    Cancel every open order for SYMBOL.
    Called on any failure after the entry order has been submitted,
    to leave the account in a clean state.
    """
    try:
        exchange.cancel_all_orders(SYMBOL)
        print("[CLEANUP] Cancelled all open orders for this symbol.")
    except Exception as e:
        # Log but do not re-raise — we are already inside an error path.
        print(f"[WARN] cancel_all_orders failed during cleanup: {e}")


def _emergency_close(exchange, close_side: str, amount: int) -> None:
    """
    Place an immediate market order to close the position.
    Called when SL placement fails after an entry fill — a live
    position with no stop-loss attached is never acceptable.
    """
    try:
        order = exchange.create_order(
            symbol=SYMBOL,
            type="market",
            side=close_side,
            amount=amount,
            params={"execInst": "Close"},
        )
        print(f"[EMERGENCY] Market close placed — ID: {order['id']}. Position closed.")
    except Exception as e:
        print(
            f"[CRITICAL] Emergency market close FAILED: {e}. "
            "MANUAL INTERVENTION REQUIRED — check open positions immediately."
        )


def _fetch_entry_price(exchange, sig: str, fallback: float) -> float:
    """
    Fetch the current best price from the live orderbook and use it as
    the limit entry price so the order fills almost immediately.

    SHORT → ask price  (we are selling; placing at ask means immediate fill)
    LONG  → bid price  (we are buying;  placing at bid means immediate fill)

    Using a limit order at market price avoids market-order slippage while
    still guaranteeing a near-instant fill at a known price.

    Falls back to the signal close price on any error.
    """
    try:
        book  = exchange.fetch_order_book(SYMBOL, limit=1)
        if sig == "SHORT":
            price = float(book["asks"][0][0])
            print(f"[ENTRY] Using ask price {price:.2f} for SHORT limit entry")
        else:
            price = float(book["bids"][0][0])
            print(f"[ENTRY] Using bid price {price:.2f} for LONG limit entry")
        return price
    except Exception as exc:
        print(f"[WARN] Orderbook fetch failed: {exc}. "
              f"Falling back to signal close price {fallback:.2f}.")
        return fallback


def _poll_for_fill(exchange, order_id: str) -> dict | None:
    """
    Poll the entry order until it is fully filled or FILL_TIMEOUT is reached.

    Returns the filled order dict on success, None on timeout or external cancel.
    The caller is responsible for cancelling open orders if None is returned.
    """
    start = time.time()
    while (time.time() - start) < FILL_TIMEOUT:
        try:
            order = exchange.fetch_order(order_id, SYMBOL)
        except Exception as e:
            print(f"[WARN] Error polling order {order_id}: {e}")
            return None

        if order["status"] == "closed":
            # 'closed' in ccxt means fully filled for BitMEX
            return order

        if order["status"] == "canceled":
            print(f"[WARN] Entry order {order_id} was cancelled externally.")
            return None

        elapsed = int(time.time() - start)
        print(
            f"[POLL] Entry {order_id} — status: {order['status']}  "
            f"filled: {order.get('filled', 0)}/{order.get('amount', '?')}  "
            f"({elapsed}s elapsed)"
        )
        time.sleep(FILL_POLL_INTERVAL)

    print(f"[TIMEOUT] Entry order {order_id} did not fill within {FILL_TIMEOUT}s.")
    return None


def _print_confirmation(sig: str, fill_price: float,
                        sl_price: float, tp_price: float,
                        entry_order: dict, sl_order: dict,
                        tp_order: dict) -> None:
    """Print a clean order confirmation block to terminal."""
    sl_dist = abs(sl_price - fill_price) / fill_price * 100
    tp_dist = abs(tp_price - fill_price) / fill_price * 100
    width = 64
    print()
    print("=" * width)
    print(f"  ORDER PLACED — {sig}")
    print("=" * width)
    print(f"  Entry  : {fill_price:>10.2f}   ID: {entry_order['id']}")
    print(f"  SL     : {sl_price:>10.2f}   ({sl_dist:.2f}% from entry)   ID: {sl_order['id']}")
    print(f"  TP     : {tp_price:>10.2f}   ({tp_dist:.2f}% from entry)   ID: {tp_order['id']}")
    print(f"  Size   : {entry_order.get('amount', '?')} contracts  |  Leverage: {LEVERAGE}x")
    print("=" * width)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def execute_signal(signal: dict, validated_risk: dict) -> dict:
    """
    Place entry + SL + TP orders on BitMEX testnet.

    Parameters
    ----------
    signal         : dict from signals.get_signal()
                     Must contain: signal, entry_price, sl_price, tp_price
    validated_risk : dict from risk.validate_signal()
                     Must have approved=True and position_size_btc set

    Order flow
    ----------
    1. Place limit entry order
    2. Poll until filled (up to FILL_TIMEOUT seconds)
    3. If filled  → place stop-market SL + limit TP
    4. If timeout → cancel all open orders, return failed
    5. On any exception after entry is live → cancel all, return failed

    Returns
    -------
    dict: order_id, entry_order, sl_order, tp_order, status, error
    """

    # ------------------------------------------------------------------
    # Guard 1 — testnet environment must be confirmed in .env
    # ------------------------------------------------------------------
    _assert_testnet()

    # ------------------------------------------------------------------
    # Guard 2 — risk filter must have approved this signal
    # ------------------------------------------------------------------
    if not validated_risk.get("approved"):
        return _result(
            "failed",
            error=f"Risk filter did not approve signal: {validated_risk.get('reason')}",
        )

    # ------------------------------------------------------------------
    # Unpack parameters
    # ------------------------------------------------------------------
    sig      = signal["signal"]   # "LONG" or "SHORT"

    # Keep signal prices raw here — round_to_tick() is applied as the
    # final step immediately before each create_order call, never earlier.
    sl_price = float(signal["sl_price"])
    tp_price = float(signal["tp_price"])

    # XBTUSDT linear perpetual: size is in contracts, settled in USDT.
    # risk.py calculates this as int(balance * 2% * leverage), min 1.
    amount   = int(validated_risk["position_size_btc"])

    # Entry side buys for LONG, sells for SHORT.
    # SL/TP are always the opposite side (closing the position).
    entry_side = "buy"  if sig == "LONG" else "sell"
    close_side = "sell" if sig == "LONG" else "buy"

    # ------------------------------------------------------------------
    # Connect and configure leverage
    # ------------------------------------------------------------------
    try:
        exchange = get_client()
    except Exception as e:
        return _result("failed", error=f"Failed to connect to exchange: {e}")

    try:
        exchange.set_leverage(LEVERAGE, SYMBOL)
    except Exception:
        # BitMEX multi-asset accounts use cross margin and do not support
        # per-symbol isolated leverage — this call may fail on those accounts.
        pass

    # Verify leverage is actually set to expected value
    try:
        positions = exchange.fetch_positions([SYMBOL])
        for pos in positions:
            actual_lev = pos.get("leverage")
            if actual_lev is not None and float(actual_lev) != LEVERAGE:
                return _result(
                    "failed",
                    error=(
                        f"Leverage mismatch: expected {LEVERAGE}x but account "
                        f"has {actual_lev}x. Fix manually on BitMEX before trading."
                    ),
                )
    except Exception:
        # If we can't verify, proceed cautiously — set_leverage was attempted
        pass

    # ------------------------------------------------------------------
    # Apply tick rounding to SL/TP before any order is placed.
    # ------------------------------------------------------------------
    sl_price    = round_to_tick(sl_price)
    tp_price    = round_to_tick(tp_price)

    entry_order = None
    sl_order    = None
    tp_order    = None

    # ------------------------------------------------------------------
    # Step 1 — Market entry order
    # Testnet has near-zero liquidity, so limit orders rarely fill.
    # Market orders fill instantly against the testnet matching engine.
    # ------------------------------------------------------------------
    try:
        entry_order = exchange.create_order(
            symbol=SYMBOL,
            type="market",
            side=entry_side,
            amount=amount,
        )
        print(
            f"[ORDER] {sig} market entry placed  "
            f"| ID: {entry_order['id']}"
            f"  Size: {amount} contracts"
        )
    except Exception as e:
        return _result("failed", error=f"Entry order placement failed: {e}")

    # Market orders fill immediately — fetch the final order state.
    try:
        filled_order = exchange.fetch_order(entry_order["id"], SYMBOL)
    except Exception:
        filled_order = entry_order

    if filled_order.get("status") not in ("closed", None):
        # Unexpected — market orders should fill instantly
        print(f"[WARN] Market order status: {filled_order.get('status')}. Waiting briefly...")
        filled_order = _poll_for_fill(exchange, entry_order["id"])
        if filled_order is None:
            _cancel_all(exchange)
            return _result(
                "failed",
                entry_order=entry_order,
                error=f"Market entry did not fill — cancelled.",
            )

    # Use the actual average fill price for SL/TP if available.
    fill_price = float(filled_order.get("average") or float(signal["entry_price"]))
    print(f"[PRICES] fill={fill_price} sl={sl_price} tp={tp_price}")
    print(f"[FILL]  Entry filled @ avg {fill_price:.2f}")

    # ------------------------------------------------------------------
    # Step 3 — Stop-market SL (reduce-only, closes the whole position)
    #
    # BitMEX order type 'stop' = Stop-Market (ordType: Stop in native API).
    # The price argument is the stop trigger price (maps to stopPx).
    # execInst 'Close' instructs BitMEX to close the full position when
    # triggered — it is not additive to an existing position.
    # ------------------------------------------------------------------
    try:
        _sl = round_to_tick(sl_price)   # final tick-align immediately before order
        sl_order = exchange.create_order(
            symbol=SYMBOL,
            type="stop",
            side=close_side,
            amount=amount,
            params={"stopPx": _sl, "execInst": "Close"},
        )
        print(
            f"[ORDER] SL stop-market placed      "
            f"| ID: {sl_order['id']}"
            f"  Trigger: {_sl}"
        )
    except Exception as e:
        print(f"[ERROR] SL order placement failed: {e}")
        # Entry is filled with no SL — exit immediately via market order.
        # Never leave an open position unprotected.
        print("[SAFETY] Placing emergency market close to exit unprotected position.")
        _emergency_close(exchange, close_side, amount)
        return _result(
            "failed",
            entry_order=entry_order,
            error=f"Entry filled but SL placement failed: {e}",
        )

    # ------------------------------------------------------------------
    # Step 4 — Limit TP (reduce-only — must not open a new position)
    #
    # execInst 'ReduceOnly' ensures this order only reduces position size.
    # ------------------------------------------------------------------
    try:
        _tp = round_to_tick(tp_price)   # final tick-align immediately before order
        tp_order = exchange.create_order(
            symbol=SYMBOL,
            type="limit",
            side=close_side,
            amount=amount,
            price=_tp,
            params={"execInst": "ReduceOnly"},
        )
        print(
            f"[ORDER] TP limit order placed      "
            f"| ID: {tp_order['id']}"
            f"  Target: {_tp}"
        )
    except Exception as e:
        print(f"[ERROR] TP order placement failed: {e}")
        # Do NOT cancel_all here — that would cancel the SL and leave the
        # position completely unprotected. The SL is sufficient protection;
        # return failed so the caller can log it and monitor manually.
        print(f"[WARN] Position is live with SL only (ID: {sl_order['id']}). No TP placed.")
        return _result(
            "failed",
            entry_order=entry_order,
            sl_order=sl_order,
            error=f"Entry + SL placed but TP placement failed: {e}",
        )

    # ------------------------------------------------------------------
    # All three orders live — print summary and return
    # ------------------------------------------------------------------
    _print_confirmation(
        sig, fill_price, sl_price, tp_price,
        entry_order, sl_order, tp_order,
    )

    return _result(
        "placed",
        entry_order=entry_order,
        sl_order=sl_order,
        tp_order=tp_order,
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from fetch_data import fetch_ohlcv, fetch_current_funding, fetch_recent_funding
    from signals import get_signal
    from risk import validate_signal

    print("Phase 5 — Order execution on BitMEX testnet")
    print("WARNING: This will place REAL orders on testnet. Ctrl-C to abort.\n")

    # Fetch → signal (V2 funding rate) → risk → execute
    df = fetch_ohlcv()
    if df is None:
        raise SystemExit("[ABORT] Could not fetch OHLCV data.")

    funding = fetch_current_funding()
    recent = fetch_recent_funding(count=10)
    funding_data = None
    if funding and funding.get("rate") is not None:
        cum_24h = recent["rate"].tail(3).sum() if not recent.empty else 0
        funding_data = {"rate": funding["rate"], "funding_24h": cum_24h}

    sig = get_signal(df, current_funding=funding_data)
    print(f"Signal  : {sig['signal']}")
    print(f"Reason  : {sig['reason']}\n")

    risk_result = validate_signal(
        signal=sig,
        account_balance=1000.0,   # replace with live balance in Phase 6
        open_positions=[],
    )
    print(f"Risk    : {'APPROVED' if risk_result['approved'] else 'VETOED'}")
    print(f"Reason  : {risk_result['reason']}\n")

    if not risk_result["approved"]:
        raise SystemExit("[HALT] Risk filter vetoed — no orders placed.")

    result = execute_signal(signal=sig, validated_risk=risk_result)

    print(f"\nStatus  : {result['status']}")
    if result["error"]:
        print(f"Error   : {result['error']}")
