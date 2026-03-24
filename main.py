"""
main.py — Phase 6

Full bot loop:
  1. Fetch candles          (fetch_data.py)
  2. Compute indicators     (indicators.py)
  3. Get signal             (signals.py)
  4. Validate with risk     (risk.py)
  5. Execute if approved    (order_manager.py)
  6. Log result             (logger.py)
  7. Sleep to next 15m bar, repeat

Stops cleanly on Ctrl-C.  Prints a heartbeat line every loop.
TESTNET ONLY — order_manager enforces BITMEX_TESTNET=true.
"""

import os
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

from bitmex_client  import get_client
from fetch_data     import fetch_ohlcv
from indicators     import compute_indicators
from signals        import get_signal
from risk           import validate_signal
from order_manager  import execute_signal
from logger         import log_trade
from monitor        import print_summary

load_dotenv()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SYMBOL          = "BTC/USDT:USDT"
CANDLE_SECONDS  = 15 * 60        # 15-minute bars = 900 s
LOOP_SLEEP      = CANDLE_SECONDS  # aim for one iteration per bar
RETRY_SLEEP     = 60              # seconds to wait before re-evaluating a failed order


# ---------------------------------------------------------------------------
# Exchange helpers  (account balance + open positions)
# ---------------------------------------------------------------------------

def _get_balance_usd(exchange, close_price: float) -> float:
    """
    Fetch free BTC balance from testnet and convert to USD at close_price.
    BitMEX testnet wallets are denominated in BTC (inverse margin).
    Returns 0.0 on any error so the risk filter safely vetoes.
    """
    try:
        bal   = exchange.fetch_balance()
        btc   = float(bal.get("BTC", {}).get("free", 0.0))
        return round(btc * close_price, 4)
    except Exception as e:
        print(f"[WARN] Could not fetch balance: {e}. Using $0.")
        return 0.0


def _get_open_positions(exchange) -> list:
    """
    Return a list of open position dicts for SYMBOL.
    An empty list means no open positions (safe to trade).
    Returns [] on any error so the risk filter safely vetoes (one-trade rule).
    """
    try:
        positions = exchange.fetch_positions([SYMBOL])
        return [
            p for p in positions
            if float(p.get("contracts") or 0) != 0
        ]
    except Exception as e:
        print(f"[WARN] Could not fetch positions: {e}. Assuming one open position (safe veto).")
        # Return a dummy entry so risk.py blocks the trade rather than
        # accidentally opening a duplicate position.
        return [{"error": str(e)}]


# ---------------------------------------------------------------------------
# Retry logic
# ---------------------------------------------------------------------------

def _should_retry(order_result: dict | None) -> bool:
    """
    Return True if the loop should sleep RETRY_SLEEP seconds and retry,
    rather than waiting for the next 15m candle boundary.

    Retry when: an order was attempted but failed for any reason —
    timeout (price never reached our limit), exchange error, SL placement
    failure, etc.  The signal may still be valid; we re-check every 60s
    until either the trade fills or conditions change.

    No retry when: no order was attempted (NO_TRADE / risk vetoed), or
    the order placed and filled successfully.
    """
    if order_result is None:
        return False
    return order_result.get("status") == "failed"


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

def _heartbeat(ts: str, signal: dict, risk: dict,
               order: dict | None, sleep_msg: str) -> None:
    """Print one compact status line per loop iteration."""
    sig_tag  = signal.get("signal", "?")
    approved = risk.get("approved", False)

    if order is None:
        action = "SKIPPED (no order attempt)"
    elif order["status"] == "placed":
        action = f"ORDER PLACED  id={order['order_id']}"
    else:
        action = f"ORDER FAILED  — {order.get('error', '?')}"

    risk_tag = "APPROVED" if approved else f"VETOED ({_short_reason(risk)})"

    print(
        f"[{ts}]  signal={sig_tag:<9}"
        f"  risk={risk_tag:<40}"
        f"  action={action}"
        f"  next={sleep_msg}"
    )


def _short_reason(risk: dict) -> str:
    """Trim the risk reason to a short tag for the heartbeat line."""
    reason = risk.get("reason", "")
    # Pull the rule number from "Rule N FAILED: …" if present
    if "Rule" in reason and "FAILED" in reason:
        parts = reason.split(":")
        return parts[0].strip()
    return reason[:40]


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 62)
    print("  BitMEX Trading Bot — Phase 6")
    print("  TESTNET ONLY.  Ctrl-C to stop cleanly.")
    print("=" * 62)
    print()

    try:
        exchange = get_client()
    except Exception as e:
        raise SystemExit(f"[ABORT] Could not create exchange client: {e}")

    loop_count = 0

    try:
        while True:
            loop_start = time.time()
            loop_count += 1
            now_utc    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

            print(f"\n{'─' * 62}")
            print(f"  Loop #{loop_count}  —  {now_utc}")
            print(f"{'─' * 62}")

            order_result = None   # reset each iteration

            # ----------------------------------------------------------
            # Step 1 — Fetch candles
            # ----------------------------------------------------------
            df_raw = fetch_ohlcv()
            if df_raw is None:
                print("[WARN] fetch_ohlcv returned None — skipping this bar.")
                _sleep_to_next_bar(loop_start)
                continue

            close_price = float(df_raw["close"].iloc[-1])

            # ----------------------------------------------------------
            # Step 2 — Indicators
            # ----------------------------------------------------------
            df = compute_indicators(df_raw)
            if df is None:
                print("[WARN] compute_indicators returned None — skipping.")
                _sleep_to_next_bar(loop_start)
                continue

            # ----------------------------------------------------------
            # Step 3 — Signal
            # ----------------------------------------------------------
            signal = get_signal(df)
            print(f"  Signal   : {signal['signal']}")
            print(f"  Reason   : {signal['reason']}")

            # ----------------------------------------------------------
            # Step 4 — Risk validation
            # ----------------------------------------------------------
            balance        = _get_balance_usd(exchange, close_price)
            open_positions = _get_open_positions(exchange)

            risk_result = validate_signal(
                signal         = signal,
                account_balance= balance,
                open_positions = open_positions,
            )

            approved = risk_result["approved"]
            print(f"  Balance  : ${balance:.2f}")
            print(f"  Risk     : {'APPROVED' if approved else 'VETOED'}")
            if not approved:
                print(f"  Reason   : {risk_result['reason']}")

            # ----------------------------------------------------------
            # Step 5 — Execute (only if risk approves)
            # ----------------------------------------------------------
            if approved:
                order_result = execute_signal(
                    signal        = signal,
                    validated_risk= risk_result,
                )

                # ----------------------------------------------------------
                # Step 6 — Log
                # ----------------------------------------------------------
                trade_dict = {
                    "order_id":          order_result.get("order_id"),
                    "signal":            signal["signal"],
                    "entry_price":       signal["entry_price"],
                    "sl_price":          signal["sl_price"],
                    "tp_price":          signal["tp_price"],
                    "position_size_btc": risk_result["position_size_btc"],
                    "leverage":          risk_result["leverage"],
                    "approved_by_risk":  True,
                    "order_status":      order_result["status"],
                }
                log_trade(trade_dict)

            # ----------------------------------------------------------
            # Heartbeat + sleep decision
            # ----------------------------------------------------------
            retry     = _should_retry(order_result)
            elapsed   = time.time() - loop_start
            remaining = max(0.0, LOOP_SLEEP - elapsed)
            sleep_msg = (
                f"Retrying in {RETRY_SLEEP}s"
                if retry else
                f"Waiting for next candle in {remaining:.0f}s"
            )
            _heartbeat(now_utc, signal, risk_result, order_result, sleep_msg)

            # ----------------------------------------------------------
            # Step 7 — Sleep: short retry on failure, full bar on success
            # ----------------------------------------------------------
            if retry:
                _sleep_retry()
            else:
                _sleep_to_next_bar(loop_start)

    except KeyboardInterrupt:
        _shutdown()


def _sleep_retry() -> None:
    """Sleep RETRY_SLEEP seconds then fall back to the top of the loop.
    Used when an order failed — the signal may still be valid, so we
    re-fetch candles and re-evaluate rather than waiting a full 15 minutes.
    """
    print(f"\n  Retrying in {RETRY_SLEEP}s...")
    time.sleep(RETRY_SLEEP)


def _sleep_to_next_bar(loop_start: float) -> None:
    """Sleep for the remainder of the 15-minute candle period."""
    elapsed    = time.time() - loop_start
    sleep_secs = max(0.0, LOOP_SLEEP - elapsed)
    wake_utc   = datetime.now(timezone.utc).replace(
        second=0, microsecond=0
    )
    print(
        f"\n  Sleeping {sleep_secs:.0f}s  "
        f"(execution took {elapsed:.1f}s of {LOOP_SLEEP}s bar)"
    )
    time.sleep(sleep_secs)


def _shutdown() -> None:
    """Clean shutdown on Ctrl-C."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\n\n[STOP] Bot stopped by user at {ts}  (KeyboardInterrupt)\n")

    # Print final summary before exiting
    print("─" * 62)
    print("  Final session summary:")
    print_summary()


if __name__ == "__main__":
    main()
