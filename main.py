"""
main.py — V2: Funding Rate Mean Reversion

Bot loop:
  1. Fetch 15m candles           (fetch_data.py)
  2. Fetch current funding rate  (fetch_data.py)
  3. Get signal                  (signals.py — funding extreme + price trigger)
  4. Validate with risk          (risk.py)
  5. Execute if approved         (order_manager.py)
  6. Log result                  (logger.py)
  7. Sleep to next 15m bar, repeat

Stops cleanly on Ctrl-C.
TESTNET ONLY — order_manager enforces BITMEX_TESTNET=true.
"""

import time
from datetime import datetime, timezone

from dotenv import load_dotenv

from bitmex_client  import get_client
from condition_logger import get_logger as get_condition_logger, log_v2_conditions
from fetch_data     import fetch_ohlcv, fetch_current_funding, fetch_recent_funding
from signals        import (get_signal, FUNDING_THRESHOLD, FUNDING_24H_THRESH,
                            VOLUME_SPIKE_MULT, VOLUME_LOOKBACK,
                            USE_SETTLEMENT_FILTER, _in_settlement_window)
from risk           import validate_signal
from order_manager  import execute_signal
from logger         import log_trade, update_trade_exit
from monitor        import print_summary

load_dotenv()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SYMBOL          = "BTC/USDT:USDT"
CANDLE_SECONDS  = 15 * 60        # 15-minute bars = 900 s
LOOP_SLEEP      = CANDLE_SECONDS
RETRY_SLEEP     = 60


# ---------------------------------------------------------------------------
# Exchange helpers
# ---------------------------------------------------------------------------

def _get_balance_usd(exchange, close_price: float) -> float:
    try:
        bal = exchange.fetch_balance()
        btc = float(bal.get("BTC", {}).get("free", 0.0))
        return round(btc * close_price, 4)
    except Exception as e:
        print(f"[WARN] Could not fetch balance: {e}. Using $0.")
        return 0.0


def _get_open_positions(exchange) -> list:
    try:
        positions = exchange.fetch_positions([SYMBOL])
        return [p for p in positions if float(p.get("contracts") or 0) != 0]
    except Exception as e:
        print(f"[WARN] Could not fetch positions: {e}.")
        return [{"error": str(e)}]


# ---------------------------------------------------------------------------
# Exit detection
# ---------------------------------------------------------------------------

def _fetch_exit_price(exchange, order_id: str, fallback: float) -> float:
    try:
        order = exchange.fetch_order(order_id, SYMBOL)
        avg = order.get("average")
        if avg:
            return float(avg)
    except Exception as e:
        print(f"[WARN] Could not fetch order {order_id} for exit price: {e}")
    return fallback


def _determine_exit_reason(signal: dict, exit_price: float) -> str:
    if signal is None:
        return "MANUAL"
    direction = signal.get("signal")
    sl = signal.get("sl_price")
    tp = signal.get("tp_price")
    if sl is None or tp is None:
        return "MANUAL"
    if direction == "SHORT":
        if exit_price <= float(tp):
            return "TP"
        if exit_price >= float(sl):
            return "SL"
    else:
        if exit_price >= float(tp):
            return "TP"
        if exit_price <= float(sl):
            return "SL"
    return "MANUAL"


# ---------------------------------------------------------------------------
# Retry + heartbeat
# ---------------------------------------------------------------------------

def _should_retry(order_result):
    if order_result is None:
        return False
    return order_result.get("status") == "failed"


def _heartbeat(ts, signal, risk, order, sleep_msg):
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


def _short_reason(risk):
    reason = risk.get("reason", "")
    if "Rule" in reason and "FAILED" in reason:
        return reason.split(":")[0].strip()
    return reason[:40]


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 62)
    print("  BitMEX Trading Bot — V2: Funding Rate Mean Reversion")
    print("  TESTNET ONLY.  Ctrl-C to stop cleanly.")
    print("=" * 62)
    print()

    try:
        exchange = get_client()
    except Exception as e:
        raise SystemExit(f"[ABORT] Could not create exchange client: {e}")

    loop_count      = 0
    active_order_id = None
    active_signal   = None

    try:
        while True:
            loop_start = time.time()
            loop_count += 1
            now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

            print(f"\n{'─' * 62}")
            print(f"  Loop #{loop_count}  —  {now_utc}")
            print(f"{'─' * 62}")

            order_result = None

            # ----------------------------------------------------------
            # Step 1 — Fetch 15m candles
            # ----------------------------------------------------------
            df = fetch_ohlcv()
            if df is None:
                print("[WARN] fetch_ohlcv returned None — skipping.")
                _sleep_to_next_bar(loop_start)
                continue

            close_price = float(df["close"].iloc[-1])

            # ----------------------------------------------------------
            # Step 2 — Fetch current funding rate + recent history
            # ----------------------------------------------------------
            funding = fetch_current_funding()
            recent  = fetch_recent_funding(count=10)

            funding_data = None
            if funding and funding.get("rate") is not None:
                # Compute cumulative 24h from recent settled rates
                cum_24h = recent["rate"].tail(3).sum() if not recent.empty else 0
                funding_data = {
                    "rate":        funding["rate"],
                    "funding_24h": cum_24h,
                }
                print(f"  Funding  : {funding['rate']:+.6f} ({funding['rate']*100:+.4f}%)"
                      f"  | 24h cumulative: {cum_24h:+.6f} ({cum_24h*100:+.4f}%)")
            else:
                print("  Funding  : unavailable")

            # ----------------------------------------------------------
            # Step 3 — Signal (funding extreme + price trigger)
            # ----------------------------------------------------------
            signal = get_signal(df, current_funding=funding_data)
            print(f"  Signal   : {signal['signal']}")
            print(f"  Reason   : {signal['reason']}")

            # -- Condition telemetry --
            try:
                cond_conn = get_condition_logger()
                curr = df.iloc[-1]
                prev = df.iloc[-2]
                vol = float(curr["volume"])
                vol_avg = float(df["volume"].iloc[-(VOLUME_LOOKBACK + 1):-1].mean())
                vol_ratio = vol / vol_avg if vol_avg > 0 else 0
                fr = funding_data.get("rate") if funding_data else None
                f24 = funding_data.get("funding_24h") if funding_data else None
                in_window = (_in_settlement_window(df.index[-1])
                             if USE_SETTLEMENT_FILTER else None)
                log_v2_conditions(
                    cond_conn,
                    funding_rate=fr,
                    funding_threshold=FUNDING_THRESHOLD,
                    funding_24h=f24,
                    funding_24h_threshold=FUNDING_24H_THRESH,
                    in_settlement_window=in_window,
                    volume_ratio=vol_ratio,
                    volume_threshold=VOLUME_SPIKE_MULT,
                    bearish_break=float(curr["close"]) < float(prev["low"]),
                    bullish_break=float(curr["close"]) > float(prev["high"]),
                    body_pct=abs(float(curr["close"]) - float(curr["open"])) / float(curr["close"]) if float(curr["close"]) > 0 else 0,
                    body_threshold=0.0005,
                )
            except Exception:
                pass  # telemetry must never break the trading loop

            # ----------------------------------------------------------
            # Step 4 — Risk validation
            # ----------------------------------------------------------
            balance        = _get_balance_usd(exchange, close_price)
            open_positions = _get_open_positions(exchange)

            # Detect position close
            if active_order_id and not open_positions:
                print(f"\n  [EXIT] Position closed — order_id={active_order_id}")
                exit_price  = _fetch_exit_price(exchange, active_order_id, close_price)
                exit_reason = _determine_exit_reason(active_signal, exit_price)
                update_trade_exit(active_order_id, exit_price, exit_reason)
                active_order_id = None
                active_signal   = None

            risk_result = validate_signal(
                signal=signal, account_balance=balance,
                open_positions=open_positions,
            )

            approved = risk_result["approved"]
            print(f"  Balance  : ${balance:.2f}")
            print(f"  Risk     : {'APPROVED' if approved else 'VETOED'}")
            if not approved:
                print(f"  Reason   : {risk_result['reason']}")

            # ----------------------------------------------------------
            # Step 5 — Execute
            # ----------------------------------------------------------
            if approved:
                order_result = execute_signal(signal=signal, validated_risk=risk_result)

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

                if order_result.get("status") == "placed":
                    active_order_id = order_result.get("order_id")
                    active_signal   = signal

            # ----------------------------------------------------------
            # Heartbeat + sleep
            # ----------------------------------------------------------
            retry     = _should_retry(order_result)
            elapsed   = time.time() - loop_start
            remaining = max(0.0, LOOP_SLEEP - elapsed)
            sleep_msg = (f"Retrying in {RETRY_SLEEP}s" if retry
                         else f"Waiting for next candle in {remaining:.0f}s")
            _heartbeat(now_utc, signal, risk_result, order_result, sleep_msg)

            if retry:
                _sleep_retry()
            else:
                _sleep_to_next_bar(loop_start)

    except KeyboardInterrupt:
        _shutdown()


def _sleep_retry():
    print(f"\n  Retrying in {RETRY_SLEEP}s...")
    time.sleep(RETRY_SLEEP)


def _sleep_to_next_bar(loop_start):
    elapsed    = time.time() - loop_start
    sleep_secs = max(0.0, LOOP_SLEEP - elapsed)
    print(f"\n  Sleeping {sleep_secs:.0f}s  (execution took {elapsed:.1f}s of {LOOP_SLEEP}s bar)")
    time.sleep(sleep_secs)


def _shutdown():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\n\n[STOP] Bot stopped by user at {ts}  (KeyboardInterrupt)\n")
    print("─" * 62)
    print("  Final session summary:")
    print_summary()


if __name__ == "__main__":
    main()
