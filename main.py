"""BitMEX Testnet research runner with fail-closed execution controls.

Signals use completed mainnet public candles and causally available settled
funding observations. Authenticated execution remains Testnet-only. Any
unresolved durable intent pauses the runner so a restart cannot duplicate or
trade around unknown exposure.
"""

from __future__ import annotations

import math
import os
import time
from datetime import datetime, timezone
from typing import Any, Mapping

import pandas as pd
from dotenv import load_dotenv

from bitmex_client import (
    attest_testnet_exchange,
    fetch_xbtusdt_position,
    get_client,
    get_data_client,
)
from condition_logger import get_logger as get_condition_logger, log_v2_conditions
from daily_loss_state import refresh_daily_loss_from_ledger
from execution_safety import build_completed_candle_decision_key
from fetch_data import fetch_ohlcv, fetch_recent_funding
from monitor import print_summary
from order_manager import execute_signal, reconcile_open_intent
from runtime_status import RuntimeStatusError, write_runner_status
from risk import validate_signal
from signals import (
    FUNDING_24H_THRESH,
    FUNDING_THRESHOLD,
    USE_SETTLEMENT_FILTER,
    VOLUME_LOOKBACK,
    VOLUME_SPIKE_MULT,
    _in_settlement_window,
    get_signal,
)
from trade_ledger import DB_PATH, LedgerError, list_open_intents


load_dotenv()

SYMBOL = "BTC/USDT:USDT"
TIMEFRAME = "15m"
CANDLE_MINUTES = 15
CANDLE_SECONDS = CANDLE_MINUTES * 60
SAFETY_MONITOR_SECONDS = 5
SAFETY_MONITOR_STATUSES = frozenset(
    {"managed_position", "paused", "protected_no_tp", "reconciling"}
)
FATAL_RUN_STATUSES = frozenset({"failed", "manual_halt"})


def _publish_runtime_status(status: str, detail_code: str) -> None:
    """Publish sanitized operator telemetry without affecting execution logic."""

    try:
        write_runner_status(status, detail_code=detail_code)
    except RuntimeStatusError as exc:
        # Heartbeat failure is visible, but never replaces the execution safety
        # result or leaks a free-form exchange error into dashboard telemetry.
        print(f"[WARN] Runtime heartbeat was not written: {exc}")


class RunnerSafetyError(RuntimeError):
    """Raised when the runner cannot prove a safe input or account state."""


def _get_usdt_balances(exchange: Any) -> tuple[float, float]:
    """Return verified total and free USDT balances, never a BTC conversion."""

    attest_testnet_exchange(exchange)
    balance = exchange.fetch_balance()
    if not isinstance(balance, Mapping):
        raise RunnerSafetyError("balance query returned an invalid response")
    currency = balance.get("USDT")
    if not isinstance(currency, Mapping):
        raise RunnerSafetyError("exchange balance omitted USDT settlement equity")
    try:
        total = float(currency["total"])
        free = float(currency["free"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RunnerSafetyError("USDT total/free balances are incomplete") from exc
    if not math.isfinite(total) or not math.isfinite(free):
        raise RunnerSafetyError("USDT balances must be finite")
    if total <= 0 or free < 0 or free > total:
        raise RunnerSafetyError("USDT total/free balances are inconsistent")
    return total, free


def _get_open_positions(exchange: Any) -> list[Mapping[str, Any]]:
    try:
        position = fetch_xbtusdt_position(exchange)
    except Exception as exc:
        raise RunnerSafetyError(f"position state is untrusted: {exc}") from exc
    return [position.raw] if position.contracts else []


def _candle_close(frame: pd.DataFrame) -> pd.Timestamp:
    if frame.empty or not isinstance(frame.index, pd.DatetimeIndex):
        raise RunnerSafetyError("completed candle frame is empty or unindexed")
    opened = pd.Timestamp(frame.index[-1])
    if opened.tzinfo is None:
        opened = opened.tz_localize("UTC")
    else:
        opened = opened.tz_convert("UTC")
    return opened + pd.Timedelta(minutes=CANDLE_MINUTES)


def _decision_key(frame: pd.DataFrame, side: str, now_ms: int) -> str:
    close_ms = int(_candle_close(frame).timestamp() * 1000)
    return build_completed_candle_decision_key(
        symbol=SYMBOL,
        timeframe=TIMEFRAME,
        candle_close_ms=close_ms,
        side=side,
        now_ms=now_ms,
    )


def _settled_funding_for_candle(
    recent: pd.DataFrame,
    candle_close: pd.Timestamp,
) -> dict[str, float] | None:
    """Select only funding observations settled by the decision timestamp."""

    if recent is None or recent.empty or not {"timestamp", "rate"} <= set(recent):
        return None
    history = recent.copy()
    history["timestamp"] = pd.to_datetime(history["timestamp"], utc=True)
    history["rate"] = pd.to_numeric(history["rate"], errors="coerce")
    history = history.dropna(subset=["timestamp", "rate"])
    history = history[history["timestamp"] <= candle_close].sort_values("timestamp")
    if history.empty:
        return None
    rate = float(history["rate"].iloc[-1])
    cumulative = float(history["rate"].tail(3).sum())
    if not math.isfinite(rate) or not math.isfinite(cumulative):
        return None
    return {"rate": rate, "funding_24h": cumulative}


def _log_conditions(frame: pd.DataFrame, funding_data: dict[str, float] | None) -> None:
    try:
        logger = get_condition_logger()
        current = frame.iloc[-1]
        previous = frame.iloc[-2]
        volume = float(current["volume"])
        average = float(
            frame["volume"].iloc[-(VOLUME_LOOKBACK + 1) : -1].mean()
        )
        volume_ratio = volume / average if average > 0 else 0
        funding_rate = funding_data.get("rate") if funding_data else None
        funding_24h = funding_data.get("funding_24h") if funding_data else None
        settlement_window = (
            _in_settlement_window(frame.index[-1])
            if USE_SETTLEMENT_FILTER
            else None
        )
        log_v2_conditions(
            logger,
            funding_rate=funding_rate,
            funding_threshold=FUNDING_THRESHOLD,
            funding_24h=funding_24h,
            funding_24h_threshold=FUNDING_24H_THRESH,
            in_settlement_window=settlement_window,
            volume_ratio=volume_ratio,
            volume_threshold=VOLUME_SPIKE_MULT,
            bearish_break=float(current["close"]) < float(previous["low"]),
            bullish_break=float(current["close"]) > float(previous["high"]),
            body_pct=(
                abs(float(current["close"]) - float(current["open"]))
                / float(current["close"])
                if float(current["close"]) > 0
                else 0
            ),
            body_threshold=0.0005,
        )
    except Exception as exc:
        # Condition telemetry is not an execution dependency, but its failure is
        # always visible instead of being silently swallowed.
        print(f"[WARN] Condition telemetry was not recorded: {exc}")


def _reconcile_unresolved_intent(
    exchange: Any,
    *,
    ledger_path: str = DB_PATH,
) -> dict[str, Any] | None:
    try:
        intents = list_open_intents(ledger_path)
    except LedgerError as exc:
        raise RunnerSafetyError(f"execution ledger cannot be read: {exc}") from exc
    if not intents:
        return None
    if len(intents) != 1:
        raise RunnerSafetyError("ledger contains multiple unresolved intents")
    outcome = reconcile_open_intent(
        exchange,
        intents[0],
        ledger_path=ledger_path,
    )
    if outcome["status"] == "reconciled_flat":
        try:
            remaining = list_open_intents(ledger_path)
        except LedgerError as exc:
            raise RunnerSafetyError(
                f"execution ledger cannot be re-read: {exc}"
            ) from exc
        if not remaining:
            return None
        raise RunnerSafetyError("reconciled intent unexpectedly remains unresolved")
    return outcome


def run_once(
    execution_exchange: Any,
    data_exchange: Any,
    *,
    now_ms: int | None = None,
    ledger_path: str = DB_PATH,
) -> dict[str, Any]:
    """Run one completed-candle decision cycle."""

    try:
        unresolved = _reconcile_unresolved_intent(
            execution_exchange,
            ledger_path=ledger_path,
        )
    except RunnerSafetyError as exc:  # agent-quality: allow: explicit paused result blocks all order work
        return {"status": "paused", "reason": str(exc)}
    if unresolved is not None:
        return unresolved
    try:
        daily_loss = refresh_daily_loss_from_ledger(
            db_path=ledger_path,
            output_path=os.path.join(os.path.dirname(ledger_path), "daily_loss.json"),
        )
        daily_loss_usdt = float(daily_loss["loss_usd"])
        if not math.isfinite(daily_loss_usdt) or daily_loss_usdt < 0:
            raise RunnerSafetyError("published daily loss is invalid")
    except Exception as exc:  # agent-quality: allow: explicit paused result blocks all order work
        return {
            "status": "paused",
            "reason": f"daily loss refresh failed: {exc}",
        }

    frame = fetch_ohlcv(data_exchange)
    if frame is None:
        return {"status": "no_data", "reason": "completed OHLCV unavailable"}
    close_time = _candle_close(frame)
    recent_funding = fetch_recent_funding(data_exchange, count=10)
    funding_data = _settled_funding_for_candle(recent_funding, close_time)
    signal = get_signal(frame, current_funding=funding_data)
    _log_conditions(frame, funding_data)

    total_usdt, free_usdt = _get_usdt_balances(execution_exchange)
    positions = _get_open_positions(execution_exchange)
    risk_result = validate_signal(
        signal,
        total_usdt,
        positions,
        instrument=execution_exchange.xbtusdt_instrument,
        free_balance_usdt=free_usdt,
        daily_loss_usdt=daily_loss_usdt,
    )
    if risk_result["approved"] is not True:
        return {
            "status": "vetoed",
            "signal": signal,
            "risk": risk_result,
            "balance_total_usdt": total_usdt,
            "balance_free_usdt": free_usdt,
        }

    current_ms = int(time.time() * 1000) if now_ms is None else now_ms
    key = _decision_key(frame, signal["signal"], current_ms)
    order = execute_signal(
        signal,
        risk_result,
        decision_key=key,
        now_ms=current_ms,
        exchange=execution_exchange,
        ledger_path=ledger_path,
    )
    return {
        "status": order["status"],
        "signal": signal,
        "risk": risk_result,
        "order": order,
        "decision_key": key,
        "balance_total_usdt": total_usdt,
        "balance_free_usdt": free_usdt,
    }


def _sleep_to_next_bar(loop_started: float) -> None:
    elapsed = time.time() - loop_started
    remaining = max(0.0, CANDLE_SECONDS - elapsed)
    print(f"[WAIT] Next completed-candle check in {remaining:.0f}s")
    time.sleep(remaining)


def _sleep_to_safety_check(loop_started: float) -> None:
    elapsed = time.time() - loop_started
    remaining = max(0.0, SAFETY_MONITOR_SECONDS - elapsed)
    print(f"[WATCH] Reconciliation/protection check in {remaining:.0f}s")
    time.sleep(remaining)


def main() -> None:
    print("BitMEXBot research runner")
    print("TESTNET ORDERS ONLY. Authenticated mainnet execution is unavailable.")
    _publish_runtime_status("STARTING", "client_init")
    try:
        execution_exchange = get_client()
        data_exchange = get_data_client()
    except Exception as exc:
        _publish_runtime_status("FAILED", "client_init_failed")
        raise SystemExit(f"[ABORT] Client initialization failed: {exc}") from exc

    try:
        while True:
            loop_started = time.time()
            _publish_runtime_status("RUNNING", "decision_cycle")
            result = run_once(execution_exchange, data_exchange)
            timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
            print(f"[{timestamp}] {result['status']}: {result.get('reason', '')}")
            if result["status"] in FATAL_RUN_STATUSES:
                heartbeat_status = (
                    "MANUAL_HALT"
                    if result["status"] == "manual_halt"
                    else "FAILED"
                )
                _publish_runtime_status(heartbeat_status, result["status"])
                raise SystemExit(
                    "[HALT] Execution state requires immediate exchange and "
                    "ledger reconciliation."
                )
            if result["status"] in SAFETY_MONITOR_STATUSES:
                _publish_runtime_status("PAUSED", "safety_reconciliation")
                _sleep_to_safety_check(loop_started)
            else:
                _publish_runtime_status("WAITING", "next_candle")
                _sleep_to_next_bar(loop_started)
    except KeyboardInterrupt:
        _publish_runtime_status("STOPPED", "operator_stop")
        print("\n[STOP] Runner stopped by user.")
        print_summary()


if __name__ == "__main__":
    main()


__all__ = [
    "RunnerSafetyError",
    "SAFETY_MONITOR_SECONDS",
    "_decision_key",
    "_get_open_positions",
    "_get_usdt_balances",
    "_publish_runtime_status",
    "_reconcile_unresolved_intent",
    "_settled_funding_for_candle",
    "run_once",
]
