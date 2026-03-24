"""
risk.py — Phase 4
Validates a signal dict against hardcoded risk rules before any order is placed.
No exchange connection. No order placement. Pure logic.
"""

import json
import os
from datetime import date

# ---------------------------------------------------------------------------
# Hardcoded constants — never overridden by signals, config, or AI output
# ---------------------------------------------------------------------------
LEVERAGE            = 15          # fixed leverage for every trade
MAX_POSITION_BTC    = 0.10        # retained for audit.py / test_risk.py reference
MAX_CONTRACTS       = 1500        # hard cap: ~0.10 BTC at typical prices
LOT_SIZE            = 100         # BitMEX minimum order increment for XBTUSDT
RISK_PER_TRADE_PCT  = 0.02        # 2 % of account balance risked per trade
MAX_DAILY_LOSS_USD  = 50.0        # bot halts for the day if this is hit

# Liquidation buffer: liq is estimated at 90 % of the theoretical margin level.
# Formula:
#   LONG  liq = entry * (1 - (1/leverage) * LIQ_BUFFER)
#   SHORT liq = entry * (1 + (1/leverage) * LIQ_BUFFER)
LIQ_BUFFER = 0.9

DAILY_LOSS_FILE = os.path.join("data", "daily_loss.json")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_daily_loss() -> float:
    """
    Read today's realised loss (USD) from data/daily_loss.json.
    Returns 0.0 if the file does not exist or today has no entry.
    Schema: {"date": "YYYY-MM-DD", "loss_usd": 35.50}
    """
    try:
        if not os.path.exists(DAILY_LOSS_FILE):
            return 0.0
        with open(DAILY_LOSS_FILE, "r") as f:
            data = json.load(f)
        if data.get("date") == str(date.today()):
            return float(data.get("loss_usd", 0.0))
        return 0.0  # stale file — different day
    except Exception as e:
        print(f"[WARN] Could not read daily loss file: {e}. Assuming $0.")
        return 0.0


def _calc_liq_price(entry: float, signal: str) -> float:
    """
    Estimate liquidation price at fixed leverage with a 10 % buffer.
    LONG  liq = entry * (1 - (1/leverage) * LIQ_BUFFER)
    SHORT liq = entry * (1 + (1/leverage) * LIQ_BUFFER)
    """
    margin_fraction = (1 / LEVERAGE) * LIQ_BUFFER
    if signal == "LONG":
        return entry * (1 - margin_fraction)
    return entry * (1 + margin_fraction)


def _calc_position_size(account_balance: float) -> int:
    """
    XBTUSDT is a linear perpetual — size is in contracts, settled in USDT.
    Formula  : contracts = account_balance * RISK_PER_TRADE_PCT * LEVERAGE
    Minimum  : 1 contract (no rounding to 100 required for linear).
    Capped   : at MAX_CONTRACTS (1500).
    Example  : $578 → 578 * 0.02 * 15 = 173.4 → 173 contracts
    """
    raw       = account_balance * RISK_PER_TRADE_PCT * LEVERAGE
    contracts = max(1, int(raw))
    return min(contracts, MAX_CONTRACTS)


def _veto(reason: str) -> dict:
    return {"approved": False, "reason": reason,
            "position_size_btc": None, "leverage": None}


def _approve(reason: str, position_size_btc: int) -> dict:
    return {"approved": True, "reason": reason,
            "position_size_btc": int(position_size_btc),
            "leverage": LEVERAGE}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate_signal(signal: dict, account_balance: float,
                    open_positions: list) -> dict:
    """
    Apply every hardcoded risk rule to a signal.

    Parameters
    ----------
    signal          : dict returned by signals.get_signal()
    account_balance : current free balance in USD
    open_positions  : list of currently open positions (empty = no open trade)

    Returns
    -------
    dict with keys: approved, reason, position_size_btc, leverage
    """

    # ------------------------------------------------------------------
    # Rule 1 — Signal must be LONG or SHORT
    # ------------------------------------------------------------------
    sig = signal.get("signal")
    if sig not in ("LONG", "SHORT"):
        return _veto(
            f"Rule 1 FAILED: signal is '{sig}'. "
            "Only LONG or SHORT may proceed to risk check."
        )

    entry    = signal.get("entry_price")
    sl_price = signal.get("sl_price")

    # Guard: entry and sl must be present (they always are for LONG/SHORT,
    # but we verify defensively)
    if entry is None or sl_price is None:
        return _veto("Rule 1 FAILED: entry_price or sl_price is None on a directional signal.")

    # ------------------------------------------------------------------
    # Rule 2 — Only one trade at a time
    # ------------------------------------------------------------------
    if open_positions:
        return _veto(
            f"Rule 2 FAILED: {len(open_positions)} open position(s) already exist. "
            "Bot trades one position at a time."
        )

    # ------------------------------------------------------------------
    # Rule 3 — Daily loss must be under the hard limit
    # ------------------------------------------------------------------
    daily_loss = _load_daily_loss()
    if daily_loss >= MAX_DAILY_LOSS_USD:
        return _veto(
            f"Rule 3 FAILED: daily loss ${daily_loss:.2f} has reached the "
            f"${MAX_DAILY_LOSS_USD:.2f} limit. Bot is halted for the day."
        )

    # ------------------------------------------------------------------
    # Rule 4 — SL must fire before the estimated liquidation price
    #
    #   LONG : price falls  → liq is BELOW entry, SL also below entry
    #          SL is safe if sl_price > liq_price  (SL hit first on way down)
    #
    #   SHORT: price rises  → liq is ABOVE entry, SL also above entry
    #          SL is safe if sl_price < liq_price  (SL hit first on way up)
    # ------------------------------------------------------------------
    liq_price = _calc_liq_price(entry, sig)

    if sig == "LONG" and sl_price <= liq_price:
        return _veto(
            f"Rule 4 FAILED (LONG): SL {sl_price:.2f} is at or below estimated "
            f"liquidation price {liq_price:.2f}. "
            "Liquidation would occur before stop-loss fires."
        )

    if sig == "SHORT" and sl_price >= liq_price:
        return _veto(
            f"Rule 4 FAILED (SHORT): SL {sl_price:.2f} is at or above estimated "
            f"liquidation price {liq_price:.2f}. "
            "Liquidation would occur before stop-loss fires."
        )

    # ------------------------------------------------------------------
    # Rule 5 — Position sizing (2 % risk, floored to 100, hard cap 1500 contracts)
    # ------------------------------------------------------------------
    position_size_btc = _calc_position_size(account_balance)
    capped = position_size_btc >= MAX_CONTRACTS

    # ------------------------------------------------------------------
    # All rules passed — build approval message
    # ------------------------------------------------------------------
    reason_parts = [
        f"All risk rules passed for {sig}.",
        f"Entry: {entry:.2f}  |  SL: {sl_price:.2f}  |  Liq (est.): {liq_price:.2f}",
        f"Position size: {position_size_btc} contracts"
        + (" (capped at 1500 max)" if capped else
           f"  ({account_balance:.2f} * 2% * {LEVERAGE}x, truncated to integer)"),
        f"Leverage: {LEVERAGE}x  |  Daily loss so far: ${daily_loss:.2f}",
    ]

    return _approve(" | ".join(reason_parts), position_size_btc)


# ---------------------------------------------------------------------------
# CLI entry point — runs against live testnet data for a quick sanity check
# ---------------------------------------------------------------------------
def _print_result(result: dict) -> None:
    verdict = "  APPROVED  " if result["approved"] else "  VETOED    "
    width = 62
    print("=" * width)
    print(f"  RISK FILTER: [{verdict}]")
    print("=" * width)
    print(f"  {result['reason']}")
    if result["approved"]:
        print(f"  Position size : {result['position_size_btc']} contracts")
        print(f"  Leverage      : {result['leverage']}x")
    print("=" * width)


if __name__ == "__main__":
    from fetch_data import fetch_ohlcv
    from indicators import compute_indicators
    from signals import get_signal

    print("Phase 4 — Risk filter check on live testnet signal\n")

    df_raw = fetch_ohlcv()
    if df_raw is None:
        raise SystemExit("[ABORT] Could not fetch OHLCV data.")

    df = compute_indicators(df_raw)
    if df is None:
        raise SystemExit("[ABORT] Indicator computation failed.")

    sig = get_signal(df)
    print(f"Signal received: {sig['signal']}  |  {sig['reason']}\n")

    # Use a dummy balance of $1,000 and no open positions for the demo
    result = validate_signal(
        signal=sig,
        account_balance=1000.0,
        open_positions=[],
    )
    _print_result(result)
