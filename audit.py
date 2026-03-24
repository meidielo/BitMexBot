"""
audit.py -- Phase 7

One-shot pre-live validation script.
Run manually:  python audit.py

Read-only -- does not modify any files or connect to the exchange.
All constants are declared locally so the audit is independent of
the modules it is checking.
"""

import json
import os
import re
import sqlite3
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Constants (mirrors of risk.py / logger.py -- kept local for independence)
# ---------------------------------------------------------------------------
DB_PATH           = os.path.join("data", "trades.db")
DAILY_LOSS_FILE   = os.path.join("data", "daily_loss.json")
MAX_DAILY_LOSS    = 50.0
MAX_POSITION_BTC  = 0.10
LEVERAGE          = 15
LIQ_BUFFER        = 0.9          # same buffer used by risk.py
MIN_TRADES        = 30
MIN_WIN_RATE      = 50.0         # percent
MIN_RR            = 1.4

# Width of the printed report
W = 64


# ---------------------------------------------------------------------------
# DB helpers  (self-contained — no import from logger.py)
# ---------------------------------------------------------------------------

def _db_available() -> bool:
    return os.path.exists(DB_PATH)


def _rows(sql: str, params: tuple = ()) -> list:
    """Return list of sqlite3.Row; empty list on any error."""
    if not _db_available():
        return []
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        result = conn.execute(sql, params).fetchall()
        conn.close()
        return result
    except Exception:
        return []


def _scalar(sql: str, params: tuple = (), default=None):
    """Return a single value or default."""
    rows = _rows(sql, params)
    if rows and rows[0][0] is not None:
        return rows[0][0]
    return default


# ---------------------------------------------------------------------------
# TRADE SAMPLE
# ---------------------------------------------------------------------------

def chk_min_trades() -> tuple[bool, str]:
    count = _scalar(
        "SELECT COUNT(*) FROM trades WHERE exit_price IS NOT NULL",
        default=0,
    )
    passed = int(count) >= MIN_TRADES
    return passed, (
        f"{count} completed trades in database  (need >= {MIN_TRADES})"
    )


# ---------------------------------------------------------------------------
# PERFORMANCE
# ---------------------------------------------------------------------------

def chk_win_rate() -> tuple[bool, str]:
    total = _scalar(
        "SELECT COUNT(*) FROM trades WHERE exit_price IS NOT NULL",
        default=0,
    )
    if not total:
        return False, "No closed trades -- cannot compute win rate"

    wins = _scalar(
        "SELECT COUNT(*) FROM trades WHERE exit_price IS NOT NULL AND pnl_usd > 0",
        default=0,
    )
    rate = wins / total * 100
    passed = rate > MIN_WIN_RATE
    return passed, (
        f"{rate:.1f}%  ({wins}W / {total - wins}L out of {total} closed trades)"
        f"  (need > {MIN_WIN_RATE}%)"
    )


def chk_rr() -> tuple[bool, str]:
    """
    Achieved R:R = average winning PnL / average absolute losing PnL.
    All-wins case is treated as infinite R:R (pass).
    """
    avg_win  = _scalar(
        "SELECT AVG(pnl_usd) FROM trades "
        "WHERE exit_price IS NOT NULL AND pnl_usd > 0"
    )
    avg_loss = _scalar(
        "SELECT AVG(ABS(pnl_usd)) FROM trades "
        "WHERE exit_price IS NOT NULL AND pnl_usd < 0"
    )

    if avg_win is None:
        return False, "No winning trades -- cannot compute R:R"

    if avg_loss is None or float(avg_loss) == 0:
        return True, (
            "No losing trades on record -- R:R is effectively infinite  (PASS)"
        )

    rr = float(avg_win) / float(avg_loss)
    passed = rr > MIN_RR
    return passed, (
        f"Achieved R:R: {rr:.2f}  "
        f"(avg win ${float(avg_win):.4f}  /  avg loss ${float(avg_loss):.4f})"
        f"  (need > {MIN_RR})"
    )


def chk_total_pnl() -> tuple[bool, str]:
    total = _scalar(
        "SELECT COALESCE(SUM(pnl_usd), 0) FROM trades "
        "WHERE exit_price IS NOT NULL",
        default=0.0,
    )
    passed = float(total) > 0
    return passed, f"Total realised PnL: ${float(total):+.4f}  (must be positive)"


def chk_max_single_loss() -> tuple[bool, str]:
    worst = _scalar(
        "SELECT MIN(pnl_usd) FROM trades "
        "WHERE exit_price IS NOT NULL AND pnl_usd < 0"
    )
    if worst is None:
        return True, "No losing trades on record  (PASS)"

    worst = float(worst)
    passed = abs(worst) < MAX_DAILY_LOSS
    return passed, (
        f"Largest single loss: ${abs(worst):.4f}  "
        f"(must be < ${MAX_DAILY_LOSS:.2f})"
    )


def chk_max_position_size() -> tuple[bool, str]:
    largest = _scalar(
        "SELECT MAX(position_size_btc) FROM trades",
        default=0.0,
    )
    largest = float(largest)
    passed  = largest <= MAX_POSITION_BTC
    return passed, (
        f"Largest position size: {largest:.8f} BTC  "
        f"(hard cap: {MAX_POSITION_BTC} BTC)"
    )


# ---------------------------------------------------------------------------
# RISK SYSTEM
# ---------------------------------------------------------------------------

def chk_halt_fired() -> tuple[bool, str]:
    """
    Check whether any calendar day in the DB accumulated realised losses
    >= MAX_DAILY_LOSS, which would have triggered the daily halt.
    """
    rows = _rows(
        """
        SELECT date(timestamp) AS trade_date,
               SUM(pnl_usd)    AS day_pnl
          FROM trades
         WHERE exit_price IS NOT NULL
         GROUP BY date(timestamp)
        HAVING day_pnl <= ?
        """,
        (-MAX_DAILY_LOSS,),
    )
    passed = len(rows) > 0
    if passed:
        dates = ", ".join(str(r["trade_date"]) for r in rows)
        return True, f"Halt-level losses recorded on {len(rows)} day(s): {dates}"
    return False, (
        f"No calendar day reached the ${MAX_DAILY_LOSS:.2f} loss limit in history. "
        f"Cannot confirm halt mechanism has been exercised."
    )


def chk_no_liq_breach() -> tuple[bool, str]:
    """
    Verify that no trade's exit_price was worse than the estimated
    liquidation price at the time of entry.
      LONG  liq = entry * (1 - (1/LEVERAGE) * LIQ_BUFFER)  -- exit must be >= liq
      SHORT liq = entry * (1 + (1/LEVERAGE) * LIQ_BUFFER)  -- exit must be <= liq
    """
    rows = _rows(
        "SELECT order_id, signal, entry_price, exit_price "
        "FROM trades WHERE exit_price IS NOT NULL"
    )
    margin = (1 / LEVERAGE) * LIQ_BUFFER
    breaches = []

    for r in rows:
        entry = float(r["entry_price"])
        exit_ = float(r["exit_price"])
        sig   = r["signal"]

        if sig == "LONG":
            liq = entry * (1 - margin)
            if exit_ < liq:
                breaches.append(
                    f"    {r['order_id']}  LONG   exit {exit_:.2f} < liq {liq:.2f}"
                )
        elif sig == "SHORT":
            liq = entry * (1 + margin)
            if exit_ > liq:
                breaches.append(
                    f"    {r['order_id']}  SHORT  exit {exit_:.2f} > liq {liq:.2f}"
                )

    passed = len(breaches) == 0
    if passed:
        return True, f"All {len(rows)} trades exited before estimated liquidation price"
    detail = f"{len(breaches)} trade(s) exited beyond liquidation price:\n" + "\n".join(breaches)
    return False, detail


def chk_all_risk_approved() -> tuple[bool, str]:
    count = _scalar(
        "SELECT COUNT(*) FROM trades WHERE approved_by_risk = 0",
        default=0,
    )
    count = int(count)
    passed = count == 0
    return passed, (
        f"{count} trade(s) logged with approved_by_risk = False  (must be 0)"
    )


# ---------------------------------------------------------------------------
# CODE SAFETY
# ---------------------------------------------------------------------------

def chk_testnet_env() -> tuple[bool, str]:
    val = os.getenv("BITMEX_TESTNET", "").strip().lower()
    passed = val == "true"
    return passed, (
        f'BITMEX_TESTNET = "{val}" in .env  (must be exactly "true")'
    )


def chk_no_hardcoded_keys() -> tuple[bool, str]:
    """
    Grep every .py file for patterns suggesting a hardcoded credential:
      - Variable named key/secret/apikey/api_key assigned to a string
        literal of 16+ alphanumeric characters.
    Ignores lines that call os.getenv / os.environ (safe pattern).
    """
    pattern = re.compile(
        r'(api[_]?key|api[_]?secret|secret)\s*=\s*["\']([a-zA-Z0-9_\-]{16,})["\']',
        re.IGNORECASE,
    )
    safe_pattern = re.compile(r'os\.(getenv|environ)', re.IGNORECASE)

    violations = []
    py_files = [f for f in os.listdir(".") if f.endswith(".py")]

    for fname in sorted(py_files):
        try:
            with open(fname, encoding="utf-8", errors="ignore") as fh:
                for lineno, line in enumerate(fh, start=1):
                    if pattern.search(line) and not safe_pattern.search(line):
                        violations.append(
                            f"    {fname}:{lineno}:  {line.strip()[:72]}"
                        )
        except Exception as e:
            violations.append(f"    Could not read {fname}: {e}")

    passed = len(violations) == 0
    if passed:
        return True, f"No hardcoded credentials found across {len(py_files)} .py file(s)"
    detail = f"{len(violations)} potential hardcoded credential(s):\n" + "\n".join(violations)
    return False, detail


def chk_testnet_guard_present() -> tuple[bool, str]:
    path = "order_manager.py"
    try:
        content = open(path, encoding="utf-8").read()
    except FileNotFoundError:
        return False, f"{path} not found"
    except Exception as e:
        return False, f"Could not read {path}: {e}"

    has_fn    = "_assert_testnet" in content
    has_env   = "BITMEX_TESTNET"  in content
    has_raise = "EnvironmentError" in content or "raise" in content

    passed = has_fn and has_env and has_raise
    parts = []
    if not has_fn:    parts.append("_assert_testnet() function missing")
    if not has_env:   parts.append("BITMEX_TESTNET env-var check missing")
    if not has_raise: parts.append("no raise/EnvironmentError found")

    if passed:
        return True, "Testnet guard confirmed in order_manager.py"
    return False, "order_manager.py guard incomplete: " + ", ".join(parts)


def chk_db_readable() -> tuple[bool, str]:
    if not os.path.exists(DB_PATH):
        return False, f"{DB_PATH} does not exist"
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("SELECT COUNT(*) FROM trades").fetchone()
        conn.close()
        size_kb = os.path.getsize(DB_PATH) / 1024
        return True, f"{DB_PATH} exists and is readable  ({size_kb:.1f} KB)"
    except Exception as e:
        return False, f"{DB_PATH} exists but query failed: {e}"


def chk_daily_loss_json_readable() -> tuple[bool, str]:
    if not os.path.exists(DAILY_LOSS_FILE):
        return False, f"{DAILY_LOSS_FILE} does not exist"
    try:
        with open(DAILY_LOSS_FILE) as fh:
            data = json.load(fh)
        d    = data.get("date", "?")
        loss = data.get("loss_usd", "?")
        return True, (
            f"{DAILY_LOSS_FILE} exists and is readable  "
            f"(date: {d}, loss_usd: ${loss})"
        )
    except Exception as e:
        return False, f"{DAILY_LOSS_FILE} exists but could not be parsed: {e}"


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------

_PASS = "PASS"
_FAIL = "FAIL"


def _print_header() -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print("=" * W)
    print(f"  BitMEX Bot -- Pre-Live Audit Report")
    print(f"  Generated: {now}")
    print("=" * W)


def _print_section(title: str) -> None:
    print(f"\n  {title}")
    print(f"  {'-' * (W - 4)}")


def _print_check(label: str, passed: bool, detail: str) -> None:
    tag    = _PASS if passed else _FAIL
    marker = "[PASS]" if passed else "[FAIL]"
    # Print the check label with tag
    print(f"  {marker}  {label}")
    # Print the detail on the next line, indented
    for line in detail.splitlines():
        print(f"         {line}")


def _print_verdict(results: list[tuple[str, bool, str]]) -> None:
    total  = len(results)
    failed = [r for r in results if not r[1]]
    passed = total - len(failed)

    print()
    print("=" * W)
    if not failed:
        print(f"  FINAL VERDICT: ALL {total} CHECKS PASSED")
    else:
        print(f"  FINAL VERDICT: FAILED {len(failed)} / {total} CHECKS -- DO NOT GO LIVE")
        print()
        print("  Failed checks:")
        for label, _, detail in failed:
            print(f"    - {label}")
    print("=" * W)

    if not failed:
        _print_live_warning()


def _print_live_warning() -> None:
    border = "*" * W
    print()
    print(border)
    print()
    print("  AUDIT PASSED. Before going live:")
    print()
    print("   1. Create new live API key -- Order permission only, no Withdraw")
    print("   2. Lock API key to your IP via CIDR")
    print("   3. Set max position to 0.01 BTC for first 30 live trades")
    print("   4. Change BITMEX_TESTNET=false in .env")
    print("   5. Monitor every trade manually for first week")
    print()
    print(border)


# ---------------------------------------------------------------------------
# Audit runner
# ---------------------------------------------------------------------------

CHECKS = [
    # (section_title, [(label, check_fn), ...])
    (
        "TRADE SAMPLE",
        [
            ("Minimum 30 completed trades in database",          chk_min_trades),
        ],
    ),
    (
        "PERFORMANCE",
        [
            ("Win rate > 50%",                                   chk_win_rate),
            ("Average R:R achieved > 1.4",                       chk_rr),
            ("Total PnL positive",                               chk_total_pnl),
            ("Largest single loss < $50",                        chk_max_single_loss),
            ("No trade exceeded max position size of 0.10 BTC",  chk_max_position_size),
        ],
    ),
    (
        "RISK SYSTEM",
        [
            ("Daily loss halt fired at least once in history",   chk_halt_fired),
            ("Zero trades where exit worse than liquidation",     chk_no_liq_breach),
            ("Zero trades logged with approved_by_risk = False",  chk_all_risk_approved),
        ],
    ),
    (
        "CODE SAFETY",
        [
            ("BITMEX_TESTNET=true in current .env",              chk_testnet_env),
            ("No API keys hardcoded in any .py file",            chk_no_hardcoded_keys),
            ("order_manager.py contains testnet guard",          chk_testnet_guard_present),
            ("data/trades.db exists and is readable",            chk_db_readable),
            ("data/daily_loss.json exists and is readable",      chk_daily_loss_json_readable),
        ],
    ),
]


def run_audit() -> None:
    _print_header()

    all_results: list[tuple[str, bool, str]] = []

    for section_title, checks in CHECKS:
        _print_section(section_title)
        for label, fn in checks:
            try:
                passed, detail = fn()
            except Exception as e:
                passed, detail = False, f"Check raised an exception: {e}"
            _print_check(label, passed, detail)
            all_results.append((label, passed, detail))

    _print_verdict(all_results)


if __name__ == "__main__":
    run_audit()
