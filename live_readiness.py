"""Read-only live trading readiness and no-go evaluator.

This module does not enable live trading. It summarizes whether the current
research/testnet evidence is strong enough for manual review, and it keeps
explicit rules for deciding that a candidate should not be promoted.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
import sys

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Handled exception in live_readiness.py:20", file=sys.stderr)
    pass


TRADE_DB_PATH = os.path.join("data", "trades.db")
RESEARCH_DB_PATH = os.path.join("data", "research_signals.db")

MIN_CLOSED_TRADES = 30
MAX_POSITION_BTC = 0.10
MAX_DAILY_LOSS_USD = 50.0
MAX_RESEARCH_AGE_HOURS = 2.5
MIN_SHADOW_ROWS_7D = 24
MIN_WATCH_CANDIDATES_FOR_REVIEW = 20
STALE_SHADOW_ROWS_30D = 8_640
MIN_WATCH_CANDIDATES_30D = 5


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        print("Handled exception in live_readiness.py:46", file=sys.stderr)
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _connect(path: str, read_only: bool = False) -> sqlite3.Connection:
    if not read_only:
        return sqlite3.connect(path, timeout=5)

    uri_path = os.path.abspath(path).replace("\\", "/")
    return sqlite3.connect(
        f"file:{uri_path}?mode=ro&immutable=1",
        uri=True,
        timeout=5,
    )


def _scalar(conn: sqlite3.Connection, query: str, params: tuple[Any, ...] = (), default: Any = None) -> Any:
    try:
        row = conn.execute(query, params).fetchone()
    except sqlite3.Error:
        print("Handled exception in live_readiness.py:68", file=sys.stderr)
        return default
    if not row or row[0] is None:
        return default
    return row[0]


def _gate(name: str, passed: bool, detail: str, required: bool = True) -> dict[str, Any]:
    return {
        "name": name,
        "status": "PASS" if passed else "FAIL",
        "required": required,
        "detail": detail,
    }


def _warn_gate(name: str, detail: str) -> dict[str, Any]:
    return {"name": name, "status": "WARN", "required": False, "detail": detail}


def _rule(name: str, status: str, detail: str) -> dict[str, str]:
    return {"name": name, "status": status, "detail": detail}


def _trade_metrics(trade_db_path: str, read_only: bool) -> dict[str, Any]:
    metrics = {
        "db_exists": os.path.exists(trade_db_path),
        "closed_trades": 0,
        "total_pnl": 0.0,
        "win_rate": 0.0,
        "max_position_btc": 0.0,
        "unapproved_trades": 0,
        "daily_halt_days": 0,
    }
    if not metrics["db_exists"]:
        return metrics

    try:
        conn = _connect(trade_db_path, read_only=read_only)
        try:
            closed = int(_scalar(conn, "SELECT COUNT(*) FROM trades WHERE exit_price IS NOT NULL", default=0))
            wins = int(
                _scalar(
                    conn,
                    "SELECT COUNT(*) FROM trades WHERE exit_price IS NOT NULL AND pnl_usd > 0",
                    default=0,
                )
            )
            metrics.update(
                {
                    "closed_trades": closed,
                    "total_pnl": float(
                        _scalar(
                            conn,
                            "SELECT COALESCE(SUM(pnl_usd), 0) FROM trades WHERE exit_price IS NOT NULL",
                            default=0.0,
                        )
                    ),
                    "win_rate": (wins / closed * 100) if closed else 0.0,
                    "max_position_btc": float(
                        _scalar(conn, "SELECT COALESCE(MAX(position_size_btc), 0) FROM trades", default=0.0)
                    ),
                    "unapproved_trades": int(
                        _scalar(conn, "SELECT COUNT(*) FROM trades WHERE approved_by_risk = 0", default=0)
                    ),
                    "daily_halt_days": int(
                        _scalar(
                            conn,
                            """
                            SELECT COUNT(*) FROM (
                                SELECT date(timestamp) AS trade_date, SUM(pnl_usd) AS day_pnl
                                FROM trades
                                WHERE exit_price IS NOT NULL
                                GROUP BY date(timestamp)
                                HAVING day_pnl <= ?
                            )
                            """,
                            (-MAX_DAILY_LOSS_USD,),
                            default=0,
                        )
                    ),
                }
            )
        finally:
            conn.close()
    except sqlite3.Error as exc:
        print("Handled exception in live_readiness.py:153", file=sys.stderr)
        metrics["error"] = type(exc).__name__
    return metrics


def _research_metrics(research_db_path: str, read_only: bool, now: datetime) -> dict[str, Any]:
    metrics = {
        "db_exists": os.path.exists(research_db_path),
        "latest_run": "",
        "latest_age_hours": None,
        "symbols_scanned": 0,
        "rows_7d": 0,
        "watch_7d": 0,
        "rows_30d": 0,
        "watch_30d": 0,
    }
    if not metrics["db_exists"]:
        return metrics

    since_7d = (now - timedelta(days=7)).replace(microsecond=0).isoformat()
    since_30d = (now - timedelta(days=30)).replace(microsecond=0).isoformat()

    try:
        conn = _connect(research_db_path, read_only=read_only)
        try:
            conn.row_factory = sqlite3.Row
            run = conn.execute(
                """
                SELECT started_at_utc, symbols_scanned
                FROM research_runs
                ORDER BY started_at_utc DESC
                LIMIT 1
                """
            ).fetchone()
            if run:
                latest = str(run["started_at_utc"])
                parsed = _parse_dt(latest)
                metrics["latest_run"] = latest
                metrics["latest_age_hours"] = (
                    (now - parsed).total_seconds() / 3600 if parsed else None
                )
                metrics["symbols_scanned"] = int(run["symbols_scanned"] or 0)

            for suffix, since in (("7d", since_7d), ("30d", since_30d)):
                metrics[f"rows_{suffix}"] = int(
                    _scalar(
                        conn,
                        "SELECT COUNT(*) FROM research_signals WHERE timestamp_utc >= ?",
                        (since,),
                        default=0,
                    )
                )
                metrics[f"watch_{suffix}"] = int(
                    _scalar(
                        conn,
                        """
                        SELECT COUNT(*)
                        FROM research_signals
                        WHERE timestamp_utc >= ? AND action != 'NO_SIGNAL'
                        """,
                        (since,),
                        default=0,
                    )
                )
        finally:
            conn.close()
    except sqlite3.Error as exc:
        print("Handled exception in live_readiness.py:219", file=sys.stderr)
        metrics["error"] = type(exc).__name__
    return metrics


def evaluate_live_readiness(
    trade_db_path: str = TRADE_DB_PATH,
    research_db_path: str = RESEARCH_DB_PATH,
    env: Mapping[str, str] | None = None,
    read_only: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    env = os.environ if env is None else env
    now = now or _utc_now()
    trades = _trade_metrics(trade_db_path, read_only=read_only)
    research = _research_metrics(research_db_path, read_only=read_only, now=now)
    testnet_value = str(env.get("BITMEX_TESTNET", "")).strip().lower()

    gates = [
        _gate(
            "Runtime testnet guard",
            testnet_value == "true",
            "BITMEX_TESTNET is true in the running environment"
            if testnet_value == "true"
            else "BITMEX_TESTNET is not confirmed true in the running environment",
        ),
        _gate(
            "Closed testnet sample",
            trades["closed_trades"] >= MIN_CLOSED_TRADES,
            f"{trades['closed_trades']} closed trades recorded, need at least {MIN_CLOSED_TRADES}",
        ),
        _gate(
            "Positive realized PnL sample",
            trades["closed_trades"] >= MIN_CLOSED_TRADES and trades["total_pnl"] > 0,
            f"PnL ${trades['total_pnl']:+.2f}, win rate {trades['win_rate']:.1f}%",
        ),
        _gate(
            "Risk approvals clean",
            trades["unapproved_trades"] == 0,
            f"{trades['unapproved_trades']} unapproved trade rows",
        ),
        _gate(
            "Position size audit clean",
            trades["max_position_btc"] <= MAX_POSITION_BTC,
            f"max logged position {trades['max_position_btc']:.8f} BTC, cap {MAX_POSITION_BTC:.2f} BTC",
        ),
        _gate(
            "Daily loss halt exercised",
            trades["daily_halt_days"] > 0,
            f"{trades['daily_halt_days']} halt-level loss day(s) in history",
        ),
        _gate(
            "Shadow scanner fresh",
            research["latest_age_hours"] is not None
            and research["latest_age_hours"] <= MAX_RESEARCH_AGE_HOURS,
            "latest run age "
            + (
                f"{research['latest_age_hours']:.1f}h"
                if research["latest_age_hours"] is not None
                else "unknown"
            ),
        ),
        _gate(
            "Shadow sample collecting",
            research["rows_7d"] >= MIN_SHADOW_ROWS_7D,
            f"{research['rows_7d']} scanner rows in 7d, need at least {MIN_SHADOW_ROWS_7D}",
        ),
        _gate(
            "Watch candidate evidence",
            research["watch_30d"] >= MIN_WATCH_CANDIDATES_FOR_REVIEW,
            f"{research['watch_30d']} watch candidates in 30d, need at least {MIN_WATCH_CANDIDATES_FOR_REVIEW}",
        ),
    ]

    reject_negative_sample = (
        trades["closed_trades"] >= MIN_CLOSED_TRADES and trades["total_pnl"] <= 0
    )
    reject_stale_shadow = (
        research["rows_30d"] >= STALE_SHADOW_ROWS_30D
        and research["watch_30d"] < MIN_WATCH_CANDIDATES_30D
    )

    no_go_rules = [
        _rule(
            "Hard gate failure",
            "TRIGGERED" if any(g["required"] and g["status"] == "FAIL" for g in gates) else "CLEAR",
            "Any failed required gate blocks real trading consideration.",
        ),
        _rule(
            "Negative testnet sample",
            "TRIGGERED" if reject_negative_sample else "ARMED",
            f"After {MIN_CLOSED_TRADES}+ closed trades, total PnL must stay positive.",
        ),
        _rule(
            "No active edge in current regime",
            "TRIGGERED" if reject_stale_shadow else "ARMED",
            (
                f"If {STALE_SHADOW_ROWS_30D:,}+ shadow rows produce fewer than "
                f"{MIN_WATCH_CANDIDATES_30D} watch candidates in 30d, mark no active edge."
            ),
        ),
        _rule(
            "Outcome expectancy",
            "PENDING",
            "Next upgrade: score matured watch candidates; reject if 50+ outcomes have non-positive expectancy.",
        ),
    ]

    hard_fail = any(g["required"] and g["status"] == "FAIL" for g in gates)
    if reject_negative_sample or reject_stale_shadow:
        verdict = "REJECT_DO_NOT_PROMOTE"
        headline = "Evidence says not to promote this strategy."
        decision = "Keep it off real trading and either retire or redesign the candidate."
    elif hard_fail:
        verdict = "NOT_READY"
        headline = "Not ready for real trading."
        decision = "Keep running testnet and shadow research. Do not use real funds."
    else:
        verdict = "READY_FOR_MANUAL_REVIEW"
        headline = "Ready only for manual review."
        decision = "All automated gates passed, but real trading still needs a human go/no-go review."

    failed = sum(1 for gate in gates if gate["status"] == "FAIL")
    return {
        "verdict": verdict,
        "headline": headline,
        "decision": decision,
        "failed_gates": failed,
        "total_gates": len(gates),
        "gates": gates,
        "no_go_rules": no_go_rules,
        "metrics": {"trades": trades, "research": research},
        "generated_at": now.replace(microsecond=0).isoformat(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Print full JSON.")
    parser.add_argument("--read-only", action="store_true", help="Open SQLite DBs in read-only immutable mode.")
    args = parser.parse_args()

    result = evaluate_live_readiness(read_only=args.read_only)
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"{result['verdict']}: {result['headline']}")
        print(result["decision"])
        for gate in result["gates"]:
            print(f"- {gate['status']}: {gate['name']} - {gate['detail']}")
    return 0 if result["verdict"] == "READY_FOR_MANUAL_REVIEW" else 1


if __name__ == "__main__":
    raise SystemExit(main())
