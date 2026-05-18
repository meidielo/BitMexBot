import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from live_readiness import evaluate_live_readiness


def make_trade_db(path: Path, closed=0, total_pnl=0.0, max_position=0.01, unapproved=0):
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE trades (
            timestamp TEXT,
            exit_price REAL,
            pnl_usd REAL,
            position_size_btc REAL,
            approved_by_risk INTEGER
        )
        """
    )
    pnl_each = total_pnl / closed if closed else 0.0
    for i in range(closed):
        conn.execute(
            """
            INSERT INTO trades
            (timestamp, exit_price, pnl_usd, position_size_btc, approved_by_risk)
            VALUES (?, 100.0, ?, ?, 1)
            """,
            (f"2026-01-{(i % 28) + 1:02d} 00:00:00", pnl_each, max_position),
        )
    for _ in range(unapproved):
        conn.execute(
            """
            INSERT INTO trades
            (timestamp, exit_price, pnl_usd, position_size_btc, approved_by_risk)
            VALUES ('2026-01-01 00:00:00', 100.0, 1.0, ?, 0)
            """,
            (max_position,),
        )
    conn.commit()
    conn.close()


def make_research_db(path: Path, rows=36, watch=0):
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE research_runs (
            run_id TEXT PRIMARY KEY,
            started_at_utc TEXT NOT NULL,
            symbols_scanned INTEGER NOT NULL,
            errors_json TEXT NOT NULL DEFAULT '[]'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE research_signals (
            timestamp_utc TEXT NOT NULL,
            strategy TEXT NOT NULL,
            symbol TEXT NOT NULL,
            action TEXT NOT NULL
        )
        """
    )
    started = "2026-05-18T10:00:00+00:00"
    conn.execute(
        "INSERT INTO research_runs (run_id, started_at_utc, symbols_scanned) VALUES ('r1', ?, 4)",
        (started,),
    )
    for i in range(rows):
        action = "WATCH_LONG" if i < watch else "NO_SIGNAL"
        conn.execute(
            """
            INSERT INTO research_signals (timestamp_utc, strategy, symbol, action)
            VALUES (?, 'funding_extreme_watch', 'BTC/USDT:USDT', ?)
            """,
            (started, action),
        )
    conn.commit()
    conn.close()


class LiveReadinessTests(unittest.TestCase):
    def test_current_style_low_sample_is_not_ready(self):
        with tempfile.TemporaryDirectory() as tmp:
            trade_db = Path(tmp) / "trades.db"
            research_db = Path(tmp) / "research.db"
            make_trade_db(trade_db, closed=1, total_pnl=1.0, max_position=190.0)
            make_research_db(research_db, rows=36, watch=0)

            result = evaluate_live_readiness(
                str(trade_db),
                str(research_db),
                env={"BITMEX_TESTNET": "true"},
                now=datetime(2026, 5, 18, 10, 30, tzinfo=timezone.utc),
            )

        self.assertEqual(result["verdict"], "NOT_READY")
        self.assertGreater(result["failed_gates"], 0)
        self.assertIn("Do not use real funds", result["decision"])

    def test_negative_large_sample_rejects_promotion(self):
        with tempfile.TemporaryDirectory() as tmp:
            trade_db = Path(tmp) / "trades.db"
            research_db = Path(tmp) / "research.db"
            make_trade_db(trade_db, closed=30, total_pnl=-5.0, max_position=0.01)
            make_research_db(research_db, rows=100, watch=25)

            result = evaluate_live_readiness(
                str(trade_db),
                str(research_db),
                env={"BITMEX_TESTNET": "true"},
                now=datetime(2026, 5, 18, 10, 30, tzinfo=timezone.utc),
            )

        self.assertEqual(result["verdict"], "REJECT_DO_NOT_PROMOTE")
        self.assertTrue(
            any(rule["name"] == "Negative testnet sample" and rule["status"] == "TRIGGERED"
                for rule in result["no_go_rules"])
        )

    def test_missing_testnet_env_blocks_readiness(self):
        with tempfile.TemporaryDirectory() as tmp:
            trade_db = Path(tmp) / "trades.db"
            research_db = Path(tmp) / "research.db"
            make_trade_db(trade_db, closed=30, total_pnl=30.0, max_position=0.01)
            make_research_db(research_db, rows=100, watch=25)

            result = evaluate_live_readiness(
                str(trade_db),
                str(research_db),
                env={},
                now=datetime(2026, 5, 18, 10, 30, tzinfo=timezone.utc),
            )

        self.assertEqual(result["verdict"], "NOT_READY")
        self.assertEqual(result["gates"][0]["status"], "FAIL")


if __name__ == "__main__":
    unittest.main()
