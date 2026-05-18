import tempfile
import unittest
from pathlib import Path

import pandas as pd

from research_scanner import (
    ALLOWED_ACTIONS,
    ResearchSignal,
    evaluate_funding_watch,
    evaluate_trend_breakout,
    evaluate_vol_spike_reversion,
    init_db,
    insert_run,
    insert_signals,
    scorecard_summary,
    utc_now_iso,
)


def make_candles(n=240, base=100.0, volume=1000.0):
    rows = []
    for i in range(n):
        close = base + i * 0.02
        rows.append(
            {
                "open": close - 0.05,
                "high": close + 0.4,
                "low": close - 0.4,
                "close": close,
                "volume": volume,
            }
        )
    index = pd.date_range("2026-01-01", periods=n, freq="15min", tz="UTC")
    return pd.DataFrame(rows, index=index)


class ResearchScannerTests(unittest.TestCase):
    def test_research_signal_rejects_executable_actions(self):
        with self.assertRaises(ValueError):
            ResearchSignal("test", "BTC/USDT:USDT", "LONG", 1, "bad")
        self.assertEqual(ALLOWED_ACTIONS, {"WATCH_LONG", "WATCH_SHORT", "NO_SIGNAL"})

    def test_trend_breakout_watch_long(self):
        df = make_candles()
        df.iloc[-21:-1, df.columns.get_loc("high")] = 103.0
        df.iloc[-1, df.columns.get_loc("open")] = 103.0
        df.iloc[-1, df.columns.get_loc("high")] = 106.0
        df.iloc[-1, df.columns.get_loc("low")] = 102.5
        df.iloc[-1, df.columns.get_loc("close")] = 105.0
        df.iloc[-1, df.columns.get_loc("volume")] = 1600.0

        result = evaluate_trend_breakout(df, "BTC/USDT:USDT")

        self.assertEqual(result.action, "WATCH_LONG")
        self.assertGreater(result.score, 0)

    def test_trend_breakout_watch_short(self):
        df = make_candles()
        df["close"] = list(reversed([100.0 + i * 0.02 for i in range(len(df))]))
        df["open"] = df["close"] + 0.05
        df["high"] = df["close"] + 0.4
        df["low"] = df["close"] - 0.4
        df.iloc[-21:-1, df.columns.get_loc("low")] = 96.0
        df.iloc[-1, df.columns.get_loc("open")] = 96.5
        df.iloc[-1, df.columns.get_loc("high")] = 96.8
        df.iloc[-1, df.columns.get_loc("low")] = 93.8
        df.iloc[-1, df.columns.get_loc("close")] = 94.5
        df.iloc[-1, df.columns.get_loc("volume")] = 1700.0

        result = evaluate_trend_breakout(df, "BTC/USDT:USDT")

        self.assertEqual(result.action, "WATCH_SHORT")
        self.assertGreater(result.score, 0)

    def test_vol_spike_downside_watch_long(self):
        df = make_candles(n=80)
        df.iloc[-1, df.columns.get_loc("open")] = 100.0
        df.iloc[-1, df.columns.get_loc("high")] = 101.0
        df.iloc[-1, df.columns.get_loc("low")] = 96.0
        df.iloc[-1, df.columns.get_loc("close")] = 96.5
        df.iloc[-1, df.columns.get_loc("volume")] = 4000.0

        result = evaluate_vol_spike_reversion(df, "BTC/USDT:USDT")

        self.assertEqual(result.action, "WATCH_LONG")

    def test_vol_spike_upside_watch_short(self):
        df = make_candles(n=80)
        df.iloc[-1, df.columns.get_loc("open")] = 100.0
        df.iloc[-1, df.columns.get_loc("high")] = 105.0
        df.iloc[-1, df.columns.get_loc("low")] = 99.0
        df.iloc[-1, df.columns.get_loc("close")] = 104.5
        df.iloc[-1, df.columns.get_loc("volume")] = 4000.0

        result = evaluate_vol_spike_reversion(df, "BTC/USDT:USDT")

        self.assertEqual(result.action, "WATCH_SHORT")

    def test_funding_watch_uses_watch_actions_only(self):
        self.assertEqual(evaluate_funding_watch("BTC/USDT:USDT", 0.0004).action, "WATCH_SHORT")
        self.assertEqual(evaluate_funding_watch("BTC/USDT:USDT", -0.0004).action, "WATCH_LONG")
        self.assertEqual(evaluate_funding_watch("BTC/USDT:USDT", 0.0).action, "NO_SIGNAL")

    def test_scorecard_summary_reads_shadow_database(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "research.db")
            conn = init_db(db_path)
            insert_run(conn, "run-1", utc_now_iso(), ["BTC/USDT:USDT"])
            insert_signals(
                conn,
                "run-1",
                utc_now_iso(),
                [
                    ResearchSignal(
                        "funding_extreme_watch",
                        "BTC/USDT:USDT",
                        "WATCH_SHORT",
                        50,
                        "test",
                    )
                ],
            )
            conn.execute(
                "UPDATE research_runs SET completed_at_utc = ?, symbols_scanned = 1 WHERE run_id = ?",
                (utc_now_iso(), "run-1"),
            )
            conn.commit()
            conn.close()

            lines = scorecard_summary(db_path)

        joined = "\n".join(lines)
        self.assertIn("watch signals: 1", joined)
        self.assertIn("WATCH_SHORT", joined)


if __name__ == "__main__":
    unittest.main()
