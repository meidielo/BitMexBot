import unittest

import pandas as pd

from fetch_data import _completed_live_candles, fetch_ohlcv


def candle(timestamp, close=100.0):
    ts = int(pd.Timestamp(timestamp).timestamp() * 1000)
    return [ts, close - 1, close + 2, close - 2, close, 10.0]


class CompletedLiveCandleTests(unittest.TestCase):
    def test_excludes_currently_forming_child_and_parent(self):
        raw = [
            candle("2026-07-13T23:45:00Z"),
            candle("2026-07-13T23:50:00Z"),
            candle("2026-07-13T23:55:00Z"),
            candle("2026-07-14T00:00:00Z"),
            candle("2026-07-14T00:05:00Z"),
            candle("2026-07-14T00:10:00Z"),
            candle("2026-07-14T00:15:00Z"),
        ]

        result = _completed_live_candles(raw, now="2026-07-14T00:16:00Z")

        self.assertEqual(len(result), 2)
        self.assertEqual(result.index[-1], pd.Timestamp("2026-07-14T00:00:00Z"))

    def test_rejects_parent_with_missing_child(self):
        raw = [
            candle("2026-07-14T00:00:00Z"),
            candle("2026-07-14T00:10:00Z"),
            candle("2026-07-14T00:15:00Z"),
            candle("2026-07-14T00:20:00Z"),
            candle("2026-07-14T00:25:00Z"),
        ]

        with self.assertRaisesRegex(ValueError, "missing children"):
            _completed_live_candles(raw, now="2026-07-14T00:31:00Z")

    def test_accepts_parent_at_exact_close(self):
        raw = [
            candle("2026-07-14T00:00:00Z"),
            candle("2026-07-14T00:05:00Z"),
            candle("2026-07-14T00:10:00Z"),
        ]

        result = _completed_live_candles(raw, now="2026-07-14T00:15:00Z")

        self.assertEqual(
            list(result.index),
            [pd.Timestamp("2026-07-14T00:00:00Z")],
        )

    def test_rejects_parent_one_millisecond_before_close(self):
        raw = [
            candle("2026-07-14T00:00:00Z"),
            candle("2026-07-14T00:05:00Z"),
            candle("2026-07-14T00:10:00Z"),
        ]

        with self.assertRaisesRegex(ValueError, "no complete 15m candles"):
            _completed_live_candles(raw, now="2026-07-14T00:14:59.999Z")

    def test_rejects_wholly_missing_parent_between_completed_parents(self):
        raw = [
            candle("2026-07-14T00:00:00Z"),
            candle("2026-07-14T00:05:00Z"),
            candle("2026-07-14T00:10:00Z"),
            candle("2026-07-14T00:30:00Z"),
            candle("2026-07-14T00:35:00Z"),
            candle("2026-07-14T00:40:00Z"),
        ]

        with self.assertRaisesRegex(ValueError, "missing children"):
            _completed_live_candles(raw, now="2026-07-14T00:45:00Z")

    def test_rejects_duplicate_timestamp(self):
        raw = [
            candle("2026-07-14T00:00:00Z"),
            candle("2026-07-14T00:00:00Z"),
            candle("2026-07-14T00:05:00Z"),
            candle("2026-07-14T00:10:00Z"),
        ]
        with self.assertRaisesRegex(ValueError, "duplicate"):
            _completed_live_candles(raw, now="2026-07-14T00:16:00Z")

    def test_fetch_returns_none_on_incomplete_data(self):
        class Exchange:
            def fetch_ohlcv(self, *args, **kwargs):
                return [candle("2099-01-01T00:00:00Z")]

        self.assertIsNone(fetch_ohlcv(Exchange()))


if __name__ == "__main__":
    unittest.main(verbosity=2)
