"""
test_v4_recovery.py — V4 Crash Recovery Tests

Tests the _recover_state() function for all state machine transitions:
  - POSITION_OPEN with position still open → stays POSITION_OPEN
  - POSITION_OPEN with position closed during downtime → transitions to COOLDOWN
  - COOLDOWN expired during downtime → transitions to IDLE
  - ARMED → stays ARMED (main loop retries execution)
"""

import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

# Patch STATE_DB_PATH before importing v4_execution
_temp_dir = tempfile.mkdtemp()
_test_db = os.path.join(_temp_dir, "test_v4_state.db")

import v4_execution
v4_execution.STATE_DB_PATH = _test_db


class TestV4Recovery(unittest.TestCase):
    """Test crash recovery for all V4 state machine states."""

    def setUp(self):
        """Create a fresh state DB for each test."""
        # Remove old test DB
        if os.path.exists(_test_db):
            os.remove(_test_db)
        self.conn = v4_execution._init_state_db()

    def tearDown(self):
        self.conn.close()
        if os.path.exists(_test_db):
            os.remove(_test_db)

    def _set_state(self, **kwargs):
        """Helper to set state fields."""
        v4_execution._save_state(self.conn, **kwargs)

    def _get_state(self) -> dict:
        return v4_execution._load_state(self.conn)

    # ------------------------------------------------------------------
    # IDLE — no recovery needed
    # ------------------------------------------------------------------
    def test_recover_idle_does_nothing(self):
        self._set_state(state=v4_execution.State.IDLE)
        state = self._get_state()
        v4_execution._recover_state(self.conn, state)
        self.assertEqual(self._get_state()["state"], v4_execution.State.IDLE)

    # ------------------------------------------------------------------
    # COOLDOWN — expired during downtime → IDLE
    # ------------------------------------------------------------------
    def test_recover_cooldown_expired(self):
        past = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        self._set_state(state=v4_execution.State.COOLDOWN, cooldown_until=past)

        state = self._get_state()
        v4_execution._recover_state(self.conn, state)

        self.assertEqual(self._get_state()["state"], v4_execution.State.IDLE)

    def test_recover_cooldown_not_expired(self):
        future = (datetime.now(timezone.utc) + timedelta(hours=10)).isoformat()
        self._set_state(state=v4_execution.State.COOLDOWN, cooldown_until=future)

        state = self._get_state()
        v4_execution._recover_state(self.conn, state)

        # Should stay in COOLDOWN
        self.assertEqual(self._get_state()["state"], v4_execution.State.COOLDOWN)

    # ------------------------------------------------------------------
    # ARMED — stays ARMED (main loop will attempt execution)
    # ------------------------------------------------------------------
    def test_recover_armed_stays_armed(self):
        self._set_state(
            state=v4_execution.State.ARMED,
            liq_long_ratio=4.5,
            liq_long_pct=0.72,
            oi_delta_pct=-0.03,
        )

        state = self._get_state()
        v4_execution._recover_state(self.conn, state)

        result = self._get_state()
        self.assertEqual(result["state"], v4_execution.State.ARMED)
        self.assertAlmostEqual(result["liq_long_ratio"], 4.5)

    # ------------------------------------------------------------------
    # POSITION_OPEN — position still open on exchange
    # ------------------------------------------------------------------
    @patch("v4_execution.get_client")
    def test_recover_position_open_still_open(self, mock_get_client):
        mock_exchange = MagicMock()
        mock_exchange.fetch_positions.return_value = [
            {"contracts": 100, "symbol": "BTC/USDT:USDT"}
        ]
        mock_get_client.return_value = mock_exchange

        self._set_state(
            state=v4_execution.State.POSITION_OPEN,
            order_id="test-123",
            entry_price=85000.0,
            sl_price=83000.0,
            tp_price=89000.0,
        )

        state = self._get_state()
        v4_execution._recover_state(self.conn, state)

        # Should stay POSITION_OPEN
        self.assertEqual(self._get_state()["state"], v4_execution.State.POSITION_OPEN)

    # ------------------------------------------------------------------
    # POSITION_OPEN — position closed during downtime
    # ------------------------------------------------------------------
    @patch("v4_execution.update_trade_exit")
    @patch("v4_execution.get_client")
    def test_recover_position_closed_during_downtime(self, mock_get_client,
                                                      mock_update_exit):
        mock_exchange = MagicMock()
        # No open positions — position was closed
        mock_exchange.fetch_positions.return_value = []
        # SL order was triggered
        mock_exchange.fetch_order.return_value = {
            "status": "closed",
            "average": 83000.0,
        }
        mock_exchange.cancel_all_orders.return_value = None
        mock_get_client.return_value = mock_exchange

        self._set_state(
            state=v4_execution.State.POSITION_OPEN,
            order_id="test-456",
            entry_price=85000.0,
            sl_price=83000.0,
            tp_price=89000.0,
            sl_order_id="sl-789",
            tp_order_id="tp-012",
        )

        state = self._get_state()
        # _recover_state calls _check_exit which transitions to COOLDOWN
        v4_execution._recover_state(self.conn, state)

        # Should have transitioned to COOLDOWN
        self.assertEqual(self._get_state()["state"], v4_execution.State.COOLDOWN)

    # ------------------------------------------------------------------
    # POSITION_OPEN — exchange connection fails during recovery
    # ------------------------------------------------------------------
    @patch("v4_execution.get_client")
    def test_recover_position_exchange_error(self, mock_get_client):
        mock_exchange = MagicMock()
        mock_exchange.fetch_positions.side_effect = Exception("Connection timeout")
        mock_get_client.return_value = mock_exchange

        self._set_state(
            state=v4_execution.State.POSITION_OPEN,
            order_id="test-err",
        )

        state = self._get_state()
        # Should not crash — just log the error
        v4_execution._recover_state(self.conn, state)

        # State unchanged (will retry next loop)
        self.assertEqual(self._get_state()["state"], v4_execution.State.POSITION_OPEN)


if __name__ == "__main__":
    unittest.main()
