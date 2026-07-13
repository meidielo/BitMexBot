import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from execution_reconciler import (
    ExitEvidenceAmbiguous,
    ExitEvidencePending,
    attribute_exit,
    fetch_execution_history,
    parse_native_execution,
    reconcile_and_close,
    reconcile_exit_from_exchange,
)
from trade_ledger import (
    get_intent,
    list_execution_events,
    register_intent,
    transition_intent,
)


def trade(
    exec_id,
    order_id,
    client_id,
    side,
    qty,
    price,
    timestamp,
    *,
    commission=100,
    realised_pnl=None,
):
    if realised_pnl is None:
        realised_pnl = -commission
    return {
        "execID": exec_id,
        "orderID": order_id,
        "clOrdID": client_id,
        "clOrdLinkID": "oco-1" if order_id != "entry-order" else None,
        "account": 42,
        "symbol": "XBTUSDT",
        "execType": "Trade",
        "side": side,
        "lastQty": qty,
        "lastPx": price,
        "execComm": commission,
        "execCommCcy": "USDt",
        "currency": "USDT",
        "realisedPnl": realised_pnl,
        "transactTime": timestamp,
    }


def funding(exec_id="funding-1", *, commission=-5000, qty=1000):
    return {
        "execID": exec_id,
        "account": 42,
        "symbol": "XBTUSDT",
        "execType": "Funding",
        "lastQty": qty,
        "execComm": commission,
        "currency": "USDT",
        "realisedPnl": -commission,
        "transactTime": "2026-07-13T08:00:00Z",
    }


def complete_history():
    return [
        trade(
            "entry-1",
            "entry-order",
            "entry-client",
            "Buy",
            400,
            100,
            "2026-07-13T00:01:00Z",
        ),
        trade(
            "entry-2",
            "entry-order",
            "entry-client",
            "Buy",
            600,
            102,
            "2026-07-13T00:02:00Z",
        ),
        funding(),
        trade(
            "exit-1",
            "target-order",
            "target-client",
            "Sell",
            500,
            110,
            "2026-07-13T09:00:00Z",
            realised_pnl=4300,
        ),
        trade(
            "exit-2",
            "target-order",
            "target-client",
            "Sell",
            500,
            112,
            "2026-07-13T09:00:01Z",
            realised_pnl=5300,
        ),
    ]


def intent_values():
    return {
        "decision_key": "decision-1",
        "strategy_version": "test-v1",
        "environment": "testnet",
        "symbol": "BTC/USDT:USDT",
        "side": "LONG",
        "decision_time_utc": "2026-07-13T00:00:00+00:00",
        "signal_entry_price_usdt": 101.2,
        "signal_stop_price_usdt": 90,
        "signal_target_price_usdt": 120,
        "requested_contracts": 1000,
        "contract_size_btc": 0.000001,
        "requested_base_btc": 0.001,
        "requested_notional_usdt": 0.1012,
        "expected_max_loss_usdt": 0.02,
        "unit_model": "xbtusdt-linear-metadata-v1",
        "entry_client_order_id": "entry-client",
        "stop_client_order_id": "stop-client",
        "target_client_order_id": "target-client",
    }


class HistoryExchange:
    id = "bitmex"
    urls = {
        "api": {
            "public": "https://testnet.bitmex.com",
            "private": "https://testnet.bitmex.com",
        }
    }

    def __init__(self, snapshots):
        self.snapshots = snapshots
        self.snapshot_index = 0
        self.calls = []

    def private_get_execution_tradehistory(self, params):
        self.calls.append(dict(params))
        snapshot = self.snapshots[min(self.snapshot_index, len(self.snapshots) - 1)]
        start = params["start"]
        count = params["count"]
        page = snapshot[start : start + count]
        if start == 0:
            self.snapshot_index += 1
        return [dict(row) for row in page]


class ExecutionReconcilerTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {"BITMEX_TESTNET": "true"})
        self.env.start()
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp.name) / "trades_v2.db")

    def tearDown(self):
        self.temp.cleanup()
        self.env.stop()

    def protected_intent(self):
        register_intent(intent_values(), self.db_path)
        transition_intent(
            "decision-1",
            "ENTRY_PENDING",
            expected_statuses={"REGISTERED"},
            updates={"entry_order_id": "entry-order"},
            db_path=self.db_path,
        )
        return transition_intent(
            "decision-1",
            "PROTECTED",
            expected_statuses={"ENTRY_PENDING"},
            updates={
                "filled_contracts": 1000,
                "actual_entry_price_usdt": 101.2,
                "stop_order_id": "stop-order",
                "target_order_id": "target-order",
            },
            db_path=self.db_path,
        )

    def test_funding_uses_signed_native_commission(self):
        credit = parse_native_execution(funding(commission=-5000))
        charge = parse_native_execution(funding("funding-2", commission=2500))

        self.assertEqual(float(credit.funding_usdt), 0.005)
        self.assertEqual(float(charge.funding_usdt), -0.0025)
        self.assertEqual(credit.last_qty, 1000)
        self.assertIsNone(credit.order_id)

    def test_trade_commission_preserves_a_signed_maker_rebate(self):
        maker_fill = parse_native_execution(
            trade(
                "maker-1",
                "target-order",
                "target-client",
                "Sell",
                1000,
                110,
                "2026-07-13T09:00:00Z",
                commission=-500,
            )
        )

        self.assertEqual(float(maker_fill.commission_usdt), -0.0005)

    def test_fetch_history_paginates_and_preserves_partial_fills(self):
        exchange = HistoryExchange([complete_history()])

        events = fetch_execution_history(
            exchange,
            start_time_utc="2026-07-13T00:00:00+00:00",
            page_size=2,
        )

        self.assertEqual(len(events), 5)
        self.assertEqual([call["start"] for call in exchange.calls], [0, 2, 4])
        attributed = attribute_exit(self.protected_intent(), events)
        self.assertEqual(attributed.exit_reason, "take_profit")
        self.assertEqual(
            [event["last_qty"] for event in attributed.events if event["event_role"] == "ENTRY"],
            [400, 600],
        )

    def test_delayed_history_must_be_complete_and_stable(self):
        exchange = HistoryExchange([[], complete_history(), complete_history()])

        attributed = reconcile_exit_from_exchange(
            exchange,
            self.protected_intent(),
            attempts=4,
            confirmations=2,
            delay_seconds=0,
        )

        self.assertEqual(attributed.exit_reason, "take_profit")
        self.assertEqual(exchange.snapshot_index, 3)

    def test_missing_exit_history_fails_closed(self):
        incomplete = complete_history()[:3]
        exchange = HistoryExchange([incomplete])

        with self.assertRaisesRegex(ExitEvidencePending, "not stable"):
            reconcile_exit_from_exchange(
                exchange,
                self.protected_intent(),
                attempts=3,
                confirmations=2,
                delay_seconds=0,
            )

    def test_unattributed_trade_during_lifetime_is_ambiguous(self):
        history = complete_history()
        history.insert(
            3,
            trade(
                "manual-1",
                "manual-order",
                "manual-client",
                "Sell",
                1,
                105,
                "2026-07-13T08:30:00Z",
            ),
        )
        events = tuple(parse_native_execution(row) for row in history)

        with self.assertRaisesRegex(ExitEvidenceAmbiguous, "unattributed"):
            attribute_exit(self.protected_intent(), events)

    def test_matching_only_one_durable_order_identifier_is_ambiguous(self):
        history = complete_history()
        history[0]["clOrdID"] = "wrong-entry-client"
        events = tuple(parse_native_execution(row) for row in history)

        with self.assertRaisesRegex(ExitEvidenceAmbiguous, "identifiers contradict"):
            attribute_exit(self.protected_intent(), events)

    def test_atomic_close_is_restart_idempotent_and_includes_funding(self):
        durable = self.protected_intent()
        exchange = HistoryExchange([complete_history()])

        first = reconcile_and_close(
            exchange,
            durable,
            ledger_path=self.db_path,
            attempts=2,
            delay_seconds=0,
        )
        second = reconcile_and_close(
            exchange,
            durable,
            ledger_path=self.db_path,
            attempts=2,
            delay_seconds=0,
        )

        self.assertEqual(first["status"], "CLOSED")
        self.assertEqual(second["id"], first["id"])
        self.assertAlmostEqual(first["actual_exit_price_usdt"], 111)
        self.assertAlmostEqual(first["fees_usdt"], 0.0004)
        self.assertAlmostEqual(first["funding_usdt"], 0.005)
        self.assertAlmostEqual(first["gross_pnl_usdt"], 0.0098)
        self.assertAlmostEqual(first["net_pnl_usdt"], 0.0144)
        self.assertEqual(first["exit_time_utc"], "2026-07-13T09:00:01+00:00")
        self.assertEqual(len(list_execution_events("decision-1", self.db_path)), 5)
        self.assertEqual(get_intent("decision-1", self.db_path)["status"], "CLOSED")

    def test_signed_maker_rebates_reduce_net_fees(self):
        durable = self.protected_intent()
        history = complete_history()
        history[-2]["execComm"] = -200
        history[-1]["execComm"] = -200
        history[-2]["realisedPnl"] = 4600
        history[-1]["realisedPnl"] = 5600

        closed = reconcile_and_close(
            HistoryExchange([history]),
            durable,
            ledger_path=self.db_path,
            attempts=2,
            delay_seconds=0,
        )

        self.assertAlmostEqual(closed["fees_usdt"], -0.0002)
        self.assertAlmostEqual(closed["net_pnl_usdt"], 0.015)

    def test_native_realised_pnl_mismatch_fails_closed(self):
        history = complete_history()
        history[-1]["realisedPnl"] = -500_000_000

        with self.assertRaisesRegex(ExitEvidenceAmbiguous, "native realised PnL"):
            attribute_exit(
                self.protected_intent(),
                tuple(parse_native_execution(row) for row in history),
            )

    def test_missing_native_realised_pnl_is_ambiguous(self):
        row = complete_history()[0]
        del row["realisedPnl"]

        with self.assertRaisesRegex(ExitEvidenceAmbiguous, "omitted native"):
            parse_native_execution(row)

    def test_duplicate_exec_id_with_changed_source_is_rejected(self):
        history = complete_history()
        changed = dict(history[0])
        changed["lastPx"] = 999
        exchange = HistoryExchange([[history[0], changed]])

        with self.assertRaisesRegex(ExitEvidenceAmbiguous, "contradictory"):
            fetch_execution_history(
                exchange,
                start_time_utc="2026-07-13T00:00:00+00:00",
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
