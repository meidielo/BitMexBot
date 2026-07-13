import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import ccxt

import order_manager
from instrument import XBTUSDTInstrument
from trade_ledger import get_intent


def instrument():
    return XBTUSDTInstrument.from_ccxt_market(
        {
            "id": "XBTUSDT",
            "symbol": "BTC/USDT:USDT",
            "type": "swap",
            "contract": True,
            "linear": True,
            "inverse": False,
            "active": True,
            "settle": "USDT",
            "contractSize": 0.000001,
            "maker": 0.0005,
            "taker": 0.0005,
            "initialMargin": 0.01,
            "maintenanceMargin": 0.005,
            "precision": {"amount": 100, "price": 0.1},
            "info": {
                "lotSize": "100",
                "tickSize": "0.1",
                "makerFee": "0.0005",
                "takerFee": "0.0005",
                "initMargin": "0.01",
                "maintMargin": "0.005",
            },
        }
    )


def corrupt_client_order_id(order, mode):
    if mode == "missing":
        order.pop("clientOrderId", None)
        order["info"].pop("clOrdID", None)
    elif mode == "mismatched":
        wrong = "00000000-0000-5000-8000-000000000099"
        order["clientOrderId"] = wrong
        order["info"]["clOrdID"] = wrong
    else:
        raise AssertionError(f"unsupported corruption mode {mode}")
    return order


class FakeExchange:
    def __init__(self, entry_mode="full", stop_fail=False, target_fail=False):
        self.id = "bitmex"
        self.urls = {
            "api": {
                "public": "https://testnet.bitmex.com",
                "private": "https://testnet.bitmex.com",
            }
        }
        self.xbtusdt_instrument = instrument()
        self.entry_mode = entry_mode
        self.stop_fail = stop_fail
        self.target_fail = target_fail
        self.orders = {}
        self.calls = []
        self.position_contracts = 0
        self.next_id = 1
        self.leverage = None
        self.position_side = None
        self.hidden_client_id = None
        self.hidden_fetches_remaining = 0
        self.unmanaged_open_orders = []

    def fetch_positions(self, symbols=None):
        signed = self.position_contracts
        side = self.position_side if signed else None
        native_qty = signed if side != "short" else -signed
        return [
            {
                "symbol": "BTC/USDT:USDT" if not symbols else symbols[0],
                "contracts": signed,
                "side": side,
                "info": {
                    "symbol": "XBTUSDT",
                    "currentQty": native_qty,
                    "isOpen": signed > 0,
                    "strategy": "OneWay",
                },
            }
        ]

    def fetch_open_orders(self):
        return [
            dict(order)
            for order in [*self.unmanaged_open_orders, *self.orders.values()]
            if order.get("status") == "open"
        ]

    def set_leverage(self, leverage, symbol):
        self.calls.append(("set_leverage", leverage, symbol))
        self.leverage = leverage

    def fetch_leverage(self, symbol):
        return {
            "symbol": symbol,
            "longLeverage": self.leverage,
            "shortLeverage": self.leverage,
        }

    def fetch_order_book(self, symbol, limit=1):
        return {"bids": [[59999.9, 10_000]], "asks": [[60000.0, 10_000]]}

    def fetch_orders(self, symbol=None, params=None):
        client_id = params["filter"]["clOrdID"]
        if (
            client_id == self.hidden_client_id
            and self.hidden_fetches_remaining > 0
        ):
            self.hidden_fetches_remaining -= 1
            return []
        return [
            dict(order)
            for order in self.orders.values()
            if order.get("clientOrderId") == client_id
        ]

    def _store(self, order):
        self.orders[order["id"]] = dict(order)
        return dict(order)

    def _order(
        self,
        *,
        order_id,
        client_id,
        order_type,
        side,
        amount,
        status,
        filled,
        remaining,
        price=None,
        stop_price=None,
        time_in_force="GoodTillCancel",
        exec_inst="",
        average=None,
        link_id=None,
        contingency_type=None,
    ):
        native_status = {
            "open": "New",
            "closed": "Filled",
            "canceled": "Canceled",
        }.get(status, status)
        return {
            "id": order_id,
            "clientOrderId": client_id,
            "symbol": "BTC/USDT:USDT",
            "type": order_type.lower(),
            "side": side,
            "amount": amount,
            "price": price,
            "triggerPrice": stop_price,
            "reduceOnly": "close" in exec_inst.lower()
            or "reduceonly" in exec_inst.lower(),
            "timeInForce": time_in_force,
            "status": status,
            "filled": filled,
            "remaining": remaining,
            "average": average,
            "info": {
                "orderID": order_id,
                "clOrdID": client_id,
                "symbol": "XBTUSDT",
                "side": side.title(),
                "orderQty": amount,
                "ordType": order_type,
                "price": price,
                "stopPx": stop_price,
                "timeInForce": time_in_force,
                "execInst": exec_inst,
                "clOrdLinkID": link_id,
                "contingencyType": contingency_type,
                "ordStatus": native_status,
                "cumQty": filled,
                "leavesQty": remaining,
                "avgPx": average,
            },
        }

    def create_order(
        self,
        symbol,
        order_type,
        side,
        amount,
        price=None,
        params=None,
    ):
        params = dict(params or {})
        self.calls.append(
            ("create_order", symbol, order_type, side, amount, price, params)
        )
        client_id = params["clientOrderId"]
        order_id = f"order-{self.next_id}"
        self.next_id += 1

        if params.get("timeInForce") == "ImmediateOrCancel":
            if self.entry_mode == "timeout":
                raise TimeoutError("ambiguous submit timeout")
            if self.entry_mode == "zero":
                filled, remaining, status = 0, amount, "canceled"
                average = None
            elif self.entry_mode == "open_zero":
                filled, remaining, status = 0, amount, "open"
                average = None
            elif self.entry_mode == "partial":
                filled, remaining, status = amount // 2, amount - amount // 2, "open"
                average = 60000.0
                self.position_contracts = filled
                self.position_side = "long" if side == "buy" else "short"
            else:
                filled, remaining, status = amount, 0, "closed"
                average = None if self.entry_mode == "missing_average" else 60000.0
                self.position_contracts = filled
                self.position_side = "long" if side == "buy" else "short"
            stored = self._store(
                self._order(
                    order_id=order_id,
                    client_id=client_id,
                    order_type="Limit",
                    side=side,
                    amount=amount,
                    status=status,
                    filled=filled,
                    remaining=remaining,
                    price=price,
                    time_in_force="ImmediateOrCancel",
                    average=average,
                )
            )
            if self.entry_mode == "timeout_delayed_fill":
                self.hidden_client_id = client_id
                self.hidden_fetches_remaining = 2
                raise TimeoutError("response lost before order became visible")
            return stored

        if "triggerPrice" in params:
            if self.stop_fail:
                raise ccxt.InvalidOrder("stop rejected")
            return self._store(
                self._order(
                    order_id=order_id,
                    client_id=client_id,
                    order_type="Stop",
                    side=side,
                    amount=amount,
                    status="open",
                    filled=0,
                    remaining=amount,
                    stop_price=params["triggerPrice"],
                    exec_inst=params.get("execInst", ""),
                    link_id=params.get("clOrdLinkID"),
                    contingency_type=params.get("contingencyType"),
                )
            )

        if order_type == "limit":
            if self.target_fail:
                raise ccxt.InvalidOrder("target rejected")
            return self._store(
                self._order(
                    order_id=order_id,
                    client_id=client_id,
                    order_type="Limit",
                    side=side,
                    amount=amount,
                    status="open",
                    filled=0,
                    remaining=amount,
                    price=price,
                    exec_inst="ReduceOnly" if params.get("reduceOnly") else "",
                    link_id=params.get("clOrdLinkID"),
                    contingency_type=params.get("contingencyType"),
                )
            )

        self.position_contracts = 0
        self.position_side = None
        return self._store(
            self._order(
                order_id=order_id,
                client_id=client_id,
                order_type="Market",
                side=side,
                amount=amount,
                status="closed",
                filled=amount,
                remaining=0,
                exec_inst=params.get("execInst", ""),
                average=60000.0,
            )
        )

    def fetch_order(self, order_id, symbol):
        return dict(self.orders[order_id])

    def cancel_order(self, order_id, symbol):
        self.calls.append(("cancel_order", order_id, symbol))
        order = self.orders[order_id]
        order["status"] = "canceled"
        order["info"]["ordStatus"] = "Canceled"
        return dict(order)


class OrderManagerTests(unittest.TestCase):
    now_ms = 1_800_001
    decision_key = "BTC/USDT:USDT|15m|1800000|LONG"

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp.name) / "trades_v2.db")
        self.signal = {
            "signal": "LONG",
            "entry_price": 60000.0,
            "sl_price": 59400.0,
            "tp_price": 60900.0,
        }
        self.risk = {
            "approved": True,
            "reason": "approved",
            "position_size_contracts": 1000,
            "position_size_btc": 0.001,
            "notional_usdt": 60.0,
            "expected_max_loss_usdt": 0.8385,
            "raw_stop_loss_usdt": 0.6,
            "estimated_costs_usdt": 0.2385,
            "entry_slippage_bps": 10.0,
            "stop_slippage_bps": 20.0,
            "taker_fee_rate": 0.0005,
            "leverage": 1,
        }
        self.env = patch.dict(os.environ, {"BITMEX_TESTNET": "true"})
        self.env.start()
        self.delay = patch.object(order_manager, "RECONCILE_DELAY_SECONDS", 0)
        self.delay.start()

    def tearDown(self):
        self.delay.stop()
        self.env.stop()
        self.temp.cleanup()

    def execute(self, exchange):
        return order_manager.execute_signal(
            self.signal,
            self.risk,
            decision_key=self.decision_key,
            now_ms=self.now_ms,
            exchange=exchange,
            ledger_path=self.db_path,
        )

    def test_rejects_non_testnet_before_exchange_use(self):
        exchange = FakeExchange()
        with patch.dict(os.environ, {"BITMEX_TESTNET": "false"}, clear=True):
            with self.assertRaises(EnvironmentError):
                self.execute(exchange)
        self.assertEqual(exchange.calls, [])

    def test_full_fill_is_durably_protected(self):
        exchange = FakeExchange()

        result = self.execute(exchange)

        self.assertEqual(result["status"], "placed")
        self.assertEqual(result["filled_contracts"], 1000)
        row = get_intent(self.decision_key, self.db_path)
        self.assertEqual(row["status"], "PROTECTED")
        self.assertEqual(row["filled_contracts"], 1000)
        self.assertIsNotNone(row["stop_order_id"])
        self.assertIsNotNone(row["target_order_id"])
        entry_call = [
            call for call in exchange.calls
            if call[0] == "create_order"
            and call[6].get("timeInForce") == "ImmediateOrCancel"
        ][0]
        self.assertEqual(entry_call[4], 1000)
        self.assertLessEqual(entry_call[5], 60060.0)
        protective_calls = [
            call
            for call in exchange.calls
            if call[0] == "create_order"
            and call[6].get("timeInForce") != "ImmediateOrCancel"
        ]
        self.assertEqual(len(protective_calls), 2)
        oco_links = {call[6].get("clOrdLinkID") for call in protective_calls}
        self.assertEqual(len(oco_links), 1)
        self.assertNotIn(None, oco_links)
        self.assertEqual(
            {call[6].get("contingencyType") for call in protective_calls},
            {"OneCancelsTheOther"},
        )

    def test_partial_ioc_cancels_remainder_and_protects_only_fill(self):
        exchange = FakeExchange(entry_mode="partial")

        result = self.execute(exchange)

        self.assertEqual(result["status"], "placed")
        self.assertEqual(result["filled_contracts"], 500)
        row = get_intent(self.decision_key, self.db_path)
        self.assertEqual(row["status"], "PROTECTED")
        self.assertEqual(row["filled_contracts"], 500)
        protective_amounts = [
            call[4]
            for call in exchange.calls
            if call[0] == "create_order"
            and call[6].get("timeInForce") != "ImmediateOrCancel"
        ]
        self.assertEqual(protective_amounts, [500, 500])

    def test_zero_fill_records_failed_flat_without_protection(self):
        exchange = FakeExchange(entry_mode="zero")

        result = self.execute(exchange)

        self.assertEqual(result["status"], "no_fill")
        row = get_intent(self.decision_key, self.db_path)
        self.assertEqual(row["status"], "FAILED_FLAT")
        self.assertEqual(len([c for c in exchange.calls if c[0] == "create_order"]), 1)

    def test_open_zero_fill_ioc_is_canceled_before_failed_flat(self):
        exchange = FakeExchange(entry_mode="open_zero")

        result = self.execute(exchange)

        self.assertEqual(result["status"], "no_fill")
        row = get_intent(self.decision_key, self.db_path)
        self.assertEqual(row["status"], "FAILED_FLAT")
        self.assertEqual(
            len([call for call in exchange.calls if call[0] == "cancel_order"]),
            1,
        )
        entry = next(iter(exchange.orders.values()))
        self.assertEqual(entry["status"], "canceled")

    def test_unproven_entry_cancel_remains_pending_and_monitored(self):
        class CancelUnprovenExchange(FakeExchange):
            def cancel_order(self, order_id, symbol):
                self.calls.append(("cancel_order", order_id, symbol))
                raise TimeoutError("cancel response lost")

        exchange = CancelUnprovenExchange(entry_mode="open_zero")

        result = self.execute(exchange)

        self.assertEqual(result["status"], "reconciling")
        row = get_intent(self.decision_key, self.db_path)
        self.assertEqual(row["status"], "ENTRY_PENDING")
        self.assertEqual(exchange.position_contracts, 0)
        self.assertTrue(any(order["status"] == "open" for order in exchange.orders.values()))

    def test_visible_entry_cancel_failure_cannot_mask_partial_fill(self):
        class FillDuringExpectedCancelExchange(FakeExchange):
            def cancel_order(self, order_id, symbol):
                self.calls.append(("cancel_order", order_id, symbol))
                self.position_contracts = 500
                self.position_side = "long"
                order = self.orders[order_id]
                order["filled"] = 500
                order["remaining"] = 500
                order["average"] = 60000.0
                order["info"]["cumQty"] = 500
                order["info"]["leavesQty"] = 500
                order["info"]["avgPx"] = 60000.0
                raise TimeoutError("cancel response lost during partial fill")

        exchange = FillDuringExpectedCancelExchange(entry_mode="open_zero")

        result = self.execute(exchange)

        self.assertEqual(result["status"], "manual_halt")
        self.assertEqual(exchange.position_contracts, 0)
        row = get_intent(self.decision_key, self.db_path)
        self.assertEqual(row["status"], "HALTED_MANUAL")

    def test_anomalous_entry_client_id_is_canceled_by_exchange_order_id(self):
        for mode in ("missing", "mismatched"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                class CorruptEntryIdExchange(FakeExchange):
                    def create_order(self, *args, _mode=mode, **kwargs):
                        order = super().create_order(*args, **kwargs)
                        if order.get("timeInForce") == "ImmediateOrCancel":
                            order = corrupt_client_order_id(order, _mode)
                            self.orders[order["id"]] = order
                        return order

                exchange = CorruptEntryIdExchange(entry_mode="open_zero")
                db_path = str(Path(temp) / "trades_v2.db")

                result = order_manager.execute_signal(
                    self.signal,
                    self.risk,
                    decision_key=self.decision_key,
                    now_ms=self.now_ms,
                    exchange=exchange,
                    ledger_path=db_path,
                )

                self.assertEqual(result["status"], "manual_halt")
                row = get_intent(self.decision_key, db_path)
                self.assertEqual(row["status"], "HALTED_MANUAL")
                self.assertIn(
                    ("cancel_order", "order-1", "BTC/USDT:USDT"),
                    exchange.calls,
                )
                self.assertEqual(exchange.orders["order-1"]["status"], "canceled")

    def test_filled_anomalous_entry_with_bad_accounting_is_closed_immediately(self):
        class FilledCorruptEntryExchange(FakeExchange):
            def create_order(self, *args, **kwargs):
                order = super().create_order(*args, **kwargs)
                if order.get("timeInForce") == "ImmediateOrCancel":
                    order = corrupt_client_order_id(order, "mismatched")
                    self.orders[order["id"]] = order
                return order

        exchange = FilledCorruptEntryExchange(entry_mode="missing_average")

        result = self.execute(exchange)

        self.assertEqual(result["status"], "manual_halt")
        row = get_intent(self.decision_key, self.db_path)
        self.assertEqual(row["status"], "HALTED_MANUAL")
        self.assertEqual(exchange.position_contracts, 0)
        emergency_orders = [
            order
            for order in exchange.orders.values()
            if order["type"] == "market" and order["status"] == "closed"
        ]
        self.assertEqual(len(emergency_orders), 1)

    def test_unexpected_resting_order_blocks_entry_before_leverage(self):
        exchange = FakeExchange()
        exchange.unmanaged_open_orders.append(
            {"id": "manual-order", "status": "open"}
        )

        result = self.execute(exchange)

        self.assertEqual(result["status"], "manual_halt")
        self.assertFalse(any(call[0] == "set_leverage" for call in exchange.calls))
        self.assertFalse(any(call[0] == "create_order" for call in exchange.calls))

    def test_other_instrument_exposure_blocks_entry(self):
        class OtherExposureExchange(FakeExchange):
            def fetch_positions(self, symbols=None):
                if symbols:
                    return super().fetch_positions(symbols)
                return [
                    *super().fetch_positions(),
                    {
                        "symbol": "ETH/USDT:USDT",
                        "contracts": 1,
                        "side": "long",
                        "info": {
                            "symbol": "ETHUSDT",
                            "currentQty": 1,
                            "isOpen": True,
                            "strategy": "OneWay",
                        },
                    },
                ]

        exchange = OtherExposureExchange()

        result = self.execute(exchange)

        self.assertEqual(result["status"], "manual_halt")
        self.assertFalse(any(call[0] == "set_leverage" for call in exchange.calls))
        self.assertFalse(any(call[0] == "create_order" for call in exchange.calls))

    def test_ambiguous_invisible_entry_remains_pending_and_never_retries(self):
        exchange = FakeExchange(entry_mode="timeout")

        result = self.execute(exchange)

        self.assertEqual(result["status"], "reconciling")
        row = get_intent(self.decision_key, self.db_path)
        self.assertEqual(row["status"], "ENTRY_PENDING")
        entry_calls = [
            c
            for c in exchange.calls
            if c[0] == "create_order"
            and c[6].get("timeInForce") == "ImmediateOrCancel"
        ]
        self.assertEqual(len(entry_calls), 1)

    def test_delayed_invisible_fill_is_closed_before_order_visibility(self):
        exchange = FakeExchange(entry_mode="timeout_delayed_fill")

        result = self.execute(exchange)

        self.assertEqual(result["status"], "manual_halt")
        self.assertEqual(exchange.position_contracts, 0)
        row = get_intent(self.decision_key, self.db_path)
        self.assertEqual(row["status"], "HALTED_MANUAL")
        self.assertEqual(row["filled_contracts"], 0)
        self.assertIn("without a visible expected order", row["halt_reason"])
        entry_calls = [
            call
            for call in exchange.calls
            if call[0] == "create_order"
            and call[6].get("timeInForce") == "ImmediateOrCancel"
        ]
        self.assertEqual(len(entry_calls), 1)

    def test_fill_without_average_is_emergency_closed(self):
        exchange = FakeExchange(entry_mode="missing_average")

        result = self.execute(exchange)

        self.assertEqual(result["status"], "manual_halt")
        self.assertEqual(exchange.position_contracts, 0)
        row = get_intent(self.decision_key, self.db_path)
        self.assertEqual(row["status"], "HALTED_MANUAL")
        self.assertIn("repeatedly verified account flat", row["halt_reason"])

    def test_stop_rejection_uses_close_and_halts_for_reconciliation(self):
        exchange = FakeExchange(stop_fail=True)

        result = self.execute(exchange)

        self.assertEqual(result["status"], "manual_halt")
        self.assertEqual(exchange.position_contracts, 0)
        row = get_intent(self.decision_key, self.db_path)
        self.assertEqual(row["status"], "HALTED_MANUAL")
        self.assertIsNotNone(row["exit_order_id"])
        self.assertIn("repeatedly verified account flat", row["halt_reason"])

    def test_target_rejection_keeps_stop_and_records_no_tp_state(self):
        exchange = FakeExchange(target_fail=True)

        result = self.execute(exchange)

        self.assertEqual(result["status"], "protected_no_tp")
        row = get_intent(self.decision_key, self.db_path)
        self.assertEqual(row["status"], "PROTECTED_NO_TP")
        self.assertIsNotNone(row["stop_order_id"])
        self.assertIsNone(row["target_order_id"])

    def test_duplicate_completed_candle_does_not_submit_again(self):
        exchange = FakeExchange()
        first = self.execute(exchange)
        creates_after_first = len([c for c in exchange.calls if c[0] == "create_order"])

        second = self.execute(exchange)

        self.assertEqual(first["status"], "placed")
        self.assertEqual(second["status"], "already_protected")
        self.assertEqual(
            len([c for c in exchange.calls if c[0] == "create_order"]),
            creates_after_first,
        )

    def test_rejects_risk_unit_mismatch_before_order(self):
        exchange = FakeExchange()
        self.risk["position_size_btc"] = 1000

        result = self.execute(exchange)

        self.assertEqual(result["status"], "failed")
        self.assertIn("instrument units", result["error"])
        self.assertEqual(exchange.calls, [])

    def test_rejects_mainnet_endpoint_even_when_env_says_testnet(self):
        exchange = FakeExchange()
        exchange.urls["api"] = {
            "public": "https://www.bitmex.com",
            "private": "https://www.bitmex.com",
        }

        result = self.execute(exchange)

        self.assertEqual(result["status"], "failed")
        self.assertIn("not exactly BitMEX Testnet", result["error"])
        self.assertEqual(exchange.calls, [])

    def test_entry_limit_is_bounded_to_signal_as_well_as_book(self):
        with self.assertRaises(order_manager.OrderExecutionError):
            order_manager._bounded_entry_limit(
                side="buy",
                signal_entry=order_manager.Decimal("60000"),
                best_bid="60099.9",
                best_ask="60100.0",
                tick_size=order_manager.Decimal("0.1"),
            )
        self.assertEqual(
            order_manager._bounded_entry_limit(
                side="buy",
                signal_entry=order_manager.Decimal("60000"),
                best_bid="59999.9",
                best_ask="60000.0",
                tick_size=order_manager.Decimal("0.1"),
            ),
            order_manager.Decimal("60060.0"),
        )

    def test_leverage_verification_rejects_fraction_bool_nan_and_mismatch(self):
        exchange = FakeExchange()
        invalid = (
            {"longLeverage": 1.9, "shortLeverage": 1},
            {"longLeverage": True, "shortLeverage": 1},
            {"longLeverage": float("nan"), "shortLeverage": 1},
            {"longLeverage": 1, "shortLeverage": 2},
            {"longLeverage": 1},
        )
        for response in invalid:
            with self.subTest(response=response):
                exchange.fetch_leverage = lambda symbol, value=response: value
                with self.assertRaises(order_manager.OrderExecutionError):
                    order_manager._set_and_verify_leverage(exchange, 1)

    def test_wrong_stop_semantics_never_become_protected(self):
        class WrongStopExchange(FakeExchange):
            def create_order(self, *args, **kwargs):
                order = super().create_order(*args, **kwargs)
                if order.get("type") == "stop":
                    order["info"]["execInst"] = "Close,LastPrice"
                    self.orders[order["id"]] = order
                return order

        exchange = WrongStopExchange()

        result = self.execute(exchange)

        self.assertEqual(result["status"], "manual_halt")
        row = get_intent(self.decision_key, self.db_path)
        self.assertEqual(row["status"], "HALTED_MANUAL")
        self.assertIsNone(row["stop_order_id"])
        self.assertEqual(exchange.position_contracts, 0)
        stop_order = next(
            order for order in exchange.orders.values() if order["type"] == "stop"
        )
        self.assertEqual(stop_order["status"], "canceled")

    def test_anomalous_stop_client_id_is_canceled_by_exchange_order_id(self):
        for mode in ("missing", "mismatched"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                class CorruptStopIdExchange(FakeExchange):
                    def create_order(self, *args, _mode=mode, **kwargs):
                        order = super().create_order(*args, **kwargs)
                        if order.get("type") == "stop":
                            order = corrupt_client_order_id(order, _mode)
                            self.orders[order["id"]] = order
                        return order

                exchange = CorruptStopIdExchange()
                db_path = str(Path(temp) / "trades_v2.db")

                result = order_manager.execute_signal(
                    self.signal,
                    self.risk,
                    decision_key=self.decision_key,
                    now_ms=self.now_ms,
                    exchange=exchange,
                    ledger_path=db_path,
                )

                self.assertEqual(result["status"], "manual_halt")
                self.assertIn(
                    ("cancel_order", "order-2", "BTC/USDT:USDT"),
                    exchange.calls,
                )
                self.assertEqual(exchange.orders["order-2"]["status"], "canceled")

    def test_wrong_target_semantics_stays_stop_only(self):
        class WrongTargetExchange(FakeExchange):
            def create_order(self, *args, **kwargs):
                order = super().create_order(*args, **kwargs)
                if (
                    order.get("type") == "limit"
                    and order.get("timeInForce") == "GoodTillCancel"
                ):
                    order["info"]["execInst"] = ""
                    order["reduceOnly"] = False
                    self.orders[order["id"]] = order
                return order

        exchange = WrongTargetExchange()

        result = self.execute(exchange)

        self.assertEqual(result["status"], "protected_no_tp")
        row = get_intent(self.decision_key, self.db_path)
        self.assertEqual(row["status"], "PROTECTED_NO_TP")
        self.assertIsNotNone(row["stop_order_id"])
        self.assertIsNone(row["target_order_id"])
        target_order = next(
            order
            for order in exchange.orders.values()
            if order["type"] == "limit"
            and order["timeInForce"] == "GoodTillCancel"
        )
        self.assertEqual(target_order["status"], "canceled")
        stop_order = exchange.orders[row["stop_order_id"]]
        self.assertEqual(stop_order["status"], "open")

    def test_anomalous_target_client_id_is_canceled_by_exchange_order_id(self):
        for mode in ("missing", "mismatched"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                class CorruptTargetIdExchange(FakeExchange):
                    def create_order(self, *args, _mode=mode, **kwargs):
                        order = super().create_order(*args, **kwargs)
                        if (
                            order.get("type") == "limit"
                            and order.get("timeInForce") == "GoodTillCancel"
                        ):
                            order = corrupt_client_order_id(order, _mode)
                            self.orders[order["id"]] = order
                        return order

                exchange = CorruptTargetIdExchange()
                db_path = str(Path(temp) / "trades_v2.db")

                result = order_manager.execute_signal(
                    self.signal,
                    self.risk,
                    decision_key=self.decision_key,
                    now_ms=self.now_ms,
                    exchange=exchange,
                    ledger_path=db_path,
                )

                self.assertEqual(result["status"], "protected_no_tp")
                self.assertIn(
                    ("cancel_order", "order-3", "BTC/USDT:USDT"),
                    exchange.calls,
                )
                self.assertEqual(exchange.orders["order-3"]["status"], "canceled")

    def test_unproven_anomalous_target_cancel_forces_manual_halt(self):
        class UnprovenAnomalousTargetExchange(FakeExchange):
            def create_order(self, *args, **kwargs):
                order = super().create_order(*args, **kwargs)
                if (
                    order.get("type") == "limit"
                    and order.get("timeInForce") == "GoodTillCancel"
                ):
                    order = corrupt_client_order_id(order, "missing")
                    self.orders[order["id"]] = order
                return order

            def cancel_order(self, order_id, symbol):
                if order_id == "order-3":
                    self.calls.append(("cancel_order", order_id, symbol))
                    raise TimeoutError("cancel response lost")
                return super().cancel_order(order_id, symbol)

        exchange = UnprovenAnomalousTargetExchange()

        result = self.execute(exchange)

        self.assertEqual(result["status"], "manual_halt")
        row = get_intent(self.decision_key, self.db_path)
        self.assertEqual(row["status"], "HALTED_MANUAL")

    def test_target_cleanup_that_cancels_oco_stop_flattens_and_halts(self):
        class OcoCancelExchange(FakeExchange):
            def create_order(self, *args, **kwargs):
                order = super().create_order(*args, **kwargs)
                if (
                    order.get("type") == "limit"
                    and order.get("timeInForce") == "GoodTillCancel"
                ):
                    order["info"]["execInst"] = ""
                    order["reduceOnly"] = False
                    self.orders[order["id"]] = order
                return order

            def cancel_order(self, order_id, symbol):
                is_target = self.orders[order_id].get("type") == "limit"
                result = super().cancel_order(order_id, symbol)
                if is_target:
                    for order in self.orders.values():
                        if order.get("type") == "stop":
                            order["status"] = "canceled"
                            order["info"]["ordStatus"] = "Canceled"
                return result

        exchange = OcoCancelExchange()

        result = self.execute(exchange)

        self.assertEqual(result["status"], "manual_halt")
        row = get_intent(self.decision_key, self.db_path)
        self.assertEqual(row["status"], "HALTED_MANUAL")
        self.assertEqual(exchange.position_contracts, 0)

    def test_unproven_bad_target_cleanup_never_claims_stop_only_safety(self):
        class UnprovenTargetCancelExchange(FakeExchange):
            def create_order(self, *args, **kwargs):
                order = super().create_order(*args, **kwargs)
                if (
                    order.get("type") == "limit"
                    and order.get("timeInForce") == "GoodTillCancel"
                ):
                    order["info"]["execInst"] = ""
                    order["reduceOnly"] = False
                    self.orders[order["id"]] = order
                return order

            def cancel_order(self, order_id, symbol):
                order = self.orders[order_id]
                if (
                    order.get("type") == "limit"
                    and order.get("timeInForce") == "GoodTillCancel"
                ):
                    self.calls.append(("cancel_order", order_id, symbol))
                    raise TimeoutError("cancel response lost")
                return super().cancel_order(order_id, symbol)

        exchange = UnprovenTargetCancelExchange()

        result = self.execute(exchange)

        self.assertEqual(result["status"], "manual_halt")
        row = get_intent(self.decision_key, self.db_path)
        self.assertEqual(row["status"], "HALTED_MANUAL")
        self.assertEqual(exchange.position_contracts, 0)

    def test_restart_reconciles_pending_fill_then_closes_without_replay(self):
        exchange = FakeExchange(entry_mode="timeout")
        first = self.execute(exchange)
        self.assertEqual(first["status"], "reconciling")
        row = get_intent(self.decision_key, self.db_path)

        exchange.entry_mode = "full"
        exchange.create_order(
            "BTC/USDT:USDT",
            "limit",
            "buy",
            1000,
            60060.0,
            {
                "timeInForce": "ImmediateOrCancel",
                "clientOrderId": row["entry_client_order_id"],
            },
        )
        before = len(
            [
                call
                for call in exchange.calls
                if call[0] == "create_order"
                and call[6].get("timeInForce") == "ImmediateOrCancel"
            ]
        )

        outcome = order_manager.reconcile_open_intent(
            exchange,
            row,
            ledger_path=self.db_path,
        )

        self.assertEqual(outcome["status"], "manual_halt")
        self.assertEqual(exchange.position_contracts, 0)
        updated = get_intent(self.decision_key, self.db_path)
        self.assertEqual(updated["status"], "HALTED_MANUAL")
        self.assertEqual(updated["filled_contracts"], 1000)
        after = len(
            [
                call
                for call in exchange.calls
                if call[0] == "create_order"
                and call[6].get("timeInForce") == "ImmediateOrCancel"
            ]
        )
        self.assertEqual(after, before)

    def test_restart_detects_invisible_late_fill_and_closes_immediately(self):
        exchange = FakeExchange(entry_mode="timeout")
        first = self.execute(exchange)
        self.assertEqual(first["status"], "reconciling")
        row = get_intent(self.decision_key, self.db_path)

        exchange.position_contracts = 1000
        exchange.position_side = "long"
        outcome = order_manager.reconcile_open_intent(
            exchange,
            row,
            ledger_path=self.db_path,
        )

        self.assertEqual(outcome["status"], "manual_halt")
        self.assertEqual(exchange.position_contracts, 0)
        updated = get_intent(self.decision_key, self.db_path)
        self.assertEqual(updated["status"], "HALTED_MANUAL")
        self.assertTrue(
            any(
                call[0] == "create_order"
                and call[2] == "market"
                and call[6].get("execInst") == "Close"
                for call in exchange.calls
            )
        )

    def test_restart_cancels_unexpected_wrong_id_pending_order_and_halts(self):
        exchange = FakeExchange(entry_mode="timeout")
        first = self.execute(exchange)
        self.assertEqual(first["status"], "reconciling")
        row = get_intent(self.decision_key, self.db_path)
        unexpected = exchange._order(
            order_id="unexpected-order",
            client_id="00000000-0000-5000-8000-000000000099",
            order_type="Limit",
            side="buy",
            amount=1000,
            status="open",
            filled=0,
            remaining=1000,
            price=60060.0,
            time_in_force="ImmediateOrCancel",
        )
        exchange._store(unexpected)

        outcome = order_manager.reconcile_open_intent(
            exchange,
            row,
            ledger_path=self.db_path,
        )

        self.assertEqual(outcome["status"], "manual_halt")
        self.assertEqual(exchange.orders["unexpected-order"]["status"], "canceled")
        self.assertIn(
            ("cancel_order", "unexpected-order", "BTC/USDT:USDT"),
            exchange.calls,
        )
        updated = get_intent(self.decision_key, self.db_path)
        self.assertEqual(updated["status"], "HALTED_MANUAL")

    def test_cancel_failure_cannot_starve_late_fill_detection(self):
        class FillDuringCancelExchange(FakeExchange):
            def cancel_order(self, order_id, symbol):
                self.calls.append(("cancel_order", order_id, symbol))
                self.position_contracts = 500
                self.position_side = "long"
                raise TimeoutError("cancel response lost during fill")

        exchange = FillDuringCancelExchange(entry_mode="timeout")
        first = self.execute(exchange)
        self.assertEqual(first["status"], "reconciling")
        row = get_intent(self.decision_key, self.db_path)
        unexpected = exchange._order(
            order_id="unexpected-order",
            client_id="00000000-0000-5000-8000-000000000099",
            order_type="Limit",
            side="buy",
            amount=1000,
            status="open",
            filled=500,
            remaining=500,
            price=60060.0,
            time_in_force="ImmediateOrCancel",
            average=60000.0,
        )
        exchange._store(unexpected)

        outcome = order_manager.reconcile_open_intent(
            exchange,
            row,
            ledger_path=self.db_path,
        )

        self.assertEqual(outcome["status"], "manual_halt")
        self.assertEqual(exchange.position_contracts, 0)
        updated = get_intent(self.decision_key, self.db_path)
        self.assertEqual(updated["status"], "HALTED_MANUAL")

    def test_cancel_failure_cannot_mask_other_instrument_exposure(self):
        class OtherExposureDuringCancelExchange(FakeExchange):
            other_exposure = False

            def fetch_positions(self, symbols=None):
                positions = super().fetch_positions(symbols)
                if symbols or not self.other_exposure:
                    return positions
                return [
                    *positions,
                    {
                        "symbol": "ETH/USDT:USDT",
                        "contracts": 1,
                        "side": "long",
                        "info": {
                            "symbol": "ETHUSDT",
                            "currentQty": 1,
                            "isOpen": True,
                            "strategy": "OneWay",
                        },
                    },
                ]

            def cancel_order(self, order_id, symbol):
                self.calls.append(("cancel_order", order_id, symbol))
                raise TimeoutError("cancel response lost")

        exchange = OtherExposureDuringCancelExchange(entry_mode="timeout")
        first = self.execute(exchange)
        self.assertEqual(first["status"], "reconciling")
        row = get_intent(self.decision_key, self.db_path)
        exchange.other_exposure = True
        unexpected = exchange._order(
            order_id="unexpected-order",
            client_id="00000000-0000-5000-8000-000000000099",
            order_type="Limit",
            side="buy",
            amount=1000,
            status="open",
            filled=0,
            remaining=1000,
            price=60060.0,
            time_in_force="ImmediateOrCancel",
        )
        exchange._store(unexpected)

        outcome = order_manager.reconcile_open_intent(
            exchange,
            row,
            ledger_path=self.db_path,
        )

        self.assertEqual(outcome["status"], "manual_halt")
        updated = get_intent(self.decision_key, self.db_path)
        self.assertEqual(updated["status"], "HALTED_MANUAL")
        self.assertIn("non-XBTUSDT", updated["halt_reason"])

    def test_restart_reverifies_existing_protection(self):
        exchange = FakeExchange()
        self.assertEqual(self.execute(exchange)["status"], "placed")
        row = get_intent(self.decision_key, self.db_path)

        outcome = order_manager.reconcile_open_intent(
            exchange,
            row,
            ledger_path=self.db_path,
        )

        self.assertEqual(outcome["status"], "managed_position")
        self.assertEqual(
            get_intent(self.decision_key, self.db_path)["status"],
            "PROTECTED",
        )

    def test_restart_missing_stop_flattens_and_halts(self):
        exchange = FakeExchange()
        self.assertEqual(self.execute(exchange)["status"], "placed")
        row = get_intent(self.decision_key, self.db_path)
        del exchange.orders[row["stop_order_id"]]

        outcome = order_manager.reconcile_open_intent(
            exchange,
            row,
            ledger_path=self.db_path,
        )

        self.assertEqual(outcome["status"], "manual_halt")
        self.assertEqual(exchange.position_contracts, 0)
        self.assertEqual(
            get_intent(self.decision_key, self.db_path)["status"],
            "HALTED_MANUAL",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
