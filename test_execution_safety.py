"""Tests for the pure execution-safety helpers."""

from decimal import Decimal
import unittest
from uuid import UUID

from execution_safety import (
    AmbiguousCreateUnresolved,
    ExecutionSafetyError,
    ReconciliationError,
    bounded_ioc_limit_price,
    build_completed_candle_decision_key,
    classify_order,
    client_order_id,
    client_order_ids,
    reconciliation_client_order_id,
    create_order_once,
    find_order_by_client_id,
    validate_completed_candle_decision_key,
)


VALID_CLIENT_ID = "12345678-1234-5678-1234-567812345678"


class FakeExchange:
    def __init__(self, responses):
        self.responses = list(responses)
        self.fetch_calls = []

    def fetch_orders(self, *args, **kwargs):
        self.fetch_calls.append((args, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


class CompletedCandleDecisionTests(unittest.TestCase):
    close_ms = 1_800_000
    now_ms = close_ms + 1_000

    def test_builds_canonical_latest_completed_candle_key(self):
        key = build_completed_candle_decision_key(
            symbol="btc/usdt:usdt",
            timeframe="15M",
            candle_close_ms=self.close_ms,
            side="long",
            now_ms=self.now_ms,
        )

        self.assertEqual(key, "BTC/USDT:USDT|15m|1800000|LONG")
        decision = validate_completed_candle_decision_key(
            key,
            now_ms=self.now_ms,
        )
        self.assertEqual(decision.symbol, "BTC/USDT:USDT")
        self.assertEqual(decision.side, "LONG")

    def test_rejects_incomplete_stale_misaligned_and_noncanonical_keys(self):
        valid = "XBTUSDT|15m|1800000|SHORT"
        cases = (
            (valid, self.close_ms - 1),
            (valid, self.close_ms + 900_000),
            ("XBTUSDT|15m|1800001|SHORT", self.now_ms),
            ("xbtusdt|15m|1800000|short", self.now_ms),
        )
        for key, now_ms in cases:
            with self.subTest(key=key, now_ms=now_ms):
                with self.assertRaises(ExecutionSafetyError):
                    validate_completed_candle_decision_key(key, now_ms=now_ms)

    def test_client_ids_are_stable_distinct_uuid5_values_within_limit(self):
        key = "XBTUSDT|15m|1800000|LONG"
        first = client_order_ids(key, now_ms=self.now_ms)
        second = client_order_ids(key, now_ms=self.now_ms)

        self.assertEqual(first, second)
        self.assertEqual(set(first), {"entry", "sl", "tp"})
        self.assertEqual(len(set(first.values())), 3)
        for value in first.values():
            self.assertEqual(len(value), 36)
            self.assertEqual(UUID(value).version, 5)

        other_decision = "XBTUSDT|15m|1800000|SHORT"
        self.assertNotEqual(
            first["entry"],
            client_order_id(other_decision, "entry", now_ms=self.now_ms),
        )

    def test_rejects_unknown_order_leg(self):
        with self.assertRaises(ExecutionSafetyError):
            client_order_id(
                "XBTUSDT|15m|1800000|LONG",
                "exit",
                now_ms=self.now_ms,
            )

    def test_emergency_close_id_is_stable_and_distinct(self):
        key = "XBTUSDT|15m|1800000|LONG"
        emergency = client_order_id(key, "emergency", now_ms=self.now_ms)
        ids = client_order_ids(key, now_ms=self.now_ms)

        self.assertEqual(len(emergency), 36)
        self.assertEqual(UUID(emergency).version, 5)
        self.assertNotIn(emergency, ids.values())
        self.assertEqual(
            reconciliation_client_order_id(key, "emergency"),
            emergency,
        )


class IocLimitPriceTests(unittest.TestCase):
    def test_buy_uses_ask_and_rounds_down_without_exceeding_bound(self):
        price = bounded_ioc_limit_price(
            side="buy",
            best_bid="99.95",
            best_ask="100.00",
            max_slippage_bps="33",
            tick_size="0.05",
        )

        self.assertEqual(price, Decimal("100.30"))
        self.assertGreaterEqual(price, Decimal("100.00"))
        self.assertLessEqual(price, Decimal("100.33"))
        self.assertEqual(price % Decimal("0.05"), 0)

    def test_sell_uses_bid_and_rounds_up_without_exceeding_bound(self):
        price = bounded_ioc_limit_price(
            side="sell",
            best_bid="99.95",
            best_ask="100.00",
            max_slippage_bps="33",
            tick_size="0.05",
        )

        worst_price = Decimal("99.95") * (Decimal(1) - Decimal("0.0033"))
        self.assertEqual(price, Decimal("99.65"))
        self.assertLessEqual(price, Decimal("99.95"))
        self.assertGreaterEqual(price, worst_price)
        self.assertEqual(price % Decimal("0.05"), 0)

    def test_rejects_crossed_or_off_tick_books_and_invalid_slippage(self):
        invalid_kwargs = (
            {"best_bid": 101, "best_ask": 100, "max_slippage_bps": 10},
            {"best_bid": 99.96, "best_ask": 100, "max_slippage_bps": 10},
            {"best_bid": 99.95, "best_ask": 100, "max_slippage_bps": -1},
            {"best_bid": 99.95, "best_ask": 100, "max_slippage_bps": 10_000},
        )
        for values in invalid_kwargs:
            with self.subTest(values=values):
                with self.assertRaises(ExecutionSafetyError):
                    bounded_ioc_limit_price(
                        side="buy",
                        tick_size="0.05",
                        **values,
                    )


class OrderClassificationTests(unittest.TestCase):
    def test_missing_and_unknown_statuses_are_never_filled(self):
        cases = (
            None,
            {"status": None, "amount": 10, "filled": 10, "remaining": 0},
            {"status": "mystery", "amount": 10, "filled": 10, "remaining": 0},
            {"status": "closed"},
        )
        for order in cases:
            with self.subTest(order=order):
                result = classify_order(order)
                self.assertFalse(result.is_filled)
                self.assertIn(result.state, {"missing", "unknown"})

    def test_affirmative_filled_status_is_filled(self):
        result = classify_order(
            {"status": "closed", "amount": "10", "filled": "10", "remaining": 0}
        )

        self.assertTrue(result.is_filled)
        self.assertTrue(result.terminal)
        self.assertEqual(result.filled, Decimal("10"))
        self.assertEqual(result.leaves, Decimal(0))

    def test_open_partial_reports_filled_and_leaves(self):
        result = classify_order(
            {"status": "open", "amount": "10", "filled": "3", "remaining": "7"}
        )

        self.assertTrue(result.is_partial)
        self.assertFalse(result.terminal)
        self.assertEqual(result.filled, Decimal("3"))
        self.assertEqual(result.leaves, Decimal("7"))

    def test_canceled_partial_remains_visible_as_terminal_partial(self):
        result = classify_order(
            {
                "info": {
                    "ordStatus": "Canceled",
                    "orderQty": 10,
                    "cumQty": 3,
                    "leavesQty": 7,
                }
            }
        )

        self.assertTrue(result.is_partial)
        self.assertTrue(result.terminal)
        self.assertEqual(result.filled, Decimal("3"))
        self.assertEqual(result.leaves, Decimal("7"))

    def test_terminal_partial_with_zero_leaves_stays_partial(self):
        result = classify_order(
            {"status": "canceled", "amount": 10, "filled": 3, "remaining": 0}
        )

        self.assertEqual(result.state, "partial")
        self.assertEqual(result.filled, Decimal("3"))
        self.assertTrue(result.terminal)
        self.assertEqual(result.leaves, Decimal("0"))

    def test_canceled_without_fill_is_not_partial(self):
        result = classify_order(
            {"status": "canceled", "amount": 10, "filled": 0, "remaining": 10}
        )

        self.assertEqual(result.state, "canceled")
        self.assertFalse(result.is_filled)
        self.assertTrue(result.terminal)


class OrderReconciliationTests(unittest.TestCase):
    def test_find_uses_clordid_filter_and_matches_nested_exchange_field(self):
        expected = {"id": "exchange-id", "info": {"clOrdID": VALID_CLIENT_ID}}
        exchange = FakeExchange([[expected]])

        found = find_order_by_client_id(
            exchange,
            VALID_CLIENT_ID,
            symbol="BTC/USDT:USDT",
        )

        self.assertIs(found, expected)
        args, kwargs = exchange.fetch_calls[0]
        self.assertEqual(args, ("BTC/USDT:USDT",))
        self.assertEqual(
            kwargs,
            {"params": {"filter": {"clOrdID": VALID_CLIENT_ID}}},
        )

    def test_find_returns_none_for_no_match(self):
        exchange = FakeExchange([[{"info": {"clOrdID": "different"}}]])
        self.assertIsNone(find_order_by_client_id(exchange, VALID_CLIENT_ID))

    def test_find_rejects_missing_exchange_response(self):
        exchange = FakeExchange([None])
        with self.assertRaises(ReconciliationError):
            find_order_by_client_id(exchange, VALID_CLIENT_ID)

    def test_find_rejects_multiple_matches_for_unique_id(self):
        matching = {"clientOrderId": VALID_CLIENT_ID}
        exchange = FakeExchange([[matching, matching.copy()]])
        with self.assertRaises(ReconciliationError):
            find_order_by_client_id(exchange, VALID_CLIENT_ID)


class CreateOrderOnceTests(unittest.TestCase):
    def test_preexisting_order_prevents_submission(self):
        existing = {"clientOrderId": VALID_CLIENT_ID, "status": "open"}
        exchange = FakeExchange([[existing]])
        submitted = []

        result = create_order_once(
            exchange,
            client_order_id=VALID_CLIENT_ID,
            create=lambda value: submitted.append(value),
        )

        self.assertEqual(result.source, "existing")
        self.assertFalse(result.submitted)
        self.assertEqual(submitted, [])

    def test_successful_submission_occurs_exactly_once(self):
        exchange = FakeExchange([[]])
        submitted = []

        def create(value):
            submitted.append(value)
            return {"clientOrderId": value, "status": "open"}

        result = create_order_once(
            exchange,
            client_order_id=VALID_CLIENT_ID,
            create=create,
        )

        self.assertEqual(result.source, "submitted")
        self.assertTrue(result.submitted)
        self.assertEqual(submitted, [VALID_CLIENT_ID])
        self.assertEqual(len(exchange.fetch_calls), 1)

    def test_timeout_reconciles_without_resubmitting(self):
        accepted = {"info": {"clOrdID": VALID_CLIENT_ID}, "status": "open"}
        exchange = FakeExchange([[], [accepted]])
        attempts = []

        def create(value):
            attempts.append(value)
            raise TimeoutError("response timed out")

        result = create_order_once(
            exchange,
            client_order_id=VALID_CLIENT_ID,
            create=create,
        )

        self.assertEqual(result.source, "reconciled")
        self.assertEqual(attempts, [VALID_CLIENT_ID])
        self.assertEqual(len(exchange.fetch_calls), 2)

    def test_unresolved_timeout_raises_and_never_retries(self):
        exchange = FakeExchange([[], []])
        attempts = []

        def create(value):
            attempts.append(value)
            raise ConnectionError("connection reset")

        with self.assertRaises(AmbiguousCreateUnresolved) as context:
            create_order_once(
                exchange,
                client_order_id=VALID_CLIENT_ID,
                create=create,
            )

        self.assertEqual(context.exception.client_order_id, VALID_CLIENT_ID)
        self.assertEqual(attempts, [VALID_CLIENT_ID])
        self.assertEqual(len(exchange.fetch_calls), 2)

    def test_none_response_is_reconciled_but_not_resubmitted(self):
        accepted = {"clientOrderId": VALID_CLIENT_ID, "status": "open"}
        exchange = FakeExchange([[], [accepted]])
        attempts = []

        result = create_order_once(
            exchange,
            client_order_id=VALID_CLIENT_ID,
            create=lambda value: attempts.append(value),
        )

        self.assertEqual(result.source, "reconciled")
        self.assertEqual(attempts, [VALID_CLIENT_ID])

    def test_malformed_response_is_reconciled_but_not_resubmitted(self):
        accepted = {"clientOrderId": VALID_CLIENT_ID, "status": "open"}
        exchange = FakeExchange([[], [accepted]])
        attempts = []

        result = create_order_once(
            exchange,
            client_order_id=VALID_CLIENT_ID,
            create=lambda value: attempts.append(value) or [],
        )

        self.assertEqual(result.source, "reconciled")
        self.assertIs(result.order, accepted)
        self.assertEqual(attempts, [VALID_CLIENT_ID])

    def test_unresolved_malformed_response_is_ambiguous(self):
        exchange = FakeExchange([[], []])

        with self.assertRaises(AmbiguousCreateUnresolved):
            create_order_once(
                exchange,
                client_order_id=VALID_CLIENT_ID,
                create=lambda _value: "unexpected response",
            )

    def test_success_without_client_id_requires_reconciliation(self):
        accepted = {"info": {"clOrdID": VALID_CLIENT_ID}, "status": "open"}
        exchange = FakeExchange([[], [accepted]])

        result = create_order_once(
            exchange,
            client_order_id=VALID_CLIENT_ID,
            create=lambda _value: {"id": "exchange-id", "status": "open"},
        )

        self.assertEqual(result.source, "reconciled")
        self.assertIs(result.order, accepted)

    def test_unresolved_missing_client_id_preserves_returned_order(self):
        exchange = FakeExchange([[], []])
        created = {"id": "exchange-id", "status": "open"}

        with self.assertRaises(AmbiguousCreateUnresolved) as context:
            create_order_once(
                exchange,
                client_order_id=VALID_CLIENT_ID,
                create=lambda _value: created,
            )

        self.assertIs(context.exception.returned_order, created)

    def test_mismatched_client_id_preserves_returned_order(self):
        exchange = FakeExchange([[]])
        created = {
            "id": "exchange-id",
            "clientOrderId": "00000000-0000-5000-8000-000000000099",
            "status": "open",
        }

        with self.assertRaises(AmbiguousCreateUnresolved) as context:
            create_order_once(
                exchange,
                client_order_id=VALID_CLIENT_ID,
                create=lambda _value: created,
            )

        self.assertIs(context.exception.returned_order, created)

    def test_definitive_create_error_is_not_treated_as_ambiguous(self):
        exchange = FakeExchange([[]])
        attempts = []

        def create(value):
            attempts.append(value)
            raise ValueError("exchange rejected the order")

        with self.assertRaisesRegex(ValueError, "exchange rejected"):
            create_order_once(
                exchange,
                client_order_id=VALID_CLIENT_ID,
                create=create,
            )

        self.assertEqual(attempts, [VALID_CLIENT_ID])
        self.assertEqual(len(exchange.fetch_calls), 1)


if __name__ == "__main__":
    unittest.main()
