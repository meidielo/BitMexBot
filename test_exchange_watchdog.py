import asyncio
import hashlib
import hmac
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import aiohttp
from aiohttp import web

import exchange_watchdog as watchdog


NOW = datetime(2026, 7, 14, 0, 0, tzinfo=timezone.utc)


def deliver(
    state: watchdog.WatchdogState,
    payload,
    *,
    monotonic: float = 10.0,
) -> str:
    return state.handle(payload, now_monotonic=monotonic, now_utc=NOW)


def acknowledge_all(state: watchdog.WatchdogState) -> None:
    for offset, topic in enumerate(watchdog.SUBSCRIPTIONS):
        deliver(
            state,
            {"success": True, "subscribe": topic},
            monotonic=1.0 + offset,
        )


def synchronize_tables(state: watchdog.WatchdogState) -> None:
    deliver(
        state,
        {
            "table": "order",
            "action": "partial",
            "keys": ["orderID"],
            "data": [
                {
                    "orderID": "private-order-id",
                    "account": 998877,
                    "symbol": "XBTUSDT",
                    "ordStatus": "New",
                    "leavesQty": 100,
                    "price": 12345.6,
                }
            ],
        },
    )
    deliver(
        state,
        {
            "table": "position",
            "action": "partial",
            "keys": ["account", "symbol", "currency"],
            "data": [
                {
                    "account": 998877,
                    "symbol": "XBTUSDT",
                    "currency": "USDt",
                    "currentQty": 125,
                    "avgEntryPrice": 12345.6,
                }
            ],
        },
    )
    deliver(
        state,
        {
            "table": "execution",
            "action": "partial",
            "keys": ["execID"],
            "data": [
                {
                    "execID": "private-execution-id",
                    "orderID": "private-order-id",
                    "lastPx": 12345.6,
                }
            ],
        },
    )
    deliver(
        state,
        {
            "table": "margin",
            "action": "partial",
            "keys": ["account", "currency"],
            "data": [
                {
                    "account": 998877,
                    "currency": "USDt",
                    "walletBalance": 7654321,
                }
            ],
        },
    )


def healthy_state() -> watchdog.WatchdogState:
    state = watchdog.WatchdogState()
    acknowledge_all(state)
    synchronize_tables(state)
    return state


class TestnetGateTests(unittest.TestCase):
    def test_requires_literal_testnet_and_dedicated_credentials(self):
        valid = {
            "BITMEX_TESTNET": "true",
            "BITMEX_TESTNET_API_KEY": "testnet-key",
            "BITMEX_TESTNET_API_SECRET": "testnet-secret",
        }
        self.assertEqual(
            watchdog.require_testnet_credentials(valid),
            ("testnet-key", "testnet-secret"),
        )
        for value in (None, "", "TRUE", " true", "true ", "false"):
            env = dict(valid)
            if value is None:
                env.pop("BITMEX_TESTNET")
            else:
                env["BITMEX_TESTNET"] = value
            with self.subTest(value=value), self.assertRaises(EnvironmentError):
                watchdog.require_testnet_credentials(env)

    def test_rejects_generic_credentials_and_missing_dedicated_values(self):
        generic_only = {
            "BITMEX_TESTNET": "true",
            "BITMEX_API_KEY": "generic-key",
            "BITMEX_API_SECRET": "generic-secret",
        }
        with self.assertRaises(EnvironmentError):
            watchdog.require_testnet_credentials(generic_only)
        for field in (
            "BITMEX_TESTNET_API_KEY",
            "BITMEX_TESTNET_API_SECRET",
        ):
            env = {
                "BITMEX_TESTNET": "true",
                "BITMEX_TESTNET_API_KEY": "key",
                "BITMEX_TESTNET_API_SECRET": "secret",
            }
            env[field] = "  "
            with self.subTest(field=field), self.assertRaises(EnvironmentError):
                watchdog.require_testnet_credentials(env)
        control_character = {
            "BITMEX_TESTNET": "true",
            "BITMEX_TESTNET_API_KEY": "key\nInjected: value",
            "BITMEX_TESTNET_API_SECRET": "secret",
        }
        with self.assertRaises(EnvironmentError):
            watchdog.require_testnet_credentials(control_character)

    def test_auth_signature_is_get_realtime_hmac_sha256(self):
        headers = watchdog.build_auth_headers("key", "secret", 1_800_000_000)
        expected = hmac.new(
            b"secret",
            b"GET/realtime1800000000",
            hashlib.sha256,
        ).hexdigest()
        self.assertEqual(
            headers,
            {
                "api-key": "key",
                "api-expires": "1800000000",
                "api-signature": expected,
            },
        )
        with self.assertRaises(ValueError):
            watchdog.build_auth_headers("key\r\nInjected", "secret", 1_800_000_000)

    def test_endpoint_and_topics_are_testnet_observer_only(self):
        self.assertEqual(
            watchdog.TESTNET_WSS_URL,
            "wss://ws.testnet.bitmex.com/realtime",
        )
        self.assertEqual(
            watchdog.SUBSCRIPTIONS,
            ("order", "position", "execution", "margin"),
        )
        self.assertLessEqual(
            watchdog.BACKOFF_MAX_SECONDS,
            30,
        )


class RedirectSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_authenticated_websocket_redirect_is_rejected_before_second_hop(self):
        first_hop_headers = []
        second_hop_headers = []

        async def redirected(request):
            first_hop_headers.append(dict(request.headers))
            raise web.HTTPFound(second_hop_url)

        async def capture(request):
            second_hop_headers.append(dict(request.headers))
            return web.Response(status=400)

        second_app = web.Application()
        second_app.router.add_get("/capture", capture)
        second_runner = web.AppRunner(second_app)
        await second_runner.setup()
        second_site = web.TCPSite(second_runner, "127.0.0.1", 0)
        await second_site.start()
        second_port = second_site._server.sockets[0].getsockname()[1]
        second_hop_url = f"ws://127.0.0.1:{second_port}/capture"

        first_app = web.Application()
        first_app.router.add_get("/realtime", redirected)
        first_runner = web.AppRunner(first_app)
        await first_runner.setup()
        first_site = web.TCPSite(first_runner, "127.0.0.1", 0)
        await first_site.start()
        first_port = first_site._server.sockets[0].getsockname()[1]

        try:
            with (
                patch.object(
                    watchdog,
                    "TESTNET_WSS_URL",
                    f"ws://127.0.0.1:{first_port}/realtime",
                ),
                self.assertRaisesRegex(
                    watchdog.WatchdogTransportError,
                    "redirects are forbidden",
                ),
            ):
                await watchdog._run_connection(
                    "dummy-key",
                    "dummy-secret",
                    watchdog.WatchdogState(),
                    "unused-status.json",
                )
        finally:
            await first_runner.cleanup()
            await second_runner.cleanup()

        self.assertEqual(len(first_hop_headers), 1)
        self.assertEqual(first_hop_headers[0]["api-key"], "dummy-key")
        self.assertEqual(second_hop_headers, [])


class DeltaTableTests(unittest.TestCase):
    def test_partial_insert_update_delete_are_applied_atomically(self):
        state = healthy_state()
        self.assertTrue(state.synchronized)
        initial = state.snapshot(now_monotonic=10.0, now_utc=NOW)
        self.assertEqual(initial["position_contracts"], 125)
        self.assertEqual(initial["open_order_count"], 1)

        deliver(
            state,
            {
                "table": "order",
                "action": "insert",
                "data": [
                    {
                        "orderID": "second-private-id",
                        "symbol": "XBTUSDT",
                        "ordStatus": "PartiallyFilled",
                        "leavesQty": 25,
                        "price": 20000,
                    }
                ],
            },
        )
        self.assertEqual(
            state.snapshot(now_monotonic=10.0, now_utc=NOW)["open_order_count"],
            2,
        )
        deliver(
            state,
            {
                "table": "order",
                "action": "update",
                "data": [
                    {
                        "orderID": "private-order-id",
                        "ordStatus": "Filled",
                        "leavesQty": 0,
                    }
                ],
            },
        )
        deliver(
            state,
            {
                "table": "position",
                "action": "update",
                "data": [
                    {
                        "account": 998877,
                        "symbol": "XBTUSDT",
                        "currency": "USDt",
                        "currentQty": -50,
                    }
                ],
            },
        )
        snapshot = state.snapshot(now_monotonic=10.0, now_utc=NOW)
        self.assertEqual(snapshot["open_order_count"], 1)
        self.assertEqual(snapshot["position_contracts"], -50)

        deliver(
            state,
            {
                "table": "order",
                "action": "delete",
                "data": [{"orderID": "second-private-id"}],
            },
        )
        self.assertEqual(
            state.snapshot(now_monotonic=10.0, now_utc=NOW)["open_order_count"],
            0,
        )

    def test_execution_and_margin_deltas_retain_only_keys(self):
        state = healthy_state()
        deliver(
            state,
            {
                "table": "execution",
                "action": "insert",
                "data": [{"execID": "new-exec", "lastPx": 99999}],
            },
        )
        deliver(
            state,
            {
                "table": "execution",
                "action": "update",
                "data": [{"execID": "new-exec", "lastPx": 100000}],
            },
        )
        deliver(
            state,
            {
                "table": "execution",
                "action": "delete",
                "data": [{"execID": "new-exec"}],
            },
        )
        execution_rows = list(state.tables["execution"].rows.values())
        margin_rows = list(state.tables["margin"].rows.values())
        self.assertEqual(execution_rows, [{"execID": "private-execution-id"}])
        self.assertEqual(
            margin_rows,
            [{"account": 998877, "currency": "USDt"}],
        )

    def test_failed_delta_does_not_partially_mutate_table(self):
        table = watchdog.DeltaTable("order")
        table.apply(
            "partial",
            [
                {
                    "orderID": "one",
                    "symbol": "XBTUSDT",
                    "ordStatus": "New",
                    "leavesQty": 10,
                }
            ],
            ["orderID"],
        )
        before = dict(table.rows)
        with self.assertRaises(watchdog.WatchdogProtocolError):
            table.apply(
                "insert",
                [
                    {
                        "orderID": "two",
                        "symbol": "XBTUSDT",
                        "ordStatus": "New",
                        "leavesQty": 5,
                    },
                    {
                        "orderID": "one",
                        "symbol": "XBTUSDT",
                        "ordStatus": "New",
                        "leavesQty": 5,
                    },
                ],
            )
        self.assertEqual(table.rows, before)

    def test_update_delete_before_partial_or_for_unknown_key_fail(self):
        table = watchdog.DeltaTable("execution")
        for action in ("update", "delete"):
            with self.subTest(action=action), self.assertRaises(
                watchdog.WatchdogProtocolError
            ):
                table.apply(action, [{"execID": "missing"}])
        table.apply("partial", [{"execID": "known"}], ["execID"])
        for action in ("update", "delete"):
            with self.subTest(action=action), self.assertRaises(
                watchdog.WatchdogProtocolError
            ):
                table.apply(action, [{"execID": "missing"}])

    def test_terminal_order_with_leaves_fails_closed(self):
        table = watchdog.DeltaTable("order")
        table.apply(
            "partial",
            [
                {
                    "orderID": "valid",
                    "symbol": "XBTUSDT",
                    "ordStatus": "New",
                    "leavesQty": 10,
                }
            ],
            ["orderID"],
        )
        before = dict(table.rows)
        with self.assertRaises(watchdog.WatchdogProtocolError):
            table.apply(
                "update",
                [
                    {
                        "orderID": "valid",
                        "ordStatus": "Filled",
                        "leavesQty": 1,
                    }
                ],
            )
        self.assertEqual(table.rows, before)


class ProtocolTests(unittest.TestCase):
    def test_decodes_only_bounded_objects_and_raw_pong(self):
        self.assertEqual(watchdog.decode_ws_text("pong"), "pong")
        self.assertEqual(watchdog.decode_ws_text('{"info":"ok"}'), {"info": "ok"})
        for value in ("not-json", "[]", "null", "1"):
            with self.subTest(value=value), self.assertRaises(
                watchdog.WatchdogProtocolError
            ):
                watchdog.decode_ws_text(value)

    def test_rejects_malformed_table_messages(self):
        acknowledged = watchdog.WatchdogState()
        deliver(acknowledged, {"success": True, "subscribe": "order"})
        cases = (
            {"table": "unknown", "action": "partial", "keys": ["id"], "data": []},
            {"table": "order", "action": "replace", "keys": ["id"], "data": []},
            {"table": "order", "action": "partial", "keys": [], "data": []},
            {
                "table": "order",
                "action": "partial",
                "keys": ["orderID", "orderID"],
                "data": [],
            },
            {"table": "order", "action": "partial", "keys": ["orderID"], "data": {}},
            {
                "table": "position",
                "action": "partial",
                "keys": ["account"],
                "data": [{"account": 1}],
            },
            {
                "table": "order",
                "action": "partial",
                "keys": ["orderID"],
                "data": [],
                "success": True,
            },
        )
        for payload in cases:
            with self.subTest(payload=payload), self.assertRaises(
                (watchdog.WatchdogProtocolError, watchdog.WatchdogSubscriptionError)
            ):
                deliver(acknowledged, payload)

    def test_table_data_requires_ack_and_only_one_partial(self):
        payload = {
            "table": "execution",
            "action": "partial",
            "keys": ["execID"],
            "data": [],
        }
        state = watchdog.WatchdogState()
        with self.assertRaises(watchdog.WatchdogSubscriptionError):
            deliver(state, payload)
        deliver(state, {"success": True, "subscribe": "execution"})
        deliver(state, payload)
        with self.assertRaises(watchdog.WatchdogProtocolError):
            deliver(state, payload)

    def test_rejects_failed_unexpected_and_duplicate_subscriptions(self):
        state = watchdog.WatchdogState()
        for payload in (
            {"success": False, "subscribe": "order"},
            {"success": True, "subscribe": "instrument"},
            {"error": "private server detail"},
            {"success": True},
        ):
            with self.subTest(payload=payload), self.assertRaises(
                watchdog.WatchdogSubscriptionError
            ):
                deliver(state, payload)
        deliver(state, {"success": True, "subscribe": "order"})
        with self.assertRaises(watchdog.WatchdogSubscriptionError):
            deliver(state, {"success": True, "subscribe": "order"})

    def test_unknown_control_message_fails_closed(self):
        with self.assertRaises(watchdog.WatchdogProtocolError):
            deliver(watchdog.WatchdogState(), {"status": 200})

    def test_stale_snapshot_is_explicitly_unhealthy(self):
        state = watchdog.WatchdogState()
        deliver(state, {"info": "welcome"}, monotonic=10.0)
        snapshot = state.snapshot(
            now_monotonic=10.0 + watchdog.STALE_AFTER_SECONDS + 0.001,
            now_utc=NOW,
            event_type="stale",
        )
        self.assertEqual(snapshot["state"], "stale")
        self.assertFalse(snapshot["fresh"])

    def test_monotonic_clock_regression_fails_closed(self):
        state = watchdog.WatchdogState()
        deliver(state, {"info": "welcome"}, monotonic=10.0)
        with self.assertRaises(watchdog.WatchdogProtocolError):
            state.snapshot(now_monotonic=9.0, now_utc=NOW)


class ConnectionProbeTests(unittest.IsolatedAsyncioTestCase):
    class FakeFrame:
        def __init__(self, data: str):
            self.type = aiohttp.WSMsgType.TEXT
            self.data = data

    class FakeWebSocket:
        def __init__(self, messages=None, *, block=False):
            self.messages = list(messages or [])
            self.block = block
            self.sent = []

        async def send_str(self, value):
            self.sent.append(value)

        async def receive(self):
            if self.block:
                await asyncio.sleep(60)
            return self.messages.pop(0)

    async def test_idle_probe_requires_and_accepts_raw_pong(self):
        websocket = self.FakeWebSocket([self.FakeFrame("pong")])
        state = watchdog.WatchdogState()
        await watchdog._prove_idle_connection(websocket, state)
        self.assertEqual(websocket.sent, ["ping"])
        self.assertIsNotNone(state.last_message_at)

    async def test_idle_probe_timeout_is_explicit_staleness(self):
        websocket = self.FakeWebSocket(block=True)
        state = watchdog.WatchdogState()
        with patch.object(watchdog, "PONG_TIMEOUT_SECONDS", 0.01):
            with self.assertRaises(watchdog.WatchdogStaleError):
                await watchdog._prove_idle_connection(websocket, state)
        self.assertEqual(websocket.sent, ["ping"])


class PublicationTests(unittest.TestCase):
    def test_status_schema_is_sanitized(self):
        state = healthy_state()
        snapshot = state.snapshot(now_monotonic=10.0, now_utc=NOW)
        self.assertEqual(
            set(snapshot),
            {
                "schema_version",
                "environment",
                "state",
                "generated_at_utc",
                "last_message_at_utc",
                "freshness_seconds",
                "fresh",
                "subscriptions",
                "position_contracts",
                "open_order_count",
                "last_event_type",
            },
        )
        serialized = json.dumps(snapshot, sort_keys=True)
        for forbidden in (
            "private-order-id",
            "private-execution-id",
            "orderID",
            "execID",
            "walletBalance",
            "avgEntryPrice",
            "12345.6",
            "998877",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, serialized)

    def test_atomic_write_replaces_complete_document(self):
        snapshot = healthy_state().snapshot(now_monotonic=10.0, now_utc=NOW)
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "watchdog_status.json"
            destination.write_text('{"old":true}\n', encoding="utf-8")
            watchdog.write_status_atomic(snapshot, destination)
            loaded = json.loads(destination.read_text(encoding="utf-8"))
            self.assertEqual(loaded, snapshot)
            self.assertTrue(destination.read_bytes().endswith(b"\n"))
            self.assertEqual(list(Path(directory).glob("*.tmp")), [])

    def test_failed_replace_keeps_previous_document_and_cleans_temp(self):
        snapshot = healthy_state().snapshot(now_monotonic=10.0, now_utc=NOW)
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "watchdog_status.json"
            original = '{"old":true}\n'
            destination.write_text(original, encoding="utf-8")
            with patch.object(watchdog.os, "replace", side_effect=OSError("blocked")):
                with self.assertRaises(watchdog.WatchdogPublishError):
                    watchdog.write_status_atomic(snapshot, destination)
            self.assertEqual(destination.read_text(encoding="utf-8"), original)
            self.assertEqual(list(Path(directory).glob("*.tmp")), [])

    def test_atomic_writer_rejects_extra_fields(self):
        snapshot = healthy_state().snapshot(now_monotonic=10.0, now_utc=NOW)
        snapshot["api_secret"] = "must-never-publish"
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(watchdog.WatchdogPublishError):
                watchdog.write_status_atomic(
                    snapshot,
                    Path(directory) / "watchdog_status.json",
                )

    def test_atomic_writer_rejects_inconsistent_freshness(self):
        snapshot = healthy_state().snapshot(now_monotonic=10.0, now_utc=NOW)
        snapshot["freshness_seconds"] = watchdog.STALE_AFTER_SECONDS + 1
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(watchdog.WatchdogPublishError):
                watchdog.write_status_atomic(
                    snapshot,
                    Path(directory) / "watchdog_status.json",
                )

    def test_alert_payload_contains_only_redacted_event_metadata(self):
        payload = watchdog.build_alert_payload("protocol_error", NOW)
        self.assertEqual(
            set(payload),
            {
                "schema_version",
                "source",
                "environment",
                "event_type",
                "generated_at_utc",
            },
        )
        serialized = json.dumps(payload)
        for forbidden in ("position", "order", "price", "balance", "key", "secret"):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, serialized.lower())
        with self.assertRaises(watchdog.WatchdogAlertError):
            watchdog.build_alert_payload("synchronized", NOW)

    def test_alert_url_is_optional_https_without_userinfo_or_fragment(self):
        self.assertIsNone(watchdog.validate_alert_webhook_url(None))
        self.assertEqual(
            watchdog.validate_alert_webhook_url(
                "https://alerts.example.test/hooks/token-value"
            ),
            "https://alerts.example.test/hooks/token-value",
        )
        for value in (
            "http://alerts.example.test/hook",
            "https://user:pass@alerts.example.test/hook",
            "https://alerts.example.test/hook#fragment",
            "https://alerts.example.test:invalid/hook",
            " https://alerts.example.test/hook",
        ):
            with self.subTest(value=value), self.assertRaises(EnvironmentError):
                watchdog.validate_alert_webhook_url(value)


if __name__ == "__main__":
    unittest.main(verbosity=2)
