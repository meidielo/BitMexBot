import ast
import base64
import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from werkzeug.security import generate_password_hash

import dashboard


def sample_intent(now: datetime) -> dict[str, object]:
    timestamp = now.isoformat().replace("+00:00", "Z")
    return {
        "environment": "TESTNET",
        "symbol": "BTC/USDT:USDT",
        "side": "LONG",
        "status": "PROTECTED",
        "requested_contracts": 1000,
        "filled_contracts": 1000,
        "closed_contracts": 0,
        "expected_max_loss_usdt": 1.25,
        "net_pnl_usdt": None,
        "created_at_utc": timestamp,
        "updated_at_utc": timestamp,
        "entry_order_id": "must-not-leak",
    }


def sample_snapshot(now: datetime) -> dict[str, object]:
    timestamp = now.isoformat().replace("+00:00", "Z")
    intent = sample_intent(now)
    return {
        "schema_version": 1,
        "generated_at_utc": timestamp,
        "data_state": "OK",
        "environment": "TESTNET",
        "production_enabled": False,
        "actions_enabled": False,
        "runner": {
            "state": "RUNNING",
            "heartbeat_at_utc": timestamp,
            "fresh": True,
            "raw_log": "must-not-leak",
        },
        "watchdog": {
            "state": "HEALTHY",
            "checked_at_utc": timestamp,
            "fresh": True,
            "raw_error": "must-not-leak",
        },
        "ledger": {
            "available": True,
            "total_intents": 1,
            "open_intents": 1,
            "manual_halts": 0,
            "protection_state": "PROTECTED",
            "latest_intent": dict(intent),
            "recent_intents": [dict(intent)],
            "database_path": "must-not-leak",
        },
        "risk": {
            "state": "CURRENT",
            "date": now.strftime("%Y-%m-%d"),
            "daily_loss_usdt": 2.5,
            "fresh": True,
            "balance": "must-not-leak",
        },
        "readiness": {
            "evidence_state": "VALID",
            "verdict": "NOT_READY",
            "stage": "RESEARCH",
            "blocker_count": 12,
            "human_approval_required": True,
            "production_enabled": False,
            "blockers": ["must-not-leak"],
        },
        "warnings": [],
        "api_key": "must-not-leak",
        "log_lines": ["must-not-leak"],
    }


class DashboardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.snapshot_path = Path(self.temp.name) / "operator_status.json"
        self.now = datetime.now(timezone.utc).replace(microsecond=0)
        self.write_snapshot(sample_snapshot(self.now))
        self.password = "correct horse battery staple"
        self.password_hash = generate_password_hash(
            self.password, method="pbkdf2:sha256:1", salt_length=8
        )
        self.app = dashboard.create_app(
            {
                "TESTING": True,
                "DASH_USER": "operator",
                "DASH_PASSWORD_HASH": self.password_hash,
                "SNAPSHOT_PATH": str(self.snapshot_path),
                "SNAPSHOT_MAX_AGE_SECONDS": 120,
                "SNAPSHOT_MAX_BYTES": 262144,
            }
        )
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def write_snapshot(self, payload: object) -> None:
        self.snapshot_path.write_text(json.dumps(payload), encoding="utf-8")

    def auth(self, user: str = "operator", password: str | None = None) -> dict[str, str]:
        password = self.password if password is None else password
        token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
        return {"Authorization": f"Basic {token}"}

    def test_only_expected_routes_exist(self) -> None:
        rules = {rule.rule for rule in self.app.url_map.iter_rules()}
        self.assertEqual(rules, {"/", "/api/v1/status", "/healthz", "/readyz"})

    def test_operator_routes_require_valid_basic_auth(self) -> None:
        for path in ("/", "/api/v1/status"):
            with self.subTest(path=path, case="missing"):
                response = self.client.get(path)
                self.assertEqual(response.status_code, 401)
                self.assertIn("Basic realm=", response.headers["WWW-Authenticate"])
            with self.subTest(path=path, case="wrong-user"):
                self.assertEqual(
                    self.client.get(path, headers=self.auth(user="intruder")).status_code,
                    401,
                )
            with self.subTest(path=path, case="wrong-password"):
                self.assertEqual(
                    self.client.get(path, headers=self.auth(password="wrong")).status_code,
                    401,
                )
            with self.subTest(path=path, case="valid"):
                self.assertEqual(
                    self.client.get(path, headers=self.auth()).status_code,
                    200,
                )

    def test_missing_auth_configuration_fails_closed(self) -> None:
        app = dashboard.create_app(
            {
                "TESTING": True,
                "DASH_USER": "operator",
                "DASH_PASSWORD_HASH": "",
                "SNAPSHOT_PATH": str(self.snapshot_path),
                "SNAPSHOT_MAX_AGE_SECONDS": 120,
                "SNAPSHOT_MAX_BYTES": 262144,
            }
        )
        client = app.test_client()

        protected = client.get("/", headers=self.auth())
        readiness = client.get("/readyz")

        self.assertEqual(protected.status_code, 503)
        self.assertEqual(protected.get_json(), {"status": "unavailable"})
        self.assertEqual(readiness.status_code, 503)
        self.assertEqual(readiness.get_json(), {"status": "unready"})

    def test_api_revalidates_and_allowlists_snapshot(self) -> None:
        response = self.client.get("/api/v1/status", headers=self.auth())
        body = response.get_json()
        serialized = json.dumps(body)

        self.assertEqual(response.status_code, 200)
        self.assertIs(body["snapshot_fresh"], True)
        self.assertIs(body["production_enabled"], False)
        self.assertIs(body["actions_enabled"], False)
        self.assertNotIn("must-not-leak", serialized)
        self.assertNotIn("api_key", serialized)
        self.assertNotIn("log_lines", serialized)
        self.assertNotIn("entry_order_id", serialized)
        self.assertNotIn("database_path", serialized)
        self.assertNotIn("balance", serialized)
        self.assertNotIn("blockers", serialized)

    def test_html_is_self_contained_and_has_no_controls(self) -> None:
        response = self.client.get("/", headers=self.auth())
        html = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn("TESTNET RESEARCH ONLY", html)
        self.assertIn("No trading controls are exposed", html)
        self.assertNotIn("<script", html.lower())
        self.assertNotIn("<form", html.lower())
        self.assertNotIn("<button", html.lower())
        self.assertNotIn("https://", html.lower())
        self.assertNotIn("must-not-leak", html)
        style = html.split("<style>", 1)[1].split("</style>", 1)[0]
        digest = base64.b64encode(hashlib.sha256(style.encode("utf-8")).digest()).decode(
            "ascii"
        )
        self.assertIn(
            f"style-src 'sha256-{digest}'",
            response.headers["Content-Security-Policy"],
        )

    def test_security_headers_are_strict_and_cors_is_absent(self) -> None:
        response = self.client.get("/api/v1/status", headers=self.auth())

        self.assertEqual(response.headers["Cache-Control"], "no-store, max-age=0")
        self.assertEqual(response.headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(response.headers["X-Frame-Options"], "DENY")
        self.assertEqual(response.headers["Referrer-Policy"], "no-referrer")
        self.assertIn("default-src 'none'", response.headers["Content-Security-Policy"])
        self.assertIn("script-src 'none'", response.headers["Content-Security-Policy"])
        self.assertNotIn("'unsafe-inline'", response.headers["Content-Security-Policy"])
        self.assertIn("max-age=31536000", response.headers["Strict-Transport-Security"])
        self.assertNotIn("Access-Control-Allow-Origin", response.headers)

    def test_health_and_readiness_reveal_only_status(self) -> None:
        health = self.client.get("/healthz")
        ready = self.client.get("/readyz")

        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.get_json(), {"status": "ok"})
        self.assertEqual(ready.status_code, 200)
        self.assertEqual(ready.get_json(), {"status": "ready"})

    def test_stale_snapshot_is_visible_but_not_ready(self) -> None:
        stale = sample_snapshot(self.now - timedelta(minutes=10))
        self.write_snapshot(stale)

        api = self.client.get("/api/v1/status", headers=self.auth())
        ready = self.client.get("/readyz")

        self.assertEqual(api.status_code, 200)
        self.assertIs(api.get_json()["snapshot_fresh"], False)
        self.assertEqual(ready.status_code, 503)
        self.assertEqual(ready.get_json(), {"status": "unready"})

    def test_unavailable_data_is_visible_but_not_ready(self) -> None:
        payload = sample_snapshot(self.now)
        payload["data_state"] = "UNAVAILABLE"
        payload["environment"] = "UNKNOWN"
        payload["warnings"] = ["LEDGER_UNAVAILABLE"]
        payload["ledger"] = {
            "available": False,
            "total_intents": 0,
            "open_intents": 0,
            "manual_halts": 0,
            "protection_state": "UNKNOWN",
            "latest_intent": None,
            "recent_intents": [],
        }
        self.write_snapshot(payload)

        api = self.client.get("/api/v1/status", headers=self.auth())
        ready = self.client.get("/readyz")

        self.assertEqual(api.status_code, 200)
        self.assertEqual(api.get_json()["data_state"], "UNAVAILABLE")
        self.assertEqual(ready.status_code, 503)
        self.assertEqual(ready.get_json(), {"status": "unready"})

    def test_real_runtime_and_watchdog_states_pass_public_validation(self) -> None:
        payload = sample_snapshot(self.now)
        payload["data_state"] = "DEGRADED"
        payload["runner"] = {
            "state": "WAITING",
            "heartbeat_at_utc": self.now.isoformat(),
            "fresh": True,
        }
        payload["watchdog"] = {
            "state": "SYNCHRONIZING",
            "checked_at_utc": self.now.isoformat(),
            "fresh": False,
        }
        payload["warnings"] = ["WATCHDOG_ATTENTION"]
        self.write_snapshot(payload)

        response = self.client.get("/api/v1/status", headers=self.auth())

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["runner"]["state"], "WAITING")
        self.assertEqual(
            response.get_json()["watchdog"]["state"], "SYNCHRONIZING"
        )

    def test_malformed_oversized_and_unsafe_snapshots_return_generic_failure(self) -> None:
        cases: list[tuple[str, str]] = [
            ("malformed", "{not-json"),
            ("non-finite", '{"schema_version":1,"value":NaN}'),
        ]
        for name, raw in cases:
            with self.subTest(case=name):
                self.snapshot_path.write_text(raw, encoding="utf-8")
                response = self.client.get("/api/v1/status", headers=self.auth())
                self.assertEqual(response.status_code, 503)
                self.assertEqual(response.get_json(), {"status": "unavailable"})
                self.assertNotIn(str(self.snapshot_path), response.get_data(as_text=True))

        unsafe = sample_snapshot(self.now)
        unsafe["production_enabled"] = True
        self.write_snapshot(unsafe)
        response = self.client.get("/api/v1/status", headers=self.auth())
        self.assertEqual(response.status_code, 503)

        self.snapshot_path.write_text("x" * 1024, encoding="utf-8")
        self.app.config["SNAPSHOT_MAX_BYTES"] = 64
        response = self.client.get("/api/v1/status", headers=self.auth())
        self.assertEqual(response.status_code, 503)

    def test_xss_shaped_symbol_is_rejected_without_reflection(self) -> None:
        payload = sample_snapshot(self.now)
        payload["ledger"]["latest_intent"]["symbol"] = "<script>alert(1)</script>"
        payload["ledger"]["recent_intents"][0]["symbol"] = "<script>alert(1)</script>"
        self.write_snapshot(payload)

        response = self.client.get("/", headers=self.auth())

        self.assertEqual(response.status_code, 503)
        self.assertNotIn("alert(1)", response.get_data(as_text=True))

    def test_mutation_and_control_routes_do_not_exist(self) -> None:
        for method in ("post", "put", "patch", "delete", "options", "head"):
            with self.subTest(method=method):
                response = getattr(self.client, method)("/api/v1/status", headers=self.auth())
                self.assertEqual(response.status_code, 405)
        for path in (
            "/start",
            "/stop",
            "/orders",
            "/positions",
            "/config",
            "/keys",
            "/go-live",
            "/logs",
            "/api/data",
        ):
            with self.subTest(path=path):
                self.assertEqual(self.client.get(path, headers=self.auth()).status_code, 404)

    def test_dashboard_has_no_trading_or_secret_loading_imports(self) -> None:
        tree = ast.parse(Path(dashboard.__file__).read_text(encoding="utf-8"))
        imported_roots: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_roots.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_roots.add(node.module.split(".")[0])

        blocked = {
            "bitmex_client",
            "ccxt",
            "dotenv",
            "live_readiness",
            "main",
            "order_manager",
            "risk",
            "trade_ledger",
        }
        self.assertTrue(imported_roots.isdisjoint(blocked), imported_roots & blocked)

    def test_dashboard_image_boundary_is_non_root_and_allowlisted(self) -> None:
        root = Path(dashboard.__file__).resolve().parent
        dockerfile = (root / "Dockerfile.dashboard").read_text(encoding="utf-8")
        dockerignore = (root / ".dockerignore").read_text(encoding="utf-8")
        requirements = (root / "requirements-dashboard.txt").read_text(
            encoding="utf-8"
        )

        self.assertIn("python:3.12.13-slim-bookworm@sha256:", dockerfile)
        self.assertIn("USER 10001:10001", dockerfile)
        self.assertIn("dashboard:app", dockerfile)
        self.assertIn("/readyz", dockerfile)
        copy_lines = [
            line.strip() for line in dockerfile.splitlines() if line.startswith("COPY ")
        ]
        self.assertEqual(
            copy_lines,
            [
                "COPY requirements-dashboard.txt /app/requirements-dashboard.txt",
                "COPY --chown=dashboard:dashboard dashboard.py /app/dashboard.py",
            ],
        )
        ignored_lines = {
            line.strip()
            for line in dockerignore.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
        for sensitive in (".git", ".env", ".env.*", ".credentials", "data", "logs"):
            self.assertIn(sensitive, ignored_lines)
        self.assertNotIn("requirements-dashboard.txt", ignored_lines)
        for forbidden in ("ccxt", "python-dotenv", "requests", "aiohttp"):
            self.assertNotIn(forbidden, requirements.lower())


if __name__ == "__main__":
    unittest.main()
