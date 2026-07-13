import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent


class RemoteDeploymentTests(unittest.TestCase):
    def setUp(self):
        self.compose = (ROOT / "compose.remote.yml").read_text(encoding="utf-8")

    def test_separates_runtime_credentials_and_private_volumes(self):
        self.assertIn("path: .env.runner", self.compose)
        self.assertIn("path: .env.watchdog", self.compose)
        self.assertIn('profiles: ["watchdog"]', self.compose)
        self.assertIn("required: false", self.compose)
        self.assertNotIn(".env.runtime", self.compose)
        self.assertIn("watchdog-status:/app/data", self.compose)
        self.assertIn("bitmexbot-data:/app/data:ro", self.compose)
        self.assertIn("watchdog-status:/watchdog:ro", self.compose)
        self.assertIn("dashboard-snapshot:/snapshot:ro", self.compose)
        self.assertNotIn("bitmexbot-data:/snapshot", self.compose)
        snapshot_block = self.compose.split("  snapshot:", 1)[1].split(
            "  dashboard:", 1
        )[0]
        self.assertNotIn("watchdog:\n", snapshot_block)

    def test_initializes_ledger_without_network_before_runner(self):
        self.assertIn("ledger-init:", self.compose)
        self.assertIn("network_mode: none", self.compose)
        self.assertIn("trade_ledger.py", self.compose)
        self.assertIn("condition: service_completed_successfully", self.compose)

    def test_dashboard_has_private_static_ingress_and_snapshot_has_no_network(self):
        dashboard_block = self.compose.split("  dashboard:", 1)[1].split(
            "\nnetworks:", 1
        )[0]
        self.assertNotIn("ports:", dashboard_block)
        self.assertIn("ipv4_address: 10.254.54.10", dashboard_block)
        self.assertIn("internal: true", self.compose)
        self.assertIn("subnet: 10.254.54.0/24", self.compose)
        self.assertIn("DASH_SNAPSHOT_PATH: /snapshot/operator_status.json", self.compose)

    def test_resolver_is_tls_only_and_uses_non_overlapping_static_address(self):
        resolver = (ROOT / "deploy" / "stubby.yml").read_text(encoding="utf-8")
        self.assertIn("GETDNS_TRANSPORT_TLS", resolver)
        self.assertIn("GETDNS_AUTHENTICATION_REQUIRED", resolver)
        self.assertNotIn("GETDNS_TRANSPORT_UDP", resolver)
        self.assertNotIn("GETDNS_TRANSPORT_TCP", resolver)
        self.assertIn("10.254.53.53", self.compose)
        self.assertNotIn("172.31.53", self.compose)

    def test_images_are_non_root_digest_pinned_and_health_is_readiness_based(self):
        runner = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        dashboard = (ROOT / "Dockerfile.dashboard").read_text(encoding="utf-8")
        resolver = (ROOT / "Dockerfile.dns").read_text(encoding="utf-8")
        for source in (runner, dashboard, resolver):
            self.assertIn("@sha256:", source)
            self.assertRegex(source, r"(?m)^USER \d+:\d+$")
        self.assertIn("/readyz", dashboard)
        self.assertIn("--no-control-socket", dashboard)
        self.assertIn("+short", resolver)

    def test_compose_bounds_processes_memory_cpu_and_logs(self):
        self.assertIn("pids_limit:", self.compose)
        self.assertIn("mem_limit:", self.compose)
        self.assertIn("cpus:", self.compose)
        self.assertIn('max-size: "10m"', self.compose)
        self.assertIn('max-file: "3"', self.compose)


if __name__ == "__main__":
    unittest.main()
