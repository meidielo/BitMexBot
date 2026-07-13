"""Read-only, fail-closed real-funds promotion evaluator.

Only the versioned XBTUSDT execution ledger and explicit promotion evidence
are eligible inputs.  The legacy ``data/trades.db`` is reported for operator
awareness but is never counted because it mixes contracts and BTC quantities.

Passing this evaluator means ``READY_FOR_CANARY_REVIEW`` only.  It never
authorizes production trading and it never changes exchange configuration.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
from collections.abc import Mapping
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from promotion import PromotionStage, evaluate_promotion
from trade_ledger import (
    ACTIVE_STATUSES,
    ALL_STATUSES,
    DB_PATH as TRADE_DB_PATH,
    LedgerError,
    SCHEMA_VERSION,
    UNIT_MODEL,
    validate_execution_ledger_schema,
    verify_terminal_execution_evidence,
)


PROMOTION_EVIDENCE_PATH = os.path.join("data", "promotion_evidence.json")
LEGACY_TRADE_DB_PATH = os.path.join("data", "trades.db")

_REQUIRED_LEDGER_COLUMNS = frozenset(
    {
        "decision_key",
        "environment",
        "side",
        "status",
        "signal_entry_price_usdt",
        "signal_stop_price_usdt",
        "signal_target_price_usdt",
        "requested_contracts",
        "filled_contracts",
        "closed_contracts",
        "contract_size_btc",
        "unit_model",
        "actual_entry_price_usdt",
        "actual_exit_price_usdt",
        "gross_pnl_usdt",
        "fees_usdt",
        "funding_usdt",
        "net_pnl_usdt",
        "exit_reason",
        "entry_time_utc",
        "exit_time_utc",
        "reconciled_at_utc",
        "evidence_source_hash",
    }
)
_UNPROTECTED_STATUSES = (
    "REGISTERED",
    "ENTRY_PENDING",
    "ENTRY_PARTIAL",
    "ENTRY_FILLED",
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _read_only_connection(path: str) -> sqlite3.Connection:
    """Open an existing SQLite database without creating or mutating it."""

    uri_path = Path(path).resolve().as_posix()
    connection = sqlite3.connect(f"file:{uri_path}?mode=ro", uri=True, timeout=5)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA query_only=ON")
    return connection


def _count(connection: sqlite3.Connection, query: str, params: tuple[Any, ...] = ()) -> int:
    row = connection.execute(query, params).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _ledger_metrics(path: str) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "path": path,
        "db_exists": os.path.isfile(path),
        "db_readable": False,
        "schema_valid": False,
        "integrity_ok": False,
        "error": "",
        "total_intents": 0,
        "closed_intents": 0,
        "active_intents": 0,
        "unprotected_intents": 0,
        "halted_manual_intents": 0,
        "failed_flat_intents": 0,
        "invalid_unit_rows": 0,
        "invalid_status_rows": 0,
        "invalid_environment_rows": 0,
        "invalid_contract_rows": 0,
        "invalid_signal_geometry_rows": 0,
        "invalid_accounting_rows": 0,
        "invalid_evidence_rows": 0,
        "incomplete_closed_rows": 0,
        "closed_net_pnl_usdt": 0.0,
    }
    if not metrics["db_exists"]:
        metrics["error"] = "versioned ledger does not exist"
        return metrics

    try:
        with closing(_read_only_connection(path)) as connection:
            metrics["db_readable"] = True
            integrity_row = connection.execute("PRAGMA integrity_check").fetchone()
            metrics["integrity_ok"] = bool(
                integrity_row and str(integrity_row[0]).strip().lower() == "ok"
            )

            columns = {
                str(row[1])
                for row in connection.execute(
                    "PRAGMA table_info(execution_intents)"
                ).fetchall()
            }
            missing = sorted(_REQUIRED_LEDGER_COLUMNS - columns)
            if missing:
                metrics["error"] = "missing ledger columns: " + ", ".join(missing)
                return metrics
            version_row = connection.execute("PRAGMA user_version").fetchone()
            if not version_row or int(version_row[0]) != SCHEMA_VERSION:
                metrics["error"] = "ledger schema version is not current"
                return metrics
            event_columns = {
                str(row[1])
                for row in connection.execute(
                    "PRAGMA table_info(execution_events)"
                ).fetchall()
            }
            required_event_columns = {
                "intent_id",
                "exec_id",
                "event_role",
                "source_hash",
                "transact_time_utc",
            }
            if not required_event_columns <= event_columns:
                metrics["error"] = "execution evidence schema is incomplete"
                return metrics
            triggers = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'trigger'"
                ).fetchall()
            }
            if not {
                "execution_events_append_only_update",
                "execution_events_append_only_delete",
            } <= triggers:
                metrics["error"] = "execution evidence is not append-only"
                return metrics
            validate_execution_ledger_schema(connection)
            metrics["schema_valid"] = True

            metrics["total_intents"] = _count(
                connection, "SELECT COUNT(*) FROM execution_intents"
            )
            metrics["closed_intents"] = _count(
                connection,
                "SELECT COUNT(*) FROM execution_intents WHERE status = 'CLOSED'",
            )
            active_placeholders = ", ".join("?" for _ in ACTIVE_STATUSES)
            metrics["active_intents"] = _count(
                connection,
                f"SELECT COUNT(*) FROM execution_intents WHERE status IN ({active_placeholders})",
                tuple(sorted(ACTIVE_STATUSES)),
            )
            unprotected_placeholders = ", ".join(
                "?" for _ in _UNPROTECTED_STATUSES
            )
            metrics["unprotected_intents"] = _count(
                connection,
                "SELECT COUNT(*) FROM execution_intents "
                f"WHERE status IN ({unprotected_placeholders})",
                _UNPROTECTED_STATUSES,
            )
            metrics["halted_manual_intents"] = _count(
                connection,
                "SELECT COUNT(*) FROM execution_intents WHERE status = 'HALTED_MANUAL'",
            )
            metrics["failed_flat_intents"] = _count(
                connection,
                "SELECT COUNT(*) FROM execution_intents WHERE status = 'FAILED_FLAT'",
            )
            metrics["invalid_unit_rows"] = _count(
                connection,
                "SELECT COUNT(*) FROM execution_intents WHERE unit_model != ?",
                (UNIT_MODEL,),
            )
            status_placeholders = ", ".join("?" for _ in ALL_STATUSES)
            metrics["invalid_status_rows"] = _count(
                connection,
                "SELECT COUNT(*) FROM execution_intents "
                f"WHERE status NOT IN ({status_placeholders})",
                tuple(sorted(ALL_STATUSES)),
            )
            metrics["invalid_environment_rows"] = _count(
                connection,
                "SELECT COUNT(*) FROM execution_intents "
                "WHERE environment NOT IN ('testnet', 'dry_run')",
            )
            metrics["invalid_contract_rows"] = _count(
                connection,
                "SELECT COUNT(*) FROM execution_intents "
                "WHERE requested_contracts <= 0 OR filled_contracts < 0 "
                "OR filled_contracts > requested_contracts OR closed_contracts < 0 "
                "OR closed_contracts > filled_contracts",
            )
            metrics["invalid_signal_geometry_rows"] = _count(
                connection,
                "SELECT COUNT(*) FROM execution_intents WHERE "
                "(side = 'LONG' AND NOT (signal_stop_price_usdt < signal_entry_price_usdt "
                "AND signal_entry_price_usdt < signal_target_price_usdt)) OR "
                "(side = 'SHORT' AND NOT (signal_target_price_usdt < signal_entry_price_usdt "
                "AND signal_entry_price_usdt < signal_stop_price_usdt)) OR "
                "side NOT IN ('LONG', 'SHORT')",
            )
            metrics["incomplete_closed_rows"] = _count(
                connection,
                "SELECT COUNT(*) FROM execution_intents WHERE "
                "(status = 'CLOSED' AND filled_contracts <= 0) OR "
                "(status IN ('CLOSED', 'FAILED_FLAT') AND filled_contracts > 0 AND ("
                "closed_contracts != filled_contracts OR actual_entry_price_usdt IS NULL "
                "OR actual_exit_price_usdt IS NULL OR gross_pnl_usdt IS NULL "
                "OR fees_usdt IS NULL OR funding_usdt IS NULL OR net_pnl_usdt IS NULL "
                "OR exit_reason IS NULL OR trim(exit_reason) = '' "
                "OR entry_time_utc IS NULL OR exit_time_utc IS NULL "
                "OR reconciled_at_utc IS NULL OR evidence_source_hash IS NULL))",
            )
            accounting_rows = connection.execute(
                "SELECT side, filled_contracts, contract_size_btc, "
                "actual_entry_price_usdt, actual_exit_price_usdt, gross_pnl_usdt, "
                "fees_usdt, funding_usdt, net_pnl_usdt FROM execution_intents "
                "WHERE status IN ('CLOSED', 'FAILED_FLAT') AND filled_contracts > 0"
            ).fetchall()
            for row in accounting_rows:
                values = (
                    row["contract_size_btc"],
                    row["actual_entry_price_usdt"],
                    row["actual_exit_price_usdt"],
                    row["gross_pnl_usdt"],
                    row["fees_usdt"],
                    row["funding_usdt"],
                    row["net_pnl_usdt"],
                )
                if any(value is None for value in values):
                    continue
                numbers = tuple(float(value) for value in values)
                if not all(math.isfinite(value) for value in numbers):
                    metrics["invalid_accounting_rows"] += 1
                    continue
                contract_size, entry, exit_price, gross, fees, funding, net = numbers
                direction = 1.0 if row["side"] == "LONG" else -1.0
                expected_gross = (
                    int(row["filled_contracts"])
                    * contract_size
                    * (exit_price - entry)
                    * direction
                )
                expected_net = gross - fees + funding
                gross_tolerance = max(1e-8, abs(expected_gross) * 1e-9)
                net_tolerance = max(1e-8, abs(expected_net) * 1e-9)
                if (
                    contract_size <= 0
                    or entry <= 0
                    or exit_price <= 0
                    or abs(gross - expected_gross) > gross_tolerance
                    or abs(net - expected_net) > net_tolerance
                ):
                    metrics["invalid_accounting_rows"] += 1
            evidence_rows = connection.execute(
                "SELECT * FROM execution_intents WHERE "
                "status IN ('CLOSED', 'FAILED_FLAT', 'HALTED_MANUAL') "
                "AND filled_contracts > 0"
            ).fetchall()
            for row in evidence_rows:
                try:
                    verify_terminal_execution_evidence(connection, row)
                except LedgerError:  # agent-quality: allow: per-row evidence failure increments an explicit failing readiness metric
                    metrics["invalid_evidence_rows"] += 1
            pnl_row = connection.execute(
                "SELECT COALESCE(SUM(net_pnl_usdt), 0) FROM execution_intents "
                "WHERE status = 'CLOSED'"
            ).fetchone()
            metrics["closed_net_pnl_usdt"] = float(pnl_row[0] or 0.0)
    except (LedgerError, OSError, sqlite3.Error, TypeError, ValueError) as exc:  # agent-quality: allow: failure is exposed in metrics.error and fails readiness
        metrics["error"] = f"{type(exc).__name__}: {exc}"
    return metrics


def _load_promotion_evidence(
    path: str,
    supplied: Any,
) -> tuple[Any, dict[str, Any]]:
    metadata = {
        "path": path,
        "source": "supplied" if supplied is not None else "file",
        "loaded": False,
        "error": "",
    }
    if supplied is not None:
        metadata["loaded"] = isinstance(supplied, Mapping)
        if not metadata["loaded"]:
            metadata["error"] = "supplied evidence is not a mapping"
        return supplied, metadata

    if not os.path.isfile(path):
        metadata["error"] = "promotion evidence file does not exist"
        return None, metadata
    try:
        with open(path, encoding="utf-8") as handle:
            evidence = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:  # agent-quality: allow: failure is exposed in evidence metadata and fails readiness
        metadata["error"] = f"{type(exc).__name__}: {exc}"
        return None, metadata
    metadata["loaded"] = isinstance(evidence, Mapping)
    if not metadata["loaded"]:
        metadata["error"] = "promotion evidence JSON must be an object"
    return evidence, metadata


def _gate(category: str, name: str, passed: bool, detail: str) -> dict[str, Any]:
    return {
        "category": category,
        "name": name,
        "status": "PASS" if passed else "FAIL",
        "required": True,
        "detail": detail,
    }


def _rule(name: str, triggered: bool, detail: str) -> dict[str, str]:
    return {
        "name": name,
        "status": "TRIGGERED" if triggered else "CLEAR",
        "detail": detail,
    }


def evaluate_live_readiness(
    trade_db_path: str = TRADE_DB_PATH,
    research_db_path: str | None = None,
    env: Mapping[str, str] | None = None,
    read_only: bool = True,
    now: datetime | None = None,
    *,
    promotion_evidence_path: str = PROMOTION_EVIDENCE_PATH,
    promotion_evidence: Any = None,
    legacy_trade_db_path: str = LEGACY_TRADE_DB_PATH,
) -> dict[str, Any]:
    """Evaluate evidence without mutating databases or enabling production.

    ``research_db_path`` and ``read_only`` remain accepted for API compatibility.
    Research scanner rows are not promotion evidence, and every database open is
    read-only regardless of the flag supplied.
    """

    del read_only
    env = os.environ if env is None else env
    now = now or _utc_now()
    ledger = _ledger_metrics(trade_db_path)
    evidence, evidence_meta = _load_promotion_evidence(
        promotion_evidence_path, promotion_evidence
    )
    promotion = evaluate_promotion(evidence)
    testnet_value = str(env.get("BITMEX_TESTNET", ""))

    invalid_rows = (
        ledger["invalid_unit_rows"]
        + ledger["invalid_status_rows"]
        + ledger["invalid_environment_rows"]
        + ledger["invalid_contract_rows"]
        + ledger["invalid_signal_geometry_rows"]
        + ledger["invalid_accounting_rows"]
        + ledger["invalid_evidence_rows"]
    )
    ledger_eligible = (
        ledger["db_readable"]
        and ledger["schema_valid"]
        and ledger["integrity_ok"]
        and not ledger["error"]
    )
    unassessable = "cannot assess until the versioned ledger is readable"
    gates = [
        _gate(
            "configuration",
            "Runtime remains testnet-only",
            testnet_value == "true",
            f"BITMEX_TESTNET={testnet_value!r}; must be exactly 'true'",
        ),
        _gate(
            "history_data",
            "Versioned execution ledger readable",
            ledger_eligible,
            ledger["error"]
            or "trades_v2.db schema and SQLite integrity check are valid",
        ),
        _gate(
            "history_data",
            "Versioned execution history present",
            ledger_eligible
            and ledger["total_intents"] > 0
            and ledger["closed_intents"] > 0,
            f"{ledger['total_intents']} total intents, {ledger['closed_intents']} closed",
        ),
        _gate(
            "history_data",
            "Ledger units and invariants clean",
            ledger_eligible and invalid_rows == 0,
            (
                f"invalid units={ledger['invalid_unit_rows']}, "
                f"statuses={ledger['invalid_status_rows']}, "
                f"environments={ledger['invalid_environment_rows']}, "
                f"contracts={ledger['invalid_contract_rows']}, "
                f"signal geometry={ledger['invalid_signal_geometry_rows']}, "
                f"accounting={ledger['invalid_accounting_rows']}, "
                f"evidence={ledger['invalid_evidence_rows']}"
            )
            if ledger_eligible
            else unassessable,
        ),
        _gate(
            "history_data",
            "No manual-halt incidents unresolved",
            ledger_eligible and ledger["halted_manual_intents"] == 0,
            f"{ledger['halted_manual_intents']} HALTED_MANUAL intent(s)"
            if ledger_eligible
            else unassessable,
        ),
        _gate(
            "history_data",
            "No open execution intents at audit snapshot",
            ledger_eligible and ledger["active_intents"] == 0,
            (
                f"{ledger['active_intents']} active intent(s), including "
                f"{ledger['unprotected_intents']} unprotected"
            )
            if ledger_eligible
            else unassessable,
        ),
        _gate(
            "history_data",
            "Closed-intent accounting complete",
            ledger_eligible
            and ledger["closed_intents"] > 0
            and ledger["incomplete_closed_rows"] == 0,
            (
                f"{ledger['incomplete_closed_rows']} incomplete of "
                f"{ledger['closed_intents']} closed intent(s)"
            )
            if ledger_eligible
            else unassessable,
        ),
        _gate(
            "promotion_evidence",
            "Promotion evidence loaded",
            evidence_meta["loaded"],
            evidence_meta["error"] or f"loaded from {evidence_meta['source']}",
        ),
        _gate(
            "promotion_evidence",
            "All automated promotion phases complete",
            promotion["stage"] == PromotionStage.CANARY_REVIEW.value,
            (
                f"highest eligible stage {promotion['stage']}; "
                f"{len(promotion['blockers'])} blocker(s)"
            ),
        ),
        _gate(
            "code_safety",
            "Production enablement remains absent",
            promotion["production_enabled"] is False,
            "promotion evaluator cannot enable production trading",
        ),
    ]

    critical_ledger_breach = any(
        (
            ledger["halted_manual_intents"] > 0,
            ledger["unprotected_intents"] > 0,
            invalid_rows > 0,
        )
    )
    hard_fail = any(gate["status"] == "FAIL" for gate in gates)
    if critical_ledger_breach:
        verdict = "REJECT_DO_NOT_PROMOTE"
        headline = "Execution history contains a safety-critical blocker."
        decision = "Keep real funds disabled and reconcile the v2 ledger before further promotion."
    elif hard_fail:
        verdict = "NOT_READY"
        headline = "Evidence is incomplete for a real-funds canary review."
        decision = "Keep real funds disabled. Satisfy the listed history and promotion gates first."
    else:
        verdict = "READY_FOR_CANARY_REVIEW"
        headline = "Automated evidence is complete for human canary review only."
        decision = (
            "Production remains disabled. A separate human decision and controlled deployment "
            "are still required."
        )

    failed_gates = [gate for gate in gates if gate["status"] == "FAIL"]
    return {
        "verdict": verdict,
        "headline": headline,
        "decision": decision,
        "failed_gates": len(failed_gates),
        "total_gates": len(gates),
        "history_data_blockers": [
            gate["name"]
            for gate in failed_gates
            if gate["category"] in {"history_data", "promotion_evidence"}
        ],
        "gates": gates,
        "no_go_rules": [
            _rule(
                "Safety-critical ledger breach",
                critical_ledger_breach,
                "Manual halts, unprotected intents, or invalid units/invariants block promotion.",
            ),
            _rule(
                "Required gate failure",
                hard_fail,
                "Every required configuration, history, and promotion gate must pass.",
            ),
        ],
        "promotion": promotion,
        "metrics": {
            "ledger_v2": ledger,
            "promotion_evidence": evidence_meta,
            "legacy_trades": {
                "path": legacy_trade_db_path,
                "exists": os.path.isfile(legacy_trade_db_path),
                "excluded_from_evidence": True,
                "reason": "legacy rows mix BTC quantities and contract counts",
            },
            "research_db": {
                "path": research_db_path,
                "exists": bool(research_db_path and os.path.isfile(research_db_path)),
                "excluded_from_promotion_evidence": True,
            },
        },
        "production_enabled": False,
        "generated_at": now.replace(microsecond=0).isoformat(),
    }


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Print full JSON.")
    parser.add_argument(
        "--ledger",
        default=TRADE_DB_PATH,
        help="Path to the versioned execution ledger.",
    )
    parser.add_argument(
        "--promotion-evidence",
        default=PROMOTION_EVIDENCE_PATH,
        help="Path to explicit promotion evidence JSON.",
    )
    parser.add_argument(
        "--read-only",
        action="store_true",
        help="Compatibility flag; database access is always read-only.",
    )
    args = parser.parse_args()

    result = evaluate_live_readiness(
        trade_db_path=args.ledger,
        promotion_evidence_path=args.promotion_evidence,
        read_only=True,
    )
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"{result['verdict']}: {result['headline']}")
        print(result["decision"])
        for gate in result["gates"]:
            print(
                f"- {gate['status']} [{gate['category']}]: "
                f"{gate['name']} - {gate['detail']}"
            )
        legacy = result["metrics"]["legacy_trades"]
        print(
            f"- INFO [excluded]: {legacy['path']} is not readiness evidence: "
            f"{legacy['reason']}"
        )
    return 0 if result["verdict"] == "READY_FOR_CANARY_REVIEW" else 1


if __name__ == "__main__":
    raise SystemExit(main())
