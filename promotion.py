"""Fail-closed evidence gate for BitMexBot strategy promotion.

The highest automated result is ``CANARY_REVIEW``.  This module deliberately
contains no exchange integration and no function that can enable production
trading.  A separate human decision and a separately controlled deployment
mechanism are required after every automated gate passes.

Expected evidence shape::

    {
        "research": {
            "independent_costed_oos_clusters": 200,
            "untouched_oos_clusters": 100,
            "oos_folds": 3,
            "acceptable_oos_folds": 3,
            "latest_oos_fold_positive": True,
            "bootstrap_expectancy_lower_bound": 0.01,
            "profit_factor_lower_bound": 1.01,
            "deflated_sharpe_probability": 0.95,
            "probability_of_backtest_overfitting": 0.20,
            "max_single_cluster_pnl_share": 0.20,
            "top_five_cluster_pnl_share": 0.50,
            "two_x_cost_net_expectancy": 0.01,
        },
        "shadow": {
            "consecutive_days": 90,
            "matured_clusters": 50,
            "order_authority_disabled": True,
        },
        "testnet_engineering": {
            "passed_lifecycle_drills": 100,
            "required_scenarios_passed": True,
            "zero_unreconciled_incidents": True,
        },
        "mainnet_dry_run": {
            "consecutive_days": 30,
            "reconciliation_differences": 0,
            "incomplete_candle_decisions": 0,
            "order_authority_disabled": True,
            "alert_drills_passed": True,
            "credential_rotation_drill_passed": True,
            "operator_response_drills_passed": True,
        },
    }
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from enum import Enum
from typing import Any, Callable


class PromotionStage(str, Enum):
    """Automated evidence stages, ordered from least to most mature."""

    RESEARCH = "RESEARCH"
    SHADOW = "SHADOW"
    TESTNET_ENGINEERING = "TESTNET_ENGINEERING"
    MAINNET_DRY_RUN = "MAINNET_DRY_RUN"
    CANARY_REVIEW = "CANARY_REVIEW"


MIN_INDEPENDENT_COSTED_OOS_CLUSTERS = 200
MIN_UNTOUCHED_OOS_CLUSTERS = 100
MIN_OOS_FOLDS = 3
MIN_ACCEPTABLE_OOS_FOLDS = 3
MIN_DEFLATED_SHARPE_PROBABILITY = 0.95
MAX_PROBABILITY_BACKTEST_OVERFITTING = 0.20
MAX_SINGLE_CLUSTER_PNL_SHARE = 0.20
MAX_TOP_FIVE_CLUSTER_PNL_SHARE = 0.50
MIN_SHADOW_DAYS = 90
MIN_MATURED_SHADOW_CLUSTERS = 50
MIN_PASSED_LIFECYCLE_DRILLS = 100
MIN_MAINNET_DRY_RUN_DAYS = 30

HUMAN_APPROVAL_NOTICE = (
    "CANARY_REVIEW does not authorize or enable production trading; separate "
    "human approval and an external, separately controlled deployment mechanism "
    "are required."
)


def _is_finite_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def _is_non_negative_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _is_probability(value: Any) -> bool:
    return _is_finite_number(value) and 0.0 <= float(value) <= 1.0


def _gate(
    section: Mapping[str, Any],
    key: str,
    name: str,
    requirement: str,
    predicate: Callable[[Any], bool],
) -> dict[str, Any]:
    if key not in section:
        return {
            "name": name,
            "passed": False,
            "detail": f"missing {key}; requires {requirement}",
        }

    value = section[key]
    passed = predicate(value)
    return {
        "name": name,
        "passed": passed,
        "detail": (
            f"{key}={value!r}; requires {requirement}"
            if not passed
            else f"{key}={value!r} satisfies {requirement}"
        ),
    }


def _section(evidence: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    value = evidence.get(name)
    return value if isinstance(value, Mapping) else {}


def _research_gates(section: Mapping[str, Any]) -> list[dict[str, Any]]:
    gates = [
        _gate(
            section,
            "independent_costed_oos_clusters",
            "Independent costed OOS clusters",
            f">= {MIN_INDEPENDENT_COSTED_OOS_CLUSTERS}",
            lambda value: _is_non_negative_integer(value)
            and value >= MIN_INDEPENDENT_COSTED_OOS_CLUSTERS,
        ),
        _gate(
            section,
            "untouched_oos_clusters",
            "Untouched OOS lockbox clusters",
            f">= {MIN_UNTOUCHED_OOS_CLUSTERS}",
            lambda value: _is_non_negative_integer(value)
            and value >= MIN_UNTOUCHED_OOS_CLUSTERS,
        ),
        _gate(
            section,
            "oos_folds",
            "Chronological OOS folds",
            f">= {MIN_OOS_FOLDS}",
            lambda value: _is_non_negative_integer(value) and value >= MIN_OOS_FOLDS,
        ),
        _gate(
            section,
            "acceptable_oos_folds",
            "Positive or acceptable OOS folds",
            f">= {MIN_ACCEPTABLE_OOS_FOLDS}",
            lambda value: _is_non_negative_integer(value)
            and value >= MIN_ACCEPTABLE_OOS_FOLDS,
        ),
        _gate(
            section,
            "latest_oos_fold_positive",
            "Latest OOS fold",
            "exactly True",
            lambda value: value is True,
        ),
        _gate(
            section,
            "bootstrap_expectancy_lower_bound",
            "Bootstrap net-expectancy lower bound",
            "> 0 after costs",
            lambda value: _is_finite_number(value) and float(value) > 0.0,
        ),
        _gate(
            section,
            "profit_factor_lower_bound",
            "Profit-factor lower bound",
            "> 1",
            lambda value: _is_finite_number(value) and float(value) > 1.0,
        ),
        _gate(
            section,
            "deflated_sharpe_probability",
            "Deflated Sharpe confidence",
            f">= {MIN_DEFLATED_SHARPE_PROBABILITY:.2f}",
            lambda value: _is_probability(value)
            and float(value) >= MIN_DEFLATED_SHARPE_PROBABILITY,
        ),
        _gate(
            section,
            "probability_of_backtest_overfitting",
            "Probability of backtest overfitting",
            f"<= {MAX_PROBABILITY_BACKTEST_OVERFITTING:.2f}",
            lambda value: _is_probability(value)
            and float(value) <= MAX_PROBABILITY_BACKTEST_OVERFITTING,
        ),
        _gate(
            section,
            "max_single_cluster_pnl_share",
            "Single-cluster PnL concentration",
            f"<= {MAX_SINGLE_CLUSTER_PNL_SHARE:.2f}",
            lambda value: _is_probability(value)
            and float(value) <= MAX_SINGLE_CLUSTER_PNL_SHARE,
        ),
        _gate(
            section,
            "top_five_cluster_pnl_share",
            "Top-five-cluster PnL concentration",
            f"<= {MAX_TOP_FIVE_CLUSTER_PNL_SHARE:.2f}",
            lambda value: _is_probability(value)
            and float(value) <= MAX_TOP_FIVE_CLUSTER_PNL_SHARE,
        ),
        _gate(
            section,
            "two_x_cost_net_expectancy",
            "Two-times-cost stress",
            "> 0 net expectancy",
            lambda value: _is_finite_number(value) and float(value) > 0.0,
        ),
    ]

    total_clusters = section.get("independent_costed_oos_clusters")
    untouched_clusters = section.get("untouched_oos_clusters")
    cluster_counts_consistent = (
        _is_non_negative_integer(total_clusters)
        and _is_non_negative_integer(untouched_clusters)
        and untouched_clusters <= total_clusters
    )
    gates.append(
        {
            "name": "OOS cluster-count consistency",
            "passed": cluster_counts_consistent,
            "detail": (
                "untouched_oos_clusters must not exceed "
                "independent_costed_oos_clusters"
            ),
        }
    )

    total_folds = section.get("oos_folds")
    acceptable_folds = section.get("acceptable_oos_folds")
    all_folds_acceptable = (
        _is_non_negative_integer(total_folds)
        and _is_non_negative_integer(acceptable_folds)
        and acceptable_folds == total_folds
    )
    gates.append(
        {
            "name": "All OOS folds acceptable",
            "passed": all_folds_acceptable,
            "detail": "acceptable_oos_folds must equal oos_folds",
        }
    )
    return gates


def _shadow_gates(section: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        _gate(
            section,
            "consecutive_days",
            "Shadow observation duration",
            f">= {MIN_SHADOW_DAYS} consecutive days",
            lambda value: _is_non_negative_integer(value) and value >= MIN_SHADOW_DAYS,
        ),
        _gate(
            section,
            "matured_clusters",
            "Matured forward shadow clusters",
            f">= {MIN_MATURED_SHADOW_CLUSTERS}",
            lambda value: _is_non_negative_integer(value)
            and value >= MIN_MATURED_SHADOW_CLUSTERS,
        ),
        _gate(
            section,
            "order_authority_disabled",
            "Shadow order authority disabled",
            "exactly True",
            lambda value: value is True,
        ),
    ]


def _testnet_gates(section: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        _gate(
            section,
            "passed_lifecycle_drills",
            "Passed deterministic lifecycle drills",
            f">= {MIN_PASSED_LIFECYCLE_DRILLS}",
            lambda value: _is_non_negative_integer(value)
            and value >= MIN_PASSED_LIFECYCLE_DRILLS,
        ),
        _gate(
            section,
            "required_scenarios_passed",
            "Required testnet scenarios",
            "exactly True",
            lambda value: value is True,
        ),
        _gate(
            section,
            "zero_unreconciled_incidents",
            "Zero unreconciled testnet incidents",
            "exactly True",
            lambda value: value is True,
        ),
    ]


def _dry_run_gates(section: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        _gate(
            section,
            "consecutive_days",
            "Mainnet dry-run duration",
            f">= {MIN_MAINNET_DRY_RUN_DAYS} consecutive days",
            lambda value: _is_non_negative_integer(value)
            and value >= MIN_MAINNET_DRY_RUN_DAYS,
        ),
        _gate(
            section,
            "reconciliation_differences",
            "Mainnet reconciliation differences",
            "exactly 0",
            lambda value: _is_non_negative_integer(value) and value == 0,
        ),
        _gate(
            section,
            "incomplete_candle_decisions",
            "Incomplete-candle decisions",
            "exactly 0",
            lambda value: _is_non_negative_integer(value) and value == 0,
        ),
        _gate(
            section,
            "order_authority_disabled",
            "Mainnet dry-run order authority disabled",
            "exactly True",
            lambda value: value is True,
        ),
        _gate(
            section,
            "alert_drills_passed",
            "Alert drills",
            "exactly True",
            lambda value: value is True,
        ),
        _gate(
            section,
            "credential_rotation_drill_passed",
            "Credential rotation drill",
            "exactly True",
            lambda value: value is True,
        ),
        _gate(
            section,
            "operator_response_drills_passed",
            "Operator response drills",
            "exactly True",
            lambda value: value is True,
        ),
    ]


def _failed_details(gates: list[dict[str, Any]]) -> list[str]:
    return [gate["detail"] for gate in gates if not gate["passed"]]


def evaluate_promotion(evidence: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return the highest evidence stage without ever enabling production.

    Missing sections, missing values, malformed types, booleans supplied as
    counts, and non-finite numbers all fail closed at the earliest affected
    stage.  ``blockers`` contains the unmet requirements for the next stage.
    """

    if not isinstance(evidence, Mapping):
        return {
            "stage": PromotionStage.RESEARCH.value,
            "highest_eligible_stage": PromotionStage.RESEARCH.value,
            "blockers": ["evidence must be a mapping with all required sections"],
            "gates": {},
            "human_approval_required": True,
            "production_enabled": False,
            "decision": "Remain in RESEARCH; malformed evidence fails closed.",
        }

    gates = {
        "research": _research_gates(_section(evidence, "research")),
        "shadow": _shadow_gates(_section(evidence, "shadow")),
        "testnet_engineering": _testnet_gates(
            _section(evidence, "testnet_engineering")
        ),
        "mainnet_dry_run": _dry_run_gates(
            _section(evidence, "mainnet_dry_run")
        ),
    }

    stage = PromotionStage.RESEARCH
    blockers = _failed_details(gates["research"])
    decision = "Remain in RESEARCH until every research gate passes."

    if not blockers:
        stage = PromotionStage.SHADOW
        blockers = _failed_details(gates["shadow"])
        decision = "Run forward shadow observation; no orders are authorized."

    if stage is PromotionStage.SHADOW and not blockers:
        stage = PromotionStage.TESTNET_ENGINEERING
        blockers = _failed_details(gates["testnet_engineering"])
        decision = "Complete testnet lifecycle drills; real funds remain prohibited."

    if stage is PromotionStage.TESTNET_ENGINEERING and not blockers:
        stage = PromotionStage.MAINNET_DRY_RUN
        blockers = _failed_details(gates["mainnet_dry_run"])
        decision = "Run mainnet data and order-intent checks without order authority."

    if stage is PromotionStage.MAINNET_DRY_RUN and not blockers:
        stage = PromotionStage.CANARY_REVIEW
        blockers = [HUMAN_APPROVAL_NOTICE]
        decision = HUMAN_APPROVAL_NOTICE

    return {
        "stage": stage.value,
        "highest_eligible_stage": stage.value,
        "blockers": blockers,
        "gates": gates,
        "human_approval_required": True,
        "production_enabled": False,
        "decision": decision,
    }


__all__ = ["PromotionStage", "evaluate_promotion"]
