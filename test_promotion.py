import math
import unittest

import promotion
from promotion import PromotionStage, evaluate_promotion


def exact_boundary_evidence():
    """Return evidence at inclusive boundaries and just above strict ones."""

    return {
        "research": {
            "independent_costed_oos_clusters": 200,
            "untouched_oos_clusters": 100,
            "oos_folds": 3,
            "acceptable_oos_folds": 3,
            "latest_oos_fold_positive": True,
            "bootstrap_expectancy_lower_bound": 1e-12,
            "profit_factor_lower_bound": 1.000000000001,
            "deflated_sharpe_probability": 0.95,
            "probability_of_backtest_overfitting": 0.20,
            "max_single_cluster_pnl_share": 0.20,
            "top_five_cluster_pnl_share": 0.50,
            "two_x_cost_net_expectancy": 1e-12,
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


class PromotionTests(unittest.TestCase):
    def test_missing_evidence_fails_closed_in_research(self):
        result = evaluate_promotion({})

        self.assertEqual(result["stage"], PromotionStage.RESEARCH.value)
        self.assertFalse(result["production_enabled"])
        self.assertTrue(result["human_approval_required"])
        self.assertGreater(len(result["blockers"]), 0)

    def test_malformed_evidence_fails_closed(self):
        malformed_values = [None, "not evidence", [], {"research": "invalid"}]
        for evidence in malformed_values:
            with self.subTest(evidence=evidence):
                result = evaluate_promotion(evidence)
                self.assertEqual(result["stage"], PromotionStage.RESEARCH.value)
                self.assertFalse(result["production_enabled"])
                self.assertGreater(len(result["blockers"]), 0)

    def test_non_finite_and_boolean_metrics_fail_closed(self):
        for key, value in (
            ("bootstrap_expectancy_lower_bound", math.nan),
            ("profit_factor_lower_bound", math.inf),
            ("independent_costed_oos_clusters", True),
        ):
            with self.subTest(key=key, value=value):
                evidence = exact_boundary_evidence()
                evidence["research"][key] = value
                result = evaluate_promotion(evidence)
                self.assertEqual(result["stage"], PromotionStage.RESEARCH.value)

    def test_contradictory_research_counts_fail_closed(self):
        inconsistent_values = (
            ("untouched_oos_clusters", 201),
            ("acceptable_oos_folds", 4),
        )
        for key, value in inconsistent_values:
            with self.subTest(key=key):
                evidence = exact_boundary_evidence()
                evidence["research"][key] = value
                result = evaluate_promotion(evidence)
                self.assertEqual(result["stage"], PromotionStage.RESEARCH.value)

    def test_every_oos_fold_must_be_acceptable(self):
        evidence = exact_boundary_evidence()
        evidence["research"]["oos_folds"] = 4

        result = evaluate_promotion(evidence)

        self.assertEqual(result["stage"], PromotionStage.RESEARCH.value)
        self.assertTrue(
            any("must equal" in blocker for blocker in result["blockers"])
        )

    def test_explicit_safety_evidence_fails_closed_when_false(self):
        cases = (
            ("shadow", "order_authority_disabled", PromotionStage.SHADOW),
            (
                "testnet_engineering",
                "required_scenarios_passed",
                PromotionStage.TESTNET_ENGINEERING,
            ),
            (
                "testnet_engineering",
                "zero_unreconciled_incidents",
                PromotionStage.TESTNET_ENGINEERING,
            ),
            (
                "mainnet_dry_run",
                "order_authority_disabled",
                PromotionStage.MAINNET_DRY_RUN,
            ),
            (
                "mainnet_dry_run",
                "alert_drills_passed",
                PromotionStage.MAINNET_DRY_RUN,
            ),
            (
                "mainnet_dry_run",
                "credential_rotation_drill_passed",
                PromotionStage.MAINNET_DRY_RUN,
            ),
            (
                "mainnet_dry_run",
                "operator_response_drills_passed",
                PromotionStage.MAINNET_DRY_RUN,
            ),
        )
        for section, key, expected_stage in cases:
            with self.subTest(section=section, key=key):
                evidence = exact_boundary_evidence()
                evidence[section][key] = False

                result = evaluate_promotion(evidence)

                self.assertEqual(result["stage"], expected_stage.value)
                self.assertTrue(any(key in item for item in result["blockers"]))

    def test_research_boundary_advances_only_to_shadow(self):
        evidence = exact_boundary_evidence()
        evidence.pop("shadow")

        result = evaluate_promotion(evidence)

        self.assertEqual(result["stage"], PromotionStage.SHADOW.value)
        self.assertTrue(any("consecutive_days" in item for item in result["blockers"]))
        self.assertFalse(result["production_enabled"])

    def test_each_completed_phase_advances_one_stage(self):
        evidence = exact_boundary_evidence()

        evidence.pop("testnet_engineering")
        result = evaluate_promotion(evidence)
        self.assertEqual(result["stage"], PromotionStage.TESTNET_ENGINEERING.value)

        evidence = exact_boundary_evidence()
        evidence.pop("mainnet_dry_run")
        result = evaluate_promotion(evidence)
        self.assertEqual(result["stage"], PromotionStage.MAINNET_DRY_RUN.value)

    def test_exact_boundaries_reach_canary_review_only(self):
        result = evaluate_promotion(exact_boundary_evidence())

        self.assertEqual(result["stage"], PromotionStage.CANARY_REVIEW.value)
        self.assertEqual(
            result["highest_eligible_stage"], PromotionStage.CANARY_REVIEW.value
        )
        self.assertFalse(result["production_enabled"])
        self.assertTrue(result["human_approval_required"])
        self.assertTrue(any("separate human approval" in item for item in result["blockers"]))

    def test_strict_positive_research_bounds_reject_zero_and_one(self):
        for key, value in (
            ("bootstrap_expectancy_lower_bound", 0.0),
            ("profit_factor_lower_bound", 1.0),
            ("two_x_cost_net_expectancy", 0.0),
        ):
            with self.subTest(key=key):
                evidence = exact_boundary_evidence()
                evidence["research"][key] = value
                result = evaluate_promotion(evidence)
                self.assertEqual(result["stage"], PromotionStage.RESEARCH.value)

    def test_no_production_enable_function_is_exposed(self):
        exposed_callables = {
            name
            for name in promotion.__all__
            if callable(getattr(promotion, name, None))
        }
        self.assertNotIn("enable_production", exposed_callables)
        self.assertEqual(
            promotion.__all__, ["PromotionStage", "evaluate_promotion"]
        )


if __name__ == "__main__":
    unittest.main()
