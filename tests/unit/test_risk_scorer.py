"""Unit tests for the rule-based risk scorer."""

from __future__ import annotations

from datetime import datetime

import pytest

from streamgraph.domain.models import ComponentSnapshot, EntityType
from streamgraph.operators.risk_scorer import compute_risk_score


def _make_component(size: int, edge_count: int | None = None) -> ComponentSnapshot:
    n = size
    ec = edge_count if edge_count is not None else n - 1
    return ComponentSnapshot(
        component_id="root",
        member_ids=frozenset(f"a{i}" for i in range(n)),
        size=n,
        edge_count=ec,
        first_seen=datetime.utcnow(),
        last_updated=datetime.utcnow(),
        entity_types=frozenset([EntityType.ACCOUNT]),
        transaction_ids=frozenset(),
    )


class TestRiskScorerEdgeCases:
    def test_zero_features_zero_score(self) -> None:
        score = compute_risk_score("txn1", "acc1", "comp1", {})
        assert score.composite_score == 0.0
        assert score.triggered_rules == []

    def test_large_component_triggers_rule(self) -> None:
        comp = _make_component(35)
        score = compute_risk_score("txn1", "acc1", "comp1", {}, component=comp)
        assert "R01_large_component" in score.triggered_rules
        assert score.composite_score > 0.0

    def test_critical_component_gives_high_score(self) -> None:
        comp = _make_component(50)
        score = compute_risk_score("txn1", "acc1", "comp1", {}, component=comp)
        # R01 alone contributes 0.30 * 1.0 = 0.30
        assert score.composite_score >= 0.25

    def test_high_velocity_triggers_rule(self) -> None:
        features = {"txn_velocity_1h": 60}
        score = compute_risk_score("txn1", "acc1", "comp1", features)
        assert "R02_high_velocity" in score.triggered_rules

    def test_amount_anomaly_triggers_rule(self) -> None:
        features = {"amount_zscore": 6.0}
        score = compute_risk_score("txn1", "acc1", "comp1", features)
        assert "R03_amount_anomaly" in score.triggered_rules

    def test_card_testing_triggers_rule(self) -> None:
        features = {"card_testing_score": 0.95}
        score = compute_risk_score("txn1", "acc1", "comp1", features)
        assert "R04_card_testing" in score.triggered_rules

    def test_composite_score_capped_at_one(self) -> None:
        # All rules firing simultaneously
        comp = _make_component(50)
        features = {
            "txn_velocity_1h": 100,
            "amount_zscore": 8.0,
            "card_testing_score": 1.0,
            "cross_border_ratio": 1.0,
            "night_txn_ratio": 1.0,
            "unique_devices_30d": 20,
            "txn_velocity_24h": 200,
            "mule_account_score": 1.0,
        }
        score = compute_risk_score("txn1", "acc1", "comp1", features, component=comp)
        assert score.composite_score <= 1.0

    def test_feature_contributions_sum_to_composite(self) -> None:
        features = {"txn_velocity_1h": 60, "amount_zscore": 4.0}
        score = compute_risk_score("txn1", "acc1", "comp1", features)
        contribution_sum = sum(score.feature_contributions.values())
        assert abs(contribution_sum - score.composite_score) < 1e-3

    def test_score_fields_populated(self) -> None:
        score = compute_risk_score("txn-abc", "acc-xyz", "comp-123", {})
        assert score.transaction_id == "txn-abc"
        assert score.account_id == "acc-xyz"
        assert score.component_id == "comp-123"
        assert score.model_version == "rules-v1"
        assert isinstance(score.evaluated_at, datetime)


class TestRiskScorerMonotonicity:
    """Larger/more-severe inputs should produce higher scores."""

    def test_larger_component_higher_score(self) -> None:
        s_small = compute_risk_score("t", "a", "c", {}, component=_make_component(3))
        s_large = compute_risk_score("t", "a", "c", {}, component=_make_component(35))
        assert s_large.composite_score >= s_small.composite_score

    def test_higher_velocity_higher_score(self) -> None:
        s_low = compute_risk_score("t", "a", "c", {"txn_velocity_1h": 5})
        s_high = compute_risk_score("t", "a", "c", {"txn_velocity_1h": 80})
        assert s_high.composite_score > s_low.composite_score

    def test_higher_zscore_higher_score(self) -> None:
        s_normal = compute_risk_score("t", "a", "c", {"amount_zscore": 1.0})
        s_anomaly = compute_risk_score("t", "a", "c", {"amount_zscore": 7.0})
        assert s_anomaly.composite_score > s_normal.composite_score
