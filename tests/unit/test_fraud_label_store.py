"""Tests for FraudLabelStore and R09 guilt-by-association rule."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from streamgraph.operators.fraud_label_store import FraudLabelStore
from streamgraph.operators.risk_scorer import compute_risk_score


# ---------------------------------------------------------------------------
# FraudLabelStore unit tests (Redis mocked)
# ---------------------------------------------------------------------------


def _store_with_mock_redis(existing_keys: set[str]) -> FraudLabelStore:
    store = FraudLabelStore(host="localhost")
    mock_client = MagicMock()
    mock_client.exists.side_effect = lambda key: 1 if key in existing_keys else 0
    mock_client.ping.return_value = True
    store._client = mock_client
    return store


class TestFraudLabelStore:
    def test_is_known_fraud_component_true(self):
        store = _store_with_mock_redis({"fraud:component:comp_abc"})
        assert store.is_known_fraud_component("comp_abc") is True

    def test_is_known_fraud_component_false(self):
        store = _store_with_mock_redis(set())
        assert store.is_known_fraud_component("comp_xyz") is False

    def test_is_known_fraud_entity_true(self):
        store = _store_with_mock_redis({"fraud:entity:acct_001"})
        assert store.is_known_fraud_entity("acct_001") is True

    def test_is_known_fraud_entity_false(self):
        store = _store_with_mock_redis(set())
        assert store.is_known_fraud_entity("acct_999") is False

    def test_fail_open_when_no_client(self):
        store = FraudLabelStore(host="localhost")
        store._client = None
        assert store.is_known_fraud_component("any") is False
        assert store.is_known_fraud_entity("any") is False

    def test_fail_open_on_redis_error(self):
        store = FraudLabelStore(host="localhost")
        mock_client = MagicMock()
        mock_client.exists.side_effect = Exception("connection refused")
        store._client = mock_client
        assert store.is_known_fraud_component("comp_1") is False

    def test_mark_fraud_writes_pipeline(self):
        store = FraudLabelStore(host="localhost")
        mock_client = MagicMock()
        mock_pipe = MagicMock()
        mock_client.pipeline.return_value = mock_pipe
        store._client = mock_client

        store.mark_fraud("comp_1", ["acct_001", "acct_002"], reason="confirmed chargeback")

        mock_client.pipeline.assert_called_once()
        mock_pipe.execute.assert_called_once()
        # component key + 2 entity keys = 3 setex calls
        assert mock_pipe.setex.call_count == 3


# ---------------------------------------------------------------------------
# R09 guilt-by-association in compute_risk_score
# ---------------------------------------------------------------------------


class TestR09KnownFraudNetwork:
    def test_no_label_store_no_r09(self):
        score = compute_risk_score("txn1", "acct1", "comp1", {}, label_store=None)
        assert "R09_known_fraud_network" not in score.triggered_rules
        assert "R09_known_fraud_network" not in score.feature_contributions

    def test_unknown_component_no_r09(self):
        store = _store_with_mock_redis(set())
        score = compute_risk_score("txn1", "acct1", "comp1", {}, label_store=store)
        assert "R09_known_fraud_network" not in score.triggered_rules

    def test_known_fraud_component_triggers_r09(self):
        store = _store_with_mock_redis({"fraud:component:comp_bad"})
        score = compute_risk_score("txn1", "acct1", "comp_bad", {}, label_store=store)
        assert "R09_known_fraud_network" in score.triggered_rules
        assert score.composite_score == pytest.approx(1.0)
        assert score.feature_contributions["R09_known_fraud_network"] == 1.0

    def test_r09_overrides_low_base_score(self):
        """Even a transaction that would score 0.0 on all other rules gets CRITICAL."""
        store = _store_with_mock_redis({"fraud:component:comp_bad"})
        score = compute_risk_score(
            "txn1", "acct1", "comp_bad",
            features={},   # all feature-based rules score 0
            label_store=store,
        )
        assert score.composite_score == pytest.approx(1.0)

    def test_r09_does_not_fire_for_clean_component(self):
        store = _store_with_mock_redis({"fraud:component:comp_other"})
        score = compute_risk_score("txn1", "acct1", "comp_clean", {}, label_store=store)
        assert "R09_known_fraud_network" not in score.triggered_rules
        assert score.composite_score == pytest.approx(0.0)
