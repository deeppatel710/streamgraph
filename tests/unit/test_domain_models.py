"""Unit tests for domain models."""

from __future__ import annotations

from datetime import datetime

import pytest

from streamgraph.domain.models import (
    AlertSeverity,
    ComponentSnapshot,
    EntityEdge,
    EntityType,
    FraudAlert,
    RelationshipType,
    RiskScore,
    Transaction,
    TransactionStatus,
)


class TestTransaction:
    def test_minimal_transaction(self) -> None:
        txn = Transaction(
            timestamp=datetime.utcnow(),
            account_id="acct_001",
            merchant_id="merch_001",
            amount_usd=49.99,
        )
        assert txn.amount_usd == 49.99
        assert txn.status == TransactionStatus.PENDING

    def test_amount_rounded(self) -> None:
        txn = Transaction(
            timestamp=datetime.utcnow(),
            account_id="a",
            merchant_id="m",
            amount_usd=10.1234567,
        )
        assert txn.amount_usd == 10.12

    def test_negative_amount_raises(self) -> None:
        with pytest.raises(Exception):
            Transaction(
                timestamp=datetime.utcnow(),
                account_id="a",
                merchant_id="m",
                amount_usd=-1.0,
            )

    def test_entity_edges_minimal(self) -> None:
        txn = Transaction(
            timestamp=datetime.utcnow(),
            account_id="acct_001",
            merchant_id="merch_001",
            amount_usd=100.0,
        )
        edges = txn.entity_edges()
        assert len(edges) == 1
        assert edges[0].relationship == RelationshipType.TRANSACTED_AT

    def test_entity_edges_with_device_and_ip(self) -> None:
        txn = Transaction(
            timestamp=datetime.utcnow(),
            account_id="acct_001",
            merchant_id="merch_001",
            amount_usd=50.0,
            device_id="dev_123",
            ip_address="192.168.1.1",
            card_last4="4242",
        )
        edges = txn.entity_edges()
        assert len(edges) == 4
        rel_types = {e.relationship for e in edges}
        assert RelationshipType.TRANSACTED_AT in rel_types
        assert RelationshipType.USED_DEVICE in rel_types
        assert RelationshipType.USED_IP in rel_types
        assert RelationshipType.LINKED_CARD in rel_types

    def test_ip_edge_has_prefix(self) -> None:
        txn = Transaction(
            timestamp=datetime.utcnow(),
            account_id="a",
            merchant_id="m",
            amount_usd=1.0,
            ip_address="10.0.0.1",
        )
        ip_edges = [e for e in txn.entity_edges() if e.relationship == RelationshipType.USED_IP]
        assert ip_edges[0].target_id == "ip:10.0.0.1"

    def test_canonical_key_is_symmetric(self) -> None:
        ts = datetime.utcnow()
        e1 = EntityEdge(
            source_id="a", source_type=EntityType.ACCOUNT,
            target_id="b", target_type=EntityType.MERCHANT,
            relationship=RelationshipType.TRANSACTED_AT,
            timestamp=ts, transaction_id="txn1",
        )
        e2 = EntityEdge(
            source_id="b", source_type=EntityType.MERCHANT,
            target_id="a", target_type=EntityType.ACCOUNT,
            relationship=RelationshipType.TRANSACTED_AT,
            timestamp=ts, transaction_id="txn1",
        )
        assert e1.canonical_key == e2.canonical_key


class TestComponentSnapshot:
    def _make(self, size: int, edges: int) -> ComponentSnapshot:
        return ComponentSnapshot(
            component_id="root",
            member_ids=frozenset(f"m{i}" for i in range(size)),
            size=size,
            edge_count=edges,
            first_seen=datetime.utcnow(),
            last_updated=datetime.utcnow(),
            entity_types=frozenset([EntityType.ACCOUNT]),
            transaction_ids=frozenset(),
        )

    def test_density_singleton(self) -> None:
        snap = self._make(1, 0)
        assert snap.density == 0.0

    def test_density_complete_graph(self) -> None:
        n = 4
        snap = self._make(n, n * (n - 1) // 2)   # complete graph
        assert abs(snap.density - 1.0) < 1e-9

    def test_density_tree(self) -> None:
        n = 5
        snap = self._make(n, n - 1)   # spanning tree
        # tree density = 2(n-1) / n(n-1) = 2/n
        expected = 2 / n
        assert abs(snap.density - expected) < 1e-9


class TestFraudAlert:
    def _make_score(self, composite: float) -> RiskScore:
        return RiskScore(
            transaction_id="txn1",
            account_id="acc1",
            component_id="comp1",
            composite_score=composite,
            evaluated_at=datetime.utcnow(),
        )

    def test_critical_score_gives_critical_alert(self) -> None:
        alert = FraudAlert.from_risk_score(self._make_score(0.95))
        assert alert.severity == AlertSeverity.CRITICAL
        assert alert.recommended_action == "block_and_review"

    def test_high_score_gives_high_alert(self) -> None:
        alert = FraudAlert.from_risk_score(self._make_score(0.80))
        assert alert.severity == AlertSeverity.HIGH
        assert alert.recommended_action == "step_up_auth"

    def test_medium_score_gives_medium_alert(self) -> None:
        alert = FraudAlert.from_risk_score(self._make_score(0.60))
        assert alert.severity == AlertSeverity.MEDIUM

    def test_low_score_gives_low_alert(self) -> None:
        alert = FraudAlert.from_risk_score(self._make_score(0.40))
        assert alert.severity == AlertSeverity.LOW

    def test_alert_inherits_score_fields(self) -> None:
        score = self._make_score(0.85)
        alert = FraudAlert.from_risk_score(score)
        assert alert.transaction_id == "txn1"
        assert alert.account_id == "acc1"
        assert alert.composite_score == pytest.approx(0.85)
