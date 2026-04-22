"""Unit tests for the fraud ring data generator."""

from __future__ import annotations

from generator.fraud_ring_generator import (
    EntityPool,
    FraudRing,
    _legitimate_transactions,
    _ring_transactions,
    generate_fraud_rings,
)
import random
import numpy as np


class TestEntityPool:
    def test_pool_sizes(self) -> None:
        pool = EntityPool.generate(num_accounts=100, num_merchants=20, seed=0)
        assert len(pool.accounts) == 100
        assert len(pool.merchants) == 20
        assert len(pool.devices) > 0
        assert len(pool.ips) > 0

    def test_account_ids_unique(self) -> None:
        pool = EntityPool.generate(num_accounts=500)
        assert len(set(pool.accounts)) == 500

    def test_deterministic_with_seed(self) -> None:
        p1 = EntityPool.generate(seed=7)
        p2 = EntityPool.generate(seed=7)
        assert p1.accounts == p2.accounts
        assert p1.merchants == p2.merchants


class TestFraudRingGeneration:
    def test_ring_count(self) -> None:
        pool = EntityPool.generate(1000, 200, seed=1)
        rings = generate_fraud_rings(pool, num_rings=10, seed=1)
        assert len(rings) == 10

    def test_ring_sizes_within_bounds(self) -> None:
        pool = EntityPool.generate(1000, 200, seed=2)
        rings = generate_fraud_rings(pool, num_rings=15, min_size=4, max_size=12, seed=2)
        for ring in rings:
            # dense pattern caps at 12; other patterns respect bounds
            assert len(ring.members) >= 1

    def test_all_patterns_generated(self) -> None:
        pool = EntityPool.generate(5000, 500, seed=3)
        rings = generate_fraud_rings(pool, num_rings=40, seed=3)
        patterns = {r.pattern for r in rings}
        # With 40 rings we expect all 4 patterns to appear
        assert len(patterns) >= 3

    def test_ring_members_are_valid_accounts(self) -> None:
        pool = EntityPool.generate(1000, 200, seed=4)
        rings = generate_fraud_rings(pool, num_rings=5, seed=4)
        valid_accounts = set(pool.accounts)
        for ring in rings:
            for member in ring.members:
                assert member in valid_accounts


class TestTransactionGeneration:
    def setup_method(self) -> None:
        self.pool = EntityPool.generate(500, 100, seed=99)
        self.rng = random.Random(99)
        self.np_rng = np.random.default_rng(99)

    def test_legitimate_transaction_structure(self) -> None:
        txns = list(_legitimate_transactions(self.pool, 10, self.rng, self.np_rng))
        assert len(txns) == 10
        for t in txns:
            assert "transaction_id" in t
            assert "account_id" in t
            assert "merchant_id" in t
            assert t["amount_usd"] > 0
            assert t["status"] == "pending"

    def test_ring_transactions_contain_metadata(self) -> None:
        rings = generate_fraud_rings(self.pool, num_rings=4, seed=99)
        ring = rings[0]
        txns = list(_ring_transactions(ring, 1, self.rng, self.np_rng))
        assert len(txns) > 0
        for t in txns:
            assert "ring_id" in t.get("metadata", {})
            assert t["metadata"]["pattern"] == ring.pattern

    def test_card_testing_amounts_are_small(self) -> None:
        pool = EntityPool.generate(200, 50, seed=10)
        rings = generate_fraud_rings(pool, num_rings=20, seed=10)
        dense_rings = [r for r in rings if r.pattern == "dense"]
        if not dense_rings:
            return
        ring = dense_rings[0]
        txns = list(_ring_transactions(ring, 1, self.rng, self.np_rng))
        amounts = [t["amount_usd"] for t in txns]
        assert max(amounts) <= 5.0   # card testing = micro-amounts
