"""
End-to-end pipeline throughput benchmark (no Flink; pure-Python path).

Measures the throughput of the risk-scoring and entity-resolution logic when
called in a tight loop — useful for sizing the Flink parallelism setting.

Run:
    python benchmarks/bench_pipeline.py
"""

from __future__ import annotations

import json
import random
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

# Make src importable when run directly
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))

from streamgraph.domain.models import Transaction
from streamgraph.graph.union_find import LocalUnionFind
from streamgraph.operators.risk_scorer import compute_risk_score
from generator.fraud_ring_generator import (
    EntityPool,
    generate_fraud_rings,
    _legitimate_transactions,
    _ring_transactions,
)


def bench_entity_resolution(num_txns: int = 100_000, seed: int = 42) -> float:
    """
    Simulate the entity-resolution operator: parse transactions, extract edges,
    union into a local UnionFind.

    Returns throughput in transactions/second.
    """
    pool = EntityPool.generate(10_000, 2_000, seed=seed)
    rings = generate_fraud_rings(pool, num_rings=20, seed=seed)

    import numpy as np
    rng = random.Random(seed)
    np_rng = np.random.default_rng(seed)

    # Pre-generate transactions
    txns: list[Transaction] = []
    legit = list(_legitimate_transactions(pool, int(num_txns * 0.9), rng, np_rng))
    fraud: list[dict] = []
    for ring in rings[:5]:
        fraud += list(_ring_transactions(ring, 2, rng, np_rng))

    all_raw = (legit + fraud[:int(num_txns * 0.1)])[:num_txns]

    uf = LocalUnionFind()

    t0 = time.perf_counter()
    for raw in all_raw:
        txn = Transaction(**raw)
        for edge in txn.entity_edges():
            uf.union(edge.source_id, edge.target_id, transaction_id=txn.transaction_id)
    elapsed = time.perf_counter() - t0

    return len(all_raw) / elapsed


def bench_risk_scoring(num_txns: int = 100_000, seed: int = 42) -> float:
    """
    Simulate the risk-scoring operator with realistic feature vectors.

    Returns throughput in scored transactions/second.
    """
    rng = random.Random(seed)
    import numpy as np
    np_rng = np.random.default_rng(seed)

    def _rand_features() -> dict:
        return {
            "txn_velocity_1h": rng.randint(0, 80),
            "txn_velocity_24h": rng.randint(0, 300),
            "amount_mean_30d": rng.uniform(10, 500),
            "amount_stddev_30d": rng.uniform(5, 200),
            "amount_zscore": float(np_rng.normal(0, 1)),
            "unique_merchants_30d": rng.randint(1, 50),
            "unique_devices_30d": rng.randint(1, 10),
            "unique_ips_30d": rng.randint(1, 20),
            "cross_border_ratio": rng.random(),
            "night_txn_ratio": rng.random(),
            "card_testing_score": rng.random() * 0.3,
            "mule_account_score": rng.random() * 0.2,
        }

    t0 = time.perf_counter()
    for _ in range(num_txns):
        compute_risk_score(
            transaction_id=str(uuid.uuid4()),
            account_id=f"acct_{rng.randint(0, 9999):04d}",
            component_id=f"comp_{rng.randint(0, 99):02d}",
            features=_rand_features(),
        )
    elapsed = time.perf_counter() - t0
    return num_txns / elapsed


if __name__ == "__main__":
    print("StreamGraph Pipeline Benchmarks")
    print("=" * 50)

    SIZES = [10_000, 100_000, 500_000]

    print("\nEntity Resolution (Union-Find) Throughput")
    print(f"  {'N':>10}  {'txns/s':>14}")
    for n in SIZES:
        tps = bench_entity_resolution(n)
        print(f"  {n:>10,}  {tps:>14,.0f}")

    print("\nRisk Scoring Throughput")
    print(f"  {'N':>10}  {'scores/s':>14}")
    for n in SIZES:
        tps = bench_risk_scoring(n)
        print(f"  {n:>10,}  {tps:>14,.0f}")
