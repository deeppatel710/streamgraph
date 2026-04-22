"""
Benchmarks for LocalUnionFind.

Run with:
    pytest benchmarks/bench_union_find.py --benchmark-sort=mean -v

Or standalone (no pytest-benchmark required):
    python benchmarks/bench_union_find.py
"""

from __future__ import annotations

import random
import time
from typing import Callable

from streamgraph.graph.union_find import LocalUnionFind


# ---------------------------------------------------------------------------
# Benchmark helpers
# ---------------------------------------------------------------------------


def _chain_benchmark(n: int) -> None:
    """Sequential chain: 0-1-2-…-(n-1).  Worst case before path compression."""
    uf = LocalUnionFind()
    for i in range(n - 1):
        uf.union(str(i), str(i + 1))
    # Force full traversal on the last node
    uf.find(str(n - 1))


def _random_union_benchmark(n: int, seed: int = 42) -> None:
    """n random unions on n//2 nodes, then n//2 random finds."""
    nodes = [str(i) for i in range(n // 2)]
    rng = random.Random(seed)
    uf = LocalUnionFind()
    for _ in range(n):
        a = rng.choice(nodes)
        b = rng.choice(nodes)
        uf.union(a, b)
    for _ in range(n // 2):
        uf.find(rng.choice(nodes))


def _fraud_ring_benchmark(num_rings: int = 100, ring_size: int = 20) -> None:
    """Simulate fraud ring detection: dense intra-ring unions, sparse inter-ring."""
    uf = LocalUnionFind()
    rng = random.Random(0)
    # Create rings
    rings = [
        [f"ring{r}_acct{a}" for a in range(ring_size)]
        for r in range(num_rings)
    ]
    # Intra-ring unions (dense)
    for ring in rings:
        for i in range(len(ring)):
            for j in range(i + 1, min(i + 4, len(ring))):
                uf.union(ring[i], ring[j])
    # Sparse cross-ring connections (5 % of rings connected)
    for _ in range(num_rings // 20):
        r1, r2 = rng.sample(rings, 2)
        uf.union(rng.choice(r1), rng.choice(r2))
    # Query all
    for ring in rings:
        for node in ring:
            uf.component_size(node)


# ---------------------------------------------------------------------------
# pytest-benchmark interface
# ---------------------------------------------------------------------------


def test_bench_chain_1k(benchmark: Callable) -> None:
    benchmark(_chain_benchmark, 1_000)


def test_bench_chain_10k(benchmark: Callable) -> None:
    benchmark(_chain_benchmark, 10_000)


def test_bench_random_unions_10k(benchmark: Callable) -> None:
    benchmark(_random_union_benchmark, 10_000)


def test_bench_random_unions_100k(benchmark: Callable) -> None:
    benchmark(_random_union_benchmark, 100_000)


def test_bench_fraud_rings_100x20(benchmark: Callable) -> None:
    benchmark(_fraud_ring_benchmark, 100, 20)


def test_bench_fraud_rings_500x25(benchmark: Callable) -> None:
    benchmark(_fraud_ring_benchmark, 500, 25)


# ---------------------------------------------------------------------------
# Standalone runner (no pytest)
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    cases: list[tuple[str, Callable, tuple]] = [
        ("chain 1k",             _chain_benchmark,      (1_000,)),
        ("chain 10k",            _chain_benchmark,      (10_000,)),
        ("chain 100k",           _chain_benchmark,      (100_000,)),
        ("random unions 10k",    _random_union_benchmark, (10_000,)),
        ("random unions 100k",   _random_union_benchmark, (100_000,)),
        ("random unions 1M",     _random_union_benchmark, (1_000_000,)),
        ("fraud rings 100×20",   _fraud_ring_benchmark,  (100, 20)),
        ("fraud rings 500×25",   _fraud_ring_benchmark,  (500, 25)),
        ("fraud rings 2000×30",  _fraud_ring_benchmark,  (2_000, 30)),
    ]

    print(f"{'Case':<30}  {'Mean (ms)':>12}  {'ops/s':>12}")
    print("-" * 58)
    RUNS = 5
    for name, fn, args in cases:
        times = []
        for _ in range(RUNS):
            t0 = time.perf_counter()
            fn(*args)
            times.append(time.perf_counter() - t0)
        mean_ms = (sum(times) / RUNS) * 1000
        ops_per_s = 1000 / mean_ms
        print(f"{name:<30}  {mean_ms:>12.2f}  {ops_per_s:>12.1f}")
