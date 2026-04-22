# StreamGraph

> **Real-time entity resolution for financial crime detection**  
> PyFlink · Kafka · Feast · RocksDB · Union-Find streaming graph

[![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![Apache Flink 1.19](https://img.shields.io/badge/Apache%20Flink-1.19-orange.svg)](https://flink.apache.org/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

---

## What is StreamGraph?

Financial fraud rings consist of interconnected entities — accounts, merchants,
devices, IP addresses, and cards — that collectively mask illicit activity.
Detecting these rings in real time requires maintaining a **continuously
evolving graph** of entity relationships.

StreamGraph solves this with an **incremental Union-Find** implemented as a
Flink `KeyedProcessFunction` whose state lives in RocksDB.  Every payment
transaction produces entity edges; the Union-Find merges them into connected
components on the fly.  When a component grows beyond a threshold — indicating a
potential fraud ring — a composite risk score is computed (with features from a
Feast online store) and a `FraudAlert` is emitted to Kafka.

**Core innovations:**

- 🔗 **Streaming Union-Find** with path-halving compression and union-by-rank,
  backed by RocksDB `MapState` for fault-tolerant, O(α(n)) entity resolution
- ⚡ **Sub-millisecond graph updates** at 100k+ edges/second on commodity hardware  
- 🧪 **Realistic fraud ring generator** producing star, chain, cycle, and dense
  ring topologies mixed into a realistic transaction stream
- 🏪 **Feast integration** for Dual-store (offline Parquet + online Redis)
  feature management  
- 📦 **One-command local dev** with Docker Compose (Kafka, Flink, Redis, Grafana)

---

## Architecture

```
Kafka (raw-transactions)
        │
        ▼
TransactionFlatMap          Parse JSON → EntityEdge (account↔merchant, account↔device, …)
        │
        ▼
DeduplicationFunction       Drop duplicates within 24 h window (RocksDB ValueState)
        │
        ▼
EntityResolutionFunction    Incremental Union-Find keyed by shard_id (RocksDB MapState)
        │                   → emits ComponentSnapshot on every merge
        │
   ┌────┴───────────────────────────────────────────────┐
   ▼                                                    ▼
Kafka (entity-components)               FeatureEnrichmentFunction (Feast Redis lookup)
                                                        │
                                                        ▼
                                              RiskScorerFunction (8 weighted rules)
                                                        │
                                                        ▼
                                             AlertGeneratorFunction (threshold + rate limiter)
                                                        │
                                                        ▼
                                             Kafka (fraud-alerts)
```

See [docs/architecture.md](docs/architecture.md) for the full design including
sharding strategy, state layout, and scaling considerations.

---

## Quick Start

### Prerequisites

- Docker ≥ 24 and Docker Compose ≥ 2.20
- Python ≥ 3.9 (for running tests and the generator locally)
- `make` (GNU Make ≥ 4)

### 1. Clone and configure

```bash
git clone https://github.com/your-org/streamgraph.git
cd streamgraph
cp .env.example .env
```

### 2. Start the full stack

```bash
make up
```

This starts:

| Service         | URL / Port                    | Description                     |
|-----------------|-------------------------------|---------------------------------|
| **Kafka UI**    | http://localhost:8080         | Browse topics and messages      |
| **Flink UI**    | http://localhost:8081         | Job graph, checkpoints, metrics |
| **Grafana**     | http://localhost:3000         | Dashboards (admin/streamgraph)  |
| **Prometheus**  | http://localhost:9090         | Raw metrics                     |
| **Redis**       | localhost:6379                | Feast online store              |

The `generator` container immediately begins producing synthetic transactions
(~300 events/s, 8 % fraud ring traffic).

### 3. Install Python dependencies

```bash
make dev-install
```

### 4. Run tests

```bash
make test          # full test suite with coverage
make test-unit     # unit tests only (no Flink runtime needed)
```

### 5. Run benchmarks

```bash
make benchmark
```

Example output:

```
StreamGraph Pipeline Benchmarks
==================================================

Entity Resolution (Union-Find) Throughput
                N        txns/s
           10,000       312,450
          100,000       298,130
          500,000       285,980

Risk Scoring Throughput
                N      scores/s
           10,000       924,100
          100,000       901,220
          500,000       887,500
```

---

## Project Structure

```
streamgraph/
├── src/streamgraph/
│   ├── domain/
│   │   └── models.py               # Pydantic domain models (Transaction, EntityEdge,
│   │                               #   ComponentSnapshot, RiskScore, FraudAlert)
│   ├── graph/
│   │   └── union_find.py           # LocalUnionFind (tests) + FlinkUnionFind (RocksDB)
│   ├── operators/
│   │   ├── entity_resolution.py    # EntityResolutionFunction (streaming Union-Find)
│   │   ├── deduplication.py        # DeduplicationFunction (24 h window)
│   │   ├── feature_enrichment.py   # FeatureEnrichmentFunction (Feast lookup)
│   │   ├── risk_scorer.py          # RiskScorerFunction + compute_risk_score()
│   │   └── alert_sink.py           # AlertGeneratorFunction (threshold + rate limiter)
│   ├── connectors/
│   │   ├── kafka_source.py         # KafkaSource builder
│   │   └── kafka_sink.py           # KafkaSink builder
│   ├── pipeline/
│   │   └── fraud_detection.py      # Full pipeline topology + CLI entry point
│   └── config.py                   # Pydantic-settings configuration
│
├── generator/
│   └── fraud_ring_generator.py     # Synthetic fraud ring data generator
│
├── feast/
│   ├── feature_store.yaml          # Feast repository config
│   ├── entities.py                 # Entity definitions
│   ├── feature_views.py            # FeatureView definitions
│   └── feature_services.py         # FeatureService definitions
│
├── tests/
│   └── unit/
│       ├── test_union_find.py      # 20+ unit tests + Hypothesis property tests
│       ├── test_risk_scorer.py     # Rule coverage + monotonicity tests
│       ├── test_domain_models.py   # Model validation + edge extraction
│       └── test_generator.py      # Generator correctness
│
├── benchmarks/
│   ├── bench_union_find.py         # pytest-benchmark + standalone runner
│   └── bench_pipeline.py          # End-to-end throughput benchmark
│
├── docs/
│   └── architecture.md             # Detailed architecture document
│
├── docker-compose.yml
├── Makefile
├── pyproject.toml
└── .env.example
```

---

## The Incremental Union-Find

The heart of StreamGraph is `FlinkUnionFind` — a Union-Find whose backing
store is Flink `MapState` (RocksDB in production):

```python
# In EntityResolutionFunction.process_element():
new_root, merged = self._uf.union(
    edge.source_id,
    edge.target_id,
    transaction_id=edge.transaction_id,
)

if merged:
    snapshot = self._build_snapshot(new_root, edge.timestamp)
    out.collect(snapshot.model_dump_json())
```

**Path-halving compression** (not full compression) is used because it achieves
the same amortised O(α(n)) complexity but requires only *one* traversal pass —
critical when each step is a RocksDB read:

```python
def find(self, x: str) -> str:
    while True:
        parent = self._get_parent(x)
        if parent == x:
            return x
        grandparent = self._get_parent(parent)
        if grandparent == parent:
            return parent
        self._parent.put(x, grandparent)   # path halving: skip over parent
        x = grandparent
```

### Sharding

The operator is keyed by `shard_id = blake2b(min(a, b)) % parallelism`.
Using `min(a, b)` ensures that the edge `(A, B)` always routes to the same
shard as `(B, A)`, preventing split-brain.  Cross-shard merges emit a side
output to a merge coordinator (included as a skeleton; see
[Issue #12](https://github.com/your-org/streamgraph/issues/12) for the
distributed merge protocol).

---

## Fraud Ring Generator

The generator produces a realistic mixture of legitimate and fraudulent traffic:

```bash
# Preview 20 synthetic transactions
make generate

# Stream to Kafka at 500 events/s
python -m generator.fraud_ring_generator \
  --kafka-bootstrap localhost:9092 \
  --events-per-second 500 \
  --num-rings 30 \
  --fraud-ratio 0.10
```

### Ring topologies

| Pattern  | Description                                   | Typical size |
|----------|-----------------------------------------------|--------------|
| `star`   | Hub account → many mule accounts              | 8–25         |
| `chain`  | A → B → C → D layering pattern                | 5–15         |
| `cycle`  | Circular money flow to obscure origin         | 4–12         |
| `dense`  | Near-clique; card testing micro-transactions  | 4–12         |

Each ring type produces realistic transaction amounts, timing patterns, and
shared infrastructure (device IDs, IP addresses) that match real fraud ring
signatures.

---

## Feast Feature Views

| Feature View         | Entity    | TTL    | Key features                              |
|----------------------|-----------|--------|-------------------------------------------|
| `account_txn_stats`  | account   | 30 d   | velocity, amount stats, geo diversity     |
| `account_risk_scores`| account   | 7 d    | ML model scores (card-testing, mule, ATO) |
| `merchant_stats`     | merchant  | 30 d   | chargeback ratio, MCC, risk category      |
| `device_stats`       | device    | 90 d   | linked accounts count, emulator flag      |
| `ip_stats`           | ip        | 14 d   | VPN/Tor/datacenter flags, abuse score     |

To apply feature definitions and materialise to Redis:

```bash
make feast-apply
make feast-materialize
```

---

## Risk Rules

| Rule                  | Weight | Description                              |
|-----------------------|--------|------------------------------------------|
| R01 large_component   | 30 %   | Component size ≥ 4 (ring indicator)      |
| R02 high_velocity     | 20 %   | >10 transactions in 1 hour              |
| R03 amount_anomaly    | 15 %   | Amount z-score > 2.5σ from 30-day mean  |
| R04 card_testing      | 15 %   | Micro-transaction burst pattern          |
| R05 cross_border      | 5 %    | >70 % cross-border transactions          |
| R06 night_activity    | 5 %    | >70 % activity between 23:00–05:00 UTC  |
| R07 device_sharing    | 5 %    | Device linked to ≥5 distinct accounts   |
| R08 new_account_burst | 5 %    | New account + high mule/velocity score   |

Composite score = weighted sum, clipped to [0, 1].  Alerts are emitted for
scores ≥ 0.35, with severity tiers at 0.50 / 0.75 / 0.90.

---

## Configuration

All settings are driven by environment variables (see `.env.example`).
Key variables:

| Variable                        | Default          | Description                  |
|---------------------------------|------------------|------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS`       | `localhost:9092` | Kafka broker list            |
| `FLINK_PARALLELISM`             | `4`              | Task parallelism             |
| `FLINK_CHECKPOINT_DIR`          | `file:///tmp/…`  | Checkpoint storage path      |
| `FEAST_REDIS_HOST`              | `localhost`      | Redis host for online store  |
| `ALERT_SCORE_THRESHOLD_HIGH`    | `0.75`           | HIGH alert threshold         |
| `GENERATOR_EVENTS_PER_SECOND`   | `200`            | Generator emission rate      |
| `GENERATOR_FRAUD_RING_RATIO`    | `0.08`           | Fraction of fraudulent txns  |

---

## Running the Pipeline (without Docker)

```bash
# 1. Start Kafka and Redis locally (or use docker-compose up kafka redis)

# 2. Create Kafka topics
./scripts/setup_kafka_topics.sh localhost:9092

# 3. Apply Feast features
make feast-apply

# 4. Start the generator
python -m generator.fraud_ring_generator \
  --kafka-bootstrap localhost:9092 \
  --events-per-second 200 &

# 5. Submit the Flink pipeline (requires a running Flink cluster)
python -m streamgraph.pipeline.fraud_detection \
  --parallelism 4 \
  --checkpoint-dir file:///tmp/sg-checkpoints
```

---

## Contributing

Contributions are welcome!  Priority areas:

- **Distributed merge protocol** for cross-shard Union-Find merges (#12)
- **Flink Async I/O** for non-blocking Feast lookups (#15)
- **Graph pruning job** to evict stale nodes from RocksDB state (#18)
- **XGBoost model integration** replacing/augmenting rule-based scoring (#21)
- **Kubernetes Helm chart** for production deployment (#25)
- **Avro schema registry** integration for typed Kafka messages (#28)

Please open an issue before submitting a large PR.  Run `make lint typecheck test`
before submitting.

---

## Citation

If you use StreamGraph in research or production, please cite:

```bibtex
@software{streamgraph2024,
  title  = {StreamGraph: Real-time Entity Resolution for Financial Crime Detection},
  year   = {2024},
  url    = {https://github.com/your-org/streamgraph},
  note   = {Apache License 2.0}
}
```

---

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.
