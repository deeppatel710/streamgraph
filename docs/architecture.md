# StreamGraph Architecture

## Overview

StreamGraph is a real-time **entity resolution pipeline** purpose-built for
financial crime detection.  Its core innovation is an **incremental Union-Find**
implemented as a Flink `KeyedProcessFunction` that maintains streaming graph
state in RocksDB.  By treating every payment transaction as a set of edges
between financial entities (accounts, merchants, devices, IPs, cards), the
pipeline continuously identifies **connected components** — the building blocks
of fraud rings.

---

## Data Model

```
Transaction
    │
    ├─── account_id  ──── EntityEdge ──── merchant_id
    │
    ├─── account_id  ──── EntityEdge ──── device_id
    │
    ├─── account_id  ──── EntityEdge ──── ip:192.168.x.x
    │
    └─── account_id  ──── EntityEdge ──── card:4242
```

A transaction with `account_id`, `merchant_id`, `device_id`, and `ip_address`
produces **4 edges**.  The Union-Find merges any two entity nodes that share an
edge into the same **connected component**.

---

## Pipeline Topology

```
┌────────────────────────────────────────────────────────────────────────┐
│  Kafka: raw-transactions (8 partitions)                                │
└───────────────────────────────┬────────────────────────────────────────┘
                                │  JSON string
                                ▼
                    ┌──────────────────────┐
                    │  TransactionFlatMap  │  parse → N EntityEdge JSON
                    │  parallelism=4       │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │  DeduplicationFn     │  keyed by canonical_edge_key
                    │  (KeyedProcessFn)    │  ValueState: first_seen_ms
                    │  TTL = 24 h          │  timer-based state eviction
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │  EntityResolution    │  keyed by shard_id
                    │  (KeyedProcessFn)    │  MapState × 6 (parent/rank/
                    │  RocksDB state       │  size/edges/members/txns)
                    │  parallelism=4       │  FlinkUnionFind (path halving)
                    └──────────┬───────────┘
                               │
               ┌───────────────┴──────────────────┐
               │ ComponentSnapshot                 │ CrossShardMergeRequest
               ▼                                   ▼ (side output)
   ┌──────────────────────┐           ┌────────────────────────┐
   │  Kafka:              │           │  ComponentMergeCoord.  │
   │  entity-components   │           │  (future work)         │
   └──────────────────────┘           └────────────────────────┘

   (parallel risk-scoring branch on raw-transactions)

                    ┌──────────────────────┐
                    │  TxnToScoringPayload │  re-parse raw transactions
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │  FeatureEnrichment   │  Feast Redis online lookup
                    │  (RichMapFunction)   │  LRU cache: 256 entries / 5 s
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │  RiskScorerFn        │  8 weighted rules → composite
                    │  (KeyedProcessFn)    │  score ∈ [0, 1]
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │  AlertGeneratorFn    │  threshold gate + rate limiter
                    │  (KeyedProcessFn)    │  (max 1 HIGH/CRIT per 10 min)
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │  Kafka: fraud-alerts │
                    └──────────────────────┘
```

---

## Incremental Union-Find

### Algorithm

The Union-Find uses **path-halving** compression and **union-by-rank**:

```
find(x):
  while parent[x] ≠ x:
    parent[x] = parent[parent[x]]   # point to grandparent (halving)
    x = parent[x]
  return x

union(x, y):
  rx = find(x); ry = find(y)
  if rx == ry: edge_count[rx]++; return (rx, False)
  if rank[rx] < rank[ry]: swap(rx, ry)
  parent[ry] = rx
  size[rx] += size[ry]
  if rank[rx] == rank[ry]: rank[rx]++
  return (rx, True)
```

Path-halving achieves amortised **O(α(n))** per operation — essentially
constant — while requiring only a single traversal pass, making it ideal for
streaming where reads are expensive (RocksDB I/O).

### Sharding Strategy

Entities are assigned to shards via:

```python
shard_id = blake2b(min(entity_a, entity_b)) % num_shards
```

Using the **lexicographically smaller** ID ensures that any two entities sharing
a direct edge always land on the same shard.  Cross-shard merges (entities
whose IDs hash to different shards but are transitively connected) are handled
by emitting `CrossShardMergeRequest` side-output events to a coordinator.

### State Layout (per shard, RocksDB MapState)

| State key      | Type             | Description                         |
|----------------|------------------|-------------------------------------|
| `uf_parent`    | MapState[str,str]| parent pointer per node             |
| `uf_rank`      | MapState[str,str]| union rank (as string)              |
| `uf_size`      | MapState[str,str]| component cardinality (root only)   |
| `uf_edges`     | MapState[str,str]| edge count (root only)              |
| `uf_members`   | MapState[str,str]| comma-joined member IDs (capped 500)|
| `uf_txns`      | MapState[str,str]| comma-joined txn IDs (capped 500)   |
| `uf_first_seen`| MapState[str,str]| ISO timestamp of first appearance   |

---

## Feast Feature Store

Features are organised in two tiers:

| Tier       | Update frequency | Latency  | Example features               |
|------------|------------------|----------|--------------------------------|
| Hot (Redis)| 15 min (batch)   | <1 ms    | txn_velocity_1h, amount_zscore |
| Cold (File)| Daily            | N/A      | ML model risk scores           |

The `FeatureEnrichmentFunction` uses a thread-local Feast client with a
256-entry / 5 s in-process LRU cache to amortise Redis round-trips.

---

## Risk Scoring Rules

| Rule ID | Name                | Weight | Trigger condition                     |
|---------|---------------------|--------|---------------------------------------|
| R01     | large_component     | 0.30   | Component size ≥ 4                    |
| R02     | high_velocity       | 0.20   | txn_velocity_1h ≥ 10                  |
| R03     | amount_anomaly      | 0.15   | amount_zscore ≥ 2.5                   |
| R04     | card_testing        | 0.15   | card_testing_score ≥ 0.5              |
| R05     | cross_border        | 0.05   | cross_border_ratio ≥ 0.7              |
| R06     | night_activity      | 0.05   | night_txn_ratio ≥ 0.7                 |
| R07     | device_sharing      | 0.05   | unique_devices_30d ≥ 5               |
| R08     | new_account_burst   | 0.05   | mule_account_score + velocity         |

Composite score = weighted sum, clipped to [0, 1].

---

## Operational Considerations

### Checkpointing

- RocksDB incremental checkpoints every 60 s to `state.checkpoints.dir`
- `EXACTLY_ONCE` mode requires Kafka transaction support
- Checkpoint timeout: 120 s; max concurrent: 1

### Backpressure

- Flink's native credit-based flow control handles slow consumers
- The Redis LRU cache prevents Feast from becoming a bottleneck at high parallelism

### State TTL

- Deduplication state: 24 h TTL with processing-time timers
- Alert rate-limiter state: 10 min TTL
- Union-Find state: no automatic TTL — components persist until the job is restarted with a fresh savepoint.  For production deployments, a periodic "graph pruning" job should evict stale nodes.

### Scaling

| Bottleneck         | Remedy                                       |
|--------------------|----------------------------------------------|
| Kafka source       | Increase `raw-transactions` partition count  |
| Entity resolution  | Increase `num_shards` and Flink parallelism  |
| Feature enrichment | Increase Redis throughput / replicas         |
| Risk scoring       | Horizontal scale (stateless map function)    |
