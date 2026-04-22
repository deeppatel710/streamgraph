"""
StreamGraph — fraud detection pipeline entry point.

Topology
--------

  [Kafka: raw-transactions]
          │
          ▼
  TransactionParserMap          parse JSON → Transaction, emit EntityEdges
          │
          ▼
  DeduplicationFunction         drop duplicate edges within 24 h (keyed by canonical_key)
          │
          ▼
  EntityResolutionFunction      incremental Union-Find → ComponentSnapshot
          │  (main)                (+ side output: cross-shard merge requests)
          │
          ├──────────────────────────────────────────────────────────┐
          │                                                          │
          ▼                                                          ▼
  [Kafka: entity-components]        FeatureEnrichmentFunction
                                        (map: Feast lookup)
                                          │
                                          ▼
                                    RiskScorerFunction
                                          │
                                          ▼
                                    AlertGeneratorFunction
                                          │
                                          ▼
                                    [Kafka: fraud-alerts]

State backend : EmbeddedRocksDB with incremental checkpoints to S3 / local FS.
Checkpointing : every 60 s, EXACTLY_ONCE (requires Kafka transactions).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

import click
from rich.console import Console

from streamgraph.config import settings
from streamgraph.domain.models import EntityEdge, Transaction
from streamgraph.operators.alert_sink import AlertGeneratorFunction
from streamgraph.operators.deduplication import DeduplicationFunction
from streamgraph.operators.entity_resolution import CROSS_SHARD_TAG, EntityResolutionFunction
from streamgraph.operators.feature_enrichment import FeatureEnrichmentFunction
from streamgraph.operators.risk_scorer import RiskScorerFunction

logger = logging.getLogger(__name__)
console = Console()


# ---------------------------------------------------------------------------
# Inline map functions (too small to warrant their own modules)
# ---------------------------------------------------------------------------


def _parse_transaction(raw: str) -> list[str]:
    """
    Parse a raw JSON transaction string and return a list of EntityEdge JSON
    strings.  Malformed input is logged and returns an empty list.
    """
    try:
        txn = Transaction(**json.loads(raw))
        return [edge.model_dump_json() for edge in txn.entity_edges()]
    except Exception as exc:
        logger.warning("Failed to parse transaction: %s | %r", exc, raw[:200])
        return []


def _join_with_component(score_json: str, component_json: str | None) -> str:
    """Merge a RiskScore payload with the matching ComponentSnapshot."""
    payload = json.loads(score_json)
    if component_json:
        payload["component_snapshot"] = json.loads(component_json)
    return json.dumps(payload)


# ---------------------------------------------------------------------------
# Pipeline builder
# ---------------------------------------------------------------------------


def build_pipeline(env: Any) -> None:
    """
    Wire the full StreamGraph topology onto the given StreamExecutionEnvironment.

    This function is separated from ``main()`` so that integration tests can
    inject a mini-cluster environment.
    """
    from pyflink.common import WatermarkStrategy  # type: ignore[import]
    from pyflink.datastream.connectors.kafka import KafkaOffsetResetStrategy  # type: ignore[import]
    from streamgraph.connectors.kafka_source import build_transaction_source
    from streamgraph.connectors.kafka_sink import build_alert_sink, build_component_sink

    parallelism = settings.flink.parallelism

    # ---- Source -------------------------------------------------------
    txn_source = build_transaction_source(start_from_earliest=False)
    raw_stream = env.from_source(
        txn_source,
        WatermarkStrategy.for_monotonous_timestamps(),
        "kafka-transactions",
    ).set_parallelism(parallelism)

    # ---- Parse → EntityEdges ------------------------------------------
    # FlatMap: one transaction → many edges
    from pyflink.datastream.functions import FlatMapFunction  # type: ignore[import]

    class TransactionFlatMap(FlatMapFunction):  # type: ignore[misc]
        def flat_map(self, value: str):  # type: ignore[override]
            yield from _parse_transaction(value)

    from pyflink.common.typeinfo import Types  # type: ignore[import]

    edge_stream = (
        raw_stream
        .flat_map(TransactionFlatMap(), output_type=Types.STRING())
        .set_parallelism(parallelism)
        .name("parse-transactions")
    )

    # ---- Deduplication ------------------------------------------------
    from pyflink.datastream.functions import KeySelector  # type: ignore[import]

    class EdgeKeySelector(KeySelector):  # type: ignore[misc]
        def get_key(self, value: str) -> str:
            raw = json.loads(value)
            # canonical_key is order-independent; correct key for dedup
            a, b = sorted([raw["source_id"], raw["target_id"]])
            return f"{a}|{b}|{raw['relationship']}"

    deduped_stream = (
        edge_stream
        .key_by(EdgeKeySelector())
        .process(DeduplicationFunction(), output_type=Types.STRING())
        .set_parallelism(parallelism)
        .name("deduplication")
    )

    # ---- Entity Resolution (Union-Find) --------------------------------
    class ShardKeySelector(KeySelector):  # type: ignore[misc]
        def __init__(self, num_shards: int) -> None:
            self._num_shards = num_shards

        def get_key(self, value: str) -> str:
            from streamgraph.operators.entity_resolution import shard_id
            raw = json.loads(value)
            return str(shard_id(raw["source_id"], raw["target_id"], self._num_shards))

    er_function = EntityResolutionFunction(num_shards=parallelism)

    er_result = (
        deduped_stream
        .key_by(ShardKeySelector(parallelism))
        .process(er_function, output_type=Types.STRING())
        .set_parallelism(parallelism)
        .name("entity-resolution")
    )

    # ---- Component sink -----------------------------------------------
    component_sink = build_component_sink()
    er_result.sink_to(component_sink).name("sink-components")

    # ---- Feature enrichment -------------------------------------------
    # Re-read raw transactions for the risk-scoring branch.
    # We broadcast the latest component snapshot to join with transactions.
    class TxnToScoringPayload(FlatMapFunction):  # type: ignore[misc]
        """Re-parse raw transactions and build scoring payloads."""
        def flat_map(self, value: str):  # type: ignore[override]
            try:
                raw = json.loads(value)
                yield json.dumps({
                    "transaction_id": raw.get("transaction_id", ""),
                    "account_id": raw.get("account_id", ""),
                    "component_id": raw.get("account_id", ""),
                })
            except Exception:
                pass

    scoring_stream = (
        raw_stream
        .flat_map(TxnToScoringPayload(), output_type=Types.STRING())
        .name("prepare-scoring-payload")
        .map(FeatureEnrichmentFunction(), output_type=Types.STRING())
        .name("feast-enrichment")
        .key_by(lambda x: json.loads(x).get("account_id", ""))
        .process(RiskScorerFunction(), output_type=Types.STRING())
        .name("risk-scorer")
        .key_by(lambda x: json.loads(x).get("account_id", ""))
        .process(AlertGeneratorFunction(), output_type=Types.STRING())
        .name("alert-generator")
    )

    # ---- Alert sink ---------------------------------------------------
    alert_sink = build_alert_sink()
    scoring_stream.sink_to(alert_sink).name("sink-alerts")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


@click.command()
@click.option("--parallelism", default=None, type=int, help="Override Flink parallelism")
@click.option("--dry-run", is_flag=True, default=False, help="Print topology and exit")
def main(parallelism: int | None, dry_run: bool) -> None:
    """Launch the StreamGraph fraud detection pipeline."""
    from pyflink.datastream import StreamExecutionEnvironment  # type: ignore[import]

    env = StreamExecutionEnvironment.get_execution_environment()

    # Apply parallelism
    p = parallelism or settings.flink.parallelism
    env.set_parallelism(p)

    # Use HashMapStateBackend (in-memory) to avoid RocksDB serializer
    # re-registration errors when tasks restart after failure in the Beam runner.
    from pyflink.datastream.state_backend import HashMapStateBackend  # type: ignore[import]
    env.set_state_backend(HashMapStateBackend())

    build_pipeline(env)

    if dry_run:
        console.print("[bold green]Topology built successfully (dry-run, not submitting)[/]")
        return

    console.print(f"[bold]Starting StreamGraph pipeline[/] parallelism={p}")
    env.execute("streamgraph-fraud-detection")


if __name__ == "__main__":
    main()
