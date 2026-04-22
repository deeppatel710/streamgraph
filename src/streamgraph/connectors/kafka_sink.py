"""
Kafka sink connectors for StreamGraph.

Uses the Flink Kafka connector (KafkaSink) with at-least-once delivery
semantics.  Exactly-once is available when the pipeline runs with
checkpointing enabled and the Kafka cluster supports transactions.
"""

from __future__ import annotations

import logging
from typing import Any

from streamgraph.config import settings

logger = logging.getLogger(__name__)


def _base_sink_builder(bootstrap_servers: str, topic: str) -> Any:
    from pyflink.datastream.connectors.kafka import (  # type: ignore[import]
        KafkaSink,
        KafkaRecordSerializationSchema,
        DeliveryGuarantee,
    )
    from pyflink.common.serialization import SimpleStringSchema  # type: ignore[import]

    serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(topic)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )

    return (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_record_serializer(serializer)
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        # Producer tuning — mirrors settings in config
        .set_property("batch.size", str(settings.kafka.batch_size))
        .set_property("linger.ms", str(settings.kafka.linger_ms))
        .set_property("compression.type", settings.kafka.compression_type)
        .build()
    )


def build_alert_sink(
    bootstrap_servers: str | None = None,
    topic: str | None = None,
) -> Any:
    """KafkaSink for FraudAlert JSON records."""
    bs = bootstrap_servers or settings.kafka.bootstrap_servers
    tp = topic or settings.kafka.alerts_topic
    return _base_sink_builder(bs, tp)


def build_component_sink(
    bootstrap_servers: str | None = None,
    topic: str | None = None,
) -> Any:
    """KafkaSink for ComponentSnapshot JSON records."""
    bs = bootstrap_servers or settings.kafka.bootstrap_servers
    tp = topic or settings.kafka.components_topic
    return _base_sink_builder(bs, tp)
