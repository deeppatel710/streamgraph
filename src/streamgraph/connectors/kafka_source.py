"""
Kafka source connectors for StreamGraph.

Wraps the Flink Kafka connector (flink-connector-kafka 3.x) with sensible
defaults and exposes typed builder functions.

All topics use UTF-8 JSON serialisation.  The deserialisation schema is
deliberately lenient (SimpleStringSchema) — validation happens in the first
Flink operator so that malformed messages are tracked in metrics rather than
crashing the source task.
"""

from __future__ import annotations

import logging
from typing import Any

from streamgraph.config import settings

logger = logging.getLogger(__name__)


def build_transaction_source(
    bootstrap_servers: str | None = None,
    topic: str | None = None,
    group_id: str | None = None,
    start_from_earliest: bool = False,
) -> Any:
    """
    Return a Flink KafkaSource for the raw-transactions topic.

    Parameters are resolved from ``settings`` when not provided explicitly,
    making this function easy to call in tests with an embedded broker.
    """
    from pyflink.datastream.connectors.kafka import (  # type: ignore[import]
        KafkaSource,
        KafkaOffsetsInitializer,
    )
    from pyflink.common.serialization import SimpleStringSchema  # type: ignore[import]

    bs = bootstrap_servers or settings.kafka.bootstrap_servers
    tp = topic or settings.kafka.transactions_topic
    gid = group_id or settings.kafka.consumer_group

    offset_initializer = (
        KafkaOffsetsInitializer.earliest()
        if start_from_earliest
        else KafkaOffsetsInitializer.latest()
    )

    return (
        KafkaSource.builder()
        .set_bootstrap_servers(bs)
        .set_topics(tp)
        .set_group_id(gid)
        .set_starting_offsets(offset_initializer)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def build_confirmation_source(
    bootstrap_servers: str | None = None,
    topic: str | None = None,
    group_id: str | None = None,
) -> Any:
    """KafkaSource for the fraud-confirmations topic (analyst feedback)."""
    from pyflink.datastream.connectors.kafka import (  # type: ignore[import]
        KafkaSource,
        KafkaOffsetsInitializer,
    )
    from pyflink.common.serialization import SimpleStringSchema  # type: ignore[import]

    bs = bootstrap_servers or settings.kafka.bootstrap_servers
    tp = topic or settings.kafka.confirmations_topic
    gid = (group_id or settings.kafka.consumer_group) + "-confirmations"

    return (
        KafkaSource.builder()
        .set_bootstrap_servers(bs)
        .set_topics(tp)
        .set_group_id(gid)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def build_component_source(
    bootstrap_servers: str | None = None,
    topic: str | None = None,
    group_id: str | None = None,
) -> Any:
    """KafkaSource for the entity-components topic (used by downstream consumers)."""
    from pyflink.datastream.connectors.kafka import (  # type: ignore[import]
        KafkaSource,
        KafkaOffsetsInitializer,
    )
    from pyflink.common.serialization import SimpleStringSchema  # type: ignore[import]

    bs = bootstrap_servers or settings.kafka.bootstrap_servers
    tp = topic or settings.kafka.components_topic
    gid = (group_id or settings.kafka.consumer_group) + "-components"

    return (
        KafkaSource.builder()
        .set_bootstrap_servers(bs)
        .set_topics(tp)
        .set_group_id(gid)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
