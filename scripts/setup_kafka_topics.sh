#!/usr/bin/env bash
# Create all StreamGraph Kafka topics.
# Usage: ./scripts/setup_kafka_topics.sh [bootstrap-server]
set -euo pipefail

BOOTSTRAP="${1:-localhost:9092}"
echo "Creating topics on $BOOTSTRAP ..."

kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic raw-transactions \
  --partitions 8 \
  --replication-factor 1 \
  --config retention.ms=86400000 \
  --config cleanup.policy=delete

kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic fraud-alerts \
  --partitions 4 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete

kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic entity-components \
  --partitions 8 \
  --replication-factor 1 \
  --config retention.ms=3600000 \
  --config cleanup.policy=delete

echo "Done."
kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
