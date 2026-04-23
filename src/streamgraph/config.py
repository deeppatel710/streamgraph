"""
Centralised configuration using pydantic-settings.
All values can be overridden via environment variables or a .env file.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KAFKA_")

    bootstrap_servers: str = "localhost:9092"
    transactions_topic: str = "raw-transactions"
    alerts_topic: str = "fraud-alerts"
    components_topic: str = "entity-components"
    confirmations_topic: str = "fraud-confirmations"
    consumer_group: str = "streamgraph-pipeline"
    auto_offset_reset: str = "earliest"
    # Kafka producer tuning
    batch_size: int = 65536
    linger_ms: int = 5
    compression_type: str = "lz4"


class FlinkSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FLINK_")

    parallelism: int = 4
    # RocksDB state backend
    state_backend: str = "rocksdb"
    checkpoint_dir: str = "file:///tmp/streamgraph-checkpoints"
    checkpoint_interval_ms: int = 60_000
    min_pause_between_checkpoints_ms: int = 30_000
    checkpoint_timeout_ms: int = 120_000
    max_concurrent_checkpoints: int = 1
    # Watermark
    watermark_idle_timeout_ms: int = 5_000
    max_out_of_orderness_ms: int = 10_000


class FeastSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FEAST_")

    repo_path: str = "./feast"
    online_store_type: str = "redis"
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    ttl_seconds: int = 86_400 * 30   # 30 days


class AlertSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ALERT_")

    score_threshold_low: float = 0.35
    score_threshold_medium: float = 0.50
    score_threshold_high: float = 0.75
    score_threshold_critical: float = 0.90
    # Component size thresholds
    component_size_medium: int = 5
    component_size_high: int = 15
    component_size_critical: int = 30


class GeneratorSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="GENERATOR_")

    kafka_bootstrap: str = "localhost:9092"
    transactions_topic: str = "raw-transactions"
    events_per_second: float = 200.0
    fraud_ring_ratio: float = 0.08          # 8 % of traffic is synthetic fraud
    num_legitimate_accounts: int = 10_000
    num_merchants: int = 2_000
    num_fraud_rings: int = 20
    ring_min_size: int = 4
    ring_max_size: int = 25
    seed: int = 42


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    flink: FlinkSettings = Field(default_factory=FlinkSettings)
    feast: FeastSettings = Field(default_factory=FeastSettings)
    alerts: AlertSettings = Field(default_factory=AlertSettings)
    generator: GeneratorSettings = Field(default_factory=GeneratorSettings)

    log_level: str = "INFO"
    metrics_port: int = 9090
    env: str = "development"


settings = Settings()
