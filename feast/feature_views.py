"""
Feast feature view definitions for StreamGraph.

Offline source: Parquet files written by the feature computation job
  (a separate Flink or Spark batch job not included here, but expected at
   data/features/<view_name>/*.parquet).

Online store: Redis — materialised by ``feast materialize-incremental``.
"""

from datetime import timedelta
from pathlib import Path

from feast import FeatureView, Field
from feast.infra.offline_stores.file_source import FileSource
from feast.types import Float64, Int64, String, UnixTimestamp

from entities import account, device, ip_address, merchant

_DATA_ROOT = Path(__file__).parent.parent / "data" / "features"


def _parquet_source(view_name: str, timestamp_field: str = "event_timestamp") -> FileSource:
    return FileSource(
        path=str(_DATA_ROOT / view_name / "*.parquet"),
        timestamp_field=timestamp_field,
    )


# ---------------------------------------------------------------------------
# Account-level transaction statistics (updated every 15 minutes by batch job)
# ---------------------------------------------------------------------------

account_txn_stats = FeatureView(
    name="account_txn_stats",
    entities=[account],
    ttl=timedelta(days=30),
    schema=[
        Field(name="txn_velocity_1h",       dtype=Int64),
        Field(name="txn_velocity_24h",      dtype=Int64),
        Field(name="amount_mean_30d",        dtype=Float64),
        Field(name="amount_stddev_30d",      dtype=Float64),
        Field(name="amount_total_30d",       dtype=Float64),
        Field(name="unique_merchants_30d",   dtype=Int64),
        Field(name="unique_devices_30d",     dtype=Int64),
        Field(name="unique_ips_30d",         dtype=Int64),
        Field(name="cross_border_ratio",     dtype=Float64),
        Field(name="night_txn_ratio",        dtype=Float64),
        Field(name="declined_ratio_7d",      dtype=Float64),
        Field(name="account_age_days",       dtype=Int64),
        Field(name="first_seen_ts",          dtype=UnixTimestamp),
    ],
    source=_parquet_source("account_txn_stats"),
    online=True,
    description="Per-account transaction behavioural statistics.",
    tags={"team": "fraud", "tier": "critical"},
)


# ---------------------------------------------------------------------------
# Account-level risk model scores (updated daily by ML batch job)
# ---------------------------------------------------------------------------

account_risk_scores = FeatureView(
    name="account_risk_scores",
    entities=[account],
    ttl=timedelta(days=7),
    schema=[
        Field(name="card_testing_score",    dtype=Float64),
        Field(name="mule_account_score",    dtype=Float64),
        Field(name="synthetic_id_score",    dtype=Float64),
        Field(name="ato_score",             dtype=Float64),   # account takeover
        Field(name="model_version",         dtype=String),
        Field(name="scored_at",             dtype=UnixTimestamp),
    ],
    source=_parquet_source("account_risk_scores"),
    online=True,
    description="ML-derived risk scores per account.",
    tags={"team": "fraud", "tier": "critical"},
)


# ---------------------------------------------------------------------------
# Merchant-level statistics
# ---------------------------------------------------------------------------

merchant_stats = FeatureView(
    name="merchant_stats",
    entities=[merchant],
    ttl=timedelta(days=30),
    schema=[
        Field(name="txn_volume_30d",        dtype=Int64),
        Field(name="unique_accounts_30d",   dtype=Int64),
        Field(name="chargeback_ratio_90d",  dtype=Float64),
        Field(name="avg_txn_amount",        dtype=Float64),
        Field(name="mcc",                   dtype=String),
        Field(name="risk_category",         dtype=String),
    ],
    source=_parquet_source("merchant_stats"),
    online=True,
    description="Per-merchant aggregate statistics.",
    tags={"team": "fraud", "tier": "standard"},
)


# ---------------------------------------------------------------------------
# Device-level statistics
# ---------------------------------------------------------------------------

device_stats = FeatureView(
    name="device_stats",
    entities=[device],
    ttl=timedelta(days=90),
    schema=[
        Field(name="linked_accounts_count", dtype=Int64),
        Field(name="first_seen_ts",          dtype=UnixTimestamp),
        Field(name="txn_velocity_24h",       dtype=Int64),
        Field(name="countries_30d",          dtype=Int64),
        Field(name="emulator_flag",          dtype=Int64),   # 0/1
    ],
    source=_parquet_source("device_stats"),
    online=True,
    description="Device behaviour statistics for fraud detection.",
    tags={"team": "fraud", "tier": "standard"},
)


# ---------------------------------------------------------------------------
# IP address statistics
# ---------------------------------------------------------------------------

ip_stats = FeatureView(
    name="ip_stats",
    entities=[ip_address],
    ttl=timedelta(days=14),
    schema=[
        Field(name="linked_accounts_count", dtype=Int64),
        Field(name="is_vpn",                dtype=Int64),
        Field(name="is_tor",                dtype=Int64),
        Field(name="is_datacenter",         dtype=Int64),
        Field(name="country_code",          dtype=String),
        Field(name="abuse_score",           dtype=Float64),
    ],
    source=_parquet_source("ip_stats"),
    online=True,
    description="IP reputation and multi-account linkage statistics.",
    tags={"team": "fraud", "tier": "standard"},
)
