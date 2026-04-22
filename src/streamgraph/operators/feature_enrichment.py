"""
FeatureEnrichmentFunction — async Feast online-store lookup.

Keyed by account_id.  For each incoming transaction the operator fetches
the latest features for that account from the Feast Redis online store and
attaches them to the payload before forwarding to the risk scorer.

The Feast SDK call is blocking; Flink's async I/O API would be more
efficient at high parallelism, but Flink's Python async I/O support is
incomplete as of 1.19.  Instead, the operator uses a thread-local Feast
store client (one per task slot) to minimise connection-setup overhead.

Cache
-----
A local per-operator LRU cache (256 entries, 5 s TTL) absorbs repeat
lookups for the same account within a short window.  This is appropriate
because feature freshness requirements for fraud scoring are on the order
of minutes, not seconds.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from functools import lru_cache
from typing import Any

from streamgraph.config import settings
from streamgraph.domain.models import RiskFeatures

logger = logging.getLogger(__name__)

try:
    from pyflink.datastream.functions import MapFunction as RichMapFunction  # type: ignore[import]
    FLINK_AVAILABLE = True
except ImportError:
    FLINK_AVAILABLE = False

    class RichMapFunction:  # type: ignore[no-redef]
        pass

_local = threading.local()

CACHE_TTL_S = 5.0
MAX_CACHE_SIZE = 256


def _get_feast_store() -> Any:
    """Return a thread-local Feast FeatureStore instance."""
    if not hasattr(_local, "store"):
        try:
            from feast import FeatureStore  # type: ignore[import]
            _local.store = FeatureStore(repo_path=settings.feast.repo_path)
        except Exception as exc:
            logger.warning("Feast store unavailable, using zero features: %s", exc)
            _local.store = None
    return _local.store


_feature_cache: dict[str, tuple[float, RiskFeatures]] = {}


def _cached_lookup(account_id: str, store: Any) -> RiskFeatures:
    """LRU lookup with TTL eviction."""
    now = time.monotonic()
    if account_id in _feature_cache:
        ts, feats = _feature_cache[account_id]
        if (now - ts) < CACHE_TTL_S:
            return feats

    if store is None:
        return RiskFeatures(entity_id=account_id)

    try:
        result = store.get_online_features(
            features=[
                "account_txn_stats:txn_velocity_1h",
                "account_txn_stats:txn_velocity_24h",
                "account_txn_stats:amount_mean_30d",
                "account_txn_stats:amount_stddev_30d",
                "account_txn_stats:unique_merchants_30d",
                "account_txn_stats:unique_devices_30d",
                "account_txn_stats:unique_ips_30d",
                "account_txn_stats:cross_border_ratio",
                "account_txn_stats:night_txn_ratio",
                "account_risk_scores:card_testing_score",
                "account_risk_scores:mule_account_score",
            ],
            entity_rows=[{"account_id": account_id}],
        ).to_dict()
    except Exception as exc:
        logger.warning("Feast lookup failed for %s: %s", account_id, exc)
        return RiskFeatures(entity_id=account_id)

    def _first(key: str, default: Any = 0) -> Any:
        vals = result.get(key, [default])
        return vals[0] if vals else default

    feats = RiskFeatures(
        entity_id=account_id,
        txn_velocity_1h=_first("txn_velocity_1h", 0),
        txn_velocity_24h=_first("txn_velocity_24h", 0),
        amount_mean_30d=_first("amount_mean_30d", 0.0),
        amount_stddev_30d=_first("amount_stddev_30d", 0.0),
        unique_merchants_30d=_first("unique_merchants_30d", 0),
        unique_devices_30d=_first("unique_devices_30d", 0),
        unique_ips_30d=_first("unique_ips_30d", 0),
        cross_border_ratio=_first("cross_border_ratio", 0.0),
        night_txn_ratio=_first("night_txn_ratio", 0.0),
        card_testing_score=_first("card_testing_score", 0.0),
        mule_account_score=_first("mule_account_score", 0.0),
    )

    # Evict old entries if cache is full
    if len(_feature_cache) >= MAX_CACHE_SIZE:
        oldest_key = min(_feature_cache, key=lambda k: _feature_cache[k][0])
        del _feature_cache[oldest_key]

    _feature_cache[account_id] = (now, feats)
    return feats


class FeatureEnrichmentFunction(RichMapFunction):  # type: ignore[misc]
    """
    Input  : JSON payload with "transaction_id", "account_id", "component_id",
             and optional "component_snapshot".
    Output : Same payload with "features" dict added.
    """

    def open(self, runtime_context: Any) -> None:
        _get_feast_store()   # warm up connection in open()

    def map(self, value: str) -> str:
        try:
            payload = json.loads(value)
            account_id = payload.get("account_id", "")
            store = _get_feast_store()
            features = _cached_lookup(account_id, store)
            payload["features"] = features.model_dump()
            return json.dumps(payload)
        except Exception as exc:
            logger.error("FeatureEnrichment error: %s", exc)
            return value
