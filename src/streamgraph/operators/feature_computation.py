"""
StreamingFeatureFunction — real-time per-account feature computation.

Replaces the Feast online-store lookup with features computed directly from
the transaction stream using Flink keyed state.  This gives sub-second
feature freshness at the cost of not having 30-day pre-computed history on
cold start (features warm up as transactions flow through).

Features computed
-----------------
txn_velocity_1h       Count of transactions in the past hour
txn_velocity_24h      Count of transactions in the past 24 hours
amount_mean           Running mean of transaction amounts (Welford)
amount_stddev         Running stddev of transaction amounts (Welford)
amount_zscore         Z-score of current transaction vs running stats
unique_devices_1h     Distinct device IDs seen in the past hour
unique_ips_1h         Distinct IP addresses seen in the past hour
card_testing_score    Ratio of micro-transactions (<$1) in past hour
night_txn_ratio       Ratio of night-time (23:00–05:00 UTC) txns in 24h
cross_border_ratio    Ratio of non-US transactions in 24h (country_code != US)

State layout (all keyed by account_id)
---------------------------------------
txn_history_state     ListState[str]  — JSON: {ts, amount, device, ip,
                                        is_micro, is_night, is_cross_border}
welford_state         MapState[str,str] — keys: n, mean, M2
"""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

try:
    from pyflink.datastream.functions import KeyedProcessFunction  # type: ignore[import]
    from pyflink.common.typeinfo import Types  # type: ignore[import]
    FLINK_AVAILABLE = True
except ImportError:
    FLINK_AVAILABLE = False

    class KeyedProcessFunction:  # type: ignore[no-redef]
        pass


ONE_HOUR_MS = 3_600_000
ONE_DAY_MS = 86_400_000
MICRO_TXN_THRESHOLD = 1.0   # USD
NIGHT_START_HOUR = 23
NIGHT_END_HOUR = 5


def _is_night(ts_ms: int) -> bool:
    hour = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).hour
    return hour >= NIGHT_START_HOUR or hour < NIGHT_END_HOUR


def compute_features_local(
    account_id: str,
    history: list[dict],
    current_amount: float,
    n: int,
    mean: float,
    m2: float,
) -> dict:
    """
    Pure function — computes the feature dict from history + Welford state.
    Usable in tests without Flink.
    """
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    cutoff_1h = now_ms - ONE_HOUR_MS
    cutoff_24h = now_ms - ONE_DAY_MS

    window_1h = [e for e in history if e["ts"] >= cutoff_1h]
    window_24h = [e for e in history if e["ts"] >= cutoff_24h]

    velocity_1h = len(window_1h)
    velocity_24h = len(window_24h)

    micro_count = sum(1 for e in window_1h if e.get("is_micro", False))
    card_testing = micro_count / max(velocity_1h, 1)

    night_count = sum(1 for e in window_24h if e.get("is_night", False))
    night_ratio = night_count / max(velocity_24h, 1)

    cross_count = sum(1 for e in window_24h if e.get("is_cross_border", False))
    cross_ratio = cross_count / max(velocity_24h, 1)

    unique_devices = len({e["device"] for e in window_1h if e.get("device")})
    unique_ips = len({e["ip"] for e in window_1h if e.get("ip")})

    stddev = math.sqrt(m2 / n) if n > 1 else 0.0
    zscore = (current_amount - mean) / stddev if stddev > 0 else 0.0

    return {
        "entity_id": account_id,
        "txn_velocity_1h": velocity_1h,
        "txn_velocity_24h": velocity_24h,
        "amount_mean_30d": round(mean, 4),
        "amount_stddev_30d": round(stddev, 4),
        "amount_zscore": round(zscore, 4),
        "unique_devices_30d": unique_devices,
        "unique_ips_30d": unique_ips,
        "card_testing_score": round(card_testing, 4),
        "night_txn_ratio": round(night_ratio, 4),
        "cross_border_ratio": round(cross_ratio, 4),
        "mule_account_score": 0.0,
        "freshness_seconds": 0,
    }


class StreamingFeatureFunction(KeyedProcessFunction):  # type: ignore[misc]
    """
    Keyed by account_id.

    Input  : raw transaction JSON string (same format as raw-transactions topic)
    Output : scoring payload JSON string:
             {transaction_id, account_id, component_id, features: {...}}

    A processing-time timer fires every hour to evict stale history entries
    and keep state bounded.
    """

    def __init__(self, history_eviction_interval_ms: int = ONE_HOUR_MS) -> None:
        self._eviction_interval_ms = history_eviction_interval_ms
        self._history_state: Any = None
        self._welford_state: Any = None

    def open(self, runtime_context: Any) -> None:
        from pyflink.datastream.state import (  # type: ignore[import]
            ListStateDescriptor,
            MapStateDescriptor,
        )
        str_t = Types.STRING()
        self._history_state = runtime_context.get_list_state(
            ListStateDescriptor("txn_history", str_t)
        )
        self._welford_state = runtime_context.get_map_state(
            MapStateDescriptor("welford", str_t, str_t)
        )

    def process_element(self, value: str, ctx: Any):  # type: ignore[override]
        try:
            raw = json.loads(value)
        except Exception as exc:
            logger.warning("StreamingFeature: parse error %s", exc)
            return

        account_id = raw.get("account_id", "")
        amount = float(raw.get("amount_usd", 0.0))
        device = raw.get("device_id") or ""
        ip = raw.get("ip_address") or ""
        country = raw.get("country_code", "US") or "US"
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        entry = json.dumps({
            "ts": now_ms,
            "amount": amount,
            "device": device,
            "ip": ip,
            "is_micro": amount < MICRO_TXN_THRESHOLD,
            "is_night": _is_night(now_ms),
            "is_cross_border": country.upper() != "US",
        })
        self._history_state.add(entry)

        # Update Welford online mean/variance
        n = int(self._welford_state.get("n") or 0) + 1
        mean = float(self._welford_state.get("mean") or 0.0)
        m2 = float(self._welford_state.get("M2") or 0.0)
        delta = amount - mean
        mean += delta / n
        m2 += delta * (amount - mean)
        self._welford_state.put("n", str(n))
        self._welford_state.put("mean", str(mean))
        self._welford_state.put("M2", str(m2))

        # Build history list, evict entries older than 24h
        cutoff_24h = now_ms - ONE_DAY_MS
        history = []
        for item in self._history_state.get():
            e = json.loads(item)
            if e["ts"] >= cutoff_24h:
                history.append(e)

        features = compute_features_local(account_id, history, amount, n, mean, m2)

        payload = json.dumps({
            "transaction_id": raw.get("transaction_id", ""),
            "account_id": account_id,
            "component_id": account_id,
            "features": features,
        })
        yield payload

        # Register hourly eviction timer (idempotent)
        next_eviction = (
            ctx.timer_service().current_processing_time()
            + self._eviction_interval_ms
        )
        ctx.timer_service().register_processing_time_timer(next_eviction)

    def on_timer(self, timestamp: int, ctx: Any) -> None:  # type: ignore[override]
        """Evict history entries older than 24h to bound state size."""
        cutoff = timestamp - ONE_DAY_MS
        surviving = []
        for item in self._history_state.get():
            try:
                if json.loads(item)["ts"] >= cutoff:
                    surviving.append(item)
            except Exception:
                pass
        self._history_state.update(surviving)
        ctx.timer_service().register_processing_time_timer(
            timestamp + self._eviction_interval_ms
        )
