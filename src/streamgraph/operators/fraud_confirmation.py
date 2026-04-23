"""
FraudConfirmationFunction — consumes analyst fraud-confirmation events and
writes confirmed component labels to the FraudLabelStore (Redis).

Input topic : fraud-confirmations
Message shape:
    {
        "component_id": "acct_001374",
        "member_ids":   ["acct_001374", "ip:1.2.3.4", "card:9999"],
        "reason":       "confirmed chargeback — ring #42",
        "analyst":      "alice@example.com",
        "confirmed_at": "2026-04-22T10:00:00Z"   # optional, defaults to now
    }

Effect:
    - Writes fraud:component:<component_id> → label JSON (90-day TTL)
    - Writes fraud:entity:<entity_id> → component_id  for every member
      (reverse index used by RiskScorerFunction to flag entities even before
       EntityResolutionFunction emits an updated snapshot)
    - Emits the original confirmation JSON to the main output for audit logging

This operator is stateless — all durable state lives in Redis.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from streamgraph.operators.fraud_label_store import FraudLabelStore

logger = logging.getLogger(__name__)

try:
    from pyflink.datastream.functions import MapFunction  # type: ignore[import]
    FLINK_AVAILABLE = True
except ImportError:
    FLINK_AVAILABLE = False

    class MapFunction:  # type: ignore[no-redef]
        pass


class FraudConfirmationFunction(MapFunction):  # type: ignore[misc]
    """
    Stateless map operator — reads each confirmation event, writes to Redis,
    and passes the event through unchanged for audit-log sinking.
    """

    def __init__(self) -> None:
        self._label_store: Any = None

    def open(self, runtime_context: Any) -> None:
        from streamgraph.config import settings
        self._label_store = FraudLabelStore(
            host=settings.feast.redis_host,
            port=settings.feast.redis_port,
            db=1,
        )
        self._label_store.connect()

    def map(self, value: str) -> str:  # type: ignore[override]
        try:
            event = json.loads(value)
            component_id = event["component_id"]
            member_ids = event.get("member_ids", [])
            reason = event.get("reason", "")
            analyst = event.get("analyst", "system")

            if self._label_store is not None:
                self._label_store.mark_fraud(
                    component_id=component_id,
                    member_ids=member_ids,
                    reason=reason,
                    analyst=analyst,
                )
                logger.info(
                    "Fraud label written: component=%s members=%d analyst=%s",
                    component_id, len(member_ids), analyst,
                )
        except Exception as exc:
            logger.error("FraudConfirmation error: %s | %r", exc, value)

        return value   # pass through for audit sink
