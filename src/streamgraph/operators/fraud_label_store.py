"""
FraudLabelStore — Redis-backed registry of confirmed-fraud component IDs.

Write path (analyst confirms fraud):
    POST /confirm  →  FraudConfirmationFunction  →  Redis

Read path (every scored transaction):
    RiskScorerFunction calls FraudLabelStore.is_known_fraud(component_id)

Redis key schema
----------------
    fraud:component:<component_id>   →  JSON {confirmed_at, reason, analyst}
    fraud:entity:<entity_id>         →  component_id   (reverse index)

The reverse index lets us answer "is this entity in a known-fraud component?"
even before the entity resolution operator has emitted the latest snapshot.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

_COMPONENT_PREFIX = "fraud:component:"
_ENTITY_PREFIX = "fraud:entity:"
_TTL_SECONDS = 90 * 24 * 3600   # 90-day retention


def _component_key(component_id: str) -> str:
    return f"{_COMPONENT_PREFIX}{component_id}"


def _entity_key(entity_id: str) -> str:
    return f"{_ENTITY_PREFIX}{entity_id}"


class FraudLabelStore:
    """
    Thin wrapper around a Redis client.  Instantiate once per operator (open())
    and reuse across process_element calls.

    All methods are synchronous and safe to call from Flink task threads.
    On Redis unavailability they fail-open (log + return safe default) to
    avoid cascading failures in the scoring pipeline.
    """

    def __init__(self, host: str, port: int = 6379, db: int = 1) -> None:
        self._host = host
        self._port = port
        self._db = db
        self._client = None

    def connect(self) -> None:
        try:
            import redis  # type: ignore[import]
            self._client = redis.Redis(
                host=self._host,
                port=self._port,
                db=self._db,
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=1,
            )
            self._client.ping()
            logger.info("FraudLabelStore connected to Redis %s:%d/db%d",
                        self._host, self._port, self._db)
        except Exception as exc:
            logger.warning("FraudLabelStore: Redis unavailable, fail-open: %s", exc)
            self._client = None

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def mark_fraud(
        self,
        component_id: str,
        member_ids: list[str],
        reason: str = "",
        analyst: str = "system",
    ) -> None:
        if self._client is None:
            return
        try:
            label = json.dumps({
                "confirmed_at": datetime.now(tz=timezone.utc).isoformat(),
                "reason": reason,
                "analyst": analyst,
                "member_count": len(member_ids),
            })
            pipe = self._client.pipeline()
            pipe.setex(_component_key(component_id), _TTL_SECONDS, label)
            for entity_id in member_ids:
                pipe.setex(_entity_key(entity_id), _TTL_SECONDS, component_id)
            pipe.execute()
        except Exception as exc:
            logger.warning("FraudLabelStore.mark_fraud error: %s", exc)

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def is_known_fraud_component(self, component_id: str) -> bool:
        if self._client is None:
            return False
        try:
            return self._client.exists(_component_key(component_id)) > 0
        except Exception as exc:
            logger.warning("FraudLabelStore.is_known_fraud_component error: %s", exc)
            return False

    def is_known_fraud_entity(self, entity_id: str) -> bool:
        """True if the entity belongs to any confirmed-fraud component."""
        if self._client is None:
            return False
        try:
            return self._client.exists(_entity_key(entity_id)) > 0
        except Exception as exc:
            logger.warning("FraudLabelStore.is_known_fraud_entity error: %s", exc)
            return False

    def get_fraud_label(self, component_id: str) -> Optional[dict]:
        if self._client is None:
            return None
        try:
            raw = self._client.get(_component_key(component_id))
            return json.loads(raw) if raw else None
        except Exception as exc:
            logger.warning("FraudLabelStore.get_fraud_label error: %s", exc)
            return None
