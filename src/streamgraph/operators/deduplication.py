"""
DeduplicationFunction — idempotent transaction filter.

Keyed by ``canonical_edge_key``.  State holds the first-seen timestamp for
each key; duplicates arriving within the TTL window are dropped.

Uses a ValueState<long> (first-seen epoch ms) and a processing-time timer
to clean up state after TTL expires.  This is cheaper than a ListState of
seen IDs and prevents unbounded state growth.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from streamgraph.domain.models import EntityEdge

logger = logging.getLogger(__name__)

try:
    from pyflink.datastream.functions import KeyedProcessFunction  # type: ignore[import]
    from pyflink.datastream.state import ValueStateDescriptor  # type: ignore[import]
    from pyflink.common.typeinfo import Types  # type: ignore[import]
    FLINK_AVAILABLE = True
except ImportError:
    FLINK_AVAILABLE = False

    class KeyedProcessFunction:  # type: ignore[no-redef]
        pass


DEFAULT_TTL_MS = 24 * 60 * 60 * 1000   # 24 hours


class DeduplicationFunction(KeyedProcessFunction):  # type: ignore[misc]
    """
    Input / Output : EntityEdge JSON strings
    Drops edges whose canonical_key was already seen within TTL_MS.
    """

    def __init__(self, ttl_ms: int = DEFAULT_TTL_MS) -> None:
        self._ttl_ms = ttl_ms
        self._seen_state: Any = None

    def open(self, runtime_context: Any) -> None:
        from pyflink.datastream.state import ValueStateDescriptor  # type: ignore[import]
        from pyflink.common.typeinfo import Types  # type: ignore[import]

        self._seen_state = runtime_context.get_state(
            ValueStateDescriptor("first_seen_ms", Types.LONG())
        )

    def process_element(self, value: str, ctx: Any):  # type: ignore[override]
        try:
            raw = json.loads(value)
            edge = EntityEdge(**raw)
        except Exception as exc:
            logger.warning("Dedup: failed to parse edge: %s", exc)
            return

        current_time = ctx.timer_service().current_processing_time()
        first_seen = self._seen_state.value()

        if first_seen is not None:
            return

        self._seen_state.update(current_time)
        ctx.timer_service().register_processing_time_timer(
            current_time + self._ttl_ms
        )
        yield value

    def on_timer(self, timestamp: int, ctx: Any):  # type: ignore[override]
        self._seen_state.clear()
