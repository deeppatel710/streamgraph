"""
AlertGeneratorFunction — converts RiskScores into FraudAlerts.

Implements a simple threshold gate plus a per-account alert-rate limiter
(max 1 HIGH/CRITICAL alert per account per 10 minutes) to prevent alert
storms during fraud ring activity.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from streamgraph.config import settings
from streamgraph.domain.models import AlertSeverity, ComponentSnapshot, FraudAlert, RiskScore

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


RATE_LIMIT_MS = 10 * 60 * 1000     # 10 minutes per account for HIGH+CRITICAL


class AlertGeneratorFunction(KeyedProcessFunction):  # type: ignore[misc]
    """
    Keyed by account_id.

    Input  : JSON payload with keys "risk_score" and optional "component_snapshot"
    Output : FraudAlert JSON (only when score >= LOW threshold)
    """

    def __init__(self) -> None:
        self._last_high_alert_ms: Any = None

    def open(self, runtime_context: Any) -> None:
        from pyflink.datastream.state import ValueStateDescriptor  # type: ignore[import]
        from pyflink.common.typeinfo import Types  # type: ignore[import]

        self._last_high_alert_ms = runtime_context.get_state(
            ValueStateDescriptor("last_high_alert_ms", Types.LONG())
        )

    def process_element(self, value: str, ctx: Any):  # type: ignore[override]
        try:
            payload = json.loads(value)
            # Support both raw RiskScore JSON and wrapped {"risk_score": {...}}
            if "risk_score" in payload:
                score = RiskScore(**payload["risk_score"])
                comp_raw = payload.get("component_snapshot")
            else:
                score = RiskScore(**payload)
                comp_raw = None

            component: ComponentSnapshot | None = None
            if comp_raw:
                component = ComponentSnapshot(**comp_raw)

        except Exception as exc:
            logger.error("AlertGenerator: parse error %s | %r", exc, value)
            return

        thresholds = settings.alerts
        if score.composite_score < thresholds.score_threshold_low:
            return

        is_high = score.composite_score >= thresholds.score_threshold_high
        if is_high:
            current_time = ctx.timer_service().current_processing_time()
            last = self._last_high_alert_ms.value()
            if last is not None and (current_time - last) < RATE_LIMIT_MS:
                return
            self._last_high_alert_ms.update(current_time)

        alert = FraudAlert.from_risk_score(score, component)
        yield alert.model_dump_json()
