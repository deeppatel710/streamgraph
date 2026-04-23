"""
RiskScorerFunction — rule-based composite risk scoring.

Input  : (RiskScore-ready dict, ComponentSnapshot JSON) as a joined stream
Output : RiskScore JSON

Each rule returns a score in [0, 1].  The final composite score is a
weighted average clipped to [0, 1].  Weights are tuned on the synthetic
dataset but can be replaced by a call to a model endpoint.

Rule catalogue
--------------
R01  large_component      Component has many members (fraud ring indicator)
R02  high_velocity        Account submitted many transactions in 1 h
R03  amount_anomaly       Transaction amount is an outlier vs 30-day history
R04  card_testing         Burst of micro-transactions (<$1) on same device/IP
R05  cross_border         Unexpectedly high cross-border activity ratio
R06  night_activity       Disproportionate late-night (23:00-05:00 UTC) volume
R07  device_sharing       Device used by more than N distinct accounts
R08  new_account_burst    Account < 7 days old with high transaction velocity
R09  known_fraud_network  Component previously confirmed as fraud (guilt by
                          association) — overrides composite score to CRITICAL
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

from streamgraph.domain.models import ComponentSnapshot, RiskScore
from streamgraph.operators.fraud_label_store import FraudLabelStore

logger = logging.getLogger(__name__)

try:
    from pyflink.datastream.functions import KeyedProcessFunction  # type: ignore[import]
    FLINK_AVAILABLE = True
except ImportError:
    FLINK_AVAILABLE = False

    class KeyedProcessFunction:  # type: ignore[no-redef]
        pass


# ---------------------------------------------------------------------------
# Rule definitions
# ---------------------------------------------------------------------------


class Rule(NamedTuple):
    name: str
    weight: float


RULES: list[Rule] = [
    Rule("R01_large_component",   0.30),
    Rule("R02_high_velocity",     0.20),
    Rule("R03_amount_anomaly",    0.15),
    Rule("R04_card_testing",      0.15),
    Rule("R05_cross_border",      0.05),
    Rule("R06_night_activity",    0.05),
    Rule("R07_device_sharing",    0.05),
    Rule("R08_new_account_burst", 0.05),
]

assert abs(sum(r.weight for r in RULES) - 1.0) < 1e-6, "Rule weights must sum to 1"


def _score_large_component(comp: Optional[ComponentSnapshot]) -> Tuple[float, bool]:
    if comp is None:
        return 0.0, False
    size = comp.size
    if size >= 30:
        return 1.0, True
    if size >= 15:
        return 0.8, True
    if size >= 7:
        return 0.5, True
    if size >= 4:
        return 0.25, True
    return 0.0, False


def _score_high_velocity(features: Dict[str, Any]) -> Tuple[float, bool]:
    vel = features.get("txn_velocity_1h", 0)
    if vel >= 50:
        return 1.0, True
    if vel >= 20:
        return 0.75, True
    if vel >= 10:
        return 0.40, True
    return 0.0, False


def _score_amount_anomaly(features: Dict[str, Any]) -> Tuple[float, bool]:
    z = abs(features.get("amount_zscore", 0.0))
    if z >= 5.0:
        return 1.0, True
    if z >= 3.5:
        return 0.7, True
    if z >= 2.5:
        return 0.4, True
    return 0.0, False


def _score_card_testing(features: Dict[str, Any]) -> Tuple[float, bool]:
    score = features.get("card_testing_score", 0.0)
    triggered = score >= 0.5
    return min(score, 1.0), triggered


def _score_cross_border(features: Dict[str, Any]) -> Tuple[float, bool]:
    ratio = features.get("cross_border_ratio", 0.0)
    if ratio >= 0.9:
        return 0.8, True
    if ratio >= 0.7:
        return 0.4, True
    return 0.0, False


def _score_night_activity(features: Dict[str, Any]) -> Tuple[float, bool]:
    ratio = features.get("night_txn_ratio", 0.0)
    if ratio >= 0.9:
        return 0.7, True
    if ratio >= 0.7:
        return 0.35, True
    return 0.0, False


def _score_device_sharing(features: Dict[str, Any]) -> Tuple[float, bool]:
    unique_devices = features.get("unique_devices_30d", 0)
    # If many different accounts share a device that's suspicious
    # (this feature would normally come from the device dimension)
    if unique_devices >= 10:
        return 0.8, True
    if unique_devices >= 5:
        return 0.4, True
    return 0.0, False


def _score_new_account_burst(features: Dict[str, Any]) -> Tuple[float, bool]:
    vel_24h = features.get("txn_velocity_24h", 0)
    # Heuristic: we'd check account_age_days from features; use mule score proxy
    mule = features.get("mule_account_score", 0.0)
    combined = min((vel_24h / 100.0) * 0.5 + mule * 0.5, 1.0)
    return combined, combined >= 0.5


# ---------------------------------------------------------------------------
# Scoring engine
# ---------------------------------------------------------------------------


def compute_risk_score(
    transaction_id: str,
    account_id: str,
    component_id: str,
    features: Dict[str, Any],
    component: Optional[ComponentSnapshot] = None,
    label_store: Optional[FraudLabelStore] = None,
) -> RiskScore:
    """
    Pure-function risk scorer; usable outside Flink for unit tests.
    Pass label_store=None to skip the Redis lookup (e.g. in tests).
    """
    evaluators = [
        ("R01_large_component",   lambda: _score_large_component(component)),
        ("R02_high_velocity",     lambda: _score_high_velocity(features)),
        ("R03_amount_anomaly",    lambda: _score_amount_anomaly(features)),
        ("R04_card_testing",      lambda: _score_card_testing(features)),
        ("R05_cross_border",      lambda: _score_cross_border(features)),
        ("R06_night_activity",    lambda: _score_night_activity(features)),
        ("R07_device_sharing",    lambda: _score_device_sharing(features)),
        ("R08_new_account_burst", lambda: _score_new_account_burst(features)),
    ]

    rule_weights = {r.name: r.weight for r in RULES}
    contributions: dict[str, float] = {}
    triggered: list[str] = []
    weighted_sum = 0.0

    for rule_name, evaluator in evaluators:
        raw_score, fired = evaluator()
        weight = rule_weights[rule_name]
        contributions[rule_name] = round(raw_score * weight, 4)
        weighted_sum += raw_score * weight
        if fired:
            triggered.append(rule_name)

    composite = min(weighted_sum, 1.0)

    # R09: guilt-by-association override — fire if the component root OR the
    # transacting account itself is in a confirmed-fraud network.
    if label_store is not None and (
        label_store.is_known_fraud_component(component_id)
        or label_store.is_known_fraud_entity(account_id)
    ):
        contributions["R09_known_fraud_network"] = 1.0
        triggered.append("R09_known_fraud_network")
        composite = 1.0

    return RiskScore(
        transaction_id=transaction_id,
        account_id=account_id,
        component_id=component_id,
        composite_score=round(composite, 4),
        feature_contributions=contributions,
        triggered_rules=triggered,
        evaluated_at=datetime.utcnow(),
    )


# ---------------------------------------------------------------------------
# Flink operator
# ---------------------------------------------------------------------------


class RiskScorerFunction(KeyedProcessFunction):  # type: ignore[misc]
    """
    Keyed by account_id.

    Expects a JSON string with shape:
      {
        "transaction_id": "...",
        "account_id": "...",
        "component_id": "...",
        "features": { ... },
        "component_snapshot": { ... }  # optional
      }

    Emits RiskScore JSON strings.
    """

    def __init__(self) -> None:
        self._label_store: Optional[FraudLabelStore] = None

    def open(self, runtime_context: Any) -> None:
        from streamgraph.config import settings
        self._label_store = FraudLabelStore(
            host=settings.feast.redis_host,
            port=settings.feast.redis_port,
            db=1,   # db=1 reserved for fraud labels; db=0 is Feast features
        )
        self._label_store.connect()

    def process_element(self, value: str, ctx: Any):  # type: ignore[override]
        try:
            payload = json.loads(value)
            transaction_id = payload["transaction_id"]
            account_id = payload["account_id"]
            component_id = payload.get("component_id", account_id)
            features = payload.get("features", {})

            comp_raw = payload.get("component_snapshot")
            component: ComponentSnapshot | None = None
            if comp_raw:
                component = ComponentSnapshot(**comp_raw)

            score = compute_risk_score(
                transaction_id=transaction_id,
                account_id=account_id,
                component_id=component_id,
                features=features,
                component=component,
                label_store=self._label_store,
            )
            yield score.model_dump_json()

        except Exception as exc:
            logger.error("RiskScorer error: %s | input=%r", exc, value)
