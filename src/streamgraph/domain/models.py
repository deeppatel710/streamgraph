"""
Core domain models for StreamGraph financial crime detection.

All models are immutable Pydantic dataclasses that travel through the Flink
pipeline serialized as JSON.  They are intentionally free of business logic —
pure data carriers — so that operators remain independently testable.
"""

from __future__ import annotations

import enum
import uuid
from datetime import datetime
from typing import Any, Dict, FrozenSet, List, Optional

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class EntityType(str, enum.Enum):
    ACCOUNT = "account"
    MERCHANT = "merchant"
    DEVICE = "device"
    IP_ADDRESS = "ip_address"
    CARD = "card"
    PHONE = "phone"
    EMAIL = "email"


class RelationshipType(str, enum.Enum):
    """Edge label in the entity graph."""
    TRANSACTED_AT = "transacted_at"
    USED_DEVICE = "used_device"
    USED_IP = "used_ip"
    LINKED_CARD = "linked_card"
    LINKED_PHONE = "linked_phone"
    LINKED_EMAIL = "linked_email"
    SHARED_MERCHANT = "shared_merchant"


class TransactionStatus(str, enum.Enum):
    PENDING = "pending"
    APPROVED = "approved"
    DECLINED = "declined"
    REVERSED = "reversed"
    FLAGGED = "flagged"


class AlertSeverity(str, enum.Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class RingPattern(str, enum.Enum):
    """Fraud ring topological patterns."""
    STAR = "star"           # one hub, many spokes
    CHAIN = "chain"         # linear sequence of accounts
    CYCLE = "cycle"         # circular money flow
    DENSE = "dense"         # nearly-complete subgraph
    LAYERED = "layered"     # layering + integration pattern


# ---------------------------------------------------------------------------
# Core domain objects
# ---------------------------------------------------------------------------


class Transaction(BaseModel):
    """Raw payment transaction as received from the core banking system."""

    model_config = {"frozen": True}

    transaction_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime
    account_id: str
    merchant_id: str
    amount_usd: float = Field(gt=0)
    currency: str = Field(default="USD", max_length=3)
    status: TransactionStatus = TransactionStatus.PENDING
    device_id: Optional[str] = None
    ip_address: Optional[str] = None
    card_last4: Optional[str] = None
    phone_number: Optional[str] = None
    email: Optional[str] = None
    mcc: Optional[str] = None
    country_code: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("amount_usd")
    @classmethod
    def round_amount(cls, v: float) -> float:
        return round(v, 2)

    def entity_edges(self) -> List[EntityEdge]:
        """Enumerate every edge implied by this transaction."""
        edges: List[EntityEdge] = []
        ts = self.timestamp

        edges.append(EntityEdge(
            source_id=self.account_id,
            source_type=EntityType.ACCOUNT,
            target_id=self.merchant_id,
            target_type=EntityType.MERCHANT,
            relationship=RelationshipType.TRANSACTED_AT,
            weight=self.amount_usd,
            timestamp=ts,
            transaction_id=self.transaction_id,
        ))
        if self.device_id:
            edges.append(EntityEdge(
                source_id=self.account_id,
                source_type=EntityType.ACCOUNT,
                target_id=self.device_id,
                target_type=EntityType.DEVICE,
                relationship=RelationshipType.USED_DEVICE,
                weight=1.0,
                timestamp=ts,
                transaction_id=self.transaction_id,
            ))
        if self.ip_address:
            edges.append(EntityEdge(
                source_id=self.account_id,
                source_type=EntityType.ACCOUNT,
                target_id=f"ip:{self.ip_address}",
                target_type=EntityType.IP_ADDRESS,
                relationship=RelationshipType.USED_IP,
                weight=1.0,
                timestamp=ts,
                transaction_id=self.transaction_id,
            ))
        if self.card_last4:
            edges.append(EntityEdge(
                source_id=self.account_id,
                source_type=EntityType.ACCOUNT,
                target_id=f"card:{self.card_last4}",
                target_type=EntityType.CARD,
                relationship=RelationshipType.LINKED_CARD,
                weight=1.0,
                timestamp=ts,
                transaction_id=self.transaction_id,
            ))
        return edges


class EntityEdge(BaseModel):
    """Directed edge in the entity graph extracted from a transaction."""

    model_config = {"frozen": True}

    edge_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_id: str
    source_type: EntityType
    target_id: str
    target_type: EntityType
    relationship: RelationshipType
    weight: float = 1.0
    timestamp: datetime
    transaction_id: str

    @property
    def canonical_key(self) -> str:
        """Deterministic key for deduplication; order-independent."""
        a, b = sorted([self.source_id, self.target_id])
        return f"{a}|{b}|{self.relationship}"


class ComponentSnapshot(BaseModel):
    """
    Point-in-time snapshot of a connected component emitted by the
    EntityResolutionFunction whenever the component changes.
    """

    model_config = {"frozen": True}

    component_id: str          # canonical root node id
    member_ids: FrozenSet[str]
    size: int
    edge_count: int
    first_seen: datetime
    last_updated: datetime
    entity_types: FrozenSet[EntityType]
    transaction_ids: FrozenSet[str]

    @property
    def density(self) -> float:
        n = self.size
        return (2 * self.edge_count) / (n * (n - 1)) if n > 1 else 0.0


class RiskFeatures(BaseModel):
    """Feature vector assembled by the Feast online store lookup."""

    model_config = {"frozen": True}

    entity_id: str
    component_size: int = 1
    component_edge_count: int = 0
    txn_velocity_1h: int = 0
    txn_velocity_24h: int = 0
    amount_mean_30d: float = 0.0
    amount_stddev_30d: float = 0.0
    amount_zscore: float = 0.0
    unique_merchants_30d: int = 0
    unique_devices_30d: int = 0
    unique_ips_30d: int = 0
    cross_border_ratio: float = 0.0
    night_txn_ratio: float = 0.0
    card_testing_score: float = 0.0    # micro-transaction burst indicator
    mule_account_score: float = 0.0
    freshness_seconds: int = 0


class RiskScore(BaseModel):
    """Output of the risk scoring operator."""

    model_config = {"frozen": True}

    score_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    transaction_id: str
    account_id: str
    component_id: str
    composite_score: float = Field(ge=0.0, le=1.0)
    feature_contributions: Dict[str, float] = Field(default_factory=dict)
    triggered_rules: List[str] = Field(default_factory=list)
    evaluated_at: datetime
    model_version: str = "rules-v1"


class FraudAlert(BaseModel):
    """Fraud alert emitted when risk score exceeds threshold."""

    model_config = {"frozen": True}

    alert_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    transaction_id: str
    account_id: str
    component_id: str
    component_size: int
    severity: AlertSeverity
    composite_score: float
    triggered_rules: List[str]
    recommended_action: str
    created_at: datetime
    risk_score: RiskScore
    component_snapshot: Optional[ComponentSnapshot] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_risk_score(
        cls,
        score: RiskScore,
        component: Optional[ComponentSnapshot] = None,
    ) -> "FraudAlert":
        if score.composite_score >= 0.9:
            severity = AlertSeverity.CRITICAL
            action = "block_and_review"
        elif score.composite_score >= 0.75:
            severity = AlertSeverity.HIGH
            action = "step_up_auth"
        elif score.composite_score >= 0.5:
            severity = AlertSeverity.MEDIUM
            action = "flag_for_review"
        else:
            severity = AlertSeverity.LOW
            action = "monitor"

        return cls(
            transaction_id=score.transaction_id,
            account_id=score.account_id,
            component_id=score.component_id,
            component_size=component.size if component else 1,
            severity=severity,
            composite_score=score.composite_score,
            triggered_rules=score.triggered_rules,
            recommended_action=action,
            created_at=score.evaluated_at,
            risk_score=score,
            component_snapshot=component,
        )
