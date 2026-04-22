"""Feast entity definitions for StreamGraph."""

from feast import Entity
from feast.value_type import ValueType

account = Entity(
    name="account",
    join_keys=["account_id"],
    value_type=ValueType.STRING,
    description="Financial account (user or business) that initiates transactions.",
)

merchant = Entity(
    name="merchant",
    join_keys=["merchant_id"],
    value_type=ValueType.STRING,
    description="Merchant or payee entity.",
)

device = Entity(
    name="device",
    join_keys=["device_id"],
    value_type=ValueType.STRING,
    description="Physical or virtual device used to initiate transactions.",
)

ip_address = Entity(
    name="ip_address",
    join_keys=["ip_address"],
    value_type=ValueType.STRING,
    description="IP address associated with a transaction session.",
)
