"""Feast feature services — define the feature vectors served at inference time."""

from feast import FeatureService

from feature_views import (
    account_risk_scores,
    account_txn_stats,
    device_stats,
    ip_stats,
    merchant_stats,
)

fraud_detection_v1 = FeatureService(
    name="fraud_detection_v1",
    features=[
        account_txn_stats[
            [
                "txn_velocity_1h",
                "txn_velocity_24h",
                "amount_mean_30d",
                "amount_stddev_30d",
                "unique_merchants_30d",
                "unique_devices_30d",
                "unique_ips_30d",
                "cross_border_ratio",
                "night_txn_ratio",
                "declined_ratio_7d",
                "account_age_days",
            ]
        ],
        account_risk_scores[
            [
                "card_testing_score",
                "mule_account_score",
                "synthetic_id_score",
                "ato_score",
            ]
        ],
    ],
    description="Core feature vector for real-time fraud scoring (v1).",
    tags={"team": "fraud", "model": "rules-v1"},
)

fraud_detection_v2 = FeatureService(
    name="fraud_detection_v2",
    features=[
        account_txn_stats,
        account_risk_scores,
        merchant_stats[["chargeback_ratio_90d", "risk_category"]],
        device_stats[["linked_accounts_count", "emulator_flag"]],
        ip_stats[["is_vpn", "is_tor", "abuse_score"]],
    ],
    description="Extended feature vector including device/IP/merchant dimensions (v2).",
    tags={"team": "fraud", "model": "xgboost-v2"},
)
