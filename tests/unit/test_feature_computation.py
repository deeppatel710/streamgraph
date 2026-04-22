"""Tests for StreamingFeatureFunction's pure computation logic."""

from __future__ import annotations

import math
import time

import pytest

from streamgraph.operators.feature_computation import (
    ONE_HOUR_MS,
    ONE_DAY_MS,
    compute_features_local,
    _is_night,
)


def _entry(ts_ms: int, amount: float, device: str = "", ip: str = "",
           is_micro: bool = False, is_night: bool = False,
           is_cross_border: bool = False) -> dict:
    return {
        "ts": ts_ms, "amount": amount, "device": device, "ip": ip,
        "is_micro": is_micro, "is_night": is_night,
        "is_cross_border": is_cross_border,
    }


def _now_ms() -> int:
    return int(time.time() * 1000)


class TestVelocity:
    def test_velocity_within_hour(self):
        now = _now_ms()
        history = [_entry(now - 60_000 * i, 10.0) for i in range(5)]
        feats = compute_features_local("acct_1", history, 10.0, 6, 10.0, 0.0)
        assert feats["txn_velocity_1h"] == 5

    def test_velocity_excludes_old(self):
        now = _now_ms()
        history = [
            _entry(now - 30_000, 10.0),           # 30s ago — in window
            _entry(now - ONE_HOUR_MS - 1, 10.0),  # just outside window
        ]
        feats = compute_features_local("acct_1", history, 10.0, 3, 10.0, 0.0)
        assert feats["txn_velocity_1h"] == 1

    def test_velocity_24h(self):
        now = _now_ms()
        history = [
            _entry(now - ONE_HOUR_MS * 2, 10.0),   # 2h ago — in 24h window
            _entry(now - ONE_DAY_MS - 1, 10.0),    # just outside 24h window
        ]
        feats = compute_features_local("acct_1", history, 10.0, 3, 10.0, 0.0)
        assert feats["txn_velocity_24h"] == 1


class TestAmountZscore:
    def test_zscore_zero_on_no_history(self):
        feats = compute_features_local("acct_1", [], 50.0, 1, 50.0, 0.0)
        assert feats["amount_zscore"] == 0.0

    def test_zscore_positive_outlier(self):
        # mean=10, stddev=0 (only 1 sample) → zscore=0 from Welford
        # simulate n=10, mean=10, M2=900 (stddev=10)
        feats = compute_features_local("acct_1", [], 30.0, 10, 10.0, 900.0)
        # stddev = sqrt(900/10) = sqrt(90) ≈ 9.49
        # zscore = (30-10)/9.49 ≈ 2.11
        assert feats["amount_zscore"] > 2.0

    def test_zscore_negative_for_low_amount(self):
        feats = compute_features_local("acct_1", [], 1.0, 10, 10.0, 900.0)
        assert feats["amount_zscore"] < 0.0


class TestCardTesting:
    def test_card_testing_score_all_micro(self):
        now = _now_ms()
        history = [_entry(now - i * 1000, 0.50, is_micro=True) for i in range(10)]
        feats = compute_features_local("acct_1", history, 0.50, 11, 0.50, 0.0)
        assert feats["card_testing_score"] == pytest.approx(1.0)

    def test_card_testing_score_no_micro(self):
        now = _now_ms()
        history = [_entry(now - i * 1000, 50.0, is_micro=False) for i in range(5)]
        feats = compute_features_local("acct_1", history, 50.0, 6, 50.0, 0.0)
        assert feats["card_testing_score"] == 0.0

    def test_card_testing_score_partial(self):
        now = _now_ms()
        history = [
            _entry(now - 1000, 0.50, is_micro=True),
            _entry(now - 2000, 50.0, is_micro=False),
            _entry(now - 3000, 50.0, is_micro=False),
            _entry(now - 4000, 50.0, is_micro=False),
        ]
        feats = compute_features_local("acct_1", history, 50.0, 5, 30.0, 0.0)
        assert feats["card_testing_score"] == pytest.approx(0.25)


class TestNightRatio:
    def test_all_night(self):
        now = _now_ms()
        history = [_entry(now - i * 60_000, 10.0, is_night=True) for i in range(5)]
        feats = compute_features_local("acct_1", history, 10.0, 6, 10.0, 0.0)
        assert feats["night_txn_ratio"] == pytest.approx(1.0)

    def test_no_night(self):
        now = _now_ms()
        history = [_entry(now - i * 60_000, 10.0, is_night=False) for i in range(5)]
        feats = compute_features_local("acct_1", history, 10.0, 6, 10.0, 0.0)
        assert feats["night_txn_ratio"] == pytest.approx(0.0)


class TestCrossBorderRatio:
    def test_all_cross_border(self):
        now = _now_ms()
        history = [_entry(now - i * 60_000, 10.0, is_cross_border=True) for i in range(4)]
        feats = compute_features_local("acct_1", history, 10.0, 5, 10.0, 0.0)
        assert feats["cross_border_ratio"] == pytest.approx(1.0)


class TestUniqueDevices:
    def test_unique_devices_counted(self):
        now = _now_ms()
        history = [
            _entry(now - 1000, 10.0, device="dev_a"),
            _entry(now - 2000, 10.0, device="dev_b"),
            _entry(now - 3000, 10.0, device="dev_a"),  # duplicate
        ]
        feats = compute_features_local("acct_1", history, 10.0, 4, 10.0, 0.0)
        assert feats["unique_devices_30d"] == 2
