"""Tests for FraudConfirmationFunction."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from streamgraph.operators.fraud_confirmation import FraudConfirmationFunction


def _make_function() -> FraudConfirmationFunction:
    fn = FraudConfirmationFunction()
    mock_store = MagicMock()
    fn._label_store = mock_store
    return fn


class TestFraudConfirmationFunction:
    def test_writes_label_to_store(self):
        fn = _make_function()
        event = {
            "component_id": "acct_001",
            "member_ids": ["acct_001", "ip:1.2.3.4"],
            "reason": "confirmed chargeback",
            "analyst": "alice@example.com",
        }
        result = fn.map(json.dumps(event))
        fn._label_store.mark_fraud.assert_called_once_with(
            component_id="acct_001",
            member_ids=["acct_001", "ip:1.2.3.4"],
            reason="confirmed chargeback",
            analyst="alice@example.com",
        )

    def test_passes_event_through_unchanged(self):
        fn = _make_function()
        event = {"component_id": "acct_002", "member_ids": [], "reason": "", "analyst": "bob"}
        original = json.dumps(event)
        result = fn.map(original)
        assert json.loads(result) == event

    def test_handles_malformed_input_gracefully(self):
        fn = _make_function()
        result = fn.map("not valid json{{")
        # Should return the bad input unchanged without raising
        assert result == "not valid json{{"
        fn._label_store.mark_fraud.assert_not_called()

    def test_handles_missing_member_ids(self):
        fn = _make_function()
        event = {"component_id": "acct_003", "reason": "test"}
        fn.map(json.dumps(event))
        fn._label_store.mark_fraud.assert_called_once_with(
            component_id="acct_003",
            member_ids=[],
            reason="test",
            analyst="system",
        )

    def test_no_label_store_does_not_raise(self):
        fn = FraudConfirmationFunction()
        fn._label_store = None
        event = {"component_id": "acct_004", "member_ids": [], "reason": "", "analyst": "x"}
        result = fn.map(json.dumps(event))
        assert json.loads(result) == event
