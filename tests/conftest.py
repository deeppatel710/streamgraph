"""
Shared pytest fixtures for StreamGraph tests.

Flink-dependent operators (DeduplicationFunction, EntityResolutionFunction,
AlertGeneratorFunction) require a live Flink mini-cluster and are skipped
automatically if PyFlink is not installed.  The LocalUnionFind, risk scorer,
domain models, and generator can be tested without any Flink dependency.
"""

from __future__ import annotations

import sys
import pytest


def pytest_collection_modifyitems(config, items):
    """Skip integration tests that require Flink if pyflink is not installed."""
    try:
        import pyflink  # noqa: F401
    except ImportError:
        skip_flink = pytest.mark.skip(reason="pyflink not installed")
        for item in items:
            if "integration" in str(item.fspath):
                item.add_marker(skip_flink)
