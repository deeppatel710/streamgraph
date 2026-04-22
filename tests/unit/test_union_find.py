"""
Unit tests for LocalUnionFind.

These tests run without any Flink dependency and cover the core DSU
invariants used by the streaming operator.
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings as h_settings
from hypothesis import strategies as st

from streamgraph.graph.union_find import LocalUnionFind


# ---------------------------------------------------------------------------
# Basic correctness
# ---------------------------------------------------------------------------


class TestLocalUnionFindBasic:
    def test_single_node_is_own_root(self) -> None:
        uf = LocalUnionFind()
        assert uf.find("a") == "a"

    def test_union_connects_two_nodes(self) -> None:
        uf = LocalUnionFind()
        root, merged = uf.union("a", "b")
        assert merged is True
        assert uf.same_component("a", "b")
        assert uf.find("a") == uf.find("b")

    def test_union_same_node_is_no_op(self) -> None:
        uf = LocalUnionFind()
        root, merged = uf.union("x", "x")
        assert merged is False
        assert uf.component_size("x") == 1

    def test_transitivity(self) -> None:
        uf = LocalUnionFind()
        uf.union("a", "b")
        uf.union("b", "c")
        assert uf.same_component("a", "c")
        assert uf.component_size("a") == 3

    def test_different_components_stay_separate(self) -> None:
        uf = LocalUnionFind()
        uf.union("a", "b")
        uf.union("c", "d")
        assert not uf.same_component("a", "c")
        assert uf.component_size("a") == 2
        assert uf.component_size("c") == 2

    def test_num_components_decreases_on_merge(self) -> None:
        uf = LocalUnionFind()
        for letter in "abcde":
            uf.find(letter)   # initialise 5 singletons
        assert uf.num_components() == 5
        uf.union("a", "b")
        assert uf.num_components() == 4
        uf.union("c", "d")
        assert uf.num_components() == 3
        uf.union("a", "c")
        assert uf.num_components() == 2

    def test_component_size_tracking(self) -> None:
        uf = LocalUnionFind()
        uf.union("a", "b")
        uf.union("b", "c")
        uf.union("c", "d")
        assert uf.component_size("a") == 4
        assert uf.component_size("d") == 4

    def test_edge_count_within_component(self) -> None:
        uf = LocalUnionFind()
        uf.union("a", "b")
        uf.union("b", "c")
        uf.union("a", "c")   # already same component → edge count increments
        meta = uf.component_meta("a")
        assert meta.edge_count == 3

    def test_root_always_stable_after_path_compression(self) -> None:
        uf = LocalUnionFind()
        # Build a long chain: a→b→c→d→e
        for left, right in zip("abcde", "bcde?"):
            if right != "?":
                uf.union(left, right)
        root_before = uf.find("a")
        # Trigger path compression multiple times
        for _ in range(10):
            assert uf.find("a") == root_before
            assert uf.find("e") == root_before

    def test_all_members_in_meta(self) -> None:
        uf = LocalUnionFind()
        members = ["alice", "bob", "carol", "dave"]
        for i in range(len(members) - 1):
            uf.union(members[i], members[i + 1])
        meta = uf.component_meta(members[0])
        assert meta.member_ids == set(members)


# ---------------------------------------------------------------------------
# Idempotency and repeated merges
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_double_union_is_idempotent(self) -> None:
        uf = LocalUnionFind()
        r1, m1 = uf.union("a", "b")
        r2, m2 = uf.union("a", "b")
        assert r1 == r2
        assert m1 is True
        assert m2 is False

    def test_repeated_finds_return_same_root(self) -> None:
        uf = LocalUnionFind()
        uf.union("x", "y")
        r1 = uf.find("x")
        r2 = uf.find("x")
        assert r1 == r2


# ---------------------------------------------------------------------------
# Large-scale correctness
# ---------------------------------------------------------------------------


class TestLargeScale:
    def test_merge_two_large_components(self) -> None:
        uf = LocalUnionFind()
        # Build two chains of 500
        for i in range(499):
            uf.union(f"a{i}", f"a{i+1}")
        for i in range(499):
            uf.union(f"b{i}", f"b{i+1}")
        assert uf.component_size("a0") == 500
        assert uf.component_size("b0") == 500
        uf.union("a0", "b0")
        assert uf.component_size("a0") == 1000

    def test_all_singleton_then_full_merge(self) -> None:
        n = 100
        uf = LocalUnionFind()
        nodes = [str(i) for i in range(n)]
        for node in nodes:
            uf.find(node)
        assert uf.num_components() == n
        for i in range(n - 1):
            uf.union(nodes[i], nodes[i + 1])
        assert uf.num_components() == 1
        assert uf.component_size(nodes[0]) == n


# ---------------------------------------------------------------------------
# Property-based tests
# ---------------------------------------------------------------------------


@given(
    edges=st.lists(
        st.tuples(
            st.text(min_size=1, max_size=5, alphabet=st.characters(whitelist_categories=("Lu",))),
            st.text(min_size=1, max_size=5, alphabet=st.characters(whitelist_categories=("Lu",))),
        ),
        min_size=1,
        max_size=50,
    )
)
@h_settings(max_examples=200)
def test_union_find_invariants(edges: list[tuple[str, str]]) -> None:
    """
    After any sequence of union operations:
    - find is idempotent
    - same_component is symmetric
    - component_size >= 1
    """
    uf = LocalUnionFind()
    for a, b in edges:
        uf.union(a, b)

    seen: dict[str, str] = {}
    for a, b in edges:
        root_a = uf.find(a)
        root_b = uf.find(b)

        # find is stable
        assert uf.find(a) == root_a
        assert uf.find(b) == root_b

        # symmetric
        assert uf.same_component(a, b) == uf.same_component(b, a)

        # size is positive
        assert uf.component_size(a) >= 1
        assert uf.component_size(b) >= 1

        # transitivity: if a-b and b-c then a-c
        if seen.get(b):
            c = seen[b]
            if uf.same_component(a, b) and uf.same_component(b, c):
                assert uf.same_component(a, c)

        seen[a] = b
