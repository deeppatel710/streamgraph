"""
Incremental Union-Find with path compression and union-by-rank.

Two implementations are provided:

  1. ``LocalUnionFind``  — pure-Python, dict-backed.  Used in unit tests,
     benchmarks, and as the authoritative reference implementation.

  2. ``FlinkUnionFind``  — thin wrapper that delegates storage to Flink
     MapState (RocksDB in production).  All mutating operations are kept
     intentionally simple so that each ``process_element`` call issues the
     minimum number of state reads/writes.

Design notes
------------
* Path compression is *halving* (point every other node to its grandparent)
  rather than full compression.  Full compression requires two traversal
  passes; halving achieves the same amortised O(α(n)) complexity in one
  pass, which is critical for hot paths inside a Flink operator.

* Union-by-rank avoids deep trees.  Component *size* is maintained separately
  from rank so downstream consumers can make decisions based on cardinality
  without an extra traversal.

* The "find" operation in FlinkUnionFind is *not* idempotent with respect to
  RocksDB — it writes path-compressed parent pointers back during the
  traversal.  Callers must not cache the result across process_element
  invocations; always call find() fresh.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Iterator, Optional, Tuple

if TYPE_CHECKING:
    # These are the PyFlink state types; imported at runtime inside operators
    # to avoid a hard dependency when running unit tests without a Flink env.
    from pyflink.datastream.state import MapState  # type: ignore[import]


# ---------------------------------------------------------------------------
# Pure-Python reference implementation
# ---------------------------------------------------------------------------


@dataclass
class ComponentMeta:
    """Metadata attached to each component root."""
    root: str
    size: int = 1
    edge_count: int = 0
    first_seen: datetime = field(default_factory=datetime.utcnow)
    last_updated: datetime = field(default_factory=datetime.utcnow)
    member_ids: set[str] = field(default_factory=set)
    transaction_ids: set[str] = field(default_factory=set)

    def update_timestamp(self) -> None:
        self.last_updated = datetime.utcnow()


class LocalUnionFind:
    """
    Thread-unsafe, in-process Union-Find used for testing and benchmarking.

    All state is held in plain Python dicts — no Flink dependency.
    """

    __slots__ = ("_parent", "_rank", "_meta")

    def __init__(self) -> None:
        self._parent: dict[str, str] = {}
        self._rank: dict[str, int] = {}
        self._meta: dict[str, ComponentMeta] = {}   # keyed by root

    # ------------------------------------------------------------------
    # Core DSU operations
    # ------------------------------------------------------------------

    def _ensure(self, x: str) -> None:
        """Lazily initialise a node as its own component root."""
        if x not in self._parent:
            self._parent[x] = x
            self._rank[x] = 0
            self._meta[x] = ComponentMeta(root=x, member_ids={x})

    def find(self, x: str) -> str:
        """
        Find the root of *x* with path-halving compression.

        Amortised O(α(n)) per call.
        """
        self._ensure(x)
        while self._parent[x] != x:
            # Path halving: point x to its grandparent
            grandparent = self._parent.get(self._parent[x], self._parent[x])
            self._parent[x] = grandparent
            x = grandparent
        return x

    def union(
        self,
        x: str,
        y: str,
        *,
        transaction_id: str = "",
    ) -> Tuple[str, bool]:
        """
        Merge the components containing *x* and *y*.

        Returns ``(new_root, merged)`` where ``merged`` is ``False`` when
        *x* and *y* were already in the same component.
        """
        self._ensure(x)
        self._ensure(y)

        rx = self.find(x)
        ry = self.find(y)

        if rx == ry:
            # Still increment edge count for density calculations
            meta = self._meta[rx]
            meta.edge_count += 1
            if transaction_id:
                meta.transaction_ids.add(transaction_id)
            meta.update_timestamp()
            return rx, False

        # Union by rank — attach smaller tree under larger
        rank_x = self._rank[rx]
        rank_y = self._rank[ry]

        if rank_x < rank_y:
            rx, ry = ry, rx   # ensure rx has higher rank

        # ry → rx
        self._parent[ry] = rx
        meta_x = self._meta.pop(rx)
        meta_y = self._meta.pop(ry)

        merged_meta = ComponentMeta(
            root=rx,
            size=meta_x.size + meta_y.size,
            edge_count=meta_x.edge_count + meta_y.edge_count + 1,
            first_seen=min(meta_x.first_seen, meta_y.first_seen),
            last_updated=datetime.utcnow(),
            member_ids=meta_x.member_ids | meta_y.member_ids,
            transaction_ids=meta_x.transaction_ids | meta_y.transaction_ids,
        )
        if transaction_id:
            merged_meta.transaction_ids.add(transaction_id)

        self._meta[rx] = merged_meta

        if rank_x == rank_y:
            self._rank[rx] += 1

        return rx, True

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def component_size(self, x: str) -> int:
        return self._meta[self.find(x)].size

    def component_meta(self, x: str) -> ComponentMeta:
        return self._meta[self.find(x)]

    def same_component(self, x: str, y: str) -> bool:
        return self.find(x) == self.find(y)

    def roots(self) -> Iterator[str]:
        """Iterate over all current component roots."""
        for node, parent in self._parent.items():
            if node == parent:
                yield node

    def num_components(self) -> int:
        return sum(1 for _ in self.roots())

    def __len__(self) -> int:
        return len(self._parent)


# ---------------------------------------------------------------------------
# Flink-backed implementation (RocksDB via MapState)
# ---------------------------------------------------------------------------


class FlinkUnionFind:
    """
    Union-Find whose backing store is Flink MapState (RocksDB).

    This class intentionally performs no lazy initialisation so that the
    caller (``EntityResolutionFunction``) can control when state is
    dirtied.  All methods assume Flink's per-key state isolation; the
    operator must be keyed such that *all* entities in a potential component
    land on the same partition (see ``EntityResolutionFunction`` for the
    sharding strategy).

    State layout (all MapState[str, str]):
        parent_state    : node_id → parent_id
        rank_state      : node_id → str(int rank)
        size_state      : root_id → str(int size)
        edges_state     : root_id → str(int edge_count)
        members_state   : root_id → comma-joined member_ids
        txn_state       : root_id → comma-joined transaction_ids (capped)

    Comma-joined sets are capped at MAX_STORED_IDS entries to prevent
    unbounded state growth.  The *count* fields are authoritative; the
    stored sets are best-effort for forensic purposes.
    """

    MAX_STORED_IDS = 500

    def __init__(
        self,
        parent_state: "MapState",
        rank_state: "MapState",
        size_state: "MapState",
        edges_state: "MapState",
        members_state: "MapState",
        txn_state: "MapState",
    ) -> None:
        self._parent = parent_state
        self._rank = rank_state
        self._size = size_state
        self._edges = edges_state
        self._members = members_state
        self._txn = txn_state

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_parent(self, node: str) -> str:
        v = self._parent.get(node)
        return v if v is not None else node

    def _get_rank(self, node: str) -> int:
        v = self._rank.get(node)
        return int(v) if v is not None else 0

    def _get_size(self, root: str) -> int:
        v = self._size.get(root)
        return int(v) if v is not None else 1

    def _get_edges(self, root: str) -> int:
        v = self._edges.get(root)
        return int(v) if v is not None else 0

    def _get_members(self, root: str) -> set[str]:
        v = self._members.get(root)
        return set(v.split(",")) if v else {root}

    def _get_txns(self, root: str) -> set[str]:
        v = self._txn.get(root)
        return set(v.split(",")) if v else set()

    def _put_members(self, root: str, members: set[str]) -> None:
        # Cap to avoid indefinite growth while preserving determinism
        capped = sorted(members)[: self.MAX_STORED_IDS]
        self._members.put(root, ",".join(capped))

    def _put_txns(self, root: str, txns: set[str]) -> None:
        capped = sorted(txns)[: self.MAX_STORED_IDS]
        self._txn.put(root, ",".join(capped))

    def _ensure(self, x: str) -> None:
        if not self._parent.contains(x):
            self._parent.put(x, x)
            self._rank.put(x, "0")
            self._size.put(x, "1")
            self._edges.put(x, "0")
            self._put_members(x, {x})

    # ------------------------------------------------------------------
    # Core DSU operations
    # ------------------------------------------------------------------

    def find(self, x: str) -> str:
        """Find root with path-halving; writes compressed pointers back."""
        self._ensure(x)
        while True:
            parent = self._get_parent(x)
            if parent == x:
                return x
            grandparent = self._get_parent(parent)
            if grandparent == parent:
                return parent
            # Halve: skip over parent
            self._parent.put(x, grandparent)
            x = grandparent

    def union(
        self,
        x: str,
        y: str,
        *,
        transaction_id: str = "",
    ) -> Tuple[str, bool]:
        """
        Merge components containing *x* and *y*.

        Returns (new_root, merged).
        """
        self._ensure(x)
        self._ensure(y)

        rx = self.find(x)
        ry = self.find(y)

        if rx == ry:
            edge_count = self._get_edges(rx) + 1
            self._edges.put(rx, str(edge_count))
            if transaction_id:
                txns = self._get_txns(rx)
                txns.add(transaction_id)
                self._put_txns(rx, txns)
            return rx, False

        rank_x = self._get_rank(rx)
        rank_y = self._get_rank(ry)

        if rank_x < rank_y:
            rx, ry = ry, rx

        # Attach ry under rx
        self._parent.put(ry, rx)

        new_size = self._get_size(rx) + self._get_size(ry)
        new_edges = self._get_edges(rx) + self._get_edges(ry) + 1
        self._size.put(rx, str(new_size))
        self._edges.put(rx, str(new_edges))

        # Merge members and transactions
        members = self._get_members(rx) | self._get_members(ry)
        self._put_members(rx, members)

        txns = self._get_txns(rx) | self._get_txns(ry)
        if transaction_id:
            txns.add(transaction_id)
        self._put_txns(rx, txns)

        # Clean up old root's aggregate state (parent pointer already updated)
        self._size.remove(ry)
        self._edges.remove(ry)
        self._members.remove(ry)
        self._txn.remove(ry)

        if rank_x == rank_y:
            self._rank.put(rx, str(rank_x + 1))

        return rx, True

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def component_size(self, x: str) -> int:
        return self._get_size(self.find(x))

    def component_edge_count(self, x: str) -> int:
        return self._get_edges(self.find(x))

    def component_members(self, x: str) -> set[str]:
        return self._get_members(self.find(x))

    def component_txns(self, x: str) -> set[str]:
        return self._get_txns(self.find(x))

    def same_component(self, x: str, y: str) -> bool:
        return self.find(x) == self.find(y)
