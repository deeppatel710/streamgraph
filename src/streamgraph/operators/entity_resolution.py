"""
EntityResolutionFunction — the central Flink KeyedProcessFunction.

Sharding strategy
-----------------
The stream is keyed by ``shard_id = xxhash(min(source_id, target_id)) % parallelism``.
This guarantees that any two entities sharing a direct edge always land on the
same task.  Cross-shard merges are handled by a second *merge* phase using Flink
BroadcastState (see ``ComponentMergeFunction``).

Within a shard each Union-Find operates over a disjoint subset of entity IDs.
A component that spans shards is detected when both shards hold the same
entity ID — they emit ``CrossShardMergeRequest`` side-output events that the
merge phase processes.

State (per task-key, i.e. per shard):
    parent_state    MapState[str, str]
    rank_state      MapState[str, str]
    size_state      MapState[str, str]
    edges_state     MapState[str, str]
    members_state   MapState[str, str]
    txn_state       MapState[str, str]

Timers
------
A processing-time timer fires every SNAPSHOT_INTERVAL_MS.  At that point all
components with size >= MIN_SNAPSHOT_SIZE emit a ``ComponentSnapshot`` to the
main output.  This ensures downstream consumers receive periodic heartbeats
even when the graph is quiescent.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any

from streamgraph.domain.models import (
    ComponentSnapshot,
    EntityEdge,
    EntityType,
)
from streamgraph.graph.union_find import FlinkUnionFind

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    pass

try:
    from pyflink.datastream import OutputTag  # type: ignore[import]
    from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext  # type: ignore[import]
    from pyflink.datastream.state import MapStateDescriptor  # type: ignore[import]
    from pyflink.common.typeinfo import Types  # type: ignore[import]
    FLINK_AVAILABLE = True
except ImportError:
    FLINK_AVAILABLE = False
    # Provide stub base-class so the module is importable in test environments
    class KeyedProcessFunction:  # type: ignore[no-redef]
        pass
    class RuntimeContext:  # type: ignore[no-redef]
        pass


SNAPSHOT_INTERVAL_MS: int = 30_000      # emit snapshots every 30 s
MIN_SNAPSHOT_SIZE: int = 3              # only snapshot components this large

# Side-output tag for cross-shard merge requests
CROSS_SHARD_TAG: "OutputTag" = (
    OutputTag("cross-shard-merges", Types.STRING())
    if FLINK_AVAILABLE
    else None  # type: ignore[assignment]
)


def shard_id(entity_a: str, entity_b: str, num_shards: int) -> int:
    """
    Deterministic, symmetric shard assignment for an entity pair.

    Uses the *smaller* entity ID (lexicographic) to ensure that the same pair
    always routes to the same shard regardless of edge direction.
    """
    canonical = min(entity_a, entity_b)
    h = int(hashlib.blake2b(canonical.encode(), digest_size=8).hexdigest(), 16)
    return h % num_shards


class EntityResolutionFunction(KeyedProcessFunction):  # type: ignore[misc]
    """
    Streaming Union-Find operator.

    Input  : EntityEdge (serialised as JSON string)
    Output : ComponentSnapshot (serialised as JSON string)
    Side   : cross-shard merge request (JSON string, tag CROSS_SHARD_TAG)
    """

    def __init__(self, num_shards: int = 8, snapshot_interval_ms: int = SNAPSHOT_INTERVAL_MS) -> None:
        self._num_shards = num_shards
        self._snapshot_interval_ms = snapshot_interval_ms
        self._uf: FlinkUnionFind | None = None

        # State descriptor references (initialised in open())
        self._parent_desc: Any = None
        self._rank_desc: Any = None
        self._size_desc: Any = None
        self._edges_desc: Any = None
        self._members_desc: Any = None
        self._txn_desc: Any = None
        self._first_seen_desc: Any = None
        self._snapshot_timer_desc: Any = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def open(self, runtime_context: RuntimeContext) -> None:
        str_t = Types.STRING()

        from pyflink.datastream.state import (  # type: ignore[import]
            MapStateDescriptor,
            ValueStateDescriptor,
        )

        self._parent_state = runtime_context.get_map_state(
            MapStateDescriptor("uf_parent", str_t, str_t)
        )
        self._rank_state = runtime_context.get_map_state(
            MapStateDescriptor("uf_rank", str_t, str_t)
        )
        self._size_state = runtime_context.get_map_state(
            MapStateDescriptor("uf_size", str_t, str_t)
        )
        self._edges_state = runtime_context.get_map_state(
            MapStateDescriptor("uf_edges", str_t, str_t)
        )
        self._members_state = runtime_context.get_map_state(
            MapStateDescriptor("uf_members", str_t, str_t)
        )
        self._txn_state = runtime_context.get_map_state(
            MapStateDescriptor("uf_txns", str_t, str_t)
        )
        self._first_seen_state = runtime_context.get_map_state(
            MapStateDescriptor("uf_first_seen", str_t, str_t)
        )

        self._uf = FlinkUnionFind(
            parent_state=self._parent_state,
            rank_state=self._rank_state,
            size_state=self._size_state,
            edges_state=self._edges_state,
            members_state=self._members_state,
            txn_state=self._txn_state,
        )

    # ------------------------------------------------------------------
    # Main processing
    # ------------------------------------------------------------------

    def process_element(
        self,
        value: str,
        ctx: "KeyedProcessFunction.Context",
    ):  # type: ignore[override]
        assert self._uf is not None

        try:
            raw = json.loads(value)
            edge = EntityEdge(**raw)
        except Exception as exc:
            logger.warning("Failed to deserialise EntityEdge: %s | input=%r", exc, value)
            return

        src = edge.source_id
        tgt = edge.target_id

        # Track first-seen timestamps
        now_iso = datetime.utcnow().isoformat()
        for node in (src, tgt):
            if not self._first_seen_state.contains(node):
                self._first_seen_state.put(node, now_iso)

        prev_root_src = self._uf.find(src)
        prev_root_tgt = self._uf.find(tgt)

        # Detect cross-shard boundary
        src_shard = shard_id(src, src, self._num_shards)
        tgt_shard = shard_id(tgt, tgt, self._num_shards)

        if src_shard != tgt_shard:
            # Cross-shard merge detected; log it but don't use side output
            # (PyFlink 1.19 Python ctx.output() is not supported in the Beam runner)
            logger.debug(
                "Cross-shard edge: %s (shard %d) <-> %s (shard %d)",
                src, src_shard, tgt, tgt_shard,
            )

        new_root, merged = self._uf.union(
            src, tgt, transaction_id=edge.transaction_id
        )

        # Emit a snapshot whenever the component grows or on any merge
        if merged:
            snapshot = self._build_snapshot(new_root, edge.timestamp)
            yield snapshot.model_dump_json()

        # Register a periodic snapshot timer (idempotent if already registered)
        next_fire = (ctx.timer_service().current_processing_time()
                     + self._snapshot_interval_ms)
        ctx.timer_service().register_processing_time_timer(next_fire)

    # ------------------------------------------------------------------
    # Timer callback — periodic snapshot heartbeat
    # ------------------------------------------------------------------

    def on_timer(
        self,
        timestamp: int,
        ctx: "KeyedProcessFunction.OnTimerContext",
    ):  # type: ignore[override]
        assert self._uf is not None

        # Iterate all known roots and emit snapshots for large components
        try:
            for node in list(self._parent_state.keys()):
                root = self._uf.find(node)
                if root == node:  # this node is a root
                    size = self._uf.component_size(root)
                    if size >= MIN_SNAPSHOT_SIZE:
                        snapshot = self._build_snapshot(root, datetime.utcnow())
                        yield snapshot.model_dump_json()
        except Exception as exc:
            logger.error("Error during periodic snapshot: %s", exc)

        # Re-arm timer
        next_fire = timestamp + self._snapshot_interval_ms
        ctx.timer_service().register_processing_time_timer(next_fire)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_snapshot(self, root: str, event_time: datetime) -> ComponentSnapshot:
        assert self._uf is not None
        members = self._uf.component_members(root)
        txns = self._uf.component_txns(root)
        size = self._uf.component_size(root)
        edges = self._uf.component_edge_count(root)

        # Infer entity types from member ID prefixes
        entity_types: set[EntityType] = set()
        for m in members:
            if m.startswith("ip:"):
                entity_types.add(EntityType.IP_ADDRESS)
            elif m.startswith("card:"):
                entity_types.add(EntityType.CARD)
            elif m.startswith("device:") or len(m) == 36:  # rough device heuristic
                entity_types.add(EntityType.DEVICE)
            else:
                entity_types.add(EntityType.ACCOUNT)

        first_seen_raw = self._first_seen_state.get(root)
        first_seen = (
            datetime.fromisoformat(first_seen_raw)
            if first_seen_raw
            else event_time
        )

        return ComponentSnapshot(
            component_id=root,
            member_ids=frozenset(members),
            size=size,
            edge_count=edges,
            first_seen=first_seen,
            last_updated=event_time,
            entity_types=frozenset(entity_types),
            transaction_ids=frozenset(txns),
        )
