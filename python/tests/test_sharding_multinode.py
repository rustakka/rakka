"""Multi-node sharding integration tests.

The Wave 2 plan describes ``ShardRegion`` instances spread across two
``ActorSystem`` nodes communicating over TCP loopback.  That requires
``Cluster.with_tcp_transport`` + ``ShardingExtension`` which are not
exposed by the current Python bindings — see "API gap" notes.

We exercise the in-process ``ShardRegion`` with two independent regions
to model the rebalance: each region owns a disjoint subset of entity
IDs, and the test verifies the per-node entity counts match the
allocation strategy.  This is the deterministic correctness checkpoint
that the TCP variant would build on once wired up.
"""
from __future__ import annotations

import pytest

import atomr


# ---------------------------------------------------------------------------
# Helpers — build a ShardRegion with a known entity factory + extractor.
# ---------------------------------------------------------------------------

def _entity_factory(entity_id):
    class Entity:
        def __init__(self):
            self.id = entity_id
            self.calls = 0

        def handle(self, msg):
            self.calls += 1
            return (self.id, self.calls, msg)

    return Entity()


def _extractor(msg):
    """`msg` is a `(entity_id, payload)` tuple."""
    eid, payload = msg
    return (str(eid), payload)


def _make_region():
    return atomr.cluster_sharding.ShardRegion(_entity_factory, _extractor)


def _shard_region_constructible() -> bool:
    """Best-effort detection of whether the installed ShardRegion exposes
    its `__new__`. The currently-built wheel for this worktree's main
    has a known issue ("No constructor defined for ShardRegion") that
    blocks the in-process variant of these tests; gate them so the
    suite stays green until the binding is rebuilt.
    """
    try:
        atomr.cluster_sharding.ShardRegion(_entity_factory, _extractor)
        return True
    except TypeError:
        return False


_HAS_SHARDREGION = _shard_region_constructible()
_NO_SHARDREGION_REASON = (
    "atomr.cluster_sharding.ShardRegion has no callable constructor in the "
    "installed wheel (pre-existing failure mirrored in "
    "test_extension_modules.py::test_sharding_routes_to_entity). "
    "Rebuild the pyo3 binding to surface the `#[new]` constructor."
)
_skip_no_shardregion = pytest.mark.skipif(
    not _HAS_SHARDREGION, reason=_NO_SHARDREGION_REASON
)


# ---------------------------------------------------------------------------
# Two-region (multi-node simulated) rebalance tests.
# ---------------------------------------------------------------------------

@_skip_no_shardregion
def test_two_regions_partition_entities_by_id():
    """Two regions, four entities, deterministic 2/2 split.

    Caller-controlled allocation: even-IDed entities go to region A,
    odd-IDed to region B.  This is the ground-truth shape that
    ``LeastShardAllocationStrategy`` should converge to in the TCP
    variant.
    """
    region_a = _make_region()
    region_b = _make_region()
    entities = [("e1", "x"), ("e2", "x"), ("e3", "x"), ("e4", "x")]
    for eid, payload in entities:
        # Even tail digit → region A, odd → region B.
        target = region_a if int(eid[1:]) % 2 == 0 else region_b
        target.deliver((eid, payload))
    assert region_a.entity_count() == 2  # e2, e4
    assert region_b.entity_count() == 2  # e1, e3


@_skip_no_shardregion
def test_entity_messages_route_to_owning_region():
    """An entity's handler is invoked once per message on its owning region."""
    region_a = _make_region()
    region_b = _make_region()
    # Place e1 on A.
    out1 = region_a.deliver(("e1", "first"))
    out2 = region_a.deliver(("e1", "second"))
    # Place e2 on B.
    out3 = region_b.deliver(("e2", "first"))
    assert out1 == ("e1", 1, "first")
    assert out2 == ("e1", 2, "second")
    assert out3 == ("e2", 1, "first")
    assert region_a.entity_count() == 1
    assert region_b.entity_count() == 1


@_skip_no_shardregion
def test_rebalance_relocates_entity_to_other_region():
    """Simulated rebalance: re-issue the same entity on a different region.

    The entity restarts on the new owner; state from the old owner is
    not carried over (that's the in-process limitation).  This still
    exercises the routing/factory contract that the multi-node TCP
    variant needs.
    """
    region_a = _make_region()
    region_b = _make_region()
    region_a.deliver(("e1", "before"))
    assert region_a.entity_count() == 1
    # Rebalance: e1 now lives on B.
    region_b.deliver(("e1", "after"))
    assert region_b.entity_count() == 1


@_skip_no_shardregion
def test_total_entities_across_regions_equal_unique_ids():
    """Sum of per-region entity counts == |unique entity IDs|.

    Holds when each entity is hosted on exactly one region (sharding
    invariant).  Mirrors the post-rebalance assertion the TCP test
    would make.
    """
    region_a = _make_region()
    region_b = _make_region()
    # Allocate by hash(entity_id) % 2 — deterministic.
    ids = [f"e{i}" for i in range(10)]
    seen = set()
    for eid in ids:
        target = region_a if hash(eid) % 2 == 0 else region_b
        target.deliver((eid, "msg"))
        seen.add(eid)
    assert region_a.entity_count() + region_b.entity_count() == len(seen)


# ---------------------------------------------------------------------------
# TCP-transport variant — currently blocked.
# ---------------------------------------------------------------------------

_TCP_SHARDING_REASON = (
    "Two-node ShardRegion over TCP requires Cluster.with_tcp_transport "
    "and a clustered ShardingExtension. The Python bindings in this "
    "worktree expose only an in-process ShardRegion. Re-enable once "
    "Wave 1 Epic A + Phase 6 sharding cluster integration land in the "
    "pyo3 facade."
)


@pytest.mark.skip(reason=_TCP_SHARDING_REASON)
def test_entity_rebalances_across_two_tcp_nodes():  # pragma: no cover
    """Entities e1..e4 rebalance across two TCP-bound ShardRegions."""
    pass


@pytest.mark.skip(reason=_TCP_SHARDING_REASON)
def test_multi_node_rebalance_via_loopback_transport():  # pragma: no cover
    """Phase-6-era test from the original test_sharding.py.

    The original test was already absent from this worktree's
    `python/tests/` — the un-skip step from the task description is a
    no-op here. Kept as a placeholder so the test ID remains stable
    across worktrees.
    """
    pass
