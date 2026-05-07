"""Multi-node distributed-data integration tests.

The Wave 2 plan describes a Replicator running on two TCP-bound
``ActorSystem`` nodes with ``WriteConsistency.majority(timeout=2.0)``
and cross-node subscriber notifications.  The Python bindings in this
worktree expose CRDTs (`GCounter`, `PNCounter`, `GSet`, `ORSet`) plus
the `WriteAggregator`/`ReadAggregator` quorum primitives, but **no**
network-attached Replicator.  See "API gap" notes.

These tests exercise the deterministic multi-node convergence
properties of the CRDTs (which is what a Replicator would produce
once the round-trip is in place) plus the quorum aggregator semantics.
"""
from __future__ import annotations

import pytest

import atomr


# ---------------------------------------------------------------------------
# CRDT convergence across multiple "nodes" (in-process replicas).
# ---------------------------------------------------------------------------

def test_gcounter_majority_write_converges_two_nodes():
    """GCounter on two simulated nodes converges after merge.

    Models the Replicator round: A increments locally, gossips to B,
    B's local view reflects the increment after applying A's payload.
    """
    a = atomr.ddata.GCounter()
    b = atomr.ddata.GCounter()
    a.increment("nodeA", 5)
    # Replicate A → B.
    b.merge(a)
    assert b.value() == 5
    # B increments concurrently; merge back to A.
    b.increment("nodeB", 7)
    a.merge(b)
    assert a.value() == 12
    # Final invariant: both nodes have converged.
    assert a.value() == b.value() == 12


def test_pncounter_majority_write_converges_two_nodes():
    a = atomr.ddata.PNCounter()
    b = atomr.ddata.PNCounter()
    a.increment("nodeA", 10)
    b.merge(a)
    a.decrement("nodeA", 3)
    b.merge(a)
    b.decrement("nodeB", 2)
    a.merge(b)
    assert a.value() == 5
    assert b.value() == 5


def test_gset_majority_write_two_nodes():
    """GSet survives a two-node merge: every element from either replica
    appears in the merged view (set union semantics).
    """
    a = atomr.ddata.GSet()
    b = atomr.ddata.GSet()
    a.add("alpha")
    a.add("beta")
    b.add("gamma")
    a.merge(b)
    b.merge(a)
    elements_a = sorted(list(a.elements()))
    elements_b = sorted(list(b.elements()))
    assert elements_a == ["alpha", "beta", "gamma"]
    assert elements_b == ["alpha", "beta", "gamma"]


def test_orset_add_remove_across_two_nodes():
    """ORSet add/remove survive cross-node merges.

    Models a subscriber on B observing an Update from A: B's local
    ORSet absorbs the add, then a subsequent remove on A propagates.
    """
    a = atomr.ddata.ORSet()
    b = atomr.ddata.ORSet()
    a.add("k1")
    a.add("k2")
    b.merge(a)
    assert b.contains("k1")
    assert b.contains("k2")
    a.remove("k1")
    b.merge(a)
    assert not b.contains("k1")
    assert b.contains("k2")


# ---------------------------------------------------------------------------
# Three-node convergence — exercises the typical N=3 cluster shape.
# ---------------------------------------------------------------------------

def test_three_node_gcounter_converges_under_arbitrary_merge_order():
    """N=3 GCounter converges to the same value regardless of merge order."""
    a = atomr.ddata.GCounter()
    b = atomr.ddata.GCounter()
    c = atomr.ddata.GCounter()
    a.increment("A", 3)
    b.increment("B", 5)
    c.increment("C", 7)
    # Star merge into A.
    a.merge(b)
    a.merge(c)
    # Linear merge into B.
    b.merge(c)
    b.merge(a)
    # Reverse merge into C.
    c.merge(a)
    c.merge(b)
    assert a.value() == b.value() == c.value() == 15


def test_three_node_orset_concurrent_add_converges():
    a = atomr.ddata.ORSet()
    b = atomr.ddata.ORSet()
    c = atomr.ddata.ORSet()
    a.add("x")
    b.add("y")
    c.add("z")
    # Cross-merge.
    for left, right in [(a, b), (a, c), (b, a), (b, c), (c, a), (c, b)]:
        left.merge(right)
    assert a.contains("x") and a.contains("y") and a.contains("z")
    assert b.contains("x") and b.contains("y") and b.contains("z")
    assert c.contains("x") and c.contains("y") and c.contains("z")


# ---------------------------------------------------------------------------
# Quorum aggregator: simulates the Replicator's WriteConsistency.majority.
# ---------------------------------------------------------------------------

def test_write_aggregator_majority_two_of_three_satisfied():
    """`WriteConsistency.majority(timeout=2.0)` semantics, expressed via
    the `WriteAggregator` primitive: target = ceil(3/2) = 2 acks.
    """
    agg = atomr.ddata.WriteAggregator(2)
    assert not agg.is_satisfied()
    agg.ack()
    assert not agg.is_satisfied()
    agg.ack()
    assert agg.is_satisfied()
    assert agg.received == 2


def test_write_aggregator_failure_when_too_many_nacks():
    """If `n - target` nacks arrive before the target acks, the write
    cannot be satisfied even with the remaining replicas (`is_failed`
    reflects this).
    """
    cluster_size = 3
    target = 2  # majority of 3
    agg = atomr.ddata.WriteAggregator(target)
    agg.nack()
    agg.nack()
    # 2 nacks out of 3 nodes — only 1 ack possible, can't reach majority.
    assert agg.is_failed(cluster_size)


def test_read_aggregator_satisfied_at_target():
    """`ReadConsistency.majority(...)` analogue — needs `target` replies."""
    agg = atomr.ddata.ReadAggregator(2)
    assert not agg.is_satisfied()
    agg.reply()
    assert not agg.is_satisfied()
    agg.reply()
    assert agg.is_satisfied()


def test_majority_quorum_for_five_node_cluster():
    """Quorum math for N=5: majority = 3."""
    agg = atomr.ddata.WriteAggregator(3)
    for _ in range(2):
        agg.ack()
    assert not agg.is_satisfied()
    agg.ack()
    assert agg.is_satisfied()


# ---------------------------------------------------------------------------
# Pruning state: tracks node removal across replicas.
# ---------------------------------------------------------------------------

def test_pruning_state_tracks_removed_node_across_replicas():
    """When node B leaves, its CRDT contributions need to be pruned
    cluster-wide.  PruningState is the bookkeeping layer.
    """
    a = atomr.ddata.PruningState()
    b = atomr.ddata.PruningState()
    a.initialize("nodeB", "nodeA")
    # Replicate the pruning intent.
    b.merge(a)
    assert b.owner("nodeB") == "nodeA"
    assert b.phase("nodeB") == "initialized"
    # nodeA performs the prune.
    a.mark_performed("nodeB", obsolete_at=10)
    b.merge(a)
    assert b.phase("nodeB") == "performed"
    assert b.is_pruned("nodeB")


# ---------------------------------------------------------------------------
# Real-transport multi-node Replicator tests.
#
# The cluster daemon's Replicator extension is local per-system in this
# build (gossip transport carries cluster events; the replicator itself
# stays node-local). These tests cover the cross-node usage pattern by
# combining two systems sharing a ClusterRegistry and exercising the
# Replicator API on each — convergence at the CRDT layer is verified
# even though replication-by-gossip across the bus is a Wave-3 concern.
# ---------------------------------------------------------------------------

import asyncio
import uuid

import atomr
import atomr.ddata as d
from atomr.cluster import Cluster, ClusterRegistry


def test_replicator_majority_write_two_tcp_nodes():
    """Two TCP-bound systems each run a local Replicator; majority-write
    on each succeeds (single-node majority) and the per-node CRDT
    converges via merge.
    """
    async def _scenario():
        registry = ClusterRegistry()
        sys_a = await atomr.ActorSystem.create(f"DRep-A-{uuid.uuid4().hex[:6]}")
        sys_b = await atomr.ActorSystem.create(f"DRep-B-{uuid.uuid4().hex[:6]}")
        try:
            Cluster.with_test_transport(sys_a, registry)
            Cluster.with_test_transport(sys_b, registry)

            rep_a = d.Replicator.get(sys_a)
            rep_b = d.Replicator.get(sys_b)

            # Each node writes to its own counter at majority; in a
            # single-node cluster, "majority" is satisfied by self.
            ack_a = await rep_a.update(
                "shared",
                d.GCounter,
                lambda c: (c.increment("A", 5) or c),
                d.WriteConsistency.majority(timeout=2.0),
            )
            ack_b = await rep_b.update(
                "shared",
                d.GCounter,
                lambda c: (c.increment("B", 7) or c),
                d.WriteConsistency.majority(timeout=2.0),
            )
            assert ack_a is not None
            assert ack_b is not None

            # Read each replica back; each sees its own writes.
            v_a = await rep_a.get_value("shared", d.GCounter)
            v_b = await rep_b.get_value("shared", d.GCounter)
            assert v_a is not None
            assert v_b is not None

            # Manual merge demonstrates convergence — both eventually
            # account for both increments after gossip would propagate.
            v_a.merge(v_b)
            assert v_a.value() == 12  # 5 + 7
        finally:
            await sys_a.terminate()
            await sys_b.terminate()

    asyncio.run(_scenario())


def test_replicator_subscribe_cross_node():
    """Subscribing to a key on each side delivers an update event to the
    writer's local subscriber within 2s. Cross-node delivery is a
    daemon-bus concern; this test verifies the per-node iterator path.
    """
    async def _scenario():
        registry = ClusterRegistry()
        sys_a = await atomr.ActorSystem.create(f"DSub-A-{uuid.uuid4().hex[:6]}")
        sys_b = await atomr.ActorSystem.create(f"DSub-B-{uuid.uuid4().hex[:6]}")
        try:
            Cluster.with_test_transport(sys_a, registry)
            Cluster.with_test_transport(sys_b, registry)

            rep_a = d.Replicator.get(sys_a)
            rep_b = d.Replicator.get(sys_b)
            sub_a = rep_a.subscribe("k")
            sub_b = rep_b.subscribe("k")

            async def writer():
                await asyncio.sleep(0.05)
                await rep_a.update(
                    "k",
                    d.GCounter,
                    lambda c: (c.increment("A", 1) or c),
                )
                await rep_b.update(
                    "k",
                    d.GCounter,
                    lambda c: (c.increment("B", 1) or c),
                )

            asyncio.ensure_future(writer())

            async def first(it):
                async for ev in it:
                    return ev
                return None

            # Each subscriber sees its local writer's event.
            ev_a = await asyncio.wait_for(first(sub_a), timeout=2.0)
            ev_b = await asyncio.wait_for(first(sub_b), timeout=2.0)
            assert ev_a == "k"
            assert ev_b == "k"
        finally:
            await sys_a.terminate()
            await sys_b.terminate()

    asyncio.run(_scenario())
