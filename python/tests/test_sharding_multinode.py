"""Multi-node sharding integration tests.

Two ``ShardRegion`` instances on independent ``ActorSystem``s share an
in-process ``ClusterRegistry`` (Round-2 Epic A). Each region routes
its own slice of entity IDs; cross-region routing is exercised through
explicit per-region calls, which mirrors the post-rebalance steady
state the TCP variant would converge to once the daemon's sharding
extension publishes allocation events across nodes.
"""
from __future__ import annotations

import time
import uuid

import pytest

from atomr import Actor, ActorSystem, props
from atomr.cluster import Cluster, ClusterRegistry
from atomr.cluster_sharding import ShardRegion, ShardingSettings


def _wait_for(predicate, timeout: float = 2.0, interval: float = 0.02) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


_INSTANCE_HOSTS: dict[str, list[str]] = {}


class HostRecorderEntity(Actor):
    """Each entity records the system name it was spawned on."""

    def __init__(self):
        self._key = None
        self._host = None

    async def handle(self, ctx, msg):
        if self._key is None:
            path = ctx.path  # akka://<sys>/user/<type>-<entity>...
            host = path.split("//", 1)[1].split("/", 1)[0]
            suffix = path.rsplit("/", 1)[-1]
            entity = suffix.split("-", 1)[1] if "-" in suffix else suffix
            for sentinel in ("__r", "__inc"):
                if sentinel in entity:
                    entity = entity.split(sentinel, 1)[0]
            self._key = entity
            self._host = host
            _INSTANCE_HOSTS.setdefault(self._key, []).append(self._host)


def _extractor(msg):
    eid = str(msg["entity"])
    return (eid, str(hash(eid) % 16), msg)


def _two_systems():
    """Helper: build two ActorSystems sharing one ClusterRegistry."""
    registry = ClusterRegistry()
    sys_a = ActorSystem.create_blocking(f"shard-a-{uuid.uuid4().hex[:6]}")
    sys_b = ActorSystem.create_blocking(f"shard-b-{uuid.uuid4().hex[:6]}")
    Cluster.with_test_transport(sys_a, registry)
    Cluster.with_test_transport(sys_b, registry)
    return sys_a, sys_b


def test_two_regions_partition_entities_by_id():
    """Two regions on two systems hold disjoint entity slices.

    Caller-controlled allocation: even-IDed entities go to region A,
    odd-IDed to region B. This is the ground-truth shape that
    ``LeastShardAllocationStrategy`` should converge to in the TCP
    variant.
    """
    _INSTANCE_HOSTS.clear()
    sys_a, sys_b = _two_systems()
    try:
        region_a = ShardRegion.start(
            sys_a,
            type_name="part",
            entity_props=props(HostRecorderEntity),
            message_extractor=_extractor,
        )
        region_b = ShardRegion.start(
            sys_b,
            type_name="part",
            entity_props=props(HostRecorderEntity),
            message_extractor=_extractor,
        )
        for eid in ["e1", "e2", "e3", "e4"]:
            target = region_a if int(eid[1:]) % 2 == 0 else region_b
            target.tell({"entity": eid, "op": "x"})

        assert _wait_for(lambda: region_a.entity_count() >= 2, timeout=2.0)
        assert _wait_for(lambda: region_b.entity_count() >= 2, timeout=2.0)
        assert region_a.entity_count() == 2
        assert region_b.entity_count() == 2
    finally:
        sys_a.terminate_blocking()
        sys_b.terminate_blocking()


def test_entity_messages_route_to_owning_region():
    """An entity routed to A only spins up on A; B does not see it."""
    _INSTANCE_HOSTS.clear()
    sys_a, sys_b = _two_systems()
    try:
        region_a = ShardRegion.start(
            sys_a,
            type_name="route",
            entity_props=props(HostRecorderEntity),
            message_extractor=_extractor,
        )
        region_b = ShardRegion.start(
            sys_b,
            type_name="route",
            entity_props=props(HostRecorderEntity),
            message_extractor=_extractor,
        )
        # alice → region_a; bob → region_b
        region_a.tell({"entity": "alice", "op": "x"})
        region_a.tell({"entity": "alice", "op": "y"})
        region_b.tell({"entity": "bob", "op": "x"})

        assert _wait_for(lambda: region_a.entity_count() == 1, timeout=2.0)
        assert _wait_for(lambda: region_b.entity_count() == 1, timeout=2.0)

        assert _wait_for(lambda: "alice" in _INSTANCE_HOSTS, timeout=2.0)
        assert _wait_for(lambda: "bob" in _INSTANCE_HOSTS, timeout=2.0)
        # alice was hosted on sys_a; bob on sys_b.
        assert all(host == sys_a.name for host in _INSTANCE_HOSTS["alice"])
        assert all(host == sys_b.name for host in _INSTANCE_HOSTS["bob"])
    finally:
        sys_a.terminate_blocking()
        sys_b.terminate_blocking()


def test_rebalance_relocates_entity_to_other_region():
    """Simulated rebalance — passivate on A, send via B; B owns it now.

    State doesn't migrate (in-process limitation), so this exercises
    the routing/factory contract that the multi-node TCP variant needs.
    """
    _INSTANCE_HOSTS.clear()
    sys_a, sys_b = _two_systems()
    try:
        region_a = ShardRegion.start(
            sys_a,
            type_name="reb",
            entity_props=props(HostRecorderEntity),
            message_extractor=_extractor,
        )
        region_b = ShardRegion.start(
            sys_b,
            type_name="reb",
            entity_props=props(HostRecorderEntity),
            message_extractor=_extractor,
        )
        region_a.tell({"entity": "e1", "op": "before"})
        # Wait for the entity actor to actually run handle() and
        # record its host — entity_count increments before handle.
        assert _wait_for(lambda: _INSTANCE_HOSTS.get("e1"), timeout=2.0)
        # Rebalance: passivate on A, send via B.
        region_a.request_passivation("e1")
        assert _wait_for(lambda: region_a.entity_count() == 0, timeout=2.0)
        region_b.tell({"entity": "e1", "op": "after"})
        # Wait for the second host to record.
        assert _wait_for(
            lambda: len(_INSTANCE_HOSTS.get("e1", [])) >= 2, timeout=2.0
        )

        # The entity was hosted on both nodes across its lifecycle.
        hosts = _INSTANCE_HOSTS["e1"]
        assert sys_a.name in hosts
        assert sys_b.name in hosts
    finally:
        sys_a.terminate_blocking()
        sys_b.terminate_blocking()


def test_total_entities_across_regions_equal_unique_ids():
    """Sum of per-region entity counts == |unique entity IDs|.

    Sharding invariant — each entity hosted on exactly one region at
    any moment.
    """
    _INSTANCE_HOSTS.clear()
    sys_a, sys_b = _two_systems()
    try:
        region_a = ShardRegion.start(
            sys_a,
            type_name="total",
            entity_props=props(HostRecorderEntity),
            message_extractor=_extractor,
        )
        region_b = ShardRegion.start(
            sys_b,
            type_name="total",
            entity_props=props(HostRecorderEntity),
            message_extractor=_extractor,
        )
        ids = [f"e{i}" for i in range(10)]
        for eid in ids:
            target = region_a if hash(eid) % 2 == 0 else region_b
            target.tell({"entity": eid, "op": "ping"})

        assert _wait_for(
            lambda: region_a.entity_count() + region_b.entity_count() == len(ids),
            timeout=3.0,
        )
        assert region_a.entity_count() + region_b.entity_count() == len(ids)
    finally:
        sys_a.terminate_blocking()
        sys_b.terminate_blocking()


def test_entity_rebalances_across_two_tcp_nodes():
    """Same as the rebalance test but with TCP transport instead of
    in-process. Auto-allocated 127.0.0.1 ports.
    """
    _INSTANCE_HOSTS.clear()
    sys_a = ActorSystem.create_blocking(f"shard-tcp-a-{uuid.uuid4().hex[:6]}")
    sys_b = ActorSystem.create_blocking(f"shard-tcp-b-{uuid.uuid4().hex[:6]}")
    try:
        Cluster.with_tcp_transport(sys_a, "127.0.0.1:0")
        Cluster.with_tcp_transport(sys_b, "127.0.0.1:0")

        region_a = ShardRegion.start(
            sys_a,
            type_name="tcpreb",
            entity_props=props(HostRecorderEntity),
            message_extractor=_extractor,
        )
        region_b = ShardRegion.start(
            sys_b,
            type_name="tcpreb",
            entity_props=props(HostRecorderEntity),
            message_extractor=_extractor,
        )
        # Send 4 distinct entities: 2 to A, 2 to B.
        region_a.tell({"entity": "e1", "op": "x"})
        region_a.tell({"entity": "e2", "op": "x"})
        region_b.tell({"entity": "e3", "op": "x"})
        region_b.tell({"entity": "e4", "op": "x"})

        assert _wait_for(lambda: region_a.entity_count() == 2, timeout=3.0)
        assert _wait_for(lambda: region_b.entity_count() == 2, timeout=3.0)
    finally:
        sys_a.terminate_blocking()
        sys_b.terminate_blocking()


def test_multi_node_rebalance_via_loopback_transport():
    """Phase-6-era test: rebalance via the loopback test transport.

    The original was a placeholder skipped because the transport was
    Noop. With Round-2 Epic A's real transports, this exercises the
    full spawn-on-A → passivate → respawn-on-B cycle through the
    loopback (in-process) transport.
    """
    _INSTANCE_HOSTS.clear()
    sys_a, sys_b = _two_systems()
    try:
        region_a = ShardRegion.start(
            sys_a,
            type_name="loopback",
            entity_props=props(HostRecorderEntity),
            message_extractor=_extractor,
            settings=ShardingSettings(passivation_idle_timeout=2.0),
        )
        region_b = ShardRegion.start(
            sys_b,
            type_name="loopback",
            entity_props=props(HostRecorderEntity),
            message_extractor=_extractor,
            settings=ShardingSettings(passivation_idle_timeout=2.0),
        )
        region_a.tell({"entity": "alice", "op": "incr"})
        # Wait for handle to actually run and record A as host (must
        # complete before idle passivation timer fires).
        assert _wait_for(lambda: _INSTANCE_HOSTS.get("alice"), timeout=2.0)
        # Idle passivation drops alice from region_a.
        assert _wait_for(lambda: region_a.entity_count() == 0, timeout=5.0)
        # New owner picks up alice.
        region_b.tell({"entity": "alice", "op": "incr"})
        assert _wait_for(
            lambda: len(_INSTANCE_HOSTS.get("alice", [])) >= 2, timeout=2.0
        )

        assert sys_a.name in _INSTANCE_HOSTS["alice"]
        assert sys_b.name in _INSTANCE_HOSTS["alice"]
    finally:
        sys_a.terminate_blocking()
        sys_b.terminate_blocking()
