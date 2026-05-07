"""Phase 6 — cluster sharding tests.

Single-node coverage of the new `ShardRegion`:
  * Routing: same `entity_id` reaches the same actor; different
    entity ids reach different actors.
  * Passivation: idle entities are stopped after the configured TTL.
  * `entity_count` reflects active entities.
  * Remember-entities: entities recover after region restart (the
    durable store is reused via the per-`ActorSystem` registry).

Multi-node rebalance via the loopback transport is deferred until
Phase 9 wires the real `GossipTransport` into the cluster daemon —
the Phase 5 daemon currently uses `NoopGossipTransport`, so a second
region in the same process does not see membership events.
"""
from __future__ import annotations

import time

import pytest

from atomr import Actor, ActorSystem, props
from atomr.cluster_sharding import ShardRegion, ShardingSettings


def _wait_for(predicate, timeout: float = 2.0, interval: float = 0.02) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


# Module-level entity actor records call counts in a module-level dict so
# tests can inspect post-mortem. CounterEntity discovers its entity_id
# from `ctx.path` on the first `handle` call (the path layout is
# `akka://<system>/user/<type_name>-<entity_id>`).
INSTANCE_COUNTS: dict[str, int] = {}
INSTANCE_INSTANCES: dict[str, int] = {}


class CounterEntity(Actor):
    _next_id = 0

    def __init__(self):
        # Each fresh instance gets a unique id so we can detect respawns.
        CounterEntity._next_id += 1
        self._instance_id = CounterEntity._next_id
        self._n = 0
        self._key = None

    async def handle(self, ctx, msg):
        if self._key is None:
            # Path looks like
            # akka://sys/user/<type_name>-<entity_id>[__incN].
            suffix = ctx.path.rsplit("/", 1)[-1]
            without_type = suffix.split("-", 1)[1] if "-" in suffix else suffix
            self._key = without_type.split("__inc", 1)[0]
            INSTANCE_INSTANCES[self._key] = (
                INSTANCE_INSTANCES.get(self._key, 0) + 1
            )
        self._n += 1
        INSTANCE_COUNTS[self._key] = self._n


def _extractor(msg):
    eid = str(msg["entity"])
    return (eid, str(hash(eid) % 16), msg)


def test_routes_messages_to_same_entity_instance():
    INSTANCE_COUNTS.clear()
    INSTANCE_INSTANCES.clear()
    sys = ActorSystem.create_blocking("shard-route-1")
    try:
        region = ShardRegion.start(
            sys,
            type_name="counters",
            entity_props=props(CounterEntity),
            message_extractor=_extractor,
        )
        for _ in range(5):
            region.tell({"entity": "alice", "op": "incr"})
        assert _wait_for(lambda: INSTANCE_COUNTS.get("alice", 0) >= 5)
        assert INSTANCE_COUNTS["alice"] == 5
        # Single instance handled all 5 messages.
        assert INSTANCE_INSTANCES["alice"] == 1
    finally:
        sys.terminate_blocking()


def test_distinct_entity_ids_get_distinct_actors():
    INSTANCE_COUNTS.clear()
    INSTANCE_INSTANCES.clear()
    sys = ActorSystem.create_blocking("shard-route-2")
    try:
        region = ShardRegion.start(
            sys,
            type_name="counters",
            entity_props=props(CounterEntity),
            message_extractor=_extractor,
        )
        region.tell({"entity": "alice", "op": "incr"})
        region.tell({"entity": "bob", "op": "incr"})
        region.tell({"entity": "alice", "op": "incr"})
        region.tell({"entity": "carol", "op": "incr"})

        assert _wait_for(lambda: region.entity_count() >= 3)
        assert region.entity_count() == 3

        # Each entity has its own state.
        assert _wait_for(
            lambda: INSTANCE_COUNTS.get("alice") == 2
            and INSTANCE_COUNTS.get("bob") == 1
            and INSTANCE_COUNTS.get("carol") == 1
        )
    finally:
        sys.terminate_blocking()


def test_entity_count_reflects_active_entities():
    INSTANCE_COUNTS.clear()
    INSTANCE_INSTANCES.clear()
    sys = ActorSystem.create_blocking("shard-count")
    try:
        region = ShardRegion.start(
            sys,
            type_name="ec",
            entity_props=props(CounterEntity),
            message_extractor=_extractor,
        )
        assert region.entity_count() == 0
        region.tell({"entity": "x", "op": "ping"})
        assert _wait_for(lambda: region.entity_count() == 1)
        region.tell({"entity": "y", "op": "ping"})
        assert _wait_for(lambda: region.entity_count() == 2)
    finally:
        sys.terminate_blocking()


def test_request_passivation_drops_entity():
    INSTANCE_COUNTS.clear()
    INSTANCE_INSTANCES.clear()
    sys = ActorSystem.create_blocking("shard-passivate-explicit")
    try:
        region = ShardRegion.start(
            sys,
            type_name="cp",
            entity_props=props(CounterEntity),
            message_extractor=_extractor,
        )
        region.tell({"entity": "alice", "op": "incr"})
        assert _wait_for(lambda: region.entity_count() == 1)
        region.request_passivation("alice")
        assert _wait_for(lambda: region.entity_count() == 0)

        # Sending again should respawn alice; two distinct actor
        # instances should now have handled alice's messages.
        region.tell({"entity": "alice", "op": "incr"})
        assert _wait_for(lambda: region.entity_count() == 1)
        ok = _wait_for(
            lambda: INSTANCE_INSTANCES.get("alice", 0) >= 2,
            timeout=3.0,
        )
        assert ok, (
            f"INSTANCE_INSTANCES={INSTANCE_INSTANCES!r} "
            f"INSTANCE_COUNTS={INSTANCE_COUNTS!r} "
            f"entity_ids={region.entity_ids()!r}"
        )
    finally:
        sys.terminate_blocking()


def test_passivation_idle_timeout_stops_idle_entity():
    INSTANCE_COUNTS.clear()
    INSTANCE_INSTANCES.clear()
    sys = ActorSystem.create_blocking("shard-passivate-idle")
    try:
        region = ShardRegion.start(
            sys,
            type_name="ci",
            entity_props=props(CounterEntity),
            message_extractor=_extractor,
            settings=ShardingSettings(passivation_idle_timeout=0.2),
        )
        region.tell({"entity": "alice", "op": "incr"})
        assert _wait_for(lambda: region.entity_count() == 1)
        # The sweeper runs at half the idle timeout (capped to 50ms),
        # so wait long enough to give it a couple of cycles.
        assert _wait_for(lambda: region.entity_count() == 0, timeout=2.5)
    finally:
        sys.terminate_blocking()


def test_remember_entities_recovers_on_region_restart():
    INSTANCE_COUNTS.clear()
    INSTANCE_INSTANCES.clear()
    sys = ActorSystem.create_blocking("shard-remember")
    try:
        # First region, with remember-entities turned on.
        r1 = ShardRegion.start(
            sys,
            type_name="re",
            entity_props=props(CounterEntity),
            message_extractor=_extractor,
            settings=ShardingSettings(remember_entities=True),
        )
        r1.tell({"entity": "alice", "op": "incr"})
        r1.tell({"entity": "bob", "op": "incr"})
        assert _wait_for(lambda: r1.entity_count() == 2)
        # Shut down the region but keep the actor system alive — the
        # remember-store is per-system-and-type, so the second region
        # will pick up the same store.
        r1.shutdown()
        assert _wait_for(lambda: r1.entity_count() == 0)

        # New region for the same type. It should rehydrate alice + bob
        # from the remember store before any new traffic arrives.
        r2 = ShardRegion.start(
            sys,
            type_name="re",
            entity_props=props(CounterEntity),
            message_extractor=_extractor,
            settings=ShardingSettings(remember_entities=True),
        )
        # Warm-up runs on a tokio task; give it time to complete.
        assert _wait_for(lambda: r2.entity_count() >= 2, timeout=3.0)
        ids = set(r2.entity_ids())
        assert "alice" in ids
        assert "bob" in ids
    finally:
        sys.terminate_blocking()


def test_pinned_allocation_strategy_starts_region():
    INSTANCE_COUNTS.clear()
    INSTANCE_INSTANCES.clear()
    sys = ActorSystem.create_blocking("shard-pinned")
    try:
        region = ShardRegion.start(
            sys,
            type_name="pin",
            entity_props=props(CounterEntity),
            message_extractor=_extractor,
            settings=ShardingSettings(allocation_strategy="pinned"),
        )
        region.tell({"entity": "alice", "op": "incr"})
        assert _wait_for(lambda: region.entity_count() == 1)
    finally:
        sys.terminate_blocking()


def test_invalid_allocation_strategy_raises():
    with pytest.raises(ValueError):
        ShardingSettings(allocation_strategy="random")


def test_three_callable_extractor_form():
    INSTANCE_COUNTS.clear()
    INSTANCE_INSTANCES.clear()
    sys = ActorSystem.create_blocking("shard-three-cb")
    try:
        region = ShardRegion.start(
            sys,
            type_name="tcb",
            entity_props=props(CounterEntity),
            message_extractor=lambda m: m["entity"],
            shard_id_extractor=lambda m: hash(m["entity"]) & 0xF,
            unwrap_extractor=lambda m: m["payload"],
            settings=ShardingSettings(number_of_shards=16),
        )
        region.tell({"entity": "alice", "payload": {"op": "incr"}})
        assert _wait_for(lambda: region.entity_count() == 1)
    finally:
        sys.terminate_blocking()


@pytest.mark.skip(
    reason=(
        "Multi-node rebalance via loopback transport is deferred until "
        "Phase 9 swaps NoopGossipTransport for a real transport. The "
        "Phase 5 cluster daemon currently never publishes gossip across "
        "regions, so a second-region rebalance cannot be exercised."
    )
)
def test_multi_node_rebalance_via_loopback_transport():  # pragma: no cover
    pass
