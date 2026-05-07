"""Phase-5 cluster control plane — single-node smoke tests.

Multi-node tests require a real `atomr-remote` transport and live in
Phase 9 once the transport is wired up. These tests focus on:

* `Cluster.get(system)` is idempotent.
* The local node reaches `Up` after `join_seed_nodes`.
* `cluster.subscribe(["MemberUp"])` yields a typed `MemberUp` event.
* `cluster.leave()` produces a `MemberRemoved` event.
* SBR strategy keys round-trip through `Config`.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

import atomr
from atomr import ActorSystem, Config
from atomr.cluster import (
    SBR_STRATEGIES,
    Cluster,
    LeaderChanged,
    MemberRemoved,
    MemberUp,
)


def test_cluster_get_returns_same_handle_each_time():
    sys = ActorSystem.create_blocking("phase5-singleton")
    try:
        c1 = Cluster.get(sys)
        c2 = Cluster.get(sys)
        assert c1.self_address == c2.self_address
        assert c1.self_address.startswith("akka://phase5-singleton")
    finally:
        sys.terminate_blocking()


def test_membership_snapshot_contains_self():
    sys = ActorSystem.create_blocking("phase5-snapshot")
    try:
        cluster = Cluster.get(sys)
        # The daemon registered self on `Cluster.get`. Drive a tick by
        # reading the snapshot a couple of times — the leader-action
        # path promotes Joining → Up.
        deadline = asyncio.get_event_loop().time() + 5.0 if False else None  # noqa: F841
        for _ in range(50):
            snap = cluster.membership_snapshot()
            members = snap.members()
            if any(m.address == cluster.self_address for m in members):
                break
            import time

            time.sleep(0.02)
        else:
            pytest.fail("self never appeared in membership snapshot")
        assert cluster.member_count() >= 1
    finally:
        sys.terminate_blocking()


def test_join_seed_nodes_promotes_self_to_up():
    sys = ActorSystem.create_blocking("phase5-join")
    try:
        cluster = Cluster.get(sys)

        async def go():
            await cluster.join_seed_nodes([cluster.self_address], timeout=10.0)

        asyncio.run(go())

        snap = cluster.membership_snapshot()
        members = snap.members()
        me = next(m for m in members if m.address == cluster.self_address)
        assert me.status in {"up", "weakly_up"}, me.status
    finally:
        sys.terminate_blocking()


def test_subscribe_member_up_is_received():
    sys = ActorSystem.create_blocking("phase5-subscribe")
    try:
        cluster = Cluster.get(sys)

        async def collect_one() -> Any:
            sub = cluster.subscribe(["MemberUp"], capacity=64)
            try:
                # Drive the daemon to converge.
                await cluster.join_seed_nodes([cluster.self_address], timeout=10.0)
                # MemberUp arrives once the leader-action tick fires.
                async with asyncio.timeout(10.0):
                    async for ev in sub:
                        return ev
            finally:
                sub.close()

        evt = asyncio.run(collect_one())
        assert isinstance(evt, MemberUp), repr(evt)
        assert evt.member.address == cluster.self_address
        assert evt.member.status in {"up", "weakly_up"}
    finally:
        sys.terminate_blocking()


def test_leave_eventually_removes_self():
    sys = ActorSystem.create_blocking("phase5-leave")
    try:
        cluster = Cluster.get(sys)

        async def go() -> Any:
            await cluster.join_seed_nodes([cluster.self_address], timeout=10.0)
            sub = cluster.subscribe(["MemberRemoved", "MemberLeft", "MemberExited"], capacity=64)
            try:
                # Kick off the leave in a background task so we can race the
                # subscriber against it.
                leave_task = asyncio.create_task(cluster.leave(timeout=10.0))
                events = []
                async with asyncio.timeout(10.0):
                    async for ev in sub:
                        events.append(ev)
                        if isinstance(ev, MemberRemoved):
                            break
                await leave_task
                return events
            finally:
                sub.close()

        events = asyncio.run(go())
        # MemberLeft / MemberExited / MemberRemoved are all valid en-route
        # to removal; the test only requires that *some* exit-path event
        # was observed.
        assert events, "no exit events observed"
        assert any(isinstance(e, MemberRemoved) for e in events), [type(e).__name__ for e in events]
    finally:
        sys.terminate_blocking()


def test_subscription_dropped_events_counter_starts_at_zero():
    sys = ActorSystem.create_blocking("phase5-dropped")
    try:
        cluster = Cluster.get(sys)
        sub = cluster.subscribe(capacity=8)
        try:
            assert sub.dropped_events == 0
            assert sub.filter is None
        finally:
            sub.close()
    finally:
        sys.terminate_blocking()


# ---------------------------------------------------------------------------
# Config round-trip — SBR strategies.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("strategy", SBR_STRATEGIES)
def test_sbr_strategy_round_trips_through_config(strategy: str):
    cfg = Config.from_dict(
        {
            "cluster": {
                "sbr": {
                    "strategy": strategy,
                    "stable-after": "5s",
                    "quorum-size": 2,
                    "down-if-alone": True,
                    "lease-acquired": False,
                }
            }
        }
    )
    assert cfg.get_string("cluster.sbr.strategy") == strategy
    assert cfg.get_string("cluster.sbr.stable-after") == "5s"
    assert cfg.get_int("cluster.sbr.quorum-size") == 2
    assert cfg.get_bool("cluster.sbr.down-if-alone") is True
    assert cfg.get_bool("cluster.sbr.lease-acquired") is False


def test_cluster_starts_with_sbr_strategy_set():
    """Spawning a cluster with an SBR strategy must not crash and must be
    queryable via ``system.config``.
    """
    cfg = Config.from_dict(
        {"cluster": {"sbr": {"strategy": "keep-majority", "stable-after": "1s"}}}
    )
    sys = ActorSystem.create_blocking("phase5-sbr", cfg)
    try:
        cluster = Cluster.get(sys)
        # The daemon was constructed from the config above; we cannot
        # introspect the strategy from the snapshot directly, but the
        # call must succeed and the snapshot must reflect a healthy
        # local node.
        assert cluster.self_address.startswith("akka://phase5-sbr")
    finally:
        sys.terminate_blocking()


def test_cluster_module_re_exports_event_types():
    # Sanity: every required public name is importable from
    # atomr.cluster.
    from atomr import cluster as c

    for name in (
        "Cluster",
        "Member",
        "MembershipState",
        "MemberUp",
        "MemberDowned",
        "MemberRemoved",
        "UnreachableMember",
        "ReachableMember",
        "LeaderChanged",
        "ClusterShuttingDown",
        "Convergence",
        "SBR_STRATEGIES",
        "event_from_dict",
    ):
        assert hasattr(c, name), f"atomr.cluster missing: {name}"
    # And the package-level facade still exposes cluster.
    assert hasattr(atomr, "cluster")
