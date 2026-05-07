"""Phase 3 — routing tests.

These tests use a global counter actor (one per test, not shared
across tests) and assert distribution properties by counting how many
messages each routee processed. We probe each routee through its
class-level state because the test actors are created with
``interpreter_role`` set to a fresh role per test, so state is per-
process and per-test.
"""

from __future__ import annotations

import time
from collections import defaultdict
from typing import Any

import pytest

import atomr
from atomr import Actor, ActorSystem, Props, props


# ---------------------------------------------------------------------------
# Test actors
# ---------------------------------------------------------------------------


# Class-level distribution counter, keyed by id(self) so each routee
# instance increments its own slot. Reset per-test in the fixtures.
_HITS: dict[int, int] = defaultdict(int)
_HITS_BY_ID: dict[Any, list[int]] = defaultdict(list)


class CountingActor(Actor):
    """Bumps a per-instance counter for every message and stores its
    own id in a registry so the test can sum by-instance hits."""

    def __init__(self):
        self._key = id(self)
        _HITS[self._key] = 0

    async def handle(self, ctx, message):
        _HITS[self._key] += 1
        # When asked, return the per-instance hit count.
        return {"hits": _HITS[self._key], "key": self._key}


class TaggingActor(Actor):
    """Records the (instance-id, message) pair for every received
    message so the test can verify routing decisions by key."""

    def __init__(self):
        self._key = id(self)
        _HITS_BY_ID[self._key] = []

    async def handle(self, ctx, message):
        _HITS_BY_ID[self._key].append(message)
        return self._key


@pytest.fixture(autouse=True)
def _reset_hits():
    _HITS.clear()
    _HITS_BY_ID.clear()
    yield
    _HITS.clear()
    _HITS_BY_ID.clear()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_round_robin_distributes_evenly():
    sys = ActorSystem.create_blocking("rr-test")
    try:
        child = props(CountingActor, interpreter_role="rr-test")
        pool = sys.actor_of(Props.round_robin(child, 3), "pool")
        for i in range(6):
            pool.tell({"i": i})
        # Give the dispatcher time to drain the mailbox.
        deadline = time.time() + 5.0
        while time.time() < deadline:
            if sum(_HITS.values()) >= 6:
                break
            time.sleep(0.02)
        # 3 children, 6 messages → each should see exactly 2 hits.
        counts = sorted(_HITS.values(), reverse=True)
        # The router spawns three children, so we expect three nonzero
        # counters. Distribution may include the parent shim instance too,
        # which never receives messages and stays at 0; filter it out.
        nonzero = [c for c in counts if c > 0]
        assert len(nonzero) == 3, f"expected 3 routees got hit, saw {nonzero}"
        assert sum(nonzero) == 6
        # round-robin → exact even distribution
        assert nonzero == [2, 2, 2]
    finally:
        sys.terminate_blocking()


def test_broadcast_delivers_to_every_routee():
    sys = ActorSystem.create_blocking("bc-test")
    try:
        child = props(CountingActor, interpreter_role="bc-test")
        pool = sys.actor_of(Props.broadcast(child, 3), "pool")
        pool.tell({"hello": "world"})
        # 1 tell → 3 deliveries
        deadline = time.time() + 5.0
        while time.time() < deadline:
            if sum(_HITS.values()) >= 3:
                break
            time.sleep(0.02)
        nonzero = [c for c in _HITS.values() if c > 0]
        assert len(nonzero) == 3, f"broadcast missed routees: {nonzero}"
        assert all(c == 1 for c in nonzero), nonzero
    finally:
        sys.terminate_blocking()


def test_consistent_hash_routes_same_key_to_same_child():
    sys = ActorSystem.create_blocking("ch-test")
    try:
        child = props(TaggingActor, interpreter_role="ch-test")
        pool = sys.actor_of(Props.consistent_hash(child, 3), "pool")
        # Send three messages with the same key 5 times each — every
        # ordered triplet must land on the same routee.
        for key in (1, 7, 42):
            for tag in range(5):
                pool.tell_with_key({"k": key, "tag": tag}, key=key)
        deadline = time.time() + 5.0
        while time.time() < deadline:
            total = sum(len(v) for v in _HITS_BY_ID.values())
            if total >= 15:
                break
            time.sleep(0.02)

        # For each key, all five messages must land on a single routee.
        per_key_routee = {}
        for instance_id, msgs in _HITS_BY_ID.items():
            keys = {m["k"] for m in msgs}
            for k in keys:
                per_key_routee.setdefault(k, set()).add(instance_id)
        for k, routees in per_key_routee.items():
            assert len(routees) == 1, f"key {k} hit {len(routees)} routees"
    finally:
        sys.terminate_blocking()


def test_consistent_hash_dropping_without_key():
    """A plain ``tell`` against a consistent-hash router emits a
    tracing warning and drops the message — make sure no panic is
    raised and no routee receives the message."""
    sys = ActorSystem.create_blocking("ch-bad-test")
    try:
        child = props(TaggingActor, interpreter_role="ch-bad-test")
        pool = sys.actor_of(Props.consistent_hash(child, 2), "pool")
        pool.tell({"oops": True})
        time.sleep(0.1)
        total = sum(len(v) for v in _HITS_BY_ID.values())
        assert total == 0
    finally:
        sys.terminate_blocking()


def test_random_distributes_across_routees():
    sys = ActorSystem.create_blocking("rand-test")
    try:
        child = props(CountingActor, interpreter_role="rand-test")
        pool = sys.actor_of(Props.random(child, 3), "pool")
        for i in range(60):
            pool.tell({"i": i})
        deadline = time.time() + 5.0
        while time.time() < deadline:
            if sum(_HITS.values()) >= 60:
                break
            time.sleep(0.05)
        nonzero = [c for c in _HITS.values() if c > 0]
        assert sum(nonzero) == 60
        # With 60 messages over 3 routees we'd expect each to get at
        # least one — flaky pseudo-random distributions are acceptable
        # so long as not all 60 land on a single routee.
        assert max(nonzero) < 60
    finally:
        sys.terminate_blocking()


def test_smallest_mailbox_picks_first_when_idle():
    sys = ActorSystem.create_blocking("sm-test")
    try:
        child = props(CountingActor, interpreter_role="sm-test")
        pool = sys.actor_of(Props.smallest_mailbox(child, 3), "pool")
        for i in range(9):
            pool.tell({"i": i})
        deadline = time.time() + 5.0
        while time.time() < deadline:
            if sum(_HITS.values()) >= 9:
                break
            time.sleep(0.02)
        nonzero = [c for c in _HITS.values() if c > 0]
        assert sum(nonzero) == 9
        # The smallest-mailbox heuristic keeps approximate balance —
        # we just assert no routee was starved.
        assert all(c >= 1 for c in nonzero)
        assert len(nonzero) == 3
    finally:
        sys.terminate_blocking()


def test_router_spawn_rejects_zero_count():
    child = props(CountingActor, interpreter_role="zero-test")
    with pytest.raises(ValueError):
        Props.round_robin(child, 0)
