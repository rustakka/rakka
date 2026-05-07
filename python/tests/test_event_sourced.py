"""Phase 4 — `EventSourcedActor` tests.

Covers:

* Live persist + replay through ``command_handler``/``event_handler``.
* State recovery across system restarts using a shared
  :class:`InMemoryJournal`.
* Snapshot emission via ``Effect.snapshot()`` and recovery short-circuit
  through the snapshot store.
* ``recovery_mode`` flag toggles between True (during replay) and
  False (during live writes).
* ``persistent_id`` validation on spawn.
"""
from __future__ import annotations

import pytest

import atomr
from atomr import ActorSystem, props
from atomr.persistence import (
    Effect,
    EventSourcedActor,
    InMemoryJournal,
    InMemorySnapshotStore,
    RecoveryPermitter,
)


# ---------------------------------------------------------------------------
# Test actor: a counter that supports incr / get / snap commands.
# ---------------------------------------------------------------------------


class CounterActor(EventSourcedActor):
    persistent_id = "counter-1"

    def initial_state(self):
        return {"count": 0}

    async def command_handler(self, state, ctx, cmd):
        op = cmd.get("op")
        if op == "incr":
            return [Effect.persist({"type": "Incremented", "by": cmd.get("by", 1)})]
        if op == "incr_many":
            events = [
                {"type": "Incremented", "by": d} for d in cmd.get("deltas", [])
            ]
            return [Effect.persist_all(events)]
        if op == "snap":
            return [Effect.snapshot()]
        if op == "get":
            return [Effect.reply_message(state["count"])]
        if op == "get_seq":
            return [Effect.reply_message(self.sequence_nr)]
        if op == "stop":
            return [Effect.stop()]
        return []

    def event_handler(self, state, event, recovery_mode=False):
        if event.get("type") == "Incremented":
            state["count"] += event["by"]
            # Track which path produced this state mutation so
            # tests can assert on the recovery_mode flag.
            seen = state.setdefault("_modes", [])
            seen.append(recovery_mode)
        return state


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _system_with_json(name: str) -> ActorSystem:
    sys = ActorSystem.create_blocking(name)
    # Default JSON codec covers all dict / list / scalar events.
    sys.use_json_codec(default=True)
    return sys


def _spawn_counter(
    sys: ActorSystem,
    name: str,
    *,
    journal: InMemoryJournal,
    snapshots: InMemorySnapshotStore | None = None,
    snapshot_every: int | None = None,
):
    def make():
        return CounterActor(
            journal=journal,
            snapshot_store=snapshots,
            snapshot_every=snapshot_every,
        )

    return sys.actor_of(props(CounterActor, factory=make), name)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_persist_and_query_live_state():
    journal = InMemoryJournal()
    sys = _system_with_json("es-test-live")
    try:
        ref = _spawn_counter(sys, "counter", journal=journal)
        ref.ask_blocking({"op": "incr", "by": 1}, 5.0)
        ref.ask_blocking({"op": "incr", "by": 2}, 5.0)
        ref.ask_blocking({"op": "incr", "by": 4}, 5.0)
        n = ref.ask_blocking({"op": "get"}, 5.0)
        assert n == 7
        # Sanity check on the underlying journal.
        assert journal.highest_sequence_nr("counter-1") == 3
    finally:
        sys.terminate_blocking()


def test_persist_all_atomic_batch():
    journal = InMemoryJournal()
    sys = _system_with_json("es-test-batch")
    try:
        ref = _spawn_counter(sys, "counter", journal=journal)
        ref.ask_blocking({"op": "incr_many", "deltas": [1, 2, 3, 4]}, 5.0)
        n = ref.ask_blocking({"op": "get"}, 5.0)
        assert n == 10
        assert journal.highest_sequence_nr("counter-1") == 4
    finally:
        sys.terminate_blocking()


def test_recovery_replays_journal_after_restart():
    journal = InMemoryJournal()

    sys1 = _system_with_json("es-test-recover-1")
    try:
        ref = _spawn_counter(sys1, "counter", journal=journal)
        for d in (1, 2, 3, 4, 5):
            ref.ask_blocking({"op": "incr", "by": d}, 5.0)
        assert ref.ask_blocking({"op": "get"}, 5.0) == 15
    finally:
        sys1.terminate_blocking()

    # Recreate the system with the SAME journal — recovery must
    # rebuild the in-memory state to match.
    sys2 = _system_with_json("es-test-recover-2")
    try:
        ref2 = _spawn_counter(sys2, "counter", journal=journal)
        recovered = ref2.ask_blocking({"op": "get"}, 5.0)
        assert recovered == 15
        assert ref2.ask_blocking({"op": "get_seq"}, 5.0) == 5
    finally:
        sys2.terminate_blocking()


def test_snapshot_short_circuits_replay():
    journal = InMemoryJournal()
    snapshots = InMemorySnapshotStore()

    sys1 = _system_with_json("es-test-snap-1")
    try:
        ref = _spawn_counter(
            sys1, "counter", journal=journal, snapshots=snapshots
        )
        for d in (1, 2, 3):
            ref.ask_blocking({"op": "incr", "by": d}, 5.0)
        # Force a snapshot at seq=3.
        ref.ask_blocking({"op": "snap"}, 5.0)
        # More events after the snapshot.
        ref.ask_blocking({"op": "incr", "by": 10}, 5.0)
        ref.ask_blocking({"op": "incr", "by": 20}, 5.0)
        assert ref.ask_blocking({"op": "get"}, 5.0) == 36
    finally:
        sys1.terminate_blocking()

    # Snapshot should be present and pin sequence 3.
    loaded = snapshots.load("counter-1")
    assert loaded is not None
    seq, _payload = loaded
    assert seq == 3

    # Recover with the same journal + snapshots; only events 4,5 should
    # replay through `event_handler(recovery_mode=True)`.
    sys2 = _system_with_json("es-test-snap-2")
    try:
        ref2 = _spawn_counter(
            sys2, "counter", journal=journal, snapshots=snapshots
        )
        n = ref2.ask_blocking({"op": "get"}, 5.0)
        assert n == 36
        assert ref2.ask_blocking({"op": "get_seq"}, 5.0) == 5
    finally:
        sys2.terminate_blocking()


def test_recovery_mode_flag_is_set_during_replay_only():
    """A specialised actor records the recovery_mode flag for each event."""

    captured: list[bool] = []

    class Recorder(EventSourcedActor):
        persistent_id = "recorder-1"

        def initial_state(self):
            return {"items": []}

        async def command_handler(self, state, ctx, cmd):
            return [Effect.persist({"type": "Saw", "value": cmd["value"]})]

        def event_handler(self, state, event, recovery_mode=False):
            captured.append(recovery_mode)
            state["items"].append(event["value"])
            return state

    journal = InMemoryJournal()

    # First incarnation: write three events live.
    sys1 = _system_with_json("es-test-mode-1")
    try:
        def make1():
            return Recorder(journal=journal)
        ref = sys1.actor_of(props(Recorder, factory=make1), "rec")
        ref.ask_blocking({"value": "a"}, 5.0)
        ref.ask_blocking({"value": "b"}, 5.0)
        ref.ask_blocking({"value": "c"}, 5.0)
    finally:
        sys1.terminate_blocking()

    # All three live writes must record recovery_mode=False.
    assert captured == [False, False, False]
    captured.clear()

    # Second incarnation: replay-only run.
    sys2 = _system_with_json("es-test-mode-2")
    try:
        def make2():
            return Recorder(journal=journal)
        ref2 = sys2.actor_of(props(Recorder, factory=make2), "rec")
        # Trigger a synchronous round trip so we know pre_start finished.
        ref2.ask_blocking({"value": "d"}, 5.0)
    finally:
        sys2.terminate_blocking()

    # First three replays were recovery, then one live write.
    assert captured == [True, True, True, False]


def test_missing_persistent_id_raises_at_spawn():
    class Bad(EventSourcedActor):
        # No `persistent_id` set — falls back to `""` and must reject.
        def initial_state(self):
            return {}

        async def command_handler(self, state, ctx, cmd):
            return []

        def event_handler(self, state, event, recovery_mode=False):
            return state

    sys = _system_with_json("es-test-missing-id")
    try:
        with pytest.raises(Exception):
            ref = sys.actor_of(props(Bad), "bad")
            # Force pre_start by interacting with the actor.
            ref.ask_blocking({"x": 1}, 1.0)
    finally:
        sys.terminate_blocking()


def test_recovery_permitter_caps_concurrent_recoveries():
    permitter = RecoveryPermitter(2)
    assert permitter.capacity() == 2
    assert permitter.available() == 2

    journal = InMemoryJournal()
    sys = _system_with_json("es-test-permitter")
    try:
        def make():
            return CounterActor(
                journal=journal, recovery_permitter=permitter
            )
        ref = sys.actor_of(props(CounterActor, factory=make), "c")
        # The permitter is acquired and released synchronously per
        # recovery — by the time `ask` returns, the permit is gone.
        n = ref.ask_blocking({"op": "get"}, 5.0)
        assert n == 0
        assert permitter.in_flight() == 0
    finally:
        sys.terminate_blocking()


def test_inmemory_journal_legacy_api_still_works():
    """Phase 4 must preserve the existing `InMemoryJournal` Python class."""
    j = atomr.persistence.InMemoryJournal()
    j.write("pid", 1, b"a")
    j.write("pid", 2, b"b")
    payloads = [bytes(p) for p in j.replay("pid")]
    assert payloads == [b"a", b"b"]
    assert j.highest_sequence_nr("pid") == 2


def test_effect_reply_message_constructor_and_value_field():
    """Epic G renamed `Effect.reply(v)` -> `Effect.reply_message(v)` and the
    payload is now read back as `effect.value` (was `effect.reply_value`).
    """
    eff = Effect.reply_message(42)
    assert eff.kind == "reply"
    assert eff.value == 42
    eff2 = Effect.reply_message({"ok": True})
    assert eff2.value == {"ok": True}
    # The old field name and constructor must both be gone.
    assert not hasattr(eff, "reply_value")
    assert not hasattr(Effect, "reply")
