"""`ctx.schedule_*` returns a Cancelable handle backed by the Rust scheduler."""

from __future__ import annotations

import threading
import time

import atomr
from atomr import Actor, ActorSystem, Cancelable, props


class TimerActor(Actor):
    """Schedules timers on `ctx`. Each scheduled callback bumps a counter
    so the test can observe whether cancellation took effect."""

    def __init__(self):
        self.fires = 0
        self._fires_lock = threading.Lock()
        self._handle: Cancelable | None = None

    def _bump(self):
        with self._fires_lock:
            self.fires += 1

    async def handle(self, ctx, message):
        cmd = message.get("cmd") if isinstance(message, dict) else message
        if cmd == "schedule":
            delay = message.get("delay", 0.05)
            self._handle = ctx.schedule_once(delay, self._bump)
            return {"scheduled": True}
        if cmd == "schedule_periodic":
            self._handle = ctx.schedule_periodically(0.0, message.get("interval", 0.03), self._bump)
            return {"scheduled": True}
        if cmd == "cancel":
            assert self._handle is not None
            self._handle.cancel()
            return {"cancelled": True, "is_cancelled": self._handle.is_cancelled()}
        if cmd == "fires":
            with self._fires_lock:
                return {"fires": self.fires}
        if cmd == "handle_repr":
            assert self._handle is not None
            return {"repr": repr(self._handle)}
        raise ValueError(f"unknown cmd: {cmd}")


def test_schedule_once_returns_cancelable_and_fires():
    sys = ActorSystem.create_blocking("ctx-schedule-fire")
    try:
        ref = sys.actor_of(props(TimerActor), "t")
        assert ref.ask_blocking({"cmd": "schedule", "delay": 0.05}, 5.0)["scheduled"] is True
        # Wait for the timer to actually fire.
        time.sleep(0.2)
        fires = ref.ask_blocking({"cmd": "fires"}, 5.0)["fires"]
        assert fires == 1
    finally:
        sys.terminate_blocking()


def test_schedule_once_cancel_prevents_delivery():
    sys = ActorSystem.create_blocking("ctx-schedule-cancel")
    try:
        ref = sys.actor_of(props(TimerActor), "t")
        ref.ask_blocking({"cmd": "schedule", "delay": 0.3}, 5.0)
        # Cancel before delivery — callback must not fire.
        cancel_resp = ref.ask_blocking({"cmd": "cancel"}, 5.0)
        assert cancel_resp["cancelled"] is True
        assert cancel_resp["is_cancelled"] is True
        time.sleep(0.4)
        fires = ref.ask_blocking({"cmd": "fires"}, 5.0)["fires"]
        assert fires == 0
    finally:
        sys.terminate_blocking()


def test_schedule_periodic_cancel_stops_further_firings():
    sys = ActorSystem.create_blocking("ctx-schedule-periodic")
    try:
        ref = sys.actor_of(props(TimerActor), "t")
        ref.ask_blocking({"cmd": "schedule_periodic", "interval": 0.04}, 5.0)
        time.sleep(0.18)
        ref.ask_blocking({"cmd": "cancel"}, 5.0)
        snapshot = ref.ask_blocking({"cmd": "fires"}, 5.0)["fires"]
        assert snapshot >= 1
        # No new firings after cancel.
        time.sleep(0.2)
        final = ref.ask_blocking({"cmd": "fires"}, 5.0)["fires"]
        assert final == snapshot, f"expected periodic to stop after cancel: {snapshot} -> {final}"
    finally:
        sys.terminate_blocking()


def test_cancelable_repr_reflects_state():
    sys = ActorSystem.create_blocking("ctx-cancelable-repr")
    try:
        ref = sys.actor_of(props(TimerActor), "t")
        ref.ask_blocking({"cmd": "schedule", "delay": 5.0}, 5.0)
        before = ref.ask_blocking({"cmd": "handle_repr"}, 5.0)["repr"]
        ref.ask_blocking({"cmd": "cancel"}, 5.0)
        after = ref.ask_blocking({"cmd": "handle_repr"}, 5.0)["repr"]
        assert "cancelled=false" in before.lower()
        assert "cancelled=true" in after.lower()
    finally:
        sys.terminate_blocking()
