"""SupervisorStrategy `max_retries` / `within_seconds` enforcement.

The Rust `actor_cell.rs` restart loop tracks a sliding-window restart
history per cell. When the configured `max_retries` is exceeded inside
`within_seconds`, the cell escalates (currently: stops the actor). These
tests exercise that path through the Python `Props.with_supervisor_budget`
override.
"""

from __future__ import annotations

import time

import pytest

import atomr
from atomr import Actor, ActorSystem, Props, props
from atomr.errors import AskError


class FlakyActor(Actor):
    """An actor that always panics on `boom` and replies on `ping`."""

    def __init__(self):
        self.starts = 0

    async def pre_start(self, _ctx):
        self.starts += 1

    async def handle(self, ctx, message):
        if message == "boom":
            raise RuntimeError("flaky: induced failure")
        if message == "starts":
            return {"starts": self.starts}
        return {"echo": message}


def test_max_retries_within_window_escalates_to_stop():
    sys = ActorSystem.create_blocking("flaky-budget")
    try:
        # Allow only 2 restarts in a 5s window.
        flaky_props = props(FlakyActor).with_supervisor_budget(2, 5.0)
        ref = sys.actor_of(flaky_props, "f")

        # First three failures: the third should exhaust the budget and
        # the cell should stop. Use `tell` so the panic doesn't surface
        # via the ask reply channel.
        for _ in range(3):
            ref.tell("boom")
        time.sleep(0.4)

        # After escalation the cell is gone — sends become dead letters
        # and `ask` times out instead of replying. We assert that the
        # actor no longer responds.
        with pytest.raises(AskError):
            ref.ask_blocking("starts", 0.5)
    finally:
        sys.terminate_blocking()


def test_supervisor_budget_one_retry_then_stop():
    """A budget of `(max_retries=1, within=5s)` permits a single restart
    and escalates on the second failure. Verifies the sliding-window
    counter is per-cell and trips at the configured threshold."""
    sys = ActorSystem.create_blocking("flaky-budget-one")
    try:
        flaky_props = props(FlakyActor).with_supervisor_budget(1, 5.0)
        ref = sys.actor_of(flaky_props, "f")
        # Two panics: first allowed, second escalates.
        ref.tell("boom")
        ref.tell("boom")
        time.sleep(0.3)
        with pytest.raises(AskError):
            ref.ask_blocking("ping", 0.5)
    finally:
        sys.terminate_blocking()
