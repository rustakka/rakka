"""Phase 3 — pattern tests: CircuitBreaker, RetrySchedule, retry, pipe_to,
and the Backoff supervisor (built from ``Props.backoff``).
"""

from __future__ import annotations

import asyncio
import time

import pytest

import atomr
from atomr import Actor, ActorSystem, Props, props
from atomr.pattern import (
    CircuitBreaker,
    CircuitBreakerOpen,
    RetrySchedule,
    pipe_to,
    retry,
)


# ---------------------------------------------------------------------------
# RetrySchedule
# ---------------------------------------------------------------------------


def test_retry_schedule_fixed():
    s = RetrySchedule.fixed(0.05)
    assert abs(s.delay_for(0) - 0.05) < 1e-6
    assert abs(s.delay_for(7) - 0.05) < 1e-6


def test_retry_schedule_exponential_caps():
    s = RetrySchedule.exponential(0.01, 0.08)
    assert abs(s.delay_for(0) - 0.01) < 1e-6
    assert abs(s.delay_for(1) - 0.02) < 1e-6
    assert abs(s.delay_for(2) - 0.04) < 1e-6
    assert abs(s.delay_for(3) - 0.08) < 1e-6
    assert abs(s.delay_for(10) - 0.08) < 1e-6


@pytest.mark.asyncio
async def test_retry_succeeds_after_transient_failures():
    calls = {"n": 0}

    async def attempt():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("not yet")
        return 42

    out = await retry(attempt, 5, RetrySchedule.fixed(0.0))
    assert out == 42
    assert calls["n"] == 3


@pytest.mark.asyncio
async def test_retry_propagates_last_error():
    async def attempt():
        raise ValueError("nope")

    with pytest.raises(ValueError):
        await retry(attempt, 3, RetrySchedule.fixed(0.0))


# ---------------------------------------------------------------------------
# CircuitBreaker
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_circuit_breaker_opens_after_max_failures():
    cb = CircuitBreaker(max_failures=2, call_timeout=0.5, reset_timeout=10.0)
    assert cb.state == "closed"

    async def fail():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        await cb.call_async(fail())
    with pytest.raises(RuntimeError):
        await cb.call_async(fail())
    # Now open — short-circuits without invoking the callable.
    assert cb.state == "open"
    with pytest.raises(CircuitBreakerOpen):
        await cb.call_async(fail())


@pytest.mark.asyncio
async def test_circuit_breaker_passes_through_when_closed():
    cb = CircuitBreaker(max_failures=3, call_timeout=0.5, reset_timeout=1.0)

    async def ok():
        return "value"

    out = await cb.call_async(ok())
    assert out == "value"


# ---------------------------------------------------------------------------
# pipe_to
# ---------------------------------------------------------------------------


class CollectorActor(Actor):
    """Stores received messages in a class-level list keyed by name."""

    received: list = []

    async def handle(self, ctx, message):
        CollectorActor.received.append(message)
        return None


@pytest.mark.asyncio
async def test_pipe_to_delivers_future_value_to_target():
    CollectorActor.received.clear()
    sys = ActorSystem.create_blocking("pipe-test")
    try:
        target = sys.actor_of(props(CollectorActor, interpreter_role="pipe-test"), "collect")

        async def producer():
            await asyncio.sleep(0.05)
            return {"piped": 1}

        await pipe_to(producer(), target)
        # Wait for delivery.
        deadline = time.time() + 2.0
        while time.time() < deadline:
            if CollectorActor.received:
                break
            await asyncio.sleep(0.02)
        assert CollectorActor.received == [{"piped": 1}]
    finally:
        sys.terminate_blocking()


# ---------------------------------------------------------------------------
# Backoff supervisor (Props.backoff)
# ---------------------------------------------------------------------------


class FlakyActor(Actor):
    """Raises on the first message of every fresh instance, then is
    quiet. Each restart creates a new instance, so the supervisor
    eventually accumulates measurable backoff delay."""

    starts: list = []

    def __init__(self):
        FlakyActor.starts.append(time.monotonic())
        self._first = True

    async def handle(self, ctx, message):
        if self._first:
            self._first = False
            raise RuntimeError("flaky")
        return "ok"


def test_props_backoff_returns_props_object():
    """Smoke check: ``Props.backoff`` produces a ``Props`` whose label
    is `"backoff"` and whose ``actor_of`` succeeds."""
    sys = ActorSystem.create_blocking("backoff-smoke")
    try:
        child = props(FlakyActor, interpreter_role="backoff-smoke")
        bo = Props.backoff(child, 0.05, 0.5, 0.0)
        assert bo.kind_label == "backoff"
        # Spawn doesn't blow up.
        ref = sys.actor_of(bo, "wrapped")
        assert ref.path.endswith("/user/wrapped")
        # The first tell triggers a panic → restart cycle. We don't
        # block waiting for restarts (the supervisor strategy decides
        # cadence), but the actor system must survive the restarts.
        ref.tell("hello")
        time.sleep(0.3)
    finally:
        sys.terminate_blocking()
