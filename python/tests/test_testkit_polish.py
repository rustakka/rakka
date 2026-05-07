"""Phase 9 — TestScheduler, EventFilter, fish_for_message."""

from __future__ import annotations

import asyncio

import pytest

from atomr.testkit import EventFilter, EventStream
from atomr.testkit import TestKit as _TestKit
from atomr.testkit import TestScheduler as _TestScheduler


def test_test_scheduler_fires_on_advance_blocking():
    sched = _TestScheduler()
    fired: list[int] = []
    token = sched.schedule_after(5.0, lambda: fired.append(1))
    assert sched.pending() == 1
    sched.advance_blocking(5.0)
    assert fired == [1]
    assert token.fired()
    assert sched.pending() == 0


def test_test_scheduler_does_not_fire_before_delay():
    sched = _TestScheduler()
    fired: list[int] = []
    sched.schedule_after(10.0, lambda: fired.append(1))
    sched.advance_blocking(9.0)
    assert fired == []
    assert sched.pending() == 1


def test_test_scheduler_cancel_prevents_fire():
    sched = _TestScheduler()
    fired: list[int] = []
    token = sched.schedule_after(1.0, lambda: fired.append(1))
    assert token.cancel() is True
    sched.advance_blocking(2.0)
    assert fired == []
    # Already cancelled — second cancel returns False.
    assert token.cancel() is False


async def test_test_scheduler_advance_async():
    """`advance` returns an awaitable for async tests."""
    sched = _TestScheduler()
    fired: list[int] = []
    sched.schedule_after(0.5, lambda: fired.append(1))
    await sched.advance(0.5)
    assert fired == [1]


def test_event_filter_counts_matching_class_path():
    stream = EventStream()
    f = EventFilter(stream, cls_path="builtins.int")
    stream.publish(42)
    stream.publish("ignored")
    stream.publish(7)
    assert f.count() == 2


def test_event_filter_message_regex():
    stream = EventStream()
    f = EventFilter(stream, message_regex=r"\bping\b")
    stream.publish({"k": "pong"})
    stream.publish({"k": "ping"})
    stream.publish("a ping in repr")
    assert f.count() == 2


def test_event_filter_combines_class_and_regex():
    stream = EventStream()
    f = EventFilter(stream, cls_path="builtins.dict", message_regex=r"hello")
    stream.publish({"hello": "world"})
    stream.publish("hello other")
    stream.publish({"goodbye": 1})
    assert f.count() == 1


async def test_event_filter_await_count_returns_truthy_on_match():
    stream = EventStream()
    f = EventFilter(stream, cls_path="builtins.str")
    stream.publish("x")
    stream.publish("y")
    assert await f.await_count(2, 1.0) is True


async def test_fish_for_message_skips_mismatches():
    kit = _TestKit()
    try:
        probe = kit.probe()
        ref = probe.ref_
        ref.tell(1)
        ref.tell(2)
        ref.tell(99)
        # Allow tells to land in the inbox.
        await asyncio.sleep(0.01)
        result = await probe.fish_for_message(lambda m: m >= 50, 1.0)
        assert result == 99
    finally:
        kit.shutdown()


async def test_fish_for_message_times_out_when_no_match():
    kit = _TestKit()
    try:
        probe = kit.probe()
        probe.ref_.tell(1)
        probe.ref_.tell(2)
        await asyncio.sleep(0.01)
        with pytest.raises(Exception):
            await probe.fish_for_message(lambda m: m == 999, 0.05)
    finally:
        kit.shutdown()
