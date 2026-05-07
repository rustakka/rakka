"""testkit facade — pytest fixtures + TestKit/TestProbe."""

from __future__ import annotations

from typing import Iterator

import pytest

from . import _native

TestKit = _native.testkit.TestKit
TestProbe = _native.testkit.TestProbe
MultiNodeOopController = _native.testkit.MultiNodeOopController
MultiNodeOopNode = _native.testkit.MultiNodeOopNode
within = _native.testkit.within

# Phase 9 — testkit polish.
TestScheduler = _native.testkit.TestScheduler
ScheduledToken = _native.testkit.ScheduledToken
EventStream = _native.testkit.EventStream
EventFilter = _native.testkit.EventFilter


@pytest.fixture
def testkit() -> Iterator[TestKit]:
    """Pytest fixture that yields a fresh :class:`TestKit` and terminates it."""
    kit = TestKit()
    try:
        yield kit
    finally:
        kit.shutdown()


__all__ = [
    "TestKit",
    "TestProbe",
    "MultiNodeOopController",
    "MultiNodeOopNode",
    "within",
    "testkit",
    "TestScheduler",
    "ScheduledToken",
    "EventStream",
    "EventFilter",
]
