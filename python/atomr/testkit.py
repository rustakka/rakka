"""testkit facade — pytest fixtures + TestKit/TestProbe."""

from __future__ import annotations

from typing import Iterator

import pytest

from . import _native

TestKit = _native.testkit.TestKit
TestProbe = _native.testkit.TestProbe


@pytest.fixture
def testkit() -> Iterator[TestKit]:
    """Pytest fixture that yields a fresh :class:`TestKit` and terminates it."""
    kit = TestKit()
    try:
        yield kit
    finally:
        kit.shutdown()


__all__ = ["TestKit", "TestProbe", "testkit"]
