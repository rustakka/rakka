"""Resilience patterns: circuit breaker, retry schedule, pipe_to.

Wraps the native ``atomr._native.pattern`` submodule with stable Python
exports. The native types do all the heavy lifting; this module exists
so that import paths stay stable as the Rust internals evolve.

Examples
--------
::

    from atomr.pattern import CircuitBreaker, RetrySchedule, retry, pipe_to

    breaker = CircuitBreaker(max_failures=3, call_timeout=0.5,
                             reset_timeout=1.0)

    async def make_call():
        return await some_remote.ask({"op": "do"}, timeout=0.5)

    result = await breaker.call_async(make_call())

    schedule = RetrySchedule.exponential(min_seconds=0.05,
                                         max_seconds=2.0)
    out = await retry(make_call, max_attempts=5, schedule=schedule)

    await pipe_to(make_call(), target_ref)
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from . import _native

CircuitBreaker = _native.pattern.CircuitBreaker
CircuitBreakerOpen = _native.pattern.CircuitBreakerOpen
RetrySchedule = _native.pattern.RetrySchedule


async def retry(
    async_fn: Callable[[], Awaitable[Any]],
    max_attempts: int,
    schedule: RetrySchedule,
) -> Any:
    """Run ``async_fn()`` up to ``max_attempts`` times, sleeping the
    schedule between attempts. ``async_fn`` MUST be a zero-argument
    callable that returns a fresh awaitable per call (not a coroutine
    object — a *callable* that returns one).
    """
    return await _native.pattern.retry(async_fn, max_attempts, schedule)


async def pipe_to(awaitable: Awaitable[Any], target: Any) -> None:
    """Await ``awaitable`` and ``tell`` the result to ``target``.

    Errors raised by the awaitable propagate to the caller and are *not*
    delivered to ``target`` — wrap a try/except yourself if you want to
    forward errors as messages.
    """
    return await _native.pattern.pipe_to(awaitable, target)


__all__ = [
    "CircuitBreaker",
    "CircuitBreakerOpen",
    "RetrySchedule",
    "retry",
    "pipe_to",
]
