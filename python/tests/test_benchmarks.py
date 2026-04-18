"""Lightweight throughput benchmarks for the ask path.

These are smoke-grade, not pytest-benchmark — we simply assert floors and
print a summary so regressions jump out in CI logs.
"""

import time

import rustakka
from rustakka import Actor, ActorSystem, InterpreterQuota, props


class Null(Actor):
    async def handle(self, ctx, msg):
        return msg


def _run_throughput(dispatcher: str, role: str, count: int, messages: int) -> float:
    sys = ActorSystem.create_blocking(f"bench-{role}")
    try:
        if dispatcher != "python-pinned":
            sys.configure_interpreter(role, dispatcher, count, InterpreterQuota())
        ref = sys.actor_of(props(Null, dispatcher=dispatcher, interpreter_role=role), "n")
        # warmup
        for _ in range(50):
            ref.ask_blocking(0, 5.0)
        t0 = time.perf_counter()
        for i in range(messages):
            ref.ask_blocking(i, 5.0)
        dt = time.perf_counter() - t0
        return messages / dt
    finally:
        sys.terminate_blocking()


def test_pinned_benchmark(capsys):
    rate = _run_throughput("python-pinned", "default", 1, 500)
    with capsys.disabled():
        print(f"\npinned ask/sec: {rate:,.0f}")
    assert rate > 50  # very conservative floor


def test_subinterpreter_pool_benchmark(capsys):
    if not rustakka.subinterpreters_supported():
        return
    rate = _run_throughput("python-subinterpreter-pool", "workers", 4, 500)
    with capsys.disabled():
        print(f"subinterp ask/sec (pool=4): {rate:,.0f}")
    assert rate > 50
