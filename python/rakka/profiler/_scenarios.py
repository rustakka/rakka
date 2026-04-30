"""Scenario actors + runners for the Python profiler."""

from __future__ import annotations

import time
from typing import Iterable, List, Optional

from rakka import Actor, ActorRef, ActorSystem, props

from ._probes import best_dispatcher_for, cpu_time_ns, peak_rss_bytes, rss_bytes
from ._report import Measurement, percentile


# --- actors (module-level so subinterpreters can import them) ---------------


class Sink(Actor):
    """Counts messages. ``ask('__count__')`` flushes and returns the total."""

    def __init__(self) -> None:
        self.n = 0

    async def handle(self, ctx, msg):
        if isinstance(msg, str) and msg == "__count__":
            return self.n
        self.n += 1
        return None


class Echo(Actor):
    async def handle(self, ctx, msg):
        return msg


class CpuWorker(Actor):
    """Each message runs a small hashing loop (matches the Rust scenario)."""

    async def handle(self, ctx, msg):
        h = (int(msg) if isinstance(msg, int) else 0) ^ 0x9E3779B97F4A7C15
        for i in range(4096):
            h = ((h * 0xBF58476D1CE4E5B9) + i) & 0xFFFFFFFFFFFFFFFF
            h = ((h << 27) | (h >> 37)) & 0xFFFFFFFFFFFFFFFF
        if isinstance(msg, str) and msg == "__count__":
            return h
        return None


# --- runners ----------------------------------------------------------------


def run_tell(system: ActorSystem, n: int) -> Measurement:
    disp, *_ = best_dispatcher_for("tell")
    ref = system.actor_of(props(Sink, dispatcher=disp), "prof-tell")
    rss0, cpu0 = rss_bytes(), cpu_time_ns()
    t0 = time.perf_counter_ns()
    for i in range(n):
        ref.tell(i)
    total = ref.ask_blocking("__count__", 30.0)
    elapsed_ns = time.perf_counter_ns() - t0
    rss1, cpu1 = rss_bytes(), cpu_time_ns()
    assert total == n, f"expected {n} processed, saw {total}"
    return _make("tell", disp, n, elapsed_ns, (), rss0, rss1, cpu0, cpu1)


def run_ask(system: ActorSystem, n: int) -> Measurement:
    disp, *_ = best_dispatcher_for("ask")
    ref = system.actor_of(props(Echo, dispatcher=disp), "prof-ask")
    samples: List[int] = []
    rss0, cpu0 = rss_bytes(), cpu_time_ns()
    t0 = time.perf_counter_ns()
    for i in range(n):
        s = time.perf_counter_ns()
        ref.ask_blocking(i, 5.0)
        samples.append(time.perf_counter_ns() - s)
    elapsed_ns = time.perf_counter_ns() - t0
    rss1, cpu1 = rss_bytes(), cpu_time_ns()
    return _make("ask", disp, n, elapsed_ns, samples, rss0, rss1, cpu0, cpu1)


def run_fanout(system: ActorSystem, n: int) -> Measurement:
    disp, *_ = best_dispatcher_for("fanout")
    rss0, cpu0 = rss_bytes(), cpu_time_ns()
    t0 = time.perf_counter_ns()
    refs: List[ActorRef] = [
        system.actor_of(props(Sink, dispatcher=disp), f"prof-fan-{i}") for i in range(n)
    ]
    for i, r in enumerate(refs):
        r.tell(i)
    for r in refs:
        assert r.ask_blocking("__count__", 30.0) == 1
    elapsed_ns = time.perf_counter_ns() - t0
    rss1, cpu1 = rss_bytes(), cpu_time_ns()
    return _make("fanout", disp, n, elapsed_ns, (), rss0, rss1, cpu0, cpu1)


def run_cpu(system: ActorSystem, n: int) -> Measurement:
    disp, role, count, quota = best_dispatcher_for("cpu")
    if disp != "python-pinned":
        system.configure_interpreter(role, disp, count, quota)
        ref = system.actor_of(
            props(CpuWorker, dispatcher=disp, interpreter_role=role), "prof-cpu"
        )
        cfg = f"{disp} pool={count}"
    else:
        ref = system.actor_of(props(CpuWorker, dispatcher=disp), "prof-cpu")
        cfg = disp
    rss0, cpu0 = rss_bytes(), cpu_time_ns()
    t0 = time.perf_counter_ns()
    for i in range(n):
        ref.tell(i)
    ref.ask_blocking("__count__", 180.0)
    elapsed_ns = time.perf_counter_ns() - t0
    rss1, cpu1 = rss_bytes(), cpu_time_ns()
    return _make("cpu", cfg, n, elapsed_ns, (), rss0, rss1, cpu0, cpu1)


# --- helpers ----------------------------------------------------------------


def _make(
    scenario: str,
    config: str,
    n: int,
    elapsed_ns: int,
    latencies_ns: Iterable[int],
    rss0: Optional[int],
    rss1: Optional[int],
    cpu0: int,
    cpu1: int,
) -> Measurement:
    throughput = (n * 1e9 / elapsed_ns) if elapsed_ns else 0.0
    lat = sorted(latencies_ns)
    return Measurement(
        runtime="python",
        scenario=scenario,
        config=config,
        messages=n,
        elapsed_ns=elapsed_ns,
        throughput_msgs_per_sec=throughput,
        p50_ns=percentile(lat, 50),
        p95_ns=percentile(lat, 95),
        p99_ns=percentile(lat, 99),
        rss_delta_bytes=(None if (rss0 is None or rss1 is None) else rss1 - rss0),
        peak_rss_bytes=peak_rss_bytes(),
        cpu_delta_ns=max(0, cpu1 - cpu0),
    )
