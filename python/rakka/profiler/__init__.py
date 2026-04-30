"""Actor memory + CPU profiler for the Python bindings.

Mirrors the ``rakka-profiler`` Rust binary — same scenarios
(``tell``, ``ask``, ``fanout``, ``cpu``), same JSON schema — so the two
runtimes can be compared head-to-head.

The profiler autoconfigures the most performant dispatcher for each
scenario: ``python-pinned`` for latency-sensitive workloads, and
``python-nogil`` → ``python-subinterpreter-pool`` → ``python-pinned``
(first one available) for CPU-bound ones.

Quick use:

.. code:: bash

   python -m rakka.profiler --scenario all --messages 2000 --format md
   python -m rakka.profiler --format json --output report.json

Or programmatically:

.. code:: python

   from rakka.profiler import run
   report = run()
   print(report.to_markdown())
"""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, List, Optional

import rakka

from ._probes import host_tag
from ._report import Measurement, ProfilerReport
from ._scenarios import run_ask, run_cpu, run_fanout, run_tell

SCENARIOS = ("tell", "ask", "fanout", "cpu")
DEFAULT_MESSAGES = {"tell": 20_000, "ask": 2_000, "fanout": 500, "cpu": 2_000}


def run(
    scenarios: Iterable[str] = SCENARIOS,
    messages: Optional[int] = None,
    system_name: str = "profiler",
) -> ProfilerReport:
    """Run the requested scenarios and return a populated report."""
    report = ProfilerReport(version=rakka.__version__, host=host_tag())
    system = rakka.ActorSystem.create_blocking(system_name)
    try:
        for s in scenarios:
            if s not in SCENARIOS:
                raise ValueError(f"unknown scenario: {s!r}")
            n = messages if messages is not None else DEFAULT_MESSAGES[s]
            if s == "tell":
                m = run_tell(system, n)
            elif s == "ask":
                m = run_ask(system, n)
            elif s == "fanout":
                m = run_fanout(system, n)
            else:
                m = run_cpu(system, n)
            report.measurements.append(m)
    finally:
        system.terminate_blocking()
    return report


def _build_cli() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m rakka.profiler",
        description="Actor memory + CPU profiler (Python bindings).",
    )
    p.add_argument(
        "--scenario",
        choices=(*SCENARIOS, "all"),
        default="all",
        help="scenario to run (default: all)",
    )
    p.add_argument(
        "-n", "--messages", type=int, default=None, help="override per-scenario message count"
    )
    p.add_argument(
        "--format", choices=("md", "json"), default="md", help="output format (default: md)"
    )
    p.add_argument("-o", "--output", default=None, help="write to FILE instead of stdout")
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = _build_cli().parse_args(argv)
    scenarios = SCENARIOS if args.scenario == "all" else (args.scenario,)
    report = run(scenarios, messages=args.messages)
    rendered = report.to_markdown() if args.format == "md" else report.to_json()
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(rendered)
    else:
        sys.stdout.write(rendered)
        if not rendered.endswith("\n"):
            sys.stdout.write("\n")
    return 0


__all__ = [
    "Measurement",
    "ProfilerReport",
    "SCENARIOS",
    "DEFAULT_MESSAGES",
    "run",
    "main",
]
