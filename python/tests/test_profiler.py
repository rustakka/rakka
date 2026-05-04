"""Smoke tests for atomr.profiler."""

from __future__ import annotations

import json

import atomr
from atomr.profiler import DEFAULT_MESSAGES, SCENARIOS, run
from atomr.profiler._probes import best_dispatcher_for, rss_bytes
from atomr.profiler._report import (
    Measurement,
    ProfilerReport,
    fmt_ns,
    fmt_rate,
    percentile,
)


def test_probe_defaults_sane():
    # rss_bytes may be None on non-Linux, but should not raise.
    v = rss_bytes()
    assert v is None or v > 0


def test_dispatcher_selection():
    disp, role, count, quota = best_dispatcher_for("tell")
    assert disp == "python-pinned"
    assert count == 1
    disp_cpu, _, count_cpu, _ = best_dispatcher_for("cpu")
    assert disp_cpu in (
        "python-pinned",
        "python-nogil",
        "python-subinterpreter-pool",
    )
    assert count_cpu >= 1


def test_percentile_and_format_helpers():
    xs = list(range(1, 101))
    assert percentile(xs, 50) == 51   # banker's rounding of 49.5 → 50 → xs[50]=51
    assert percentile(xs, 0) == 1
    assert percentile(xs, 100) == 100
    assert percentile([], 50) is None
    assert fmt_ns(10_000) == "10.00µs"
    assert fmt_rate(2_500) == "2.50k/s"


def test_run_tiny_subset_produces_valid_report():
    report: ProfilerReport = run(("tell", "ask"), messages=200)
    assert report.runtime == "python"
    assert report.version == atomr.__version__
    assert len(report.measurements) == 2
    tell = next(m for m in report.measurements if m.scenario == "tell")
    ask = next(m for m in report.measurements if m.scenario == "ask")
    assert tell.messages == 200
    assert tell.throughput_msgs_per_sec > 0
    assert ask.p50_ns is not None  # ask populates latencies


def test_report_json_roundtrip_has_schema():
    report = run(("ask",), messages=20)
    blob = report.to_json()
    data = json.loads(blob)
    assert data["runtime"] == "python"
    assert isinstance(data["measurements"], list)
    m = data["measurements"][0]
    for key in ("scenario", "messages", "elapsed_ns", "throughput_msgs_per_sec"):
        assert key in m


def test_default_message_counts_cover_all_scenarios():
    assert set(DEFAULT_MESSAGES) == set(SCENARIOS)
    for n in DEFAULT_MESSAGES.values():
        assert n > 0
