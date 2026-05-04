"""Dataclasses + table formatting for the Python profiler.

Kept schema-compatible with ``atomr-profiler`` so reports can be
merged across runtimes.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import List, Optional


@dataclass
class Measurement:
    runtime: str
    scenario: str
    config: str
    messages: int
    elapsed_ns: int
    throughput_msgs_per_sec: float
    p50_ns: Optional[int] = None
    p95_ns: Optional[int] = None
    p99_ns: Optional[int] = None
    rss_delta_bytes: Optional[int] = None
    peak_rss_bytes: Optional[int] = None
    cpu_delta_ns: Optional[int] = None

    def to_dict(self) -> dict:
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class ProfilerReport:
    runtime: str = "python"
    version: str = ""
    host: str = ""
    measurements: List[Measurement] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps(
            {
                "runtime": self.runtime,
                "version": self.version,
                "host": self.host,
                "measurements": [m.to_dict() for m in self.measurements],
            },
            indent=2,
        )

    def to_markdown(self) -> str:
        rows = [
            f"# atomr profiler — {self.runtime} ({self.version})",
            "",
            f"host: `{self.host}`",
            "",
            "| scenario | config | msgs | elapsed | throughput | p50 | p95 | p99 | ΔRSS | CPU |",
            "|---|---|---|---|---|---|---|---|---|---|",
        ]
        for m in self.measurements:
            rows.append(
                "| {s} | {c} | {n} | {e} | {t} | {p50} | {p95} | {p99} | {rss} | {cpu} |".format(
                    s=m.scenario,
                    c=m.config,
                    n=m.messages,
                    e=fmt_ns(m.elapsed_ns),
                    t=fmt_rate(m.throughput_msgs_per_sec),
                    p50=fmt_opt_ns(m.p50_ns),
                    p95=fmt_opt_ns(m.p95_ns),
                    p99=fmt_opt_ns(m.p99_ns),
                    rss=fmt_opt_delta(m.rss_delta_bytes),
                    cpu=fmt_opt_ns(m.cpu_delta_ns),
                )
            )
        return "\n".join(rows) + "\n"


def fmt_ns(ns: int) -> str:
    if ns >= 1_000_000_000:
        return f"{ns / 1e9:.2f}s"
    if ns >= 1_000_000:
        return f"{ns / 1e6:.2f}ms"
    if ns >= 1_000:
        return f"{ns / 1e3:.2f}µs"
    return f"{ns}ns"


def fmt_opt_ns(v: Optional[int]) -> str:
    return fmt_ns(v) if v is not None else "n/a"


def fmt_rate(v: float) -> str:
    if v >= 1e6:
        return f"{v / 1e6:.2f}M/s"
    if v >= 1e3:
        return f"{v / 1e3:.2f}k/s"
    return f"{v:.2f}/s"


def fmt_opt_delta(v: Optional[int]) -> str:
    if v is None:
        return "n/a"
    sign = "-" if v < 0 else "+"
    n = abs(v)
    if n >= 1 << 30:
        return f"{sign}{n / (1 << 30):.2f}GiB"
    if n >= 1 << 20:
        return f"{sign}{n / (1 << 20):.2f}MiB"
    if n >= 1 << 10:
        return f"{sign}{n / (1 << 10):.2f}KiB"
    return f"{sign}{n}B"


def percentile(sorted_vals: List[int], p: float) -> Optional[int]:
    if not sorted_vals:
        return None
    idx = min(len(sorted_vals) - 1, max(0, round((p / 100.0) * (len(sorted_vals) - 1))))
    return sorted_vals[idx]
