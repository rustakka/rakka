#!/usr/bin/env python3
"""Cross-runtime profiler orchestrator.

Runs the Rust profiler binary and the Python profiler module,
deserializes both JSON reports, and emits a side-by-side comparison
table.

Usage::

    python scripts/profile.py                        # all scenarios, default counts
    python scripts/profile.py --messages 5000        # override message counts
    python scripts/profile.py --output out.md        # write markdown to file
    python scripts/profile.py --json out.json        # also dump the merged json
    python scripts/profile.py --skip rust            # only run python (or vice-versa)

The orchestrator expects:

* ``cargo`` on PATH (to build/run the Rust binary), OR
* ``target/release/atomr-profiler`` to already exist.

and the Python bindings to already be built
(``maturin develop --release``).
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import shutil
import subprocess
import sys
from typing import Dict, List, Optional, Tuple

ROOT = pathlib.Path(__file__).resolve().parent.parent
SCENARIOS = ("tell", "ask", "fanout", "cpu")


def run_rust(messages: Optional[int]) -> dict:
    """Build (if needed) and invoke the Rust profiler, return parsed JSON."""
    exe = ROOT / "target" / "release" / "atomr-profiler"
    if not exe.exists():
        cargo = shutil.which("cargo")
        if not cargo:
            raise RuntimeError(
                "Rust profiler binary not found and `cargo` missing from PATH. "
                "Build with `cargo build --release -p atomr-profiler`."
            )
        subprocess.check_call(
            [cargo, "build", "--release", "-p", "atomr-profiler"], cwd=ROOT
        )
    args = [str(exe), "--format", "json", "--scenario", "all"]
    if messages is not None:
        args += ["--messages", str(messages)]
    out = subprocess.check_output(args, cwd=ROOT).decode("utf-8")
    return json.loads(out)


def run_python(messages: Optional[int]) -> dict:
    """Run the Python profiler module in the current interpreter."""
    from atomr.profiler import run

    report = run(SCENARIOS, messages=messages)
    return json.loads(report.to_json())


def _fmt_ns(ns: Optional[int]) -> str:
    if ns is None:
        return "n/a"
    if ns >= 1_000_000_000:
        return f"{ns / 1e9:.2f}s"
    if ns >= 1_000_000:
        return f"{ns / 1e6:.2f}ms"
    if ns >= 1_000:
        return f"{ns / 1e3:.2f}µs"
    return f"{ns}ns"


def _fmt_rate(v: float) -> str:
    if v >= 1e6:
        return f"{v / 1e6:.2f}M/s"
    if v >= 1e3:
        return f"{v / 1e3:.2f}k/s"
    return f"{v:.2f}/s"


def _fmt_delta(v: Optional[int]) -> str:
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


def _index(report: dict) -> Dict[str, dict]:
    return {m["scenario"]: m for m in report.get("measurements", [])}


def merge_markdown(rust: Optional[dict], py: Optional[dict]) -> str:
    out: List[str] = ["# atomr profiler — cross-runtime comparison", ""]
    for label, rpt in (("rust", rust), ("python", py)):
        if rpt:
            out.append(f"- **{label}** v{rpt.get('version')} host=`{rpt.get('host')}`")
    out.append("")
    out.append(
        "| scenario | runtime | config | msgs | elapsed | throughput | p50 | p95 | p99 | ΔRSS | CPU |"
    )
    out.append("|---|---|---|---|---|---|---|---|---|---|---|")
    rust_idx = _index(rust) if rust else {}
    py_idx = _index(py) if py else {}
    for s in SCENARIOS:
        for runtime, idx in (("rust", rust_idx), ("python", py_idx)):
            m = idx.get(s)
            if not m:
                continue
            out.append(
                "| {s} | {r} | {c} | {n} | {e} | {t} | {p50} | {p95} | {p99} | {rss} | {cpu} |".format(
                    s=m["scenario"],
                    r=runtime,
                    c=m["config"],
                    n=m["messages"],
                    e=_fmt_ns(m["elapsed_ns"]),
                    t=_fmt_rate(m["throughput_msgs_per_sec"]),
                    p50=_fmt_ns(m.get("p50_ns")),
                    p95=_fmt_ns(m.get("p95_ns")),
                    p99=_fmt_ns(m.get("p99_ns")),
                    rss=_fmt_delta(m.get("rss_delta_bytes")),
                    cpu=_fmt_ns(m.get("cpu_delta_ns")),
                )
            )
    # Ratios row
    out.append("")
    out.append("## Python overhead factor (python-throughput / rust-throughput)")
    out.append("")
    out.append("| scenario | rust | python | python/rust |")
    out.append("|---|---|---|---|")
    for s in SCENARIOS:
        r = rust_idx.get(s)
        p = py_idx.get(s)
        if not (r and p):
            continue
        rr = r["throughput_msgs_per_sec"]
        pp = p["throughput_msgs_per_sec"]
        ratio = (pp / rr) if rr else float("nan")
        out.append(
            f"| {s} | {_fmt_rate(rr)} | {_fmt_rate(pp)} | {ratio:.2%} |"
        )
    return "\n".join(out) + "\n"


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Run Rust + Python profilers and compare.")
    p.add_argument("--messages", type=int, default=None)
    p.add_argument("--skip", choices=("rust", "python"), default=None)
    p.add_argument("--output", default=None, help="markdown output file")
    p.add_argument("--json", default=None, help="merged json output file")
    args = p.parse_args(argv)

    rust: Optional[dict] = None
    py: Optional[dict] = None
    if args.skip != "rust":
        print("→ running Rust profiler ...", file=sys.stderr)
        rust = run_rust(args.messages)
    if args.skip != "python":
        print("→ running Python profiler ...", file=sys.stderr)
        py = run_python(args.messages)

    md = merge_markdown(rust, py)
    if args.output:
        pathlib.Path(args.output).write_text(md, encoding="utf-8")
    else:
        sys.stdout.write(md)
    if args.json:
        merged = {"rust": rust, "python": py}
        pathlib.Path(args.json).write_text(json.dumps(merged, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
