# Performance profiler

rakka ships a cross-runtime actor profiler that measures the same
four scenarios in both Rust and Python and emits a shared JSON schema
so the results can be compared directly.

| scenario | what it exercises | default messages |
|----------|-------------------|------------------|
| `tell`   | fire-and-forget throughput into a null actor | 100 000 (rust) / 20 000 (py) |
| `ask`    | sequential ask latency + throughput          | 5 000 (rust) / 2 000 (py) |
| `fanout` | spawning + first-message cost across N actors | 2 000 (rust) / 500 (py) |
| `cpu`    | CPU-bound handler (xxHash-lite compute loop)   | 10 000 (rust) / 2 000 (py) |

Per-scenario output: elapsed time, throughput, p50/p95/p99 latency (ask
only), resident-set delta, peak RSS, and process CPU time. Memory and
CPU probes come from `/proc/self/{status,stat}` on Linux and return
`n/a` elsewhere.

## Quick start

```bash
# Rust only
cargo run --release -p rakka-profiler -- --scenario all --format md

# Python only (after maturin develop --release)
python -m rakka.profiler --scenario all --format md

# Both, side-by-side (also writes merged JSON)
python scripts/profile.py --output docs/reports/profiler.md \
                          --json   docs/reports/profiler.json
```

`--messages N` overrides the per-scenario count for quick sanity runs
or for longer, steadier-state measurements.

## Python dispatcher autoselection

For Python, each scenario is automatically configured for best-case
performance:

| scenario           | dispatcher chosen                                         |
|--------------------|-----------------------------------------------------------|
| `tell` / `ask` / `fanout` | `python-pinned` — lowest per-message latency         |
| `cpu` (CPython 3.13t)     | `python-nogil` — free-threaded, max parallelism      |
| `cpu` (CPython 3.12+)     | `python-subinterpreter-pool` sized to `os.cpu_count()` |
| `cpu` (older Python)      | `python-pinned` (fallback)                           |

Override by importing the scenario runners directly and calling
`configure_interpreter` yourself.

## Baseline (Linux / aarch64 / 20 cpus / py 3.12)

Captured by `python scripts/profile.py --output docs/reports/profiler-baseline.md`.

| scenario | runtime | config | throughput | notes |
|---|---|---|---|---|
| tell   | rust   | default-dispatcher               | ~5.8M msg/s  | mailbox + scheduler overhead |
| tell   | python | python-pinned                    | ~25k msg/s   | full Python handler per message |
| ask    | rust   | default-dispatcher               | ~92k msg/s   | p99 ≈ 20 µs |
| ask    | python | python-pinned                    | ~14k msg/s   | p99 ≈ 380 µs |
| fanout | rust   | default-dispatcher               | ~171k msg/s  | 2 000 actors spawned in ~12 ms |
| fanout | python | python-pinned                    | ~6k msg/s    | includes N asks for delivery proof |
| cpu    | rust   | cpu-bound-handler                | ~185k msg/s  | single-core compute |
| cpu    | python | python-subinterpreter-pool pool=8| ~1.5k msg/s  | 8-way parallel Python compute |

Numbers vary by host; treat them as an order-of-magnitude sanity
check. The orchestrator prints a `python / rust` ratio column so a
regression in overhead shows up as a percentage drop.

## JSON schema

Every measurement (both runtimes) serializes to:

```json
{
  "runtime": "rust",
  "scenario": "ask",
  "config": "default-dispatcher",
  "messages": 5000,
  "elapsed_ns": 54220000,
  "throughput_msgs_per_sec": 92212.0,
  "p50_ns": 9760,
  "p95_ns": 15660,
  "p99_ns": 19890,
  "rss_delta_bytes": 0,
  "peak_rss_bytes": 83820544,
  "cpu_delta_ns": 80000000
}
```

The top-level report adds `runtime`, `version`, `host`, and a
`measurements` list. The orchestrator's `--json` flag writes
`{"rust": <report>, "python": <report>}`.

## Developer notes

- The Rust scenarios live in
  [`crates/rakka-profiler/src/scenarios.rs`](https://github.com/rustakka/rakka/blob/main/crates/rakka-profiler/src/scenarios.rs);
  the CLI is
  [`crates/rakka-profiler/src/bin/rakka_profiler.rs`](https://github.com/rustakka/rakka/blob/main/crates/rakka-profiler/src/bin/rakka_profiler.rs).
- The Python counterpart is the
  [`rakka.profiler`](https://github.com/rustakka/rakka/blob/main/python/rakka/profiler/__init__.py)
  sub-package (`_probes.py`, `_report.py`, `_scenarios.py`,
  `__main__.py`).
- Resource probes are Linux-only today (`VmRSS`, `VmHWM`,
  `/proc/self/stat` user+sys ticks). On other OSes the profiler still
  runs — it just reports `n/a` for memory/CPU columns.
- Keep scenario code identical across runtimes. If you change a
  workload on one side, mirror it on the other so the comparison
  remains apples-to-apples.
