# rustakka

`rustakka` is an idiomatic Rust port of [Akka.NET][akkanet] with
first-class Python bindings. It mirrors the Akka.NET module structure so
upstream changes can be tracked, while using native Rust patterns for
configuration (TOML), transport (Tokio + Prost), and serialization
(Serde). No wire compatibility with JVM/CLR Akka.

For a concise **why**—native execution, the same actor idea from cores to
cluster, and how that lines up with **agentic** and **distributed**
design—read [Actors and agentic computing](actors-and-agentic-computing.md).
For **LLM agent workflows**, companion layers **`rustakka-langgraph`**
(LangGraph-style **state graphs** on actors) and **`rustakka-agents`**
(**patterns and practices** above the graph) compose with the core
crates. **Telemetry and dashboard** (see [Dashboard](dashboard.md)) add
**visualization hooks** so behavior across `rustakka-core`, cluster,
persistence, remote, streams, and more is visible in one service.

## At a glance

- Typed actors with compile-time message dispatch (similar to Akka Typed,
  actix, ractor).
- Full Akka.NET surface: supervision, FSM, stash, watch/death-watch,
  ask/pipe-to, dispatchers, mailboxes, schedulers, event stream,
  coordinated shutdown, extensions.
- Cluster stack: gossip, membership, reachability, heartbeat, SBR (5
  strategies), cluster-tools, cluster-sharding, distributed data,
  cluster-metrics.
- Persistence: journal + snapshot plugin traits, query, TCK,
  at-least-once delivery, in-memory implementation.
- Streams: Source / Flow / Sink / BidiFlow / GraphDsl / ActorMaterializer.
- Contrib: coordination (Lease), discovery, DI, hosting.
- Python bindings for every subsystem via PyO3, with GIL-isolated
  interpreter pools (`python-pinned`, `python-subinterpreter-pool`,
  `python-nogil`, `python-subprocess`) and per-pool
  `InterpreterQuota` / `InterpreterMetrics`.
- `xtask` upstream-sync tool; quarterly CI job reports Akka.NET diffs.

## Getting started

### Rust

```bash
cargo build --workspace
cargo test  --workspace
cargo run   -p pingpong
```

### Python

```bash
python -m venv .venv && source .venv/bin/activate
pip install maturin pytest pytest-asyncio
maturin develop --release
pytest python/tests -v
python python/examples/ml_inference.py
```

## Documentation map

- [Actors and agentic computing](actors-and-agentic-computing.md) — value
  proposition: native efficiency, Akka-style clarity, agent-like systems,
  **`rustakka-langgraph`** / **`rustakka-agents`**, determinism vs
  real-world non-determinism.
- [Dashboard](dashboard.md) — telemetry **visualization**; behavior across
  crates in one API + Web UI + WebSocket; cluster-wide views.
- [Python bindings](python.md) — install, actor API, GIL tuning guide,
  interpreter quotas, metrics, C-extension compatibility registry.
- [Persistence providers](persistence-providers.md) — SQL, Redis,
  MongoDB, Cassandra, DynamoDB, and Azure Table Storage crates plus the
  shared TCK.
- [Profiler](profiler.md) — cross-runtime actor memory + CPU profiler,
  shared JSON schema, baseline numbers.
- [Parity](parity.md) — generated crate-by-crate presence report.
- [`../README.md`](../README.md) — repository overview and quick start.
- [`../PORTING.md`](../PORTING.md) — upstream Akka.NET tracking commits.
- [`../PORTING_TODO.md`](../PORTING_TODO.md) — phase progress checklist.

## Status

All in-scope phases landed. 84 Rust tests and 23 Python tests pass in
CI. See `PORTING_TODO.md` for per-phase detail.

[akkanet]: https://getakka.net/
