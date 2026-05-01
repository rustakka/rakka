# rakka

`rakka` is an idiomatic Rust port of [Akka.NET][akkanet] with
first-class Python bindings. It mirrors the Akka.NET module structure so
upstream changes can be tracked, while using native Rust patterns for
configuration (TOML), transport (Tokio + bincode), and serialization
(Serde). No wire compatibility with JVM/CLR Akka.

For a concise **why**—native execution, the same actor idea from cores to
cluster, and how that lines up with **agentic** and **distributed**
design—read [Actors and agentic computing](actors-and-agentic-computing.md).
For **LLM agent workflows**, companion layers **`rakka-langgraph`**
(LangGraph-style **state graphs** on actors) and **`rakka-agents`**
(**patterns and practices** above the graph) compose with the core
crates. **Telemetry and dashboard** (see [Dashboard](dashboard.md)) add
**visualization hooks** so behavior across `rakka-core`, cluster,
persistence, remote, streams, and more is visible in one service.

## At a glance

- Typed actors with compile-time message dispatch (similar to Akka Typed,
  actix, ractor).
- Full Akka.NET surface: supervision, FSM, stash, watch/death-watch,
  ask/pipe-to, dispatchers, mailboxes, schedulers, event stream,
  coordinated shutdown, extensions.
- Cross-process remoting: TCP transport, Akka-protocol handshake,
  ack'd delivery, EndpointManager state machine, RemoteActorRefProvider,
  RemoteWatcher, throttle / failure-injector / test transport adapters.
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
  **`rakka-langgraph`** / **`rakka-agents`**, determinism vs
  real-world non-determinism.
- [Dashboard](dashboard.md) — telemetry **visualization**; behavior across
  crates in one API + Web UI + WebSocket; cluster-wide views.
- [Python bindings](python.md) — install, actor API, GIL tuning guide,
  interpreter quotas, metrics, C-extension compatibility registry.
- [Persistence providers](persistence-providers.md) — SQL, Redis,
  MongoDB, Cassandra, DynamoDB, and Azure Table Storage crates plus the
  shared TCK.
- [Remoting](remoting.md) — `RemoteSystem`, transports, handshake,
  `actor_selection` across processes, cluster + sharding integration.
- [Profiler](profiler.md) — cross-runtime actor memory + CPU profiler,
  shared JSON schema, baseline numbers.
- [Parity](parity.md) — generated crate-by-crate presence report.
- [Full port plan](full-port-plan.md) — depth audit + 15-phase roadmap to
  close the gap with upstream Akka.NET in idiomatic Rust.
- [Idiomatic Rust principles](idiomatic-rust.md) — 12 invariants every
  PR is reviewed against (no `Box<dyn Any>`, type-state lifecycle,
  compile-time supervision contracts, …).
- [Audit 2026-04](audit-2026-04.md) — empirical depth + anti-pattern
  baseline; tracked by `cargo xtask audit --check` in CI.
- [Architecture](architecture.md) — the layered crate stack and
  concept-by-concept Akka.NET ↔ rakka mapping.
- [Migrating from Akka.NET](migrating-from-akka-net.md) — translation
  table, idiom-by-idiom diff, migration playbook.
- [`../README.md`](../README.md) — repository overview and quick start.
- [`../PORTING.md`](../PORTING.md) — upstream Akka.NET tracking commits.
- [`../PORTING_TODO.md`](../PORTING_TODO.md) — phase progress checklist.

## Status

**Scaffolding-complete, depth-in-progress.** Every Akka.NET subsystem
has a crate that builds and passes its unit tests (174+ Rust, 23
Python), but a 2026-04-30 audit found that most subsystems cover only
~1–10% of upstream LOC and skip critical protocol machinery (active
gossip, leader election, shard rebalance, recovery permitter, real
persistence backends, …). See [`full-port-plan.md`](full-port-plan.md)
for the audit and the 15-phase roadmap, and [`parity.md`](parity.md)
for per-crate depth grades.

[akkanet]: https://getakka.net/
