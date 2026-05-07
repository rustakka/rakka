# atomr

A native Rust runtime for actor-based concurrent and distributed
systems, with first-class Python bindings. One programming model —
addressable units of state plus behavior, communicating by
asynchronous message passing — that scales from a single core to a
cluster, and increasingly from a CPU to a GPU.

The runtime is small at the edge — typed actors, supervised
hierarchies, deterministic mailboxes — and grows outward into
remoting, clustering, sharded entities, replicated CRDTs, event
sourcing, reactive streams, and live introspection. The same message
contract holds at every layer.

## Why this design

**Agentic systems want this shape.** Autonomous, collaborating,
stateful processes that coordinate through messages and survive
partial failure are exactly what supervised actors describe. Each
agent is an actor; conversations are mailboxes; tool invocations are
typed messages; failure is observed and supervised, not silently
swallowed.

**Heterogeneous compute wants this shape, too.** Modern workloads
straddle CPU and GPU. Inference, embedding, simulation, scoring all
prefer accelerator memory; coordination, control flow, persistence,
and I/O prefer the host. Today's stacks bridge the two with ad-hoc
batching layers, queues, and serialization shims. The actor model
already encodes the right boundary — a message *is* the dispatch unit
— so a system built on actors can put CPU mailboxes and CUDA-backed
dispatchers behind the same `actor_ref.tell(msg)` call, with the same
supervision, the same backpressure, and the same observability. That
is the unified-compute thesis: don't write two programs glued at the
seam, write one program whose dispatch can target either side
explicitly and efficiently.

**Rust earns the granularity.** Zero-cost abstractions, ownership-as-
concurrency-safety, and predictable resource use mean per-message
overhead stays low and per-actor footprint stays small enough that
millions of fine-grained actors are tractable. The same precision
lets the runtime push backpressure, mailboxes, and supervision down
into primitives that don't need to be rebuilt at every layer above.

A longer argument lives in
[Actors and agentic computing](actors-and-agentic-computing.md).

## At a glance

- **Typed actors** with compile-time message dispatch.
- **Supervision, FSM, stash, watch / death-watch, ask / pipe-to,
  dispatchers, mailboxes, schedulers, event stream, coordinated
  shutdown, extensions.**
- **Remoting** — TCP transport, framed PDU codec, ack'd delivery,
  endpoint state machine, watcher, throttle / failure-injector / test
  transports.
- **Cluster** — gossip, membership, reachability, heartbeat,
  split-brain resolvers, cluster-tools (singleton, pub/sub, client),
  cluster-sharding (regions, rebalance, remember-entities), cluster
  metrics, distributed data (CRDTs).
- **Persistence** — event sourcing with journal + snapshot traits,
  recovery permitter, async snapshotting, query streams; storage
  adapters for SQL, Redis, MongoDB, Cassandra, DynamoDB, Azure Table.
- **Streams** — typed `Source` / `Flow` / `Sink` / `BidiFlow` / graph
  DSL with materializer, junctions, hubs, framing, file IO, kill
  switches, lifecycle hooks.
- **Coordination, discovery, DI, hosting** — the contrib toolkit you
  reach for when you compose larger systems.
- **Telemetry + dashboard** — tracing, metrics, exporters, plus a
  live web UI over the running system.
- **Python bindings** for every subsystem: real `Context` (spawn /
  watch / stash / become / cancellable timers / sender), configurable
  `SupervisorStrategy` with enforced retry budgets, routers and
  resilience patterns, multi-node TCP + in-process cluster transports
  with per-system codec registries, event-sourced actors, the full
  CRDT suite + `Replicator`, real cluster sharding with allocation /
  passivation / remember-entities, the streams DSL on arbitrary
  Python objects, GIL-isolated interpreter strategies (`python-
  pinned`, `python-subinterpreter-pool` per PEP 684, `python-nogil`
  per PEP 703, `python-subprocess`) with per-pool `InterpreterQuota`
  / `InterpreterMetrics`.
- **Cross-runtime profiler** — same scenarios in Rust and Python,
  shared JSON schema, side-by-side comparison.

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
pip install atomr
python python/examples/ml_inference.py
```

## Documentation map

- [Actors and agentic computing](actors-and-agentic-computing.md) — the
  argument: native efficiency, supervised concurrency, agentic systems,
  the unified CPU + CUDA compute model.
- [Architecture](architecture.md) — runtime layout, dispatch, cluster
  topology, the hooks where heterogeneous backends slot in.
- [Idiomatic Rust principles](idiomatic-rust.md) — twelve invariants
  every PR is reviewed against (no `Box<dyn Any>` mailboxes,
  type-state lifecycle, compile-time supervision contracts, …).
- [Python bindings](python.md) — install, actor API, GIL strategy
  guide, quotas, metrics, compatibility registry.
- [Remoting](remoting.md) — cross-process actor messaging.
- [Persistence providers](persistence-providers.md) — storage adapters
  and the shared TCK.
- [Streams](https://github.com/rustakka/atomr#whats-in-the-box) — reactive stream DSL.
- [Dashboard](dashboard.md) — live system UI.
- [Observability](observability.md) — exporters and integration
  points.
- [Profiler](profiler.md) — cross-runtime profiler with baseline
  numbers.
- [Release pipeline](release-pipeline.md) — how artifacts ship.
- [Parity](parity.md) — feature surface and depth grades by crate.
- [Depth program](full-port-plan.md) — long-form depth roadmap.
- [Audit 2026-04](audit-2026-04.md) — empirical depth + anti-pattern
  baseline tracked by CI.
- [`../README.md`](https://github.com/rustakka/atomr) — repository overview.
- [Alignment ledger](alignment-ledger.md) — crate-by-crate alignment
  of the runtime surface.
- [Depth roadmap](depth-roadmap.md) — depth roadmap.
