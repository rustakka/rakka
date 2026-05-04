# atomr

A native Rust runtime for **actor-based concurrent and distributed
systems**, with first-class Python bindings. atomr gives you a single
mental model — addressable units of state plus behavior, communicating
by asynchronous messages — that scales from a single core to a cluster,
and increasingly from a CPU to a GPU.

```rust
use atomr::prelude::*;

#[derive(Default)]
struct Greeter;

#[async_trait::async_trait]
impl Actor for Greeter {
    type Msg = String;
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: String) {
        println!("hi {msg}");
    }
}
```

## Why an actor runtime, in Rust, now

The actor model is the same idea wherever it runs: a small, addressable
unit of state plus behavior, talking to other actors by asynchronous
message passing. That model is a good fit for two converging trends.

**Agentic systems.** Long-lived, autonomous, collaborating processes
that reason, call tools, and coordinate are exactly what supervised,
addressable actors describe. Each agent is an actor; conversations are
mailboxes; tool calls are typed messages; failure is supervised, not
silently swallowed. atomr gives that model a runtime that doesn't trade
performance for safety.

**Unified compute.** Modern workloads no longer live entirely on the
CPU. Inference, embedding, scoring, simulation — they want a GPU.
Coordination, control flow, I/O, persistence — they want a CPU.
Today's stacks force you to glue the two with ad-hoc batching layers,
queues, and serialization boundaries. The actor model already encodes
the right boundary: a message *is* the dispatch unit. atomr is built so
that the same `actor_ref.tell(msg)` can target a CPU mailbox today and
a CUDA-backed dispatcher tomorrow — with the same supervision, the
same backpressure, the same observability. The runtime is explicit
about *where* work runs without forcing the developer to write two
programs.

**Granular efficiency.** Rust gives us deterministic resource use,
zero-cost abstractions, and ownership-as-concurrency-safety.
Per-message cost stays low. Per-actor footprint stays small. The
scheduler can hand work to a `tokio` worker, a dedicated dispatcher,
or — by design — a GPU stream, without changing the message contract.
That same precision lets the runtime push backpressure, mailboxes, and
supervision down to a level where they don't need to be rebuilt at
every layer above.

A longer argument is in
[`docs/actors-and-agentic-computing.md`](docs/actors-and-agentic-computing.md).

## What's in the box

| Crate | What it does |
|---|---|
| `atomr` | Umbrella facade re-exporting the core types |
| `atomr-core` | Actors, supervision, dispatch, mailboxes, FSMs, event stream, coordinated shutdown |
| `atomr-config` | HOCON-style layered configuration |
| `atomr-macros` | Ergonomic derives and helpers |
| `atomr-testkit` | Probes, virtual time, deterministic test scaffolding |
| `atomr-remote` | Location-transparent messaging across processes (TCP + framed PDU + reliable delivery) |
| `atomr-cluster` | Membership, gossip, reachability, split-brain resolution |
| `atomr-cluster-tools` | Singleton, pub/sub, cluster-client patterns |
| `atomr-cluster-sharding` | Shard regions, rebalance, remember-entities, persistent coordinator |
| `atomr-cluster-metrics` | Adaptive load balancing |
| `atomr-distributed-data` | Convergent replicated data types (CRDTs) over the cluster |
| `atomr-persistence` | Event sourcing — journals, snapshots, recovery, async snapshotting |
| `atomr-persistence-query` | Tagged event streams over journals |
| `atomr-persistence-{sql,redis,mongodb,cassandra,aws,azure}` | Storage adapters |
| `atomr-persistence-tck` | Conformance suite for journal + snapshot implementations |
| `atomr-streams` | Typed reactive streams (sources, flows, sinks, junctions, hubs, kill switches) |
| `atomr-coordination` | Lease-based leadership primitives |
| `atomr-discovery` | Pluggable service discovery |
| `atomr-di` | Dependency-injection container |
| `atomr-hosting` | Builder API for wiring system + config + DI together |
| `atomr-telemetry` | Tracing, metrics, exporters |
| `atomr-dashboard` | Live web UI over the running system |

Plus a Python facade — `pip install atomr` — that exposes the same
actor model with GIL-isolated interpreter pools for CPU-bound work and
async-native `tell` / `ask`.

## Quick start (Rust)

The umbrella crate is published on crates.io as **`atomr`**:

```toml
[dependencies]
atomr = { version = "0.1", features = ["cluster", "persistence"] }
```

Or pull in subsystem crates directly — `atomr-core`, `atomr-cluster`,
`atomr-persistence`, `atomr-streams`, etc. are all on crates.io.

```rust
use atomr::prelude::*;

#[derive(Default)]
struct Greeter;

#[async_trait::async_trait]
impl Actor for Greeter {
    type Msg = String;
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: String) {
        println!("hi {msg}");
    }
}

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let system = ActorSystem::create("S", Config::empty()).await?;
let greeter = system.actor_of(Props::create(Greeter::default), "greeter")?;
greeter.tell("world".to_string());
system.terminate().await;
# Ok(()) }
```

## Quick start (Python)

```bash
python -m venv .venv && source .venv/bin/activate
pip install atomr
```

```python
from atomr import Actor, ActorSystem, props

class Greeter(Actor):
    async def handle(self, ctx, msg):
        return f"hello, {msg}"

system = ActorSystem.create_blocking("app")
ref = system.actor_of(props(Greeter), "greeter")
print(ref.ask_blocking("world", timeout=5.0))   # -> "hello, world"
system.terminate_blocking()
```

See [`docs/python.md`](docs/python.md) for the GIL-strategy guide
(`python-pinned`, `python-subinterpreter-pool` per PEP 684,
`python-nogil` per PEP 703, `python-subprocess`) and the C-extension
compatibility registry.

## Building from source

```bash
# Rust
cargo build --workspace
cargo test --workspace

# Python bindings (requires maturin + a Python dev toolchain)
maturin develop --release
pytest python/tests -v

# Docs (optional)
pip install mkdocs-material
mkdocs serve
```

## Profiling

atomr ships with a cross-runtime profiler that measures the same four
scenarios (`tell`, `ask`, `fanout`, `cpu`) in Rust and Python and emits
a shared JSON schema so the two paths can be compared directly.

```bash
cargo run --release -p atomr-profiler -- --scenario all --format md
python -m atomr.profiler --scenario all --format md
```

See [`docs/profiler.md`](docs/profiler.md).

## Layout

```
crates/                 Rust workspace
crates/py-bindings/     PyO3 bridge crates
python/atomr/           Python package
python/tests/           Python integration tests
examples/               Runnable Rust examples
benches/                Criterion benches
scripts/                Cross-runtime tooling
docs/                   mkdocs-material source
xtask/                  Cargo xtask (audit, profile, bump, verify)
```

## Learn more

- [`docs/actors-and-agentic-computing.md`](docs/actors-and-agentic-computing.md)
  — the case for actors as the substrate for agentic + heterogeneous
  compute.
- [`docs/architecture.md`](docs/architecture.md) — runtime structure.
- [`docs/idiomatic-rust.md`](docs/idiomatic-rust.md) — design choices.
- [`docs/python.md`](docs/python.md) — Python bindings + GIL strategies.
- [`docs/remoting.md`](docs/remoting.md) — cross-process actor remoting.
- [`docs/persistence-providers.md`](docs/persistence-providers.md) — storage adapters.
- [`docs/dashboard.md`](docs/dashboard.md) — live system UI.
- [`docs/observability.md`](docs/observability.md) — tracing + metrics exporters.
- [`docs/profiler.md`](docs/profiler.md) — cross-runtime profiler.
- [`PORTING.md`](PORTING.md) — alignment with prior-art runtimes.
- [`PORTING_TODO.md`](PORTING_TODO.md) — depth roadmap.
