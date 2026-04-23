# rustakka

An idiomatic Rust port of [Akka.NET](https://github.com/akkadotnet/akka.net)
with first-class Python bindings. The Rust crates mirror the Akka.NET
module layout so upstream changes can be tracked, while using native Rust
solutions for configuration (TOML), transport (Tokio + Prost), and
serialization (Serde). Python users get the same actor model with
GIL-isolated interpreter pools for CPU-bound workloads.

**Why it matters:** The actor model is the same programming idea whether it
runs on the JVM, on .NET, or here as **native code** in Rust. You get a
**small, addressable unit of state plus behavior** (an actor) that
communicates by **asynchronous message passing**, which maps cleanly onto
**autonomous, collaborative processes** in agentic and distributed
systems. On one machine, actors spread work across **cores** without
shared-memory soup; across machines, the **same abstraction** (location
transparent addresses, cluster, sharding) extends that idea to a fleet.
**Deterministic** designs are possible per actor (ordered mailbox, local
state machine); **non-determinism** from concurrency, the network, and
failure is **explicit and supervised** rather than an accident of raw
threads. For a full argument, see
[`docs/actors-and-agentic-computing.md`](docs/actors-and-agentic-computing.md).

**Agentic stack (ecosystem):** [LangGraph](https://github.com/langchain-ai/langgraph)-style
**agent state graphs** map naturally onto supervised actors. Companion
crates in the same family—**`rustakka-langgraph`** (embed LangGraph agent
state graphs in the runtime) and **`rustakka-agents`** (patterns, tooling,
and practices *above* the graph layer: orchestration, tools, and
operational playbooks)—sit on top of the core in-tree crates. The doc above
goes into depth.

## Status

All in-scope porting phases are landed with passing unit tests:
**84 Rust tests** and **23 Python tests**. See
[`PORTING_TODO.md`](PORTING_TODO.md) for per-phase checkboxes and
[`PORTING.md`](PORTING.md) for upstream Akka.NET tracking.

## Workspace layout

### Rust crates

| Crate | Mirrors |
|-------|---------|
| `rustakka` | `Akka` facade |
| `rustakka-core` | `src/core/Akka` |
| `rustakka-config` | `src/core/Akka/Configuration` |
| `rustakka-macros` | n/a (ergonomics) |
| `rustakka-testkit` | `src/core/Akka.TestKit` |
| `rustakka-remote` | `src/core/Akka.Remote` |
| `rustakka-cluster` | `src/core/Akka.Cluster` |
| `rustakka-cluster-tools` | `src/contrib/cluster/Akka.Cluster.Tools` |
| `rustakka-cluster-sharding` | `src/contrib/cluster/Akka.Cluster.Sharding` |
| `rustakka-cluster-metrics` | `src/contrib/cluster/Akka.Cluster.Metrics` |
| `rustakka-distributed-data` | `src/contrib/cluster/Akka.DistributedData` |
| `rustakka-persistence` | `src/core/Akka.Persistence` |
| `rustakka-persistence-query` | `src/core/Akka.Persistence.Query` |
| `rustakka-persistence-query-inmemory` | in-memory read journal |
| `rustakka-persistence-tck` | `src/core/Akka.Persistence.TCK` |
| `rustakka-streams` | `src/core/Akka.Streams` |
| `rustakka-coordination` | `src/core/Akka.Coordination` |
| `rustakka-discovery` | `src/core/Akka.Discovery` |
| `rustakka-di` | `src/contrib/dependencyinjection/Akka.DependencyInjection` |
| `rustakka-hosting` | `Akka.Hosting` (external) |

### Python bindings

| Rust sub-crate | Python module |
|----------------|---------------|
| `crates/py-bindings/pycore` | `rustakka` + `rustakka._native` |
| `crates/py-bindings/pytestkit` | `rustakka.testkit` |
| `crates/py-bindings/pycluster` | `rustakka.cluster` |
| `crates/py-bindings/pycluster-tools` | `rustakka.cluster_tools` |
| `crates/py-bindings/pycluster-sharding` | `rustakka.cluster_sharding` |
| `crates/py-bindings/pyddata` | `rustakka.ddata` |
| `crates/py-bindings/pypersistence` | `rustakka.persistence` |
| `crates/py-bindings/pystreams` | `rustakka.streams` |
| `crates/py-bindings/pycoordination` | `rustakka.coordination` |
| `crates/py-bindings/pydiscovery` | `rustakka.discovery` |
| `crates/py-bindings/pydi` | `rustakka.di` |
| `crates/py-bindings/pyhosting` | `rustakka.hosting` |

The sub-crates are aggregation placeholders — Python bindings for every
subsystem are compiled into the single `rustakka._native` cdylib by
`pycore`. Individual wheels can be carved out later without renaming the
Python facade.

## Quick start (Rust)

```rust
use rustakka::prelude::*;

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
pip install maturin pytest pytest-asyncio
maturin develop --release
```

> **Sandboxed host without `python3-dev`?** Copy
> `.cargo/pyo3-config.txt.example` to `.cargo/pyo3-config.txt`, edit the
> paths for your interpreter, and `export
> PYO3_CONFIG_FILE="$PWD/.cargo/pyo3-config.txt"`. The helper
> `source scripts/dev-env.sh` automates the whole venv + env-var setup.

```python
from rustakka import Actor, ActorSystem, props

class Greeter(Actor):
    async def handle(self, ctx, msg):
        return f"hello, {msg}"

system = ActorSystem.create_blocking("app")
ref = system.actor_of(props(Greeter), "greeter")
print(ref.ask_blocking("world", timeout=5.0))   # -> "hello, world"
system.terminate_blocking()
```

See [`docs/python.md`](docs/python.md) for the full GIL tuning guide —
`python-pinned`, `python-subinterpreter-pool` (PEP 684),
`python-nogil` (PEP 703), `python-subprocess`, plus
`InterpreterQuota`, `InterpreterMetrics`, and the C-extension
compatibility registry.

## Profiling

A cross-runtime actor profiler ships with the repo. It measures the same
four scenarios (`tell`, `ask`, `fanout`, `cpu`) in Rust and Python and
emits a shared JSON schema so the two stacks can be compared directly.

```bash
# Rust only
cargo run --release -p rustakka-profiler -- --scenario all --format md
cargo xtask profile -- --scenario cpu --messages 5000

# Python only (after maturin develop --release)
python -m rustakka.profiler --scenario all --format md

# Both side-by-side, with a merged JSON artifact
python scripts/profile.py --output docs/reports/profiler.md \
                          --json   docs/reports/profiler.json
```

See [`docs/profiler.md`](docs/profiler.md) for the full guide and a
baseline captured on Linux / aarch64 / 20 cpus / py 3.12.

## Building and testing

```bash
# Rust
cargo build --workspace
cargo test --workspace

# Python (requires maturin + a Python dev toolchain)
maturin develop --release
pytest python/tests -v

# Docs (optional)
pip install mkdocs-material
mkdocs serve
```

## Layout on disk

```
crates/           Rust crates (one per Akka.NET subsystem)
crates/py-bindings/   PyO3 bridge crates + sub-crate placeholders
python/rustakka/      Python facade package (pure Python)
python/tests/         pytest suite for the native extension
python/examples/      Python examples (pingpong, ml_inference, ...)
examples/             Rust examples (pingpong, chat, fault-tolerance)
benches/              Criterion benches
scripts/              Cross-runtime tooling (e.g. profile.py orchestrator)
docs/                 mkdocs-material source (index, actors-and-agentic-computing,
                      parity, python, profiler, dashboard, observability)
docs/reports/         profiler baselines (markdown + json)
xtask/                Cargo xtask (upstream sync, parity report, profile)
akka.net/             Upstream clone — gitignored, created on demand by
                      scripts/sync-upstream.py (never committed)
```

## Learn more

- [`docs/actors-and-agentic-computing.md`](docs/actors-and-agentic-computing.md)
  — why native Akka-style actors align with agentic systems and
  distributed execution.
- [`docs/index.md`](docs/index.md) — project overview.
- [`docs/python.md`](docs/python.md) — Python bindings + GIL tuning.
- [`docs/parity.md`](docs/parity.md) — generated crate-by-crate status.
- [`PORTING.md`](PORTING.md) — upstream Akka.NET tracking commits.
- [`PORTING_TODO.md`](PORTING_TODO.md) — phase progress checklist.
