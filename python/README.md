# rakka — Python bindings

First-class Python bindings for the Rust [`rakka`](../) actor
framework. Write actors in Python, run them under the Rust scheduler
with supervision, clustering, persistence, and streams — and pick a
dispatcher that matches your workload's GIL tolerance.

## Install (development)

```bash
python -m venv .venv && source .venv/bin/activate
pip install maturin pytest pytest-asyncio msgpack
maturin develop --release
```

If your host lacks Python dev headers and you can't `sudo apt install
python3-dev`, use the bundled helper instead:

```bash
source scripts/dev-env.sh        # creates .venv, installs deps, exports PYO3_CONFIG_FILE
maturin develop --release
```

See `.cargo/pyo3-config.txt.example` for the custom PyO3 config
template. Nothing in `.venv/`, `.venv-build/`, or `.cargo/pyo3-config.txt`
is committed — each developer builds their own.

Supported Python: 3.10+ (abi3). 3.12 enables subinterpreters;
3.13 free-threaded (PEP 703) enables the `python-nogil` dispatcher.

## Hello, actor

```python
from rakka import Actor, ActorSystem, props

class Greeter(Actor):
    async def handle(self, ctx, msg):
        return f"hello, {msg}"

system = ActorSystem.create_blocking("app")
ref = system.actor_of(props(Greeter), "greeter")
print(ref.ask_blocking("world", timeout=5.0))
system.terminate_blocking()
```

## Package layout

```
python/
├── rakka/                Python facade — import this
│   ├── __init__.py          re-exports Actor / ActorSystem / ...
│   ├── actor.py             Actor base class
│   ├── system.py            ActorSystem, Props, ActorRef, props()
│   ├── errors.py            RakkaError, InterpreterOverloaded, ...
│   ├── interpreter.py       InterpreterQuota + capability probes
│   ├── compat.py            C-extension compatibility registry
│   ├── testkit.py           TestKit, TestProbe, pytest fixture
│   ├── cluster.py           Member, MembershipState, VectorClock
│   ├── cluster_tools.py     DistributedPubSub
│   ├── cluster_sharding.py  ShardRegion + Python extractors
│   ├── ddata.py             GCounter, PNCounter, GSet, ORSet
│   ├── persistence.py       InMemoryJournal
│   ├── streams.py           map_reduce helper
│   ├── coordination.py      InMemoryLease
│   ├── discovery.py         StaticDiscovery
│   ├── di.py                ServiceContainer
│   └── hosting.py           Builder, ActorSystemBuilder
├── tests/                   pytest suite (23 tests)
└── examples/                runnable examples
    ├── pingpong.py
    ├── ml_inference.py      subinterpreter pool demo
    └── persistence_counter.py
```

## GIL dispatchers

| dispatcher | parallelism | best for |
|---|---|---|
| `python-pinned` (default) | 1 interpreter, 1 thread | low-latency, I/O-bound |
| `python-subinterpreter-pool` | N interpreters, N threads, N GILs | CPU-bound Python, subinterpreter-safe C ext |
| `python-nogil` | 1 interpreter, no GIL (3.13t) | CPU-bound on free-threaded Python |
| `python-subprocess` | N processes | untrusted handlers, hard RSS caps |

Capability probes:

```python
import rakka
rakka.subinterpreters_supported()   # True on CPython 3.12+
rakka.nogil_supported()             # True on CPython 3.13t
```

### Quotas per interpreter pool

```python
from rakka import InterpreterQuota

system.configure_interpreter(
    "ml-inference",
    "python-subinterpreter-pool",
    count=4,
    quota=InterpreterQuota(
        max_actors=32,
        max_mailbox_total=10_000,
        memory_soft_limit_bytes=2 * 1024**3,
        cpu_share=0.5,
        max_handler_ms=250,
        module_allowlist=["numpy", "torch", "rakka"],
        import_policy="eager",
    ),
)
```

### Metrics

```python
for pool in rakka._native.interpreter_metrics():
    print(pool["label"], pool["kind"], pool["messages_handled"])
```

Fields: `actors_hosted`, `messages_handled`, `gil_hold_ns_total`,
`mailbox_depth_total`, `handler_panics`, `long_handlers`.

### C-extension compatibility

Before spawning an interpreter pool the runtime consults a registry of
known C extensions. Baseline defaults ship for stdlib, `numpy`,
`msgpack`, `pydantic`, etc. Libraries or operators can declare their
own:

```python
import rakka
rakka.declare_compat(
    "my_fast_lib",
    subinterpreter_safe=True,
    nogil_safe=False,
    notes="verified against release 1.4",
)
```

## Profiling

A `rakka.profiler` sub-package mirrors the Rust `rakka-profiler`
binary:

```bash
python -m rakka.profiler --scenario all --format md
python -m rakka.profiler --scenario cpu --messages 5000 --format json -o cpu.json
```

It autoconfigures the fastest dispatcher per scenario (`python-nogil` →
`python-subinterpreter-pool` → `python-pinned` for CPU-bound,
`python-pinned` for latency-sensitive ones). For a side-by-side Rust +
Python table, run `python scripts/profile.py` from the repo root. Full
guide in [`../docs/profiler.md`](../docs/profiler.md).

## Testing

Use the `testkit` fixture:

```python
from rakka.testkit import testkit

def test_my_actor(testkit):
    probe = testkit.probe()
    # ... send via probe.ref_ and consume probe.messages() ...
```

Run the full suite:

```bash
pytest python/tests -v
```

Smoke benchmarks live in `python/tests/test_benchmarks.py` — they print
ask-per-second for `python-pinned` vs `python-subinterpreter-pool`.

## More

See [`../docs/python.md`](../docs/python.md) for the full GIL tuning
guide and architectural background.
