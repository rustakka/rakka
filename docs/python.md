# atomr Python bindings

`atomr` ships first-class Python bindings that let you author actors in
Python while keeping the Rust scheduler, mailbox, supervision, clustering,
persistence, and streams machinery below. The native extension is built
with [PyO3] + [maturin]; the Python facade lives in `python/atomr/`.

## Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install maturin
maturin develop --release            # development install
# or:
maturin build --release              # produce a wheel
```

Supported Python: 3.10+ (abi3). 3.12 enables subinterpreters; 3.13t
(PEP 703 free-threaded) enables the `python-nogil` dispatcher.

## Hello, actor

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

## Module layout

The Python package mirrors the Rust workspace:

| Rust crate                     | Python module                |
|--------------------------------|------------------------------|
| `atomr-core`                | `atomr` (Actor, Props…)   |
| `atomr-testkit`             | `atomr.testkit`           |
| `atomr-cluster`             | `atomr.cluster`           |
| `atomr-cluster-tools`       | `atomr.cluster_tools`     |
| `atomr-cluster-sharding`    | `atomr.cluster_sharding`  |
| `atomr-distributed-data`    | `atomr.ddata`             |
| `atomr-persistence`         | `atomr.persistence`       |
| `atomr-streams`             | `atomr.streams`           |
| `atomr-coordination`        | `atomr.coordination`      |
| `atomr-discovery`           | `atomr.discovery`         |
| `atomr-di`                  | `atomr.di`                |
| `atomr-hosting`             | `atomr.hosting`           |

## GIL tuning guide

The framework offers four dispatcher shapes. Pick one per workload.

### `python-pinned` (default)

One interpreter, one OS thread, one GIL. Best latency for small actor
graphs where handlers are short and the bulk of the work is I/O or
delegated to Rust.

```python
system.configure_interpreter("default", "python-pinned")
```

### `python-subinterpreter-pool` (recommended for CPU-bound)

N subinterpreters on N OS threads. Each interpreter has its own GIL, so
CPU-bound Python handlers actually run in parallel (assuming the C
extensions you import are subinterpreter-safe; see the compatibility
registry below).

```python
from atomr import InterpreterQuota

system.configure_interpreter(
    "ml-inference",
    "python-subinterpreter-pool",
    count=4,
    quota=InterpreterQuota(
        max_actors=32,
        max_handler_ms=250,
        memory_soft_limit_bytes=2 * 1024**3,
        module_allowlist=["numpy", "torch", "atomr"],
        import_policy="eager",
    ),
)
```

### `python-nogil`

Free-threaded CPython 3.13+ (PEP 703). Single interpreter, but no GIL;
`count` becomes the number of OS worker threads. Only useful if your
deployment runs a no-GIL CPython build — check with
`atomr.nogil_supported()`.

### `python-subprocess`

Each interpreter runs in a separate OS process. Strongest isolation —
used for untrusted handlers or hard memory caps.

### Quotas

`InterpreterQuota` exposes the same knobs on every dispatcher:

| knob                       | purpose                                   |
|----------------------------|-------------------------------------------|
| `max_actors`               | reject new spawns when the pool is full   |
| `max_mailbox_total`        | back-pressure: reject `tell` past budget  |
| `memory_soft_limit_bytes`  | log/restart when RSS exceeds the budget   |
| `cpu_share`                | advisory scheduler hint                   |
| `max_handler_ms`           | flag long-running handlers in metrics     |
| `module_allowlist/denylist`| enforced by the C-ext compat gate at boot |
| `import_policy`            | `lazy` (default) or `eager` warm-up       |

### Metrics

```python
for pool in atomr._native.interpreter_metrics():
    print(pool["label"], pool["kind"], pool["messages_handled"])
```

Fields: `actors_hosted`, `messages_handled`, `gil_hold_ns_total`,
`mailbox_depth_total`, `handler_panics`, `long_handlers`.

### C-extension compatibility registry

Before spawning an interpreter pool we consult the compatibility
registry. Defaults ship for stdlib, `numpy`, `msgpack`, `pydantic`, etc.
Operators or library authors can declare their own:

```python
import atomr

atomr.declare_compat(
    "my_fast_lib",
    subinterpreter_safe=True,
    nogil_safe=False,
    notes="verified against release 1.4",
)
```

Handlers that try to import a module flagged as unsafe for the
selected dispatcher raise `atomr.InterpreterCompatError` — see
`atomr.compat_list()` for the current registry contents.

## Testing

```python
from atomr.testkit import testkit  # pytest fixture

def test_my_actor(testkit):
    probe = testkit.probe()
    # ... interact with your actor via probe.ref_ and probe.messages() ...
```

## Examples

- `python/examples/pingpong.py` — smoke test + throughput.
- `python/examples/ml_inference.py` — subinterpreter pool.
- `python/examples/persistence_counter.py` — Rust journal from Python.

## API surface summary

```python
atomr.Actor                       # subclass and implement async def handle
atomr.ActorSystem                 # .create / .create_blocking / .actor_of
atomr.Props, atomr.props()     # (factory, dispatcher, interpreter_role, mailbox)
atomr.ActorRef                    # .tell / .ask (asyncio) / .ask_blocking
atomr.Context                     # .self_ref, .path
atomr.Config                      # .from_toml / .empty

atomr.InterpreterQuota            # per-pool resource + import policy
atomr.subinterpreters_supported() # CPython >= 3.12
atomr.nogil_supported()           # CPython 3.13t (free-threaded)
atomr.declare_compat / compat_flags / compat_list

atomr.testkit.TestKit / TestProbe / testkit (pytest fixture)
atomr.cluster.Member / MembershipState / VectorClock
atomr.cluster_tools.DistributedPubSub
atomr.cluster_sharding.ShardRegion
atomr.ddata.GCounter / PNCounter / GSet / ORSet
atomr.persistence.InMemoryJournal
atomr.streams.map_reduce
atomr.coordination.InMemoryLease
atomr.discovery.StaticDiscovery
atomr.di.ServiceContainer
atomr.hosting.Builder / ActorSystemBuilder
```

## Known limitations

- Process-local only today. `pyremote` + pluggable Python codecs
  (msgpack / pickle / JSON) are tracked under Phase P3 in
  `PORTING_TODO.md` and are deferred until native remote has crossed a
  process boundary.
- `Context.spawn_child` / `watch` / `set_receive_timeout` are not yet
  exposed to Python; use the Rust API or ask me to prioritize them.
- Subinterpreters on CPython 3.12 still share many CPython singletons;
  treat `python-subinterpreter-pool` as a strong scalability lever but
  still audit heavy C extensions with `compat_flags`.

[PyO3]: https://pyo3.rs
[maturin]: https://www.maturin.rs
