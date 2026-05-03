---
name: rakka-python
description: Use when working with rakka's Python bindings — authoring Python actors, choosing a GIL/dispatcher strategy (python-pinned / python-subinterpreter-pool / python-nogil / python-subprocess), using ask/tell from async or sync code, or declaring C-extension subinterpreter compatibility. Triggers when `pip install rakka`, `from rakka import …`, or any Python file calling `ActorSystem`, `Props`, `Actor`, etc.
---

# rakka from Python

The Python facade (`pip install rakka`) gives you the rakka actor
model with the Rust scheduler, mailbox, supervision, clustering,
persistence, and streams machinery underneath. Heavy lifting happens
in `rakka._native` (built with PyO3 + maturin); the Python package is
ergonomic wrappers and the `Actor` base class.

Supported Python: 3.10+ (abi3). 3.12 enables subinterpreters; 3.13t
(PEP 703 free-threaded) enables the `python-nogil` dispatcher.

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

There is also an async variant — `await ActorSystem.create("app")`,
`await ref.ask("world", timeout=5.0)` — for use inside `asyncio`
applications. Don't mix the blocking and async APIs in the same coroutine.

## Module layout

The Python package mirrors the Rust workspace:

| Rust crate                     | Python module              |
|--------------------------------|----------------------------|
| `rakka-core`                   | `rakka` (`Actor`, `Props`, `ActorSystem`) |
| `rakka-testkit`                | `rakka.testkit`            |
| `rakka-cluster`                | `rakka.cluster`            |
| `rakka-cluster-tools`          | `rakka.cluster_tools`      |
| `rakka-cluster-sharding`       | `rakka.cluster_sharding`   |
| `rakka-distributed-data`       | `rakka.ddata`              |
| `rakka-persistence`            | `rakka.persistence`        |
| `rakka-streams`                | `rakka.streams`            |
| `rakka-coordination`           | `rakka.coordination`       |
| `rakka-discovery`              | `rakka.discovery`          |
| `rakka-di`                     | `rakka.di`                 |
| `rakka-hosting`                | `rakka.hosting`            |

## Choosing a dispatcher / GIL strategy

This is the most important decision in any non-trivial rakka-Python
deployment. There are four shapes; pick one per workload.

### `python-pinned` (default)

One interpreter, one OS thread, one GIL. Best latency for small actor
graphs where handlers are short and the bulk of the work is I/O or is
delegated to Rust.

```python
system.configure_interpreter("default", "python-pinned")
```

### `python-subinterpreter-pool` (recommended for CPU-bound)

N subinterpreters on N OS threads (PEP 684). Each interpreter has its
own GIL, so CPU-bound Python handlers actually run in parallel —
provided the C extensions you import are subinterpreter-safe.

```python
from rakka import InterpreterQuota

system.configure_interpreter(
    "ml-inference",
    "python-subinterpreter-pool",
    quota=InterpreterQuota(n=4),
)
```

Validate via `subinterpreters_supported()`. Declare third-party C
extensions via `rakka.declare_compat(...)`; see
`rakka.compat.compat_list()` for what's already known.

### `python-nogil` (PEP 703)

One interpreter, no GIL. Requires Python 3.13t (the free-threaded
build). Validate via `nogil_supported()`. Best for highly concurrent
Python handlers that share state, when subinterpreter isolation
breaks something you need.

### `python-subprocess`

One subprocess per dispatch. Heaviest; use only when you must run
incompatible C extensions or untrusted code with an isolation boundary.

## Ask vs tell, sync vs async

| Method | Returns | Use from |
|---|---|---|
| `ref.tell(msg)` | `None` | anywhere |
| `await ref.ask(msg, timeout=…)` | reply | `async def` |
| `ref.ask_blocking(msg, timeout=…)` | reply | sync code, `__main__`, REPL |

`tell` is fire-and-forget. `ask` allocates a oneshot and waits for the
handler's return value (or an explicit reply). Don't `ask_blocking`
from inside an `async` actor — it deadlocks the dispatcher.

## C-extension compatibility

Subinterpreters and free-threaded Python both stress C extensions in
ways that the GIL'd single-interpreter world did not. Before deploying
on `python-subinterpreter-pool` or `python-nogil`:

- Run `rakka.compat.compat_list()` to see which extensions are flagged.
- Declare your own with `rakka.declare_compat("my_pkg", flags=...)`.
- For a third-party package that's not yet safe, fall back to
  `python-pinned` for that dispatcher.

## Errors

The Python facade exposes typed exceptions:

| Exception | Raised when |
|---|---|
| `RakkaError` | base class — catch this to net everything |
| `ActorSystemError` | `ActorSystem` startup/shutdown failure |
| `SpawnError` | `actor_of` failed (name conflict, factory raised, …) |
| `AskError` | `ask` timed out or the actor was stopped |
| `InterpreterOverloaded` | subinterpreter pool saturated |
| `InterpreterCompatError` | extension declared incompatible with strategy |

## Testing

`rakka.testkit` mirrors `rakka-testkit`:

```python
from rakka.testkit import TestKit

kit = TestKit()
probe = kit.probe("p")
ref.tell("hi")
assert probe.expect_msg(timeout=2.0) == "hi"
```

## Profiler

The cross-runtime profiler is callable from Python:

```bash
python -m rakka.profiler --scenario all --format md
```

The output schema matches the Rust profiler's, so Rust and Python
runs are directly comparable. See `docs/profiler.md`.

## Canonical references

- `docs/python.md` — bindings overview + GIL strategy guide
- `docs/profiler.md` — cross-runtime profiler
- `python/rakka/__init__.py` — public API surface
- `python/rakka/actor.py` — `Actor` base class
- `python/rakka/system.py` — `ActorSystem`, `Props`, `ActorRef`, `Context`
- `python/rakka/interpreter.py` — interpreter quotas, capability checks
- `python/rakka/compat.py` — C-extension compatibility registry
- `python/tests/` — integration tests (good usage examples)

## Common mistakes

- **Mixing `*_blocking` and `await` APIs in the same coroutine.** Pick one.
- **Calling `ask_blocking` from inside an actor handler.** Deadlocks
  the dispatcher; use `ask` (and `await` it) instead.
- **Assuming subinterpreter parallelism without checking
  `subinterpreters_supported()`.** Pre-3.12 builds silently fall back.
- **Skipping `declare_compat` for in-house C extensions.** They will
  appear unsafe and the dispatcher will refuse to load them in the
  pool.
- **Letting an actor handler raise without translating to an
  application error.** Unhandled Python exceptions trigger supervisor
  restart, same as Rust panics.
