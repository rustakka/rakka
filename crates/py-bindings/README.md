# crates/py-bindings

PyO3 bridge crates. All bindings currently compile into a single cdylib
(`atomr._native`) produced by [`pycore`](pycore/). The sibling
directories are structural placeholders so individual wheels can be
split out later without renaming the Python facade.

## Contents

| Crate | Purpose |
|-------|---------|
| [`pycore`](pycore/) | **The actual bindings crate.** Exposes every subsystem as a submodule of `atomr._native`. |
| `pytestkit` / `pyremote` / `pycluster` / `pycluster-tools` / `pycluster-sharding` / `pyddata` / `pypersistence` / `pystreams` / `pycoordination` / `pydiscovery` / `pydi` / `pyhosting` | Placeholder crates that mirror the Rust workspace layout. `src/lib.rs` is empty on purpose. |

## Why a single cdylib?

- One `maturin build` produces one wheel, making CI simpler.
- PyO3 extension modules don't re-export cleanly across `cdylib`s, so
  cross-module types (e.g. passing an `ActorRef` from `pycore` to
  `pycluster`) would require re-plumbing every class.
- Splitting into per-crate wheels is a downstream packaging decision
  and can happen later without touching the Python facade (`python/atomr/`).

## Building

See [`../../python/README.md`](../../python/README.md) for the user
flow. For bindings developers:

```bash
# Fast iteration cycle
maturin develop --release

# Re-run Python tests after a Rust change
pytest python/tests -v

# Type-check / clippy the bindings crate only
cargo check  -p atomr-pycore
cargo clippy -p atomr-pycore --no-deps
```

## Directory layout of `pycore`

```
crates/py-bindings/pycore/src/
├── lib.rs                  PyO3 #[pymodule] entry point
├── runtime.rs              shared Tokio runtime (bridged to asyncio)
├── errors.rs               exception hierarchy
├── config.rs               Config
├── actor_system.rs         ActorSystem, interpreter registry
├── actor_ref.rs            ActorRef (tell/ask/ask_blocking)
├── context.rs              Context shim
├── props.rs                Props builder
├── dispatcher.rs           dispatcher-name → InterpreterKind
├── interpreter.rs          InterpreterInstance, Quota, Metrics, workers
├── compat.rs               C-extension compatibility registry
├── metrics.rs              aggregate metrics helper
├── py_actor.rs             the Rust Actor that forwards to Python
└── ext_*.rs                one module per subsystem submodule
```

## Conventions

- Every extension submodule registers itself inside
  `_native::_native()` via a `pub fn register(py, m)` function.
- Runtime-bound APIs (spawn, ask, journal write, lease acquire) enter
  the shared Tokio runtime via `runtime::runtime()` and release the GIL
  with `py.allow_threads`.
- Errors returned to Python use `crate::errors::map(e)` which wraps
  anything `Display` as `AtomrError` or its subclasses.
