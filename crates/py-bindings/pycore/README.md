# rustakka-pycore

PyO3 bindings for the `rustakka` workspace. Compiles to
`rustakka._native`, a single cdylib that hosts every subsystem as a
Python submodule.

See [`../README.md`](../README.md) for the broader py-bindings layout
and [`../../../python/README.md`](../../../python/README.md) for the
user-facing Python API.

## Hot path

The centrepiece is [`py_actor.rs`](src/py_actor.rs) — a Rust `Actor`
implementation whose `handle` forwards every message to a Python
instance via its assigned `InterpreterInstance`. Rust's mailbox stays
lock-free regardless of GIL contention: the Rust side hands the task
to an interpreter worker over an `mpsc` channel, then awaits a
`oneshot` reply. Metrics record `gil_hold_ns_total` and flag handlers
that exceed `max_handler_ms`.

## Building

```bash
maturin develop --release       # editable install into venv
maturin build   --release       # produce a wheel
cargo check -p rustakka-pycore  # type-check only (no Python needed at link time thanks to abi3)
```

## Feature flags

- `extension-module` (default) — tells PyO3 to skip linking against
  libpython. This is what maturin expects.

## Python requirements

- CPython 3.10+ via abi3.
- 3.12+ unlocks `python-subinterpreter-pool`.
- 3.13t unlocks `python-nogil`.
