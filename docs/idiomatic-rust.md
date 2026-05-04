# Idiomatic Rust principles

These are the non-negotiable invariants for atomr. They exist because
the actor model has been transliterated from many languages over the
years and it would be easy to ship Rust shapes that *compile* but
fight the borrow checker, blow up the type-erasure budget, and bury
runtime panics where the type system would have caught them.

Every PR is reviewed against this list. The lint set in `Cargo.toml`
mechanically enforces the parts that can be caught by the compiler;
the rest is reviewer discipline.

The numbering matches [`full-port-plan.md`](full-port-plan.md) so
audit reports and PR descriptions can reference principle "P-7"
without ambiguity.

## P-1. No `Box<dyn Any>` in any public API

Rust's whole value-add for an actor runtime is compile-time message
contracts. The moment you erase a sender or a payload type, you have
to `downcast` on every reply, which means every reply path has a
runtime branch that can only fail. The runtime models senders as a
typed enum:

```rust
pub enum Sender {
    Local(UntypedActorRef),
    Remote(RemoteRef),
    None,
}
```

`UntypedActorRef` is a sealed enum over the local-ref shapes (it is
*not* `Box<dyn Any>`); `RemoteRef` is a typed handle that knows the
target `ActorPath`. Replies flow through `Sender::tell_serialized`,
which serializes via the registry chosen at handshake.

## P-2. No `Any::downcast<T>()` in hot paths

If you need to look something up by type, use:

- a typed key like `ExtensionId<E>(PhantomData<E>)`, or
- a sealed enum (`Serializer::{Bincode, Json, System(SystemSerializer)}`).

`TypeId`-keyed maps are acceptable in cold-path registries
(extension registration, props lookup at spawn) but never in the
mailbox pump or gossip loop.

## P-3. Actor state, not `RwLock<HashMap>`

If two tasks share mutable state, model the state as an actor and
let messages serialize access. The actor is the synchronization
primitive. A workspace-wide `RwLock<HashMap<K, V>>` over
"replicator state" or "shard allocation" or "endpoint manager" is
the kind of thing that always grows into a contention hot spot.

Acceptable uses of `RwLock` / `Mutex`:

- Read-mostly leaf caches (e.g. an LRU of compiled regex patterns).
- Lazily-initialized `OnceLock` / `OnceCell` registries.
- Internal scratch space inside a single actor's task that doesn't
  cross `await` points.

Unacceptable: any subsystem coordinator that has its own logical
identity (replicator, mediator, shard coordinator, cluster daemon,
endpoint manager, service container). Those are actors.

## P-4. No `panic!`, `unwrap()`, `expect()`, `unimplemented!()`, or
       `todo!()` in library code

Library crates compile under:

```toml
[lints.clippy]
unwrap_used     = "deny"
expect_used     = "deny"
panic           = "deny"
todo            = "deny"
unimplemented   = "deny"
```

(Tests can opt out via `#[allow(...)]` at the test-module level.)

Errors travel via `thiserror` enums per crate. Public error enums
carry `#[non_exhaustive]` (P-11).

## P-5. Async-first; never block the runtime

- `tokio::spawn_blocking` for CPU-bound and filesystem work.
- `tokio::time::sleep` — never `std::thread::sleep`.
- `tokio::sync::{Mutex, RwLock}` only when state must be held
  *across* an `await`. Otherwise `parking_lot::Mutex` for in-task
  scratch.
- No `block_on` inside a Tokio worker. The dashboard's
  `tokio::task::block_in_place` boundary is the one exception, and
  it is documented at the call site.

## P-6. Persistent / immutable structures for snapshots

Hot snapshot paths (gossip state, replicator state, sharding
allocation table) use `imbl::HashMap` / `imbl::Vector`, or
`arc-swap::ArcSwap<Arc<State>>` for whole-snapshot swap.

The pattern:

```rust
let snapshot: Arc<State> = self.state.load_full();
// read freely from snapshot, no lock held
```

Mutation produces a new `Arc<State>` and CAS-swaps it in. This
avoids a "lock held while iterating" anti-pattern that .NET
codebases get away with via `ImmutableDictionary` but Rust would
naturally route through `RwLock`.

## P-7. Type-state for actor lifecycle

`Context<A, Phase>` is phantom-typed:

```rust
pub struct Context<A: Actor, P: Phase = Running> { /* … */ }

pub struct Starting; impl Phase for Starting {}
pub struct Running;  impl Phase for Running  {}
pub struct Stopping; impl Phase for Stopping {}
```

Phase-only APIs are `impl Context<A, Phase>` blocks:

- `become_` and `unbecome` only on `Running`.
- `unstash_all` only on `Running`.
- `set_receive_timeout` only on `Running`.
- `spawn_child` only on `Starting | Running`.

This makes "called `become` from `pre_start`" a compile error.

## P-8. Compile-time supervision contracts

The parent declares which child types it can supervise:

```rust
impl SupervisorOf<Worker> for Boss {
    type ChildError = WorkerError;
    fn decide(&self, err: &WorkerError) -> Directive { /* … */ }
}

ctx.spawn_child::<Worker>(props!(Worker { … }), "worker-1")?;
//   ^ requires Boss: SupervisorOf<Worker>
```

The blanket fallback `impl<P, C> SupervisorOf<C> for P { … }` uses
`OneForOneStrategy::default()` so existing call sites keep working;
opting in to a custom decider is a typed override.

## P-9. `tracing` everywhere

No `println!` / `eprintln!` / `dbg!` in library code. Logging goes
through the `tracing` crate with structured fields:

```rust
tracing::info!(
    actor.path = %ctx.path(),
    actor.uid  = ctx.uid(),
    system     = %ctx.system().name(),
    "actor started",
);
```

The audit task counts stray `println!` sites; CI fails on
regression.

## P-10. Sealed traits over open inheritance

Traits that are part of the framework contract (`Actor`, `Message`,
`Serializer`, `Transport`, `Journal`, `SnapshotStore`, `CrdtType`)
are sealed via the standard pattern:

```rust
mod private {
    pub trait Sealed {}
}
pub trait Actor: private::Sealed + Send + 'static { /* … */ }
```

Downstream extends through composition (a struct that contains an
`Actor` plus its own state) or through a trait we explicitly mark
as un-sealed.

## P-11. `#[non_exhaustive]` on every public enum

```rust
#[non_exhaustive]
pub enum JournalError {
    Backend(BackendError),
    Serialization(SerializationError),
    NotFound,
}
```

Adding a variant is a non-breaking change. Pattern matches in
downstream code must include `_ => …`.

## P-12. Feature flags for optional integrations

- Persistence backends (`sql`, `redis`, `mongodb`, `cassandra`,
  `aws`, `azure`).
- Serializers (`bincode`, `json`, `messagepack`, …).
- Transports (`tcp`, `tls`).
- Telemetry exporters (`metrics-prometheus`, `metrics-otel`,
  `otel-otlp-grpc`, `otel-otlp-http`, `otel-stdout`).

Each crate's `default-features = false` produces a lean build for
embedded / WASM use. CI builds the no-default-features matrix on
every PR.

---

## When to deviate

Sometimes a principle costs more than it saves. The escape hatch
is documented per-call-site:

```rust
// Justified deviation from P-3:
// `routing-table` is read on every message and never mutated after
// `pre_start`. ArcSwap snapshot rebuild dominates; a plain
// RwLock is 30 ns lighter per read. See bench
// `routing/throughput-2026-05`.
let table: RwLock<RoutingTable> = RwLock::new(initial);
```

The audit task scans for these comments and tracks them in
`docs/reports/audit-*.json` so reviewers can see drift over time.

## Why these matter for unified compute

The granularity argument in
[`actors-and-agentic-computing.md`](actors-and-agentic-computing.md)
only pays off if per-message overhead stays low. The invariants
above are how we keep it low:

- P-1 / P-2 keep the dispatch path branch-free and allocation-free
  on the hot loop, so adding a new dispatcher backend (e.g. a CUDA
  stream) doesn't have to inherit a chain of `downcast` checks.
- P-3 keeps coordinator state inside actors, which means the same
  coordination protocol works whether the actors run on host
  threads or accelerator-resident dispatchers.
- P-6 keeps snapshots immutable and cheap to share, so a host-side
  reader and an accelerator-side dispatcher can hold concurrent
  views without a lock.
- P-12 keeps the dependency surface lean. A future `gpu`
  feature flag will gate accelerator dispatchers; the rest of the
  workspace continues to build slim.
