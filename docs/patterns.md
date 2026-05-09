# Patterns

`atomr-patterns` is a DDD/CQRS pattern library that layers
opinionated, ready-to-instantiate scaffolding on top of the runtime
primitives shipped by `atomr-core`, `atomr-persistence`,
`atomr-persistence-query`, and `atomr-streams`.

The runtime primitives are deliberately unopinionated — they tell you
*how* to write event-sourced actors, persist events, or fold events
into projections, but they leave the wiring to you. Most teams end up
re-implementing the same plumbing: a gateway actor that routes
commands by aggregate id, a per-id entity actor that recovers from the
journal, a tail-following reader that updates a projection, a saga
that watches events and dispatches commands, a publisher that
guarantees at-least-once delivery to a downstream system.

This crate gives you those patterns as fluent builders. You bring the
*domain types* (the aggregate, the commands, the events, the
projections); the framework provides the wiring, the supervision, the
extension points, and a stable handle surface.

## When to reach for it

You want this crate when you have:

- A domain you'd naturally describe in DDD terms — there are
  *aggregates* (consistency boundaries), *commands* that change them,
  *events* recording what happened, and *read models* serving queries.
- Multiple aggregates whose behavior you'd like to coordinate without
  point-to-point coupling — that's a [saga](#saga-pattern).
- A need to reliably republish persisted events to a downstream system
  (Kafka, SNS, a webhook) — that's the [outbox](#outbox-pattern).
- Two bounded contexts that exchange messages but speak different
  vocabularies — that's the [anti-corruption layer](#anti-corruption-layer).

You do **not** want this crate when:

- Your write side is a stateless RPC handler.
- You don't want event sourcing — `atomr-persistence::Eventsourced`
  is the foundation here, and aggregates are required to implement it.
- You only need pub/sub between actors in the same process — use
  `atomr-cluster-tools::DistributedPubSub` directly.

## Mental model

```
                       ┌─────────────────────────────────────────┐
                       │   crates/atomr-patterns                 │
                       │                                         │
                       │   ┌───────────────────────────────────┐ │
                       │   │ Repository::send(cmd)             │ │
   user code ──────────┼──▶│                                   │ │
                       │   │       ↓                           │ │
                       │   │ CommandGateway actor              │ │
                       │   │ ├─ on_command(...) interceptors   │ │
                       │   │ ├─ aggregate.command_to_events    │ │
                       │   │ ├─ journal.write_messages         │ │
                       │   │ ├─ aggregate.apply_event          │ │
                       │   │ ├─ check_invariants               │ │
                       │   │ ├─ on_event(...) listeners        │ │
                       │   │ └─ tap_events ──▶ tokio mpsc      │ │
                       │   │                                   │ │
                       │   │              (per-pid offset)     │ │
                       │   │   ReaderRunner ◀── ReadJournal    │ │
                       │   │       ↓                           │ │
                       │   │   ProjectionHandle::snapshot      │ │
                       │   └───────────────────────────────────┘ │
                       └─────────────────────────────────────────┘
                                       │
   atomr-persistence    Journal ◀──────┤
                        ReadJournal ◀──┤
                                       │
   atomr-streams        Source / Sink ◀┘
```

The crate is organized in layers:

1. **DDD vocabulary** (`ddd::`) — `Entity`, `ValueObject`, `Command`,
   `DomainEvent`, `AggregateRoot`, `Repository`. Pure traits; no
   actors, no streams. These are the words you use to describe your
   domain.
2. **Patterns** (`cqrs::`, `saga::`, `bus::`, `outbox::`, `acl::`) —
   each is a fluent builder that produces a [`Topology`] fragment.
3. **Materialization** — calling `topology.materialize(&system).await`
   spawns the pattern's actors under a single named root in the user
   guardian (`/user/<name>`) so the [dashboard](dashboard.md)
   topology view renders the pattern as a coherent subtree, and
   returns strongly-typed handles back to you.

## Quick start

A single counter aggregate with a totals projection. Run with
`cargo test -p atomr-patterns --test cqrs_counter`.

```rust
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use atomr_config::Config;
use atomr_core::actor::ActorSystem;
use atomr_patterns::prelude::*;
use atomr_persistence::{Eventsourced, InMemoryJournal};
use atomr_persistence_query_inmemory::read_journal;

// 1. Domain types ----------------------------------------------------

#[derive(Debug, thiserror::Error)]
enum CounterErr { #[error("would underflow")] Underflow }

#[derive(Default, Debug)]
struct CounterState { n: i64 }

#[derive(Clone, Debug)]
enum CounterEvent { Adjusted(i64) }

impl DomainEvent for CounterEvent {
    fn tags(&self) -> Vec<String> { vec!["counter".into()] }
}

#[derive(Debug)]
enum CounterCmd { Add(i64), Sub(i64) }

impl Command for CounterCmd {
    type AggregateId = String;
    fn aggregate_id(&self) -> String { "the-counter".into() }
}

struct Counter { id: String }

#[async_trait]
impl Eventsourced for Counter {
    type Command = CounterCmd;
    type Event = CounterEvent;
    type State = CounterState;
    type Error = CounterErr;

    fn persistence_id(&self) -> String { self.id.clone() }

    fn command_to_events(&self, state: &CounterState, cmd: CounterCmd)
        -> Result<Vec<CounterEvent>, CounterErr>
    {
        let delta = match cmd { CounterCmd::Add(n) => n, CounterCmd::Sub(n) => -n };
        if state.n + delta < 0 { return Err(CounterErr::Underflow); }
        Ok(vec![CounterEvent::Adjusted(delta)])
    }

    fn apply_event(state: &mut CounterState, e: &CounterEvent) {
        match e { CounterEvent::Adjusted(d) => state.n += d }
    }

    fn encode_event(e: &CounterEvent) -> Result<Vec<u8>, String> {
        match e { CounterEvent::Adjusted(d) => Ok(d.to_le_bytes().to_vec()) }
    }
    fn decode_event(b: &[u8]) -> Result<CounterEvent, String> {
        let arr: [u8; 8] = b.try_into().map_err(|_| "bad len".to_string())?;
        Ok(CounterEvent::Adjusted(i64::from_le_bytes(arr)))
    }
}

impl AggregateRoot for Counter {
    type Id = String;
    fn aggregate_id(&self) -> &Self::Id { &self.id }
}

// 2. Read model -------------------------------------------------------

#[derive(Default)]
struct Totals { total: i64, n: u64 }

struct TotalsReader;

#[async_trait]
impl Reader for TotalsReader {
    type Event = CounterEvent;
    type Projection = Totals;
    type Error = std::io::Error;

    fn name(&self) -> &str { "totals" }
    fn tag(&self)  -> Option<String> { Some("counter".into()) }
    fn decode(b: &[u8]) -> Result<Self::Event, String> { Counter::decode_event(b) }

    async fn apply(&mut self, p: &mut Totals, e: CounterEvent)
        -> Result<(), Self::Error>
    {
        match e { CounterEvent::Adjusted(d) => { p.total += d; p.n += 1; } }
        Ok(())
    }
}

// 3. Wire it up -------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let system  = ActorSystem::create("demo", Config::reference()).await?;
    let journal = Arc::new(InMemoryJournal::default());
    let rj      = Arc::new(read_journal(journal.clone()));

    let (builder, totals) = CqrsPattern::<Counter>::builder(journal.clone())
        .name("counter-cqrs")
        .factory(|id| Counter { id })
        .read_journal(rj)
        .poll_interval(Duration::from_millis(20))
        .with_reader(TotalsReader);

    let handles = builder.build()?.materialize(&system).await?;
    let repo    = handles.repository();

    repo.send(CounterCmd::Add(5)).await?;
    repo.send(CounterCmd::Add(10)).await?;
    repo.send(CounterCmd::Sub(3)).await?;

    // Wait for projection to catch up.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let snap = totals.read(|p| (p.total, p.n)).await;
    println!("totals = {:?}", snap);   // (12, 3)

    system.terminate().await;
    Ok(())
}
```

## DDD vocabulary

The `ddd` module defines the trait vocabulary that every pattern
speaks. None of these traits are runtime-aware — they describe
*shape*, not behavior.

### `Entity`

Anything with a stable identity. Two entity instances with the same
`Id` represent the same conceptual thing even if their other fields
differ.

```rust
pub trait Entity {
    type Id: Clone + Eq + Hash + Send + Sync + 'static;
    fn id(&self) -> &Self::Id;
}
```

### `ValueObject`

Marker trait for immutable, equality-by-value, identityless types
(money, ranges, addresses, codes). Carries no methods; bundles the
standard `Clone + Eq + Hash + Send + Sync + 'static` bounds.

### `Command`

An intent to change exactly one aggregate's state. Surfaces the
target id so the framework can route without dynamic dispatch.

```rust
pub trait Command: Send + 'static {
    type AggregateId: Clone + Eq + Hash + Send + Sync + 'static;
    fn aggregate_id(&self) -> Self::AggregateId;
}
```

### `DomainEvent`

A persisted fact. Two optional pieces of metadata the read side and
sagas need:

```rust
pub trait DomainEvent: Clone + Send + 'static {
    fn tags(&self) -> Vec<String> { Vec::new() }
    fn correlation_id(&self) -> Option<&str> { None }
}
```

`tags` populates `PersistentRepr.tags` so readers can subscribe via
`events_by_tag`. `correlation_id` is what sagas use to thread events
together (see [Saga pattern](#saga-pattern)).

### `AggregateRoot`

The transactional consistency boundary. Layers DDD identity +
invariants over `atomr_persistence::Eventsourced`:

```rust
pub trait AggregateRoot: Eventsourced {
    type Id: Clone + Eq + Hash + Send + Sync + 'static;

    fn aggregate_id(&self) -> &Self::Id;

    /// Optional post-apply invariant. Returning `Err` after a command
    /// has persisted surfaces `PatternError::Domain(_)` to the caller.
    fn check_invariants(_state: &Self::State) -> Result<(), Self::Error> { Ok(()) }
}
```

Every aggregate is event-sourced. The framework drives the same
`Eventsourced` trait you'd implement directly, so all the persistence
ergonomics — `recover`, `RecoveryPermitter`, snapshots — keep working.

The constraint that `<Self as Eventsourced>::Command: Command` and
`<Self as Eventsourced>::Event: DomainEvent` is *not* expressed as a
supertrait `where`-clause. Stable Rust propagates such clauses
awkwardly through every usage site. Patterns that consume the bounds
(e.g. `CqrsPattern`) re-state them at their builder / impl sites.

### `Repository`

The public dispatch surface for commands. Hides the routing (which
actor gets the command, whether it's local or sharded).

```rust
#[async_trait]
pub trait Repository: Send + Sync {
    type Aggregate: AggregateRoot;
    async fn send(&self, cmd: <Self::Aggregate as Eventsourced>::Command)
        -> Result<Vec<<Self::Aggregate as Eventsourced>::Event>,
                  PatternError<<Self::Aggregate as Eventsourced>::Error>>;
}
```

Implementations are produced by `CqrsPattern::builder().build().materialize()`.
Users don't normally implement this trait by hand.

## CQRS pattern

`CqrsPattern<A>` is the workhorse. It wires up four moving parts:

1. A **`CommandGateway`** actor that owns one `AggregateRoot`
   instance per aggregate id, persists events to the configured
   `Journal`, and applies them to in-memory state.
2. A **`Repository`** handle callers use to dispatch commands.
3. Zero or more **readers** — async tasks that follow the
   `ReadJournal`, decode events, and fold them into projection state.
4. **Extension hooks** — pre-handler interceptors, post-persist event
   listeners, and async event taps.

### Builder

```rust
let (builder, totals_handle) = CqrsPattern::<Counter>::builder(journal.clone())
    .name("counter-cqrs")              // user-guardian name
    .factory(|id| Counter { id })      // construct A from Id
    .read_journal(rj)                  // required iff readers are added
    .recovery_permits(8)               // concurrent-recovery cap
    .poll_interval(Duration::from_millis(50))
    .repository_timeout(Duration::from_secs(5))
    .writer_uuid("svc-1")              // stamped onto every PersistentRepr
    .on_command(|cmd| {                // interceptor (named slot)
        if denied(cmd) { Err(PatternError::Intercepted("denied".into())) }
        else { Ok(()) }
    })
    .on_event(|ev: &CounterEvent| {    // sync post-persist listener
        metrics::record(ev);
    })
    .tap_events(tap_tx)                // async post-persist channel
    .with_reader(TotalsReader);        // returns a ProjectionHandle

let topology = builder.build()?;        // CqrsTopology<A, J>
let handles  = topology.materialize(&system).await?;
let repo     = handles.repository();
```

### Command lifecycle

When `repo.send(cmd).await` is called, the gateway runs each step
in order:

| Step                      | Failure surface                            |
|---------------------------|---------------------------------------------|
| `on_command` interceptors | `PatternError::Intercepted` (or any variant the closure constructs) |
| Pull / create entity      | `PatternError::Invariant("recovery permit denied")` |
| Lazy `Eventsourced::recover` from journal | `PatternError::{Journal,Codec,Domain}` |
| `command_to_events` (validation lives here) | `PatternError::Domain(E)` |
| Encode events             | `PatternError::Codec(String)` |
| `journal.write_messages` (atomic per command) | `PatternError::Journal(_)` (state rollback) |
| Apply events to state     | infallible |
| `check_invariants(&state)` (post-condition) | `PatternError::Domain(E)` |
| `on_event` listeners + event taps | side effects only |
| Reply with `Vec<Event>` to caller | — |

Two important guarantees:

- **Sequence rollback.** If the journal write fails, the entity's
  in-memory `seq` is reverted before the error is returned. Subsequent
  commands aren't poisoned by the gap.
- **Lazy recovery.** Each entity recovers from the journal on its
  *first* command, not at gateway startup. If you have 10⁶ aggregate
  ids, the gateway doesn't load them all up front.

### Readers

A `Reader` folds journal events into a typed projection. Implement
the trait once per read model:

```rust
#[async_trait]
pub trait Reader: Send + 'static {
    type Event:      Send + Clone + 'static;
    type Projection: Default + Send + Sync + 'static;
    type Error:      std::error::Error + Send + 'static;

    fn name(&self) -> &str;
    fn tag(&self)  -> Option<String> { None }   // None = every event
    fn decode(b: &[u8]) -> Result<Self::Event, String>;
    async fn apply(&mut self, p: &mut Self::Projection, e: Self::Event)
        -> Result<(), Self::Error>;
}
```

The runner is an async tokio task that:

1. Calls `read_journal.all_persistence_ids().await`.
2. For each pid, calls `events_by_persistence_id(pid, last_seen+1, u64::MAX)`.
3. Filters by tag (if set), decodes via `Reader::decode`, applies via
   `Reader::apply`, advances the per-pid offset.
4. Sleeps `poll_interval`, repeats.

Failures during `apply` are logged at `warn` level; the runner
advances past the offending event so it doesn't get stuck.

You retrieve the projection by calling
`ProjectionHandle::snapshot().await` (read lock) or
`ProjectionHandle::read(|p| f(p)).await`. The handle also exposes the
current `offset()` — useful in tests when you want to wait for the
projection to catch up.

### Extension hooks

Two flavors, both opt-in:

| Slot                | Type                                              | Failure semantics                                |
|---------------------|---------------------------------------------------|--------------------------------------------------|
| `on_command(...)`   | sync `Fn(&Cmd) -> Result<(), PatternError<E>>`    | rejection short-circuits the persist             |
| `on_event(...)`     | sync `Fn(&Ev)`                                    | side-effect only                                 |
| `tap_events(...)`   | `tokio::sync::mpsc::UnboundedSender<Ev>`          | closed receiver is silently pruned               |

`tap_events` is the bridge to anything async / out-of-process: a
saga's input channel, a webhook publisher, a metrics aggregator. The
sender is cheap; the gateway pushes a clone of every persisted event
without blocking on subscribers.

### Multiple aggregate ids

The gateway demultiplexes by `Cmd::aggregate_id()`. To exercise
multiple aggregate instances under one pattern, return distinct ids
from your `Command::aggregate_id`:

```rust
impl Command for AcctCmd {
    type AggregateId = String;
    fn aggregate_id(&self) -> String {
        match self {
            AcctCmd::Deposit { account, .. } | AcctCmd::Withdraw { account, .. } => account.clone(),
        }
    }
}
```

Each id gets its own `EntityState<A>` — its own `aggregate`, `state`,
and `seq`. Recovery is independent per id.

## Saga pattern

A `Saga` reacts to domain events and dispatches commands to drive a
long-running business process across multiple aggregates. State is
keyed by a correlation id derived from each event; the saga
accumulates state per correlation id until it explicitly `Complete`s.

```rust
#[async_trait]
pub trait Saga: Send + 'static {
    type Event:   Send + Clone + 'static;
    type Command: Send + 'static;
    type State:   Default + Send + 'static;
    type Error:   std::error::Error + Send + 'static;

    fn correlation_id(event: &Self::Event) -> Option<String>;

    async fn handle(&mut self, state: &mut Self::State, event: Self::Event)
        -> Result<Vec<SagaAction<Self::Command>>, Self::Error>;
}
```

Actions the saga may emit:

```rust
pub enum SagaAction<C> {
    Send(C),                         // dispatch immediately
    Schedule(C, Duration),           // dispatch after delay
    Compensate(Vec<C>),              // rollback chain
    Complete,                        // saga done; clear state
}
```

### Wiring

```rust
let saga_topology = SagaPattern::<TransferSaga>::builder()
    .saga(TransferSaga)
    .events(event_rx)                     // typically `tap_events` from a CqrsPattern
    .dispatcher(move |cmd: AcctCmd| {     // returns `Future<Output = bool>`
        let r = repo.clone();
        async move { r.send(cmd).await.is_ok() }
    })
    .build()?;
saga_topology.materialize(&system).await?;
```

The dispatcher returns `bool` so the saga can decide whether to fire
compensation: `false` triggers any `Compensate` chain you've queued
in the `Vec<SagaAction>` returned from the same `handle` call.

### Money-transfer example

```rust
async fn handle(&mut self, state: &mut TransferState, event: AcctEvent)
    -> Result<Vec<SagaAction<AcctCmd>>, SErr>
{
    match event {
        AcctEvent::Withdrawn { from, to, amount, transfer_id }
            if !state.deposit_dispatched =>
        {
            state.deposit_dispatched = true;
            Ok(vec![SagaAction::Send(AcctCmd::Deposit {
                account: to, from, amount, transfer_id,
            })])
        }
        AcctEvent::Deposited { .. } => Ok(vec![SagaAction::Complete]),
        _ => Ok(vec![]),
    }
}
```

Run with `cargo test -p atomr-patterns --test saga_money_transfer`.

## Domain Event Bus

In-process broadcast of domain events to interested subscribers.
Useful as glue between a write-side `CqrsPattern` and downstream
readers / sagas / external integrations:

```rust
let bus = DomainEventBus::<MyEvent>::builder().build().materialize(&system).await?;

let publisher = bus.clone();
cqrs_builder = cqrs_builder.on_event(move |e: &MyEvent| publisher.publish(e.clone()));

let mut subscriber = bus.subscribe();   // UnboundedReceiver<MyEvent>
while let Some(event) = subscriber.recv().await {
    // ...
}
```

The `bus-cluster` Cargo feature (gated on `atomr-cluster-tools`) is
reserved for a cluster-wide variant. v1 ships only the local
broadcast.

## Outbox pattern

Tail-follows the read journal and re-emits events into a publish
callback, persisting offsets so restarts don't double-publish. Use it
when a side-effect (Kafka, webhook, SNS) must occur "at-least-once
after every successful aggregate write":

```rust
let outbox = OutboxPattern::<MyEvent>::builder()
    .read_journal(rj.clone())
    .poll_interval(Duration::from_millis(50))
    .offset_store(Arc::new(InMemoryOffsetStore::new()))
    .decode(|bytes| MyEvent::deserialize(bytes))
    .publish(|event| async move {
        kafka.send(topic, event).await.is_ok()
    })
    .build()?
    .materialize(&system)
    .await?;
```

The publish callback returns `bool`. On `false`, the outbox **stops
advancing the offset for that pid** — the event is retried on the
next tick. On `true`, the offset advances and `published()` counter
ticks up.

`OutboxOffsetStore` is pluggable. The shipped `InMemoryOffsetStore`
survives publisher restarts inside the same process; use it for tests
or single-process workloads. Production code should implement the
trait against a durable store (the same Postgres / Redis / etc. the
journal uses, ideally).

To stop the publisher loop, call `handles.stop()`.

Run with `cargo test -p atomr-patterns --test outbox_publish`.

## Anti-Corruption Layer

Translates between two bounded contexts. Provide a `Translator`
mapping `External -> Option<Internal>` (returning `None` drops the
value):

```rust
struct EvenOnly;
impl Translator for EvenOnly {
    type External = i64;
    type Internal = i64;
    fn translate(&self, ext: i64) -> Option<i64> {
        if ext % 2 == 0 { Some(ext * 10) } else { None }
    }
}

let mut handles = AntiCorruption::<i64, i64>::builder(EvenOnly)
    .build()
    .materialize(&system)
    .await?;

handles.input.send(42)?;
let translated = handles.output.recv().await;     // Some(420)
```

The pattern is a single tokio task that reads `External` items off an
unbounded mpsc, applies the translator, and forwards survivors to the
output channel. Closing the input channel drains and exits cleanly.

## Composing patterns

The patterns are independent and stack naturally. A typical
order-management bounded context might wire:

```
   ┌─────────────────────┐
   │ CqrsPattern<Order>  │──── on_event ─────┐
   └──────────┬──────────┘                   ▼
              │                ┌─────────────────────────┐
              │                │ DomainEventBus<OrderEv> │
              │                └────────────┬────────────┘
              │                  subscribe  │  subscribe
              │                       ┌─────┴─────┐
              │                       ▼           ▼
              │           ┌──────────────────┐  ┌─────────────────────────┐
              │           │ SagaPattern<...> │  │ OutboxPattern<OrderEv>  │
              │           └────────┬─────────┘  └────────────┬────────────┘
              │                    │ Repository.send         │ publish to Kafka
              ▼                    ▼                         ▼
   ┌─────────────────────────────────────────────────────────────────────┐
   │                          atomr-persistence::Journal                 │
   └─────────────────────────────────────────────────────────────────────┘
```

Each pattern is `materialize`d once on the same `ActorSystem`. Their
actors live as siblings under the user guardian; the dashboard shows
each pattern as its own subtree.

## Testing patterns

Three integration-test idioms ship in `crates/atomr-patterns/tests/`:

- **`cqrs_counter`** — round-trip a few commands, then poll
  `ProjectionHandle::read(|p| ...)` until the projection catches up.
  No mocks; use `atomr_persistence::InMemoryJournal` and
  `atomr_persistence_query_inmemory::read_journal`.
- **`cqrs_with_extensions`** — inject probe `on_command` + `on_event`
  closures backed by `Arc<AtomicUsize>` counters; assert they fire in
  the expected order around accepted vs. rejected commands.
- **`saga_money_transfer`** — wire a real `CqrsPattern`, a `tap_events`
  channel, and a `SagaPattern` whose dispatcher uses the same
  repository. Verify the saga's command dispatch by reading the
  affected aggregate's state through another command.
- **`outbox_publish`** — write events directly to the journal, run
  the publisher to drain them, stop, write more, run a fresh
  publisher with the same offset store, assert exactly-once delivery.
- **`acl_translate`** — push 100 `External` items, drop the input,
  collect `Internal` items, compare against the expected filter+map.

Patterns are testable end-to-end with the in-memory journal + a real
`ActorSystem`. There's no special test harness, no mocks of the
internal trait surface; you assert against the same handles your
production code uses.

## v1 limitations

The current release intentionally trades scope for clarity. Track
these against the pattern crate's roadmap:

| Limitation | Workaround |
|------------|-----------|
| Named slots (`on_command`, `on_event`) are typed closures — not `UntypedActorRef`-based actor interceptors. | Closures cover sync auth / validation. For async work, push to `tap_events` and react out-of-band. |
| Taps are `tokio::sync::mpsc::UnboundedSender<T>`, not `atomr_streams::Sink<T>` directly. | Wrap the receiver as a `Source` if you need stream-DSL composition. |
| No sharding feature — gateway holds entities in a process-local `HashMap<Id, EntityState>`. | Single-node deployments are unaffected. Multi-node sharding will arrive behind a `sharding` feature gated on `atomr-cluster-sharding`. |
| No automatic state-snapshotting in the gateway. Recovery uses journal replay end-to-end. | Wire a custom `AsyncSnapshotter` against the same journal yourself, or accept full replay. |
| Reader runner sleeps `poll_interval` between cycles — no live-tail subscription model. | Tune `poll_interval` to your latency budget. The in-memory journal poll is cheap. |
| `bus-cluster` feature is reserved for a `DistributedPubSub`-backed bus; v1 only ships the local variant. | Use `atomr-cluster-tools::DistributedPubSub` directly for cross-process fan-out. |

## API reference highlights

The full rustdoc is generated by
`cargo +nightly doc -p atomr-patterns --no-deps --all-features` and
respects the workspace rustdoc gate (`-D rustdoc::broken-intra-doc-links`).

Module map:

```
atomr_patterns
├── prelude::*               // re-exports of everything below
├── PatternError<E>          // unified error
├── Topology                 // common materialization trait
├── ddd::                    // DDD vocabulary traits
│   ├── Entity
│   ├── ValueObject
│   ├── Command
│   ├── DomainEvent
│   ├── AggregateRoot
│   └── Repository
├── extensions::             // shared hook plumbing
│   ├── ExtensionSlots<C, EV, DE>
│   ├── CommandInterceptor<C, E>
│   └── EventListener<EV>
├── cqrs::
│   ├── CqrsPattern<A>
│   ├── CqrsBuilder<A, J>
│   ├── CqrsTopology<A, J>
│   ├── CqrsHandles<A>
│   ├── Reader
│   └── ProjectionHandle<P>
├── saga::
│   ├── SagaPattern<S>
│   ├── SagaBuilder<S>
│   ├── SagaTopology<S>
│   ├── Saga
│   └── SagaAction<C>
├── bus::
│   ├── DomainEventBus<E>
│   ├── BusBuilder<E>
│   ├── BusTopology<E>
│   └── BusHandles<E>
├── outbox::
│   ├── OutboxPattern<E>
│   ├── OutboxBuilder<E>
│   ├── OutboxTopology<E>
│   ├── OutboxHandles
│   ├── OutboxOffsetStore
│   └── InMemoryOffsetStore
└── acl::
    ├── AntiCorruption<X, I>
    ├── AclBuilder<T>
    ├── AclTopology<T>
    ├── AclHandles<X, I>
    └── Translator
```

## See also

- [Architecture](architecture.md) — runtime layout and dispatch.
- [Persistence providers](persistence-providers.md) — pluggable
  journal backends.
- [Dashboard](dashboard.md) — visualizing pattern subtrees in the
  topology view.
- [Idiomatic Rust principles](idiomatic-rust.md) — invariants every
  contribution is reviewed against (no `Box<dyn Any>` mailboxes,
  type-state lifecycle, …).
