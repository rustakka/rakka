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
  point-to-point coupling — that's a [saga](#saga-pattern), or a
  [process manager](#process-manager) when the state space is bounded
  and you want a typed FSM.
- A need to reliably republish persisted events to a downstream system
  (Kafka, SNS, a webhook) — that's the [outbox](#outbox-pattern).
- A need to suppress duplicate inbound messages keyed by an
  idempotency key — that's the [inbox](#inbox-pattern).
- Two bounded contexts that exchange messages but speak different
  vocabularies — that's the [anti-corruption layer](#anti-corruption-layer).
- A pure event-driven side-effect (notifications, logs, metrics) that
  doesn't need correlation state — that's the [reactor](#reactor).
- Composable predicates over a domain type — that's the
  [specification](#specification-pattern) trait.

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
    .read_journal(rj)                  // required unless event_bus drives readers
    .recovery_permits(8)               // concurrent-recovery cap
    .poll_interval(Duration::from_millis(50))
    .repository_timeout(Duration::from_secs(5))
    .writer_uuid("svc-1")              // stamped onto every PersistentRepr
    .shards(64)                        // intra-process command sharding (v2)
    .snapshot_store(snap_store)        // v2 — opt-in snapshot durability
    .snapshot_policy(SnapshotPolicy::Periodic { every: 100 })
    .snapshot_keep_last(2)
    .dedupe_window(1024)               // v2 — command_id LRU dedupe
    .with_event_codecs(codecs)         // v2 — manifest-keyed decoders
    .with_reader_retry(5, RetrySchedule::exponential(...))  // v2
    .with_event_bus(bus_handles)       // v2 — drives readers via live-tail
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
handles.rebuild_projection("totals").await?;  // v2 — admin rebuild
```

### Command lifecycle

When `repo.send(cmd).await` is called, the gateway runs each step
in order:

| Step                      | Failure surface                            |
|---------------------------|---------------------------------------------|
| `on_command` interceptors | `PatternError::Intercepted` (or any variant the closure constructs) |
| Pull / create entity      | `PatternError::Invariant("recovery permit denied")` |
| Snapshot-first recovery (v2) — load from `SnapshotStore`, replay tail | `PatternError::{Journal,Codec,Domain}` |
| Lazy `Eventsourced::recover` from journal (no snapshot) | `PatternError::{Journal,Codec,Domain}` |
| Command dedupe lookup (v2) — return cached events if `command_id` was seen | — |
| `expected_version` check (v2) | `PatternError::ConcurrencyConflict { expected, actual }` |
| `command_to_events` (validation lives here) | `PatternError::Domain(E)` |
| Encode events             | `PatternError::Codec(String)` |
| `journal.write_messages` (atomic per command) | `PatternError::Journal(_)` (state rollback) |
| Apply events to state     | infallible |
| `check_invariants(&state)` (post-condition) | `PatternError::Domain(E)` |
| Snapshot save (v2) — if `SnapshotPolicy::Periodic` triggers | logged on failure |
| `on_event` listeners + event taps + bus publish | side effects only |
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
    fn filter(&self) -> ReaderFilter { ReaderFilter::All }   // v2
    fn tag(&self)    -> Option<String> { None }              // legacy
    fn decode(b: &[u8]) -> Result<Self::Event, String>;
    async fn apply(&mut self, p: &mut Self::Projection, e: Self::Event)
        -> Result<(), Self::Error>;
}

pub enum ReaderFilter {
    All,
    Tag(String),
    PersistenceId(String),
    PersistenceIds(Vec<String>),
}
```

The runner has two modes selected by builder configuration:

- **Polling** (default) — sleeps `poll_interval`, lists persistence
  ids each tick, and tail-reads each pid. Filtering happens in-loop
  per `ReaderFilter`. Decoding consults an `EventCodecRegistry` if
  configured, falling back to `Reader::decode` otherwise.
- **Live-tail** (v2, opt-in via `with_event_bus(...)`) — subscribes
  to a `DomainEventBus` topic and applies each broadcast event
  directly. Latency drops from `poll_interval` to fanout latency.

Failures during `apply` retry per `with_reader_retry(max, schedule)`
(v2) before being logged at `warn` level and advancing past the
offending event. Without retry config the runner advances on first
failure (same as v1).

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

### Durable saga state (v2)

By default saga state lives in-memory in the runner. Wire a
`SagaStateStore` and the runner rehydrates in-flight correlations on
restart, then persists state after every `handle` call:

```rust
let store = Arc::new(JournalSagaStateStore::new(journal.clone(), "transfer-saga"));
SagaPattern::<TransferSaga>::builder()
    .saga(TransferSaga)
    .events(rx)
    .dispatcher(...)
    .state_store(store)            // v2 — rehydrate-on-start + persist-on-handle
    .build()?;
```

Two impls ship: `InMemorySagaStateStore` (default, tests) and
`JournalSagaStateStore<J>` (event-sourced per-correlation stream
under pid `saga::<saga-name>::<corr-id>`). For state encoding,
implement `Saga::encode_state` / `decode_state`.

Run with `cargo test -p atomr-patterns --test saga_state_persistence`.

## Process Manager

A `ProcessManager` is a typed FSM-driven sibling of `Saga`. Use it
when the state space is bounded and you want exhaustive
pattern-matching over `(state, event)` rather than free-form mutation.

```rust
pub trait ProcessManager: Send + 'static {
    type Event: Send + Clone + 'static;
    type Command: Send + 'static;
    type State: Default + Clone + Send + 'static;
    type Error: std::error::Error + Send + 'static;

    fn correlation_id(event: &Self::Event) -> Option<String>;
    fn transition(state: &Self::State, event: Self::Event)
        -> Result<Transition<Self::State, Self::Command>, Self::Error>;
}

pub enum Transition<S, C> {
    Stay,
    Goto { next: S, commands: Vec<C> },
    Complete { commands: Vec<C> },
}
```

The runner walks each correlation id forward through the FSM, drops
state on `Complete`, and dispatches every emitted command via your
async dispatcher closure (same shape as Saga).

```rust
ProcessManagerPattern::<OrderProcess>::builder()
    .events(rx)
    .dispatcher(|c| async move { send(c).await; true })
    .build()?
    .materialize(&system).await?;
```

Run with `cargo test -p atomr-patterns --test process_manager`.

## Reactor

Pure event-driven side effects. Subscribes to events, runs a closure
per event, doesn't dispatch commands, no per-correlation state.
Lighter than `Saga` and `ProcessManager`, but with its own
materialization, naming, and dashboard subtree (vs. the bare
`tap_events` channel which has none of those).

```rust
ReactorPattern::<MyEvent>::builder()
    .name("notifier")
    .events(rx)
    .reaction(|e| async move { send_notification(e).await })
    .build()?
    .materialize(&system).await?;
```

Run with `cargo test -p atomr-patterns --test reactor`.

## Specification pattern

Composable predicates over a domain type. Pure trait + a few
combinators; no actors or runtime involvement.

```rust
pub trait Specification<T>: Send + Sync {
    fn is_satisfied_by(&self, t: &T) -> bool;
    fn and<S: Specification<T>>(self, other: S) -> AndSpec<Self, S>
        where Self: Sized { ... }
    fn or<S: Specification<T>>(self, other: S) -> OrSpec<Self, S>
        where Self: Sized { ... }
    fn not(self) -> NotSpec<Self> where Self: Sized { ... }
}
```

`FnSpec(closure)` wraps a `Fn(&T) -> bool` if you want a one-off
without a fresh struct. Use specifications for query filters,
invariant checks, command routing.

Run with `cargo test -p atomr-patterns --test specification`.

## Inbox pattern

Mirror image of the outbox: a deduplicating intake of inbound
messages keyed by an idempotency key. Reuses an `InboxStore` to
record seen keys; redelivery is suppressed.

```rust
InboxPattern::<MyEvent>::builder()
    .name("orders-inbox")
    .key(|e: &MyEvent| e.message_id.clone())
    .source(rx)
    .store(Arc::new(InMemoryInboxStore::new()))
    .handler(|e: MyEvent| async move { process(e).await; true })
    .build()?
    .materialize(&system).await?;
```

The handler returns `bool`; on `true` the key is committed to the
store via `mark_processed`, on `false` the key is left as `record_seen`
so the next delivery retries. `InMemoryInboxStore` ships out of the
box; durable backends implement the `InboxStore` trait against any
persistence backend.

Run with `cargo test -p atomr-patterns --test inbox`.

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

### Cluster mode (v2)

Behind the `bus-cluster` Cargo feature, the same `DomainEventBus`
materializes against a `ClusterPubSub` mediator transport. Builder
gains `.cluster(transport)`, `.topic(name)`, and `.codec(encode, decode)`.
A `BusRouter<E>` actor bridges the mediator topic into the local
mpsc subscriber list so callers don't notice the change.

```rust
DomainEventBus::<MyEvent>::builder()
    .cluster(Arc::new(ClusterPubSub::new(...)))
    .topic("orders")
    .codec(|e| bincode::encode(e), |b| bincode::decode(b))
    .build()
    .materialize(&system).await?;
```

Without `.cluster(...)` the behaviour is identical to v1 (process-local
broadcast).

Run with `cargo test -p atomr-patterns --test cluster_distribution
--features bus-cluster`.

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

`OutboxOffsetStore` is pluggable. v2 ships:

- `InMemoryOffsetStore` — survives publisher restarts inside the same
  process; default for tests.
- `JournalOffsetStore<J>` — durable, backed by any `Journal` impl. The
  full offset map is event-sourced as a single payload under
  `outbox::<outbox-name>::offsets`. On restart the runner replays the
  highest sequence record and resumes from there.

```rust
.offset_store(Arc::new(JournalOffsetStore::new(journal.clone(), "kafka").await))
```

To stop the publisher loop, call `handles.stop()`.

Run with `cargo test -p atomr-patterns --test outbox_publish` and
`cargo test -p atomr-patterns --test outbox_durable_offsets`.

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

## Snapshot recovery (v2)

Wire a `SnapshotStore` into `CqrsBuilder` and the gateway loads the
latest snapshot before replaying the tail, instead of replaying from
sequence 1. Aggregates opt in by implementing `encode_state` /
`decode_state` on `AggregateRoot`:

```rust
impl AggregateRoot for Counter {
    type Id = String;
    fn aggregate_id(&self) -> &Self::Id { &self.id }
    fn encode_state(s: &CounterState) -> Option<Result<Vec<u8>, String>> {
        Some(Ok(s.n.to_le_bytes().to_vec()))
    }
    fn decode_state(b: &[u8]) -> Result<CounterState, String> {
        Ok(CounterState { n: i64::from_le_bytes(b.try_into().map_err(|_| "len")?) })
    }
}
```

Snapshot policy controls when the gateway writes:

- `SnapshotPolicy::Manual` (default) — never autosaves.
- `SnapshotPolicy::Periodic { every: N }` — saves every Nth commit.

`snapshot_keep_last(k)` retains the last *k* snapshots and deletes the
rest. Old aggregates that don't implement `encode_state` continue to
recover from the journal head — the feature is fully opt-in.

Run with `cargo test -p atomr-patterns --test cqrs_snapshot_recovery`.

## Sharding (v2)

`CqrsBuilder::shards(n)` switches the gateway from a single in-process
actor to **n** sibling gateway actors, with commands routed by
`hash(aggregate_id) mod n`. This gives intra-process parallelism for
write-heavy workloads while keeping per-aggregate FIFO ordering.

Cluster-wide sharding (using `atomr-cluster-sharding::ShardRegion`)
remains a v3 concern — `ShardRegion`'s sync `EntityHandler` shape is
incompatible with the gateway's async command flow without a
larger refactor.

Run with `cargo test -p atomr-patterns --test cqrs_sharded`.

## Idempotency (v2)

Three tiers, all opt-in.

**1. Command dedupe.** Set `dedupe_window(N)` on the builder and
implement `Command::command_id(&self) -> Option<&str>`. The gateway
caches the result of each unique command id (per aggregate, bounded
LRU of size N) and short-circuits duplicates by replaying the cached
events without re-invoking `command_to_events`.

**2. Optimistic concurrency.** Implement
`Command::expected_version(&self) -> Option<u64>` and the gateway
compares against the entity's current `seq` before persisting. A
mismatch returns `PatternError::ConcurrencyConflict { expected, actual }`.
Use this when a UI sends a command annotated with the version it was
viewing.

**3. Inbox pattern.** Suppress duplicate inbound deliveries by
idempotency key — see [Inbox pattern](#inbox-pattern).

Run with `cargo test -p atomr-patterns --test dedupe` and
`cargo test -p atomr-patterns --test optimistic_concurrency`.

## Event upcasting (v2)

Long-lived aggregates change their event schema. The
`EventCodecRegistry<E>` lets you register a different decoder per
manifest string, with a default fallback for events written before
manifests were threaded through:

```rust
let codecs = EventCodecRegistry::<OrderEvent>::new()
    .register("order.v1", |b| OrderEvent::decode_v1(b))
    .register("order.v2", |b| OrderEvent::decode_v2(b))
    .with_default(|b| OrderEvent::decode_v1(b));   // legacy fallback

CqrsPattern::<Order>::builder(journal)
    .with_event_codecs(codecs)
    .with_reader(MyReader);
```

Old events keep decoding through the v1 closure, new writes use the
manifest your aggregate's `event_manifest()` returns. Readers and the
gateway both consult the registry.

Run with `cargo test -p atomr-patterns --test event_upcasting`.

## AuditLog reader

A built-in `Reader` impl that records every event into a bounded
`AuditProjection<E>` ring buffer. Useful for debugging, compliance
views, and "what happened recently" UIs.

```rust
let (builder, audit) = builder.with_reader(AuditLog::with_capacity(10_000));
audit.read(|p| p.recent(50)).await
```

## Projection rebuild

`CqrsHandles::rebuild_projection(name)` resets a named projection to
`Default::default()`, walks every relevant event, and re-applies. This
is the admin escape hatch when a reader's `apply` logic changes or
needs to recompute from scratch.

```rust
handles.rebuild_projection("totals").await?;
```

Run with `cargo test -p atomr-patterns --test projection_rebuild`.

## Scheduled commands

`atomr_patterns::cqrs::scheduled::schedule_command` is a thin facade
over `atomr_core::actor::scheduler::Scheduler::schedule_once`. Inside
a saga, process manager, or any code that holds a `Repository`, queue
a follow-up command after a delay:

```rust
schedule_command(&system, repo.clone(), Duration::from_secs(60),
    OrderCmd::CheckPaymentTimeout { id });
```

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

Integration tests live in `crates/atomr-patterns/tests/`:

| Test | Covers |
|------|--------|
| `cqrs_counter` | round-trip commands + projection read |
| `cqrs_with_extensions` | `on_command` / `on_event` interceptor ordering |
| `saga_money_transfer` | saga across two aggregates |
| `outbox_publish` | publish + offset survival across publisher restarts |
| `acl_translate` | filter+map across bounded contexts |
| `cqrs_snapshot_recovery` | v2 — snapshot-first recovery |
| `saga_state_persistence` | v2 — saga rehydrates on restart |
| `outbox_durable_offsets` | v2 — `JournalOffsetStore` |
| `cqrs_sharded` | v2 — intra-process command sharding |
| `cluster_distribution` | v2 — cluster bus across `MultiNodeSpec` |
| `event_upcasting` | v2 — manifest-keyed event decoders |
| `reader_live_tail` | v2 — bus-driven reader + retry |
| `dedupe` | v2 — `command_id` LRU dedupe |
| `optimistic_concurrency` | v2 — `expected_version` conflict |
| `inbox` | v2 — duplicate-key suppression |
| `specification` | v2 — `and` / `or` / `not` predicate composition |
| `reactor` | v2 — fire-and-forget event handler |
| `projection_rebuild` | v2 — admin reset + full replay |
| `process_manager` | v2 — typed FSM walks `Pending → Paid → Shipped → done` |

Patterns are testable end-to-end with the in-memory journal + a real
`ActorSystem`. There's no special test harness, no mocks of the
internal trait surface; you assert against the same handles your
production code uses.

## v2 release notes

v2 closes every limitation from the v1 ship list and layers on common
DDD/EDA patterns we'd deferred. The v1 surface — fluent builders,
`Topology` materialization, named slots, `PatternError<E>` — is
unchanged; everything below extends rather than replaces it.

| v1 limitation | v2 resolution |
|---------------|---------------|
| No automatic state-snapshotting in the gateway. | `SnapshotStore` + `SnapshotPolicy` + `encode_state` / `decode_state` on `AggregateRoot`. See [Snapshot recovery](#snapshot-recovery-v2). |
| No sharding feature; gateway holds entities in a process-local `HashMap`. | Intra-process sharding via `.shards(n)`. Cluster-wide sharding remains v3. See [Sharding](#sharding-v2). |
| `bus-cluster` feature reserved but not implemented. | `DomainEventBus` now materializes against `ClusterPubSub` behind the `bus-cluster` feature. See [Cluster mode](#cluster-mode-v2). |
| Reader runner only polls; no live-tail subscription model. | `with_event_bus(...)` switches readers to live-tail via the bus. See [Readers](#readers). |
| Saga state was in-memory only. | `SagaStateStore` trait with `JournalSagaStateStore<J>` impl. See [Durable saga state](#durable-saga-state-v2). |
| Outbox offsets were in-memory only. | `JournalOffsetStore<J>` durable backend. See [Outbox pattern](#outbox-pattern). |
| All events decoded through one `Reader::decode`; no schema versioning. | `EventCodecRegistry<E>` keyed by manifest. See [Event upcasting](#event-upcasting-v2). |
| Reader `apply` failures advanced past the offending event. | `with_reader_retry(max, schedule)` retries before advancing. |
| No command-level idempotency. | `command_id()` + `dedupe_window(N)` LRU cache. See [Idempotency](#idempotency-v2). |
| No optimistic concurrency at the command boundary. | `Command::expected_version()` + `PatternError::ConcurrencyConflict`. |
| Process manager / reactor / specification / inbox / audit / projection-rebuild patterns absent. | All shipped. See subsections above. |

### Deferred to v3

- **Cluster-wide command sharding.** Wraps the gateway around
  `atomr-cluster-sharding::ShardRegion` and threads the
  oneshot-reply envelope through the sync `EntityHandler` shape.
  Intra-process sharding is enough for v2 use cases.
- **CRDT-backed projections.** Cross-node read-side replication via
  `atomr-distributed-data`. Today projections are per-node.
- **Time-travel debugging.** Replaying a projection to a specific
  point. `ProjectionRebuild` is the foundation; versioned projection
  storage is the missing piece.
- **API polish (Phase F).** ActorRef-based interceptors, sink-based
  taps, and `#[derive(AggregateRoot)]` / `#[derive(DomainEvent)]` /
  `#[derive(Command)]` macros. Closures and unbounded mpsc cover the
  current ergonomic story; macros land when the trait surface
  stabilises.

## API reference highlights

The full rustdoc is generated by
`cargo +nightly doc -p atomr-patterns --no-deps --all-features` and
respects the workspace rustdoc gate (`-D rustdoc::broken-intra-doc-links`).

Module map:

```
atomr_patterns
├── prelude::*               // re-exports of everything below
├── PatternError<E>          // unified error (incl. ConcurrencyConflict v2)
├── Topology                 // common materialization trait
├── ddd::                    // DDD vocabulary traits
│   ├── Entity
│   ├── ValueObject
│   ├── Command              // + command_id, expected_version (v2)
│   ├── DomainEvent
│   ├── AggregateRoot        // + encode_state, decode_state (v2)
│   └── Repository
├── extensions::             // shared hook plumbing
│   ├── ExtensionSlots<C, EV, DE>
│   ├── CommandInterceptor<C, E>
│   └── EventListener<EV>
├── cqrs::
│   ├── CqrsPattern<A>
│   ├── CqrsBuilder<A, J>    // + shards, snapshots, dedupe, codecs, retry, bus (v2)
│   ├── CqrsTopology<A, J>
│   ├── CqrsHandles<A>       // + rebuild_projection (v2)
│   ├── Reader
│   ├── ReaderFilter         // v2
│   ├── ProjectionHandle<P>
│   ├── EventCodecRegistry<E>     // v2 — manifest-keyed decoders
│   ├── AuditLog<E>               // v2 — built-in ring-buffer reader
│   ├── AuditProjection<E>        // v2
│   ├── SnapshotPolicy            // v2 — Manual / Periodic { every }
│   └── scheduled::schedule_command   // v2
├── saga::
│   ├── SagaPattern<S>
│   ├── SagaBuilder<S>       // + state_store (v2)
│   ├── SagaTopology<S>
│   ├── Saga                 // + encode_state, decode_state (v2)
│   ├── SagaAction<C>
│   ├── SagaStateStore            // v2
│   ├── InMemorySagaStateStore    // v2
│   └── JournalSagaStateStore<J>  // v2
├── process_manager::        // v2 — typed FSM
│   ├── ProcessManager
│   ├── ProcessManagerPattern<P>
│   ├── ProcessManagerBuilder<P>
│   ├── ProcessManagerTopology<P>
│   ├── ProcessManagerHandles
│   └── Transition<S, C>
├── reactor::                // v2 — fire-and-forget side effects
│   ├── ReactorPattern<E>
│   ├── ReactorBuilder<E>
│   ├── ReactorTopology<E>
│   └── ReactorHandles
├── specification::          // v2 — composable predicates
│   ├── Specification<T>
│   ├── AndSpec, OrSpec, NotSpec
│   └── FnSpec<F>
├── inbox::                  // v2 — duplicate-suppressed intake
│   ├── InboxPattern<E>
│   ├── InboxBuilder<E>
│   ├── InboxTopology<E>
│   ├── InboxHandles
│   ├── InboxStore
│   └── InMemoryInboxStore
├── bus::
│   ├── DomainEventBus<E>
│   ├── BusBuilder<E>        // + cluster, topic, codec (v2)
│   ├── BusTopology<E>
│   └── BusHandles<E>
├── outbox::
│   ├── OutboxPattern<E>
│   ├── OutboxBuilder<E>
│   ├── OutboxTopology<E>
│   ├── OutboxHandles
│   ├── OutboxOffsetStore
│   ├── InMemoryOffsetStore
│   └── JournalOffsetStore<J>     // v2
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
