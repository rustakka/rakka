# Migrating from Akka.NET to rakka

A pragmatic translation guide. Follows
[`docs/idiomatic-rust.md`](idiomatic-rust.md) and the gap-aware
[`docs/full-port-plan.md`](full-port-plan.md).

`rakka` is **not** wire-compatible with Akka.NET; the goal is
**conceptual** parity with idiomatic Rust shapes. Most concepts map
1:1; a few (Hyperion, F#, multi-jvm test harness) are intentionally
absent — see [`PORTING.md`](../PORTING.md) for the full deferred list.

## At-a-glance translation table

| Akka.NET concept                       | rakka equivalent                                         |
|----------------------------------------|----------------------------------------------------------|
| `IActorRef`                            | `ActorRef<M>` (typed) / `UntypedActorRef`                |
| `IUntypedActorContext` / `IActorContext`| `Context<A>` (typed by actor; phase-typed in 1.C)        |
| `Props.Create<T>(args)`                | `Props::create(\|\| T::new(args))` or `props!(T { … })`  |
| `Tell` / `Forward`                     | `ActorRef::tell(msg)` / `tell_from(msg, Sender)`         |
| `Sender` (untyped)                     | `Sender::Local(_)` / `Sender::Remote { … }` / `None`     |
| `Ask` (returning `Task<T>`)            | `ActorRef::ask_with(\|tx\| MyMsg::Get(tx), timeout)`     |
| `ReceiveActor`                         | `#[derive(Receive)]` + `on_<variant>` methods (Phase 1.E)|
| `ActorBase` lifecycle hooks            | `Actor::{pre_start, post_stop, pre_restart, post_restart}` |
| `OneForOneStrategy(... decider)`       | `OneForOneStrategy::new().with_decider(\|e\| …)` or `impl SupervisorOf<Child> for Parent` |
| `IStash`                               | `Context::stash` / `Context::unstash_all`                |
| `BackoffSupervisor`                    | `pattern::BackoffSupervisor` + `BackoffOptions`          |
| `CircuitBreaker`                       | `pattern::CircuitBreaker` (now with reset-timeout fix)   |
| `Patterns.Retry`                       | `pattern::retry(op, max_attempts, RetrySchedule)`        |
| `EventStream.Subscribe`                | `system.event_stream().subscribe(...)` (predicate API in Phase 3.5) |
| `CoordinatedShutdown.Get(…).Run()`     | `system.coordinated_shutdown().run().await`              |
| `Cluster.Get(system).Subscribe(...)`   | `ClusterEventBus::subscribe(\|event\| …)`                |
| `Cluster.Leader`                       | `cluster::elect_leader(&membership_state)` (Phase 6.B)   |
| `DistributedPubSub.Mediator`           | `cluster_tools::DistributedPubSub::publish_msg::<M>(…)`  |
| `Akka.Distributed.Data.Replicator`     | `distributed_data::Replicator::{update, get, subscribe}` |
| `IShardAllocationStrategy`             | `cluster_sharding::ShardAllocationStrategy` trait        |
| `LeastShardAllocationStrategy`         | `cluster_sharding::LeastShardAllocationStrategy`         |
| `Passivate(stop_message)`              | `PassivationTracker::record_activity` + `idle_since`     |
| `Eventsourced` / `ReceivePersistentActor` | `Eventsourced` trait / `ReceivePersistent` closure helper |
| `RecoveryPermitter`                    | `persistence::RecoveryPermitter::new(N)`                 |
| `IReadJournal.EventsByTag`             | `persistence_query::ReadJournal::events_by_tag(tag, Offset)` |
| `Source/Flow/Sink`                     | `streams::{Source, Flow, Sink}`                          |
| `GroupedWithin(n, t)`                  | `streams::grouped_within(src, n, dur)`                   |
| `Recover` / `RecoverWith`              | `streams::recover(src, f)` / `streams::recover_with(src, replacement)` |
| `Partition` / `Balance` / `Unzip`      | `streams::partition` / `balance` / `unzip`               |
| `KillSwitch`                           | `streams::KillSwitch`                                    |
| `RestartSource`                        | `streams::RestartSource` + `RestartSettings`             |
| `Akka.Configuration` (HOCON)           | `Config::from_hocon_str` / `from_hocon_file` (Phase 2)   |
| `Akka.TestKit` / `TestProbe`           | `rakka_testkit::TestProbe` (matchers in Phase 4)         |
| `MultiNodeSpec`                        | `rakka_testkit::MultiNodeSpec` (in-process N-node)       |

## Idioms that look different

### Type-checked sender

In Akka.NET, `Sender` is an `IActorRef` — typeless from the
recipient's perspective. In rakka, the `Sender` enum preserves the
sender's identity at compile time:

```rust
use rakka_core::actor::{Sender, UntypedActorRef};
recipient.tell_from(MyMsg::Ping, Sender::Local(self_ref.clone()));
// inside the recipient:
match ctx.sender() {
    Sender::Local(r) => /* reply via r */ ,
    Sender::Remote { path, handle } => /* serialize a reply */ ,
    Sender::None => { /* no sender attached */ }
}
```

There is no `Any::downcast` on the reply path. See
[`docs/idiomatic-rust.md`](idiomatic-rust.md) P-1.

### Compile-time supervision contracts (opt-in)

```rust
impl SupervisorOf<Worker> for Boss {
    type ChildError = WorkerError;
    fn decide(&self, e: &WorkerError) -> Directive {
        match e {
            WorkerError::Recoverable => Directive::Restart,
            WorkerError::Fatal => Directive::Stop,
        }
    }
}
```

Rust's coherence rules forbid the "blanket + override" pattern, so
`SupervisorOf<C>` is **opt-in**. Actors without an explicit impl fall
back to the closure-based `Props::supervisor_strategy`. See
[`docs/idiomatic-rust.md`](idiomatic-rust.md) P-8.

### Eventsourced via trait, not inheritance

```rust
#[async_trait::async_trait]
impl Eventsourced for BankAccount {
    type Command = BankCmd;
    type Event = BankEvent;
    type State = AccountState;
    type Error = BankError;

    fn persistence_id(&self) -> String { format!("acct-{}", self.id) }

    fn command_to_events(&self, st: &AccountState, cmd: BankCmd)
        -> Result<Vec<BankEvent>, BankError>
    { /* validate, derive 0..N events */ }

    fn apply_event(st: &mut AccountState, e: &BankEvent) { /* mutate */ }

    fn encode_event(e: &BankEvent) -> Result<Vec<u8>, String> { … }
    fn decode_event(b: &[u8]) -> Result<BankEvent, String> { … }
}
```

Closure-style equivalent of Akka.NET `ReceivePersistentActor` is
`ReceivePersistent::new(id).on_command(...).on_event(...).with_codec(...)`.

### Streams: `Sink::collect` over `Sink.Seq`

Akka.NET writes:

```csharp
var result = await Source.From(items).RunWith(Sink.Seq<int>(), mat);
```

rakka:

```rust
let result = Sink::collect(Source::from_iter(items)).await;
```

Recovery operators sit alongside `Source` rather than as
methods on the value (since they require `Source<Result<T, E>>`):

```rust
let recovered = streams::recover(s, |e| Some(default(e)));
```

### Cluster events: callback subscription, not `Subscribe(IActorRef, …)`

```rust
let bus = cluster::ClusterEventBus::new();
let _handle = bus.subscribe(|ev| match ev {
    cluster::ClusterEvent::MemberUp(m) => log::info!("up: {}", m.address),
    cluster::ClusterEvent::LeaderChanged { from, to } => …,
    _ => {}
});
// _handle drop unsubscribes (RAII)
```

## Things that don't translate

| Akka.NET                   | rakka equivalent                                  |
|----------------------------|---------------------------------------------------|
| Hyperion serializer        | `rakka-serialization-hyperion` is a Serde/bincode shim — no CLR wire compat |
| `Akka.Remote.DotNetty`     | Tokio TCP transport (different wire format)       |
| F# DSL (`Akka.FSharp`)     | n/a — `rakka-macros` covers ergonomics            |
| `MultiJvmTestKit`          | `MultiNodeSpec` (in-process barriers); cross-process variant deferred |
| `Akka.HTTP`                | Carved out as `rakka-http` (separate crate, Phase 12.10) |
| Aeron transport            | n/a (akka.net itself doesn't ship it)             |

## Migration playbook

1. Translate message enums first (Akka.NET classes → Rust `enum`s
   with `#[actor_msg]` for `Debug` + non_exhaustive).
2. Convert each actor: `ReceiveActor` → `#[derive(Actor)]` (or
   `#[derive(Receive)]` for unit-variant dispatch).
3. Replace `IActorRef` parameters with `ActorRef<M>` typed handles.
4. For supervision: keep `Props.WithSupervisorStrategy` semantics by
   writing `OneForOneStrategy::new().with_decider(...)` for now; opt
   in to `SupervisorOf<C>` typed contracts as you firm up child
   error types.
5. Persistence: replace `Eventsourced` inheritance with the
   `Eventsourced` trait; pin a `RecoveryPermitter` per system.
6. Cluster: subscribe to `ClusterEventBus` instead of registering an
   `IActorRef` as a listener.
7. Run `cargo xtask audit` regularly — CI will gate on no
   regressions vs the baseline at `docs/reports/audit-2026-04.json`.

## Reference

- [`docs/full-port-plan.md`](full-port-plan.md) — depth audit + 15-phase
  roadmap; per-phase status in [`PORTING_TODO.md`](../PORTING_TODO.md).
- [`docs/idiomatic-rust.md`](idiomatic-rust.md) — the 12 invariants
  that explain *why* the Rust shape differs.
- [`docs/parity.md`](parity.md) — per-crate depth grades.
