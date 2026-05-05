# Migrating to atomr from a prior-art actor runtime

A pragmatic guide for engineers coming from a mature typed-actor
runtime in another language. The vocabulary here is common to most of
those runtimes — `IActorRef`, `Props`, `ReceiveActor`, `Eventsourced`,
`DistributedPubSub`, `ShardRegion` — and translates onto the atomr
surface with predictable Rust shapes.

atomr is **not** wire-compatible with any prior runtime. The goal is
**conceptual** alignment with Rust-native shapes, plus a forward path
to features prior art does not have (e.g. accelerator dispatchers; see
[`actors-and-agentic-computing.md`](actors-and-agentic-computing.md)).

## At-a-glance translation table

| Prior-art concept | atomr equivalent |
|---|---|
| Untyped `IActorRef` | `ActorRef<M>` (typed) / `UntypedActorRef` |
| Untyped actor context | `Context<A>` (typed by actor) |
| `Props.Create<T>(args)` | `Props::create(\|\| T::new(args))` or `props!(T { … })` |
| `Tell` / `Forward` | `ActorRef::tell(msg)` / `tell_from(msg, Sender)` |
| Untyped `Sender` | `Sender::Local(_)` / `Sender::Remote { … }` / `Sender::None` |
| `Ask` returning a future | `ActorRef::ask_with(\|tx\| MyMsg::Get(tx), timeout)` |
| `ReceiveActor` | `#[derive(Receive)]` + `on_<variant>` methods |
| Lifecycle hooks | `Actor::{pre_start, post_stop, pre_restart, post_restart}` |
| `OneForOneStrategy(decider)` | `OneForOneStrategy::new().with_decider(\|e\| …)` or `impl SupervisorOf<Child> for Parent` |
| `IStash` | `Context::stash` / `Context::unstash_all` |
| `BackoffSupervisor` | `pattern::BackoffSupervisor` + `BackoffOptions` |
| `CircuitBreaker` | `pattern::CircuitBreaker` |
| `Patterns.Retry` | `pattern::retry(op, max_attempts, RetrySchedule)` |
| `EventStream.Subscribe` | `system.event_stream().subscribe(...)` |
| `CoordinatedShutdown.Run()` | `system.coordinated_shutdown().run().await` |
| Cluster event subscription | `ClusterEventBus::subscribe(\|ev\| …)` |
| Cluster leader query | `cluster::elect_leader(&membership_state)` |
| `DistributedPubSub` mediator | `cluster_tools::DistributedPubSub::publish_msg::<M>(…)` |
| Replicator (CRDTs) | `distributed_data::Replicator::{update, get, subscribe}` |
| `IShardAllocationStrategy` | `cluster_sharding::ShardAllocationStrategy` trait |
| Least-shard allocator | `cluster_sharding::LeastShardAllocationStrategy` |
| Entity passivation | `PassivationTracker::record_activity` + `idle_since` |
| `Eventsourced` / `ReceivePersistentActor` | `Eventsourced` trait / `ReceivePersistent` closure helper |
| `RecoveryPermitter` | `persistence::RecoveryPermitter::new(N)` |
| Read-journal `EventsByTag` | `persistence_query::ReadJournal::events_by_tag(tag, Offset)` |
| `Source` / `Flow` / `Sink` | `streams::{Source, Flow, Sink}` |
| `GroupedWithin` | `streams::grouped_within(src, n, dur)` |
| `Recover` / `RecoverWith` | `streams::recover(src, f)` / `streams::recover_with(src, replacement)` |
| `Partition` / `Balance` / `Unzip` | `streams::partition` / `balance` / `unzip` |
| `KillSwitch` | `streams::KillSwitch` |
| `RestartSource` | `streams::RestartSource` + `RestartSettings` |
| HOCON configuration | `Config::from_hocon_str` / `from_hocon_file` |
| TestKit / TestProbe | `atomr_testkit::TestProbe` |
| Multi-node spec | `atomr_testkit::MultiNodeSpec` (in-process N-node) or `atomr_testkit::multinode_oop` (out-of-process controller + line protocol) |

## Idioms that look different

### Type-checked sender

The sender enum preserves identity at compile time:

```rust
use atomr_core::actor::{Sender, UntypedActorRef};
recipient.tell_from(MyMsg::Ping, Sender::Local(self_ref.clone()));

match ctx.sender() {
    Sender::Local(r) => /* reply via r */,
    Sender::Remote { path, handle } => /* serialize a reply */,
    Sender::None => { /* no sender attached */ }
}
```

There is no `Any::downcast` on the reply path. See
[`idiomatic-rust.md`](idiomatic-rust.md) P-1.

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
back to the closure-based `Props::supervisor_strategy`.

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

The closure-style equivalent is
`ReceivePersistent::new(id).on_command(...).on_event(...).with_codec(...)`.

### Streams: `Sink::collect` instead of materializer-bound run

```rust
let result = Sink::collect(Source::from_iter(items)).await;
```

Recovery operators sit alongside `Source` rather than as methods on
the value (because they require `Source<Result<T, E>>`):

```rust
let recovered = streams::recover(s, |e| Some(default(e)));
```

### Cluster events: callback subscription

```rust
let bus = cluster::ClusterEventBus::new();
let _handle = bus.subscribe(|ev| match ev {
    cluster::ClusterEvent::MemberUp(m) => log::info!("up: {}", m.address),
    cluster::ClusterEvent::LeaderChanged { from, to } => …,
    _ => {}
});
// drop the handle to unsubscribe (RAII)
```

## Things that don't translate

- **Reflection-based serializers.** Wire formats that depend on
  runtime reflection (e.g. CLR Hyperion) have no atomr equivalent.
  Use the typed serializer registry: bincode for inter-atomr traffic,
  or register a typed codec when you need a specific shape.
- **Process-only test harnesses** that need separate JVM/CLR
  processes per node. `MultiNodeSpec` runs in-process with shared
  barriers; the out-of-process variant (`atomr_testkit::
  multinode_oop`) is now shipped — a TCP-loopback rendezvous
  controller plus a language-agnostic line protocol so child nodes
  in any language can join a barrier-synchronized run.
- **Reactive HTTP integrations** that ship with the upstream actor
  runtime. atomr keeps HTTP out of the core; reach for an HTTP crate
  from the wider ecosystem.

## Migration playbook

1. Translate message enums first (classes → Rust `enum`s with
   `#[actor_msg]` for `Debug` + `non_exhaustive`).
2. Convert each actor: `ReceiveActor` → `#[derive(Actor)]` (or
   `#[derive(Receive)]` for unit-variant dispatch).
3. Replace untyped `IActorRef` parameters with typed `ActorRef<M>`
   handles.
4. For supervision: keep the existing semantics with
   `OneForOneStrategy::new().with_decider(...)` while you migrate;
   opt in to `SupervisorOf<C>` typed contracts as child error types
   firm up.
5. Persistence: replace inheritance with the `Eventsourced` trait;
   pin a `RecoveryPermitter` per system.
6. Cluster: subscribe to `ClusterEventBus` instead of registering an
   actor as a listener.
7. Run `cargo xtask audit` regularly — CI gates on no regressions vs
   the baseline at `docs/reports/audit-2026-04.json`.

## Reference

- [`full-port-plan.md`](full-port-plan.md) — depth roadmap.
- [`idiomatic-rust.md`](idiomatic-rust.md) — twelve invariants that
  explain *why* the Rust shape differs.
- [`parity.md`](parity.md) — per-crate depth grades.
- [`actors-and-agentic-computing.md`](actors-and-agentic-computing.md)
  — what atomr adds beyond the inherited shape (agentic systems +
  unified CPU + GPU compute).
