# Architecture

How atomr is laid out, what each crate does, where the dispatch and
supervision boundaries fall, and where the heterogeneous-compute hooks
slot in. This is the map for somebody who wants to understand or
extend the runtime.

## Crate stack (bottom → top)

```
                         atomr-dashboard / atomr-telemetry
                                        ▲
                                        │
       ┌────────────┬───────────┬───────┼────────┬───────────┐
       │            │           │       │        │           │
       ▼            ▼           ▼       ▼        ▼           ▼
atomr-streams  atomr-persistence  atomr-cluster-sharding  atomr-cluster-tools
       │            │           │       │        │
       │            │           ▼       ▼        ▼
       │            │     atomr-cluster ─► atomr-distributed-data
       │            │           │       │
       ▼            ▼           ▼       ▼
                atomr-remote ◄──────── atomr-coordination
                        ▲              atomr-discovery
                        │              atomr-di
                        │              atomr-hosting
                        ▼
                  atomr-core
                        ▲
                        │
                  atomr-config
                  atomr-macros
```

Each crate owns one concern — picking it up in isolation gives you the
contract; pulling more in adds capability without changing the
contract.

## Concept by concept

### Actors

- `Actor` trait + `async_trait`. No inheritance hierarchy.
- `ActorRef<M>` is **typed** — sender / receiver types are checked at
  compile time. There is no untyped `IActorRef` you can pass around
  without the type info; for fan-out across actor types use
  `UntypedActorRef` for the path plus an enum for the message.
- The `Sender` enum (`Local` / `Remote` / `None`) is the only thing
  attached to an envelope's sender slot. No `Box<dyn Any>` anywhere on
  the public surface — see
  [Idiomatic Rust principles](idiomatic-rust.md) P-1.

### Supervision

- Closure-based `OneForOneStrategy` / `AllForOneStrategy` for ad-hoc
  deciders (default).
- `SupervisorOf<C>` trait for compile-time per-(parent, child) typed
  policies (opt-in).
- `SupervisionError` is the catch-all error type for actors that don't
  yet have a domain-specific error.

### Dispatchers and mailboxes

- The default dispatcher runs on the multi-thread tokio runtime.
- Pluggable dispatcher trait — pinned single-thread, work-stealing
  pool, or custom backends. **The same trait is the seam through which
  GPU / accelerator dispatchers slot in:** a dispatcher is anything
  that pulls envelopes from a mailbox and runs `Actor::handle` to
  completion. A future `cuda` dispatcher batches messages on the host
  side, schedules a kernel, and produces results back through the same
  envelope contract.
- Mailboxes are unbounded by default; bounded and priority variants
  available. The mailbox trait is small enough that an accelerator
  dispatcher can carry its own queue type when host-side queueing is
  the wrong shape.

### Persistence

- `Eventsourced` trait owns the (Command → Events → State) shape.
- `RecoveryPermitter` (semaphore-bounded) caps concurrent recoveries
  so a fleet-wide restart doesn't melt the journal.
- `ReceivePersistent` is the closure-style ergonomic helper.
- `PersistenceQuery` exposes typed `Offset`, `events_by_tag`,
  `current_*` variants.
- Storage adapters (`-sql`, `-redis`, `-mongodb`, `-cassandra`,
  `-aws`, `-azure`) implement the journal + snapshot traits. The TCK
  in `atomr-persistence-tck` is the conformance contract — every
  backend must pass.

### Cluster

- `MembershipState` + `Reachability` + five split-brain resolvers
  (`KeepMajority`, `StaticQuorum`, `KeepOldest`, `KeepReferee`,
  `LeaseMajority`).
- `ClusterEventBus`, `elect_leader`, `is_converged` are the pure
  helpers behind the daemon.
- The cluster daemon owns gossip, leader actions, and SBR ticks; it
  emits PDUs through a pluggable `GossipTransport`.

### Cluster tools

- `DistributedPubSub.Mediator`: typed `publish_msg::<M>` with topic +
  group routing. Cluster-aware via the mediator transport.
- `ClusterSingleton` and `ClusterClient` patterns over the cluster
  daemon.

### Sharding

- `ShardAllocationStrategy` trait with `LeastShard` and `Pinned`
  strategies; the persistent (event-sourced) coordinator owns the
  durable allocation table.
- `PassivationTracker` decides which entities to passivate after a
  configurable idle TTL.
- Three-phase handoff state machine for safe shard movement.

### Distributed data

- CRDTs: `GCounter`, `PNCounter`, `GSet`, `OrSet`, `LwwRegister`,
  `Flag`, `ORMap<K, V>`, `LWWMap<K, V>`, `PNCounterMap<K>`.
- `Replicator` ships them with a `subscribe(key, fn)` notification API
  (`SubscriptionToken` is RAII).
- Delta-CRDT propagation and durable storage available; consistency
  levels are first-class on read and write.

### Streams

- `Source` / `Flow` / `Sink` linear ops, plus seven junctions
  (broadcast, merge, zip, partition, balance, …).
- Recovery operators (`recover`, `map_error`, `recover_with`).
- Time-windowed (`grouped_within`, `idle_timeout`).
- Routing (`partition`, `balance`, `unzip`).
- Substreams (`group_by`, `split_when`), hub patterns
  (`BroadcastHub`, `MergeHub`), supervision deciders, stream refs
  for cross-process flows.
- Framing and file IO ship as first-class operators.

### Remote

- TCP transport, framed PDU codec, ack'd delivery, endpoint state
  machine, watcher, remote system daemon.
- Failure-detector registry; transport adapters for throttle,
  failure-injection, and tests.
- Reader / writer task split for full-duplex socket use; LRU caches
  for inflight envelope tracking.

### Configuration

- Layered TOML loader.
- HOCON-subset parser supporting `include`, `${path}`, `${?ENV}`,
  dotted keys, triple-quoted strings.

### Testkit

- `TestKit` (preconfigured `ActorSystem`) + `TestProbe` matchers
  (`expect_msg_class`, `expect_all_of`, `receive_n`, `receive_while`,
  `fish_for_message`).
- `TestScheduler` for virtual-time tests.
- `MultiNodeSpec` for in-process N-node harnesses with shared
  `tokio::sync::Barrier`.
- `EventFilter` for system event-stream assertions.

### Telemetry and dashboard

- `atomr-telemetry` exposes probes for actors, dead letters, cluster,
  sharding, persistence, remote, streams, and distributed data.
- `atomr-dashboard` is an `axum` REST + WebSocket server with an
  embedded React UI (`embed-ui` feature). Cluster-mode aggregator fans
  out across peers so the same UI shows the whole fleet.
- Prometheus and OTLP exporters cover the same metric surface.

### Macros

- `#[derive(Actor)]` and `#[actor_msg]` for ergonomics.
- `#[derive(Receive)]` (unit-variant dispatch via
  `#[receive(unit_variants(…))]`).
- `props!(EXPR)` — terse `Props::create(|| EXPR)`.

## Where heterogeneous compute slots in

The dispatch boundary is the place where the runtime can fan out
beyond CPU.

```
ActorRef<M> ─► Mailbox<M> ─► Dispatcher::poll() ─► Actor::handle()
                                  │
                                  ├── tokio worker pool   (CPU)
                                  ├── pinned thread       (CPU, blocking-friendly)
                                  └── accelerator stream  (GPU, planned)
```

The `Dispatcher` trait is small and the message is opaque to it. A
dispatcher whose backend is a CUDA stream would:

1. accept envelopes destined for actors annotated as accelerator-
   resident,
2. coalesce a window of compatible envelopes into a host buffer,
3. submit a kernel and wait on a stream event,
4. produce reply messages from the kernel result and feed them back
   into the actor system.

Supervision still applies. Backpressure still applies. Dead-letter
routing still applies. Telemetry still applies. The shape of the
program above the dispatcher does not change.

That's the value: the cost of moving a workload onto an accelerator
shouldn't be a new framework — it should be a `with_dispatcher("gpu")`.

## Design constraints

- **No wire compatibility** with any prior actor runtime — tokio TCP
  plus serde / bincode for the framed PDU codec.
- **No reflection-driven typing** — `ActorRef<M>` is typed,
  `Box<dyn Any>` is forbidden in public APIs.
- **Async-first** — every `await` boundary uses tokio; no blocking
  inside `Actor::handle`.
- **Persistent / immutable structures** on hot snapshot paths
  (gossip, replicator, sharding allocation) where copy-on-write would
  hurt p99.
- **Sealed traits** on framework markers (`Actor`, `Message`,
  `Serializer`) so downstream extends through composition.

## See also

- [Actors and agentic computing](actors-and-agentic-computing.md) —
  the argument for the model.
- [Idiomatic Rust principles](idiomatic-rust.md) — invariants that
  preserve the granularity above.
- [Remoting](remoting.md) — the transport layer.
- [Persistence providers](persistence-providers.md) — the storage
  adapter contract.
- [Streams](https://github.com/rustakka/atomr#whats-in-the-box) — reactive stream DSL.
- [Dashboard](dashboard.md) — live system view.
- [Full port plan](full-port-plan.md) — depth roadmap.
