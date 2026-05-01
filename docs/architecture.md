# rakka architecture

How the layered crate stack maps onto upstream Akka.NET — concept by
concept, with a note on the cases where rakka deliberately diverges.

## Crate stack (bottom → top)

```
                                 rakka-dashboard / rakka-telemetry
                                                ▲
                                                │
            ┌────────────┬───────────┬──────────┼───────────┬───────────┐
            │            │           │          │           │           │
            ▼            ▼           ▼          ▼           ▼           ▼
    rakka-streams  rakka-persistence rakka-cluster-sharding rakka-cluster-tools
            │            │           │          │           │
            │            │           ▼          ▼           ▼
            │            │     rakka-cluster ─► rakka-distributed-data
            │            │           │          │
            ▼            ▼           ▼          ▼
                          rakka-remote ◄──────── rakka-coordination
                                  ▲              rakka-discovery
                                  │              rakka-di
                                  │              rakka-hosting
                                  ▼
                            rakka-core
                                  ▲
                                  │
                            rakka-config
                            rakka-macros
```

Each crate keeps to its single Akka.NET module so upstream-sync
diffing stays tractable (see [`PORTING.md`](../PORTING.md)).

## Concept-by-concept

### Actors

* `Actor` trait + `async_trait` (no inheritance hierarchy).
* `ActorRef<M>` is **typed** — sender / receiver types are checked at
  compile time. There is no `IActorRef` analogue you can pass around
  without type info; for fan-out across actor types use
  `UntypedActorRef` for the path + an enum for the message.
* The `Sender` enum (`Local`/`Remote`/`None`) is the only thing
  attached to an envelope's sender slot. No `Box<dyn Any>` — see
  [`docs/idiomatic-rust.md`](idiomatic-rust.md) P-1.

### Supervision

* Closure-based `OneForOneStrategy` / `AllForOneStrategy` for
  ad-hoc deciders (default).
* `SupervisorOf<C>` trait for compile-time per-(parent, child) typed
  policies (opt-in; Rust coherence forbids blanket-with-override).
* `SupervisionError` is the catch-all error type for actors that
  don't yet have a domain-specific error.

### Dispatchers / mailboxes

* The default `Tokio` multi-thread runtime is the dispatcher.
* Pluggable dispatcher trait + per-actor pinned dispatcher land in
  Phase 3.1 — see `docs/full-port-plan.md`.
* Mailboxes are unbounded by default; bounded / priority variants
  in Phase 3.2.

### Persistence

* `Eventsourced` trait owns the (Command → Events → State) shape.
* `RecoveryPermitter` (semaphore-bounded) caps concurrent recoveries.
* `ReceivePersistent` is the closure-style helper (Akka.NET
  `ReceivePersistentActor`).
* `PersistenceQuery` exposes typed `Offset`, `events_by_tag`,
  `current_*` variants.
* Storage backends (`-sql`, `-redis`, `-mongodb`, `-cassandra`,
  `-aws`, `-azure`) are placeholder skeletons today; Phase 11.G fills
  them in against the expanded TCK.

### Cluster

* In-memory `MembershipState` + `Reachability` + 5 SBR strategies are
  shipped today (`KeepMajority`, `StaticQuorum`, `KeepOldest`,
  `KeepReferee`, `LeaseMajority`).
* `ClusterEventBus` + `elect_leader` + `is_converged` are the pure
  helpers; an active gossip dissemination loop and convergence-driven
  leader transition need Phases 6.D / 6.E (gossip transport on top of
  rakka-remote).

### Cluster-tools

* `DistributedPubSub.Mediator`: typed `publish_msg::<M>` with topic +
  group routing; cross-node gossip integration is Phase 7.B.
* `ClusterSingleton` + `ClusterClient` exist as type sketches; the
  full handover protocol is Phase 7.C / 7.D.

### Sharding

* `ShardAllocationStrategy` trait + `LeastShard`/`Pinned` strategies.
* `ShardCoordinator::allocate_with_strategy` /
  `rebalance_with_strategy` / `region_shard_counts` are the typed
  entry points; the persistent (event-sourced) coordinator + 3-phase
  handoff state machine land in Phase 9.D-H.
* `PassivationTracker` decides which entities to passivate after a
  configurable idle TTL.

### Distributed data

* CRDTs: `GCounter`, `PNCounter`, `GSet`, `OrSet`, `LwwRegister`,
  `Flag`, `ORMap<K, V>`, `LWWMap<K, V>`, `PNCounterMap<K>`.
* `Replicator` stores them in-memory with a `subscribe(key, fn)`
  notification API (`SubscriptionToken` is RAII).
* Delta-CRDT propagation, durable storage, and consistency-level
  reads/writes land in Phase 8.C-G.

### Streams

* `Source` / `Flow` / `Sink` linear ops + 7 junctions + framing + IO.
* Recovery operators (`recover` / `map_error` / `recover_with`).
* Time-windowed (`grouped_within`, `idle_timeout`).
* Routing (`partition`, `balance`, `unzip`).
* Substreams (`groupBy`, `splitWhen`), Hub patterns, supervision
  deciders, and StreamRefs are Phase 12.1/12.4/12.5/12.9.
* HTTP integration carved out as `rakka-http` (Phase 12.10).

### Remote

* TCP transport, AkkaProtocol handshake, AckedDelivery, EndpointManager
  state machine, RemoteWatcher, RemoteSystemDaemon, FailureDetector
  registry, transport adapters (`Throttle`, `FailureInjector`,
  `Test`).
* `AssociationState` + `RemoteError` (typed) + `peer_state` /
  `purge_tombstones` queries.
* Reader/writer task split, TLS, message chunking, send-queue
  backpressure, LRU caches, `RemoteProps` are Phase 5.D-K.

### Configuration

* Native TOML loader.
* HOCON-subset parser (`include`, `${path}`, `${?ENV}`, dotted keys,
  triple-quoted strings) — Phase 2.

### Test-kit

* `TestKit` (preconfigured `ActorSystem`) + `TestProbe` matchers
  (`expect_msg_class`, `expect_all_of`, `receive_n`, `receive_while`,
  `fish_for_message`).
* `TestScheduler` (virtual-time clock).
* `MultiNodeSpec` (in-process N-node harness with shared
  `tokio::sync::Barrier`).
* `EventFilter` for system event-stream assertions.

### Telemetry / dashboard

* `rakka-telemetry` exposes probes for actors / dead-letters / cluster
  / sharding / persistence / remote / streams / distributed-data.
* `rakka-dashboard` is an `axum` REST + WebSocket server with an
  embedded React UI (`embed-ui` feature). Cluster-mode aggregator
  fans out across peers.
* Prometheus + OTLP exporters cover the same metric set.

### Macros

* `#[derive(Actor)]` + `#[actor_msg]` for ergonomics.
* `#[derive(Receive)]` (unit-variant dispatch via
  `#[receive(unit_variants(…))]`).
* `props!(EXPR)` — terse `Props::create(|| EXPR)`.

## Deliberate divergences from Akka.NET

* **No wire compatibility** with JVM/CLR Akka — Tokio TCP +
  Serde/bincode, not DotNetty + Hyperion.
* **No reflection-driven typing** — `IActorRef` becomes typed
  `ActorRef<M>`; `Box<dyn Any>` is forbidden in public APIs.
* **Async-first** — every `await` boundary uses tokio; no blocking
  inside `Receive`.
* **Persistent / immutable structures** for hot snapshot paths
  (gossip, replicator, sharding allocation) once Phase 13 lands.
* **Sealed traits** on framework markers (`Actor`, `Message`,
  `Serializer`) so downstream extends through composition; partial
  today, completed in Phase 13.

For the full audit + roadmap see
[`docs/full-port-plan.md`](full-port-plan.md).
