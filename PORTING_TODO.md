# Depth roadmap

This document tracks the **alignment surface** of each rakka subsystem
against the feature shape that mature actor runtimes have converged on
over the last decade. It is not a percent-complete ledger. The goal
is to call out, per subsystem, where we are aligned and where we
deliberately go further or differently.

For measured depth metrics (LOC ratios, anti-pattern counts) see
[`docs/audit-2026-04.md`](docs/audit-2026-04.md). For per-crate depth
grades see [`docs/parity.md`](docs/parity.md). For the longer
architectural plan see [`docs/full-port-plan.md`](docs/full-port-plan.md).

## Foundations

### `rakka-core`

The actor primitives, supervision, dispatch, mailbox, FSM, event
stream, and coordinated shutdown.

- **Aligned**: actor system + provider, typed `ActorRef<M>`, props,
  context, sender (typed enum, no `Box<dyn Any>`), supervision
  strategies (one-for-one, all-for-all, supervisor-of), FSM (state
  + data + transition machine), stash, watch / death-watch, ask /
  pipe-to, scheduler, event stream, coordinated shutdown,
  extensions, dispatcher trait + thread-pool / calling-thread /
  pinned variants, mailbox kinds (unbounded, bounded, priority).
- **Native**: type-state lifecycle (`Starting` / `Running` /
  `Stopping`) at the runtime level, `props!` macro,
  `#[derive(Receive)]` ergonomic dispatch, `SupervisorOf<C>` trait
  for compile-time per-(parent, child) policies.
- **Forward-looking**: dispatcher trait designed to accept GPU /
  accelerator backends. The same `Mailbox<T>` contract pulls work
  whether the dispatcher is a tokio worker pool, a pinned thread,
  or a CUDA stream.

### `rakka-config`

Layered HOCON-style configuration.

- **Aligned**: TOML loader, dotted keys, nested objects, arrays,
  comments, triple-quoted strings, `include`, `${path}` strict
  substitution, `${?ENV}` optional substitution.
- **Native**: integrated typed deserialization via serde.

### `rakka-testkit`

- **Aligned**: `TestKit`, `TestProbe` matchers (`expect_msg_class`,
  `expect_all_of`, `receive_n`, `receive_while`,
  `fish_for_message`), virtual-time `TestScheduler`,
  `MultiNodeSpec` for in-process N-node harnesses, `EventFilter`.

### `rakka-macros`

- **Aligned**: derive-and-attribute helpers for actor + receive
  ergonomics.
- **Native**: `props!(EXPR)`, `#[derive(Receive)]` with unit-variant
  dispatch.

## Distribution

### `rakka-remote`

Cross-process actor messaging with framed PDU codec, ack'd delivery,
and an endpoint state machine.

- **Aligned**: TCP transport, framed PDU (Associate / Disassociate
  / Heartbeat / Payload / Ack), pluggable serializer registry
  (bincode + json + manifest-keyed lookup), endpoint reader/writer
  pair with heartbeat tick + sliding-window resend buffer, endpoint
  manager state machine (Idle → Pending → Connected → Quarantined →
  Tombstoned), `RemoteActorRefImpl` + provider, `actor_selection`
  across processes, watcher with failure-detector backed
  `Terminated`, system daemon for inbound dispatch, transport
  adapters (throttle, failure-injector, in-memory test transport),
  per-address phi-accrual failure detector registry, address-uid
  extension for incarnation tracking.
- **Native**: serde / bincode native wire format. No JVM/CLR wire
  compatibility — clean tokio-native transport.
- **Depth in progress**: typed `Props` over the wire (the deployer
  ships `(manifest, bytes)` today), TLS, message chunking,
  send-queue backpressure tuning, LRU caches for inflight envelope
  tracking.

### `rakka-cluster`

Membership, gossip, reachability, split-brain resolution.

- **Aligned**: `MembershipState`, `Reachability`, vector clock,
  five SBR strategies (`KeepMajority`, `StaticQuorum`,
  `KeepOldest`, `KeepReferee`, `LeaseMajority`),
  `ClusterEventBus`, `elect_leader`, `is_converged`, cluster
  daemon with active gossip / leader-action / SBR ticks, pluggable
  `GossipTransport`.
- **Depth in progress**: distributed leader-election handover over
  remote, multi-DC tagging.

### `rakka-cluster-tools`

- **Aligned**: distributed pub/sub mediator (typed
  `publish_msg::<M>`, topic + group routing), cluster singleton,
  cluster client.

### `rakka-cluster-sharding`

- **Aligned**: `ShardAllocationStrategy` (`LeastShard`, `Pinned`),
  shard region, persistent (event-sourced) coordinator, three-phase
  handoff state machine, `PassivationTracker`, remember-entities,
  remote forwarder for cross-node entity messages.

### `rakka-cluster-metrics`

- **Aligned**: adaptive load balancing using cluster metrics
  snapshots.

### `rakka-distributed-data`

- **Aligned**: `GCounter`, `PNCounter`, `GSet`, `OrSet`,
  `LwwRegister`, `Flag`, `ORMap<K, V>`, `LWWMap<K, V>`,
  `PNCounterMap<K>`. `Replicator` with `subscribe(key, fn)` API
  (RAII `SubscriptionToken`), delta-CRDT propagation, durable
  store, consistency-level reads/writes.

## Persistence

### `rakka-persistence`

- **Aligned**: `Eventsourced` trait (Command → Events → State),
  recovery permitter, async snapshot store, `ReceivePersistent`
  closure helper.

### `rakka-persistence-query`

- **Aligned**: typed `Offset`, `events_by_tag`, `current_*`
  variants over journals.

### `rakka-persistence-tck`

- **Aligned**: journal + extended journal + concurrent + tag
  + snapshot suites. Every storage adapter must pass.

### Storage adapters

- `rakka-persistence-sql` — SQL backends with a shared schema and
  per-dialect migrations.
- `rakka-persistence-redis` — sorted-set journal, hash snapshot
  store, transactional batches.
- `rakka-persistence-mongodb` — indexed collections, atomic
  multi-document inserts, BSON payloads.
- `rakka-persistence-cassandra` — partitioned journal tables,
  prepared-statement replay.
- `rakka-persistence-aws` — DynamoDB single-table design with
  `E#` / `S#` sort keys, conditional writes.
- `rakka-persistence-azure` — Azure Table Storage with a
  SharedKeyLite client.

All adapters share the TCK as their conformance contract.

## Reactive streams

### `rakka-streams`

- **Aligned**: `Source` / `Flow` / `Sink` linear operators
  (map / filter / take / skip / scan / grouped / concat / prepend
  / delay / throttle / map_async / map_async_unordered /
  intersperse / buffer / wire_tap / tick / unfold / repeat / cycle
  / from_future / from_receiver), seven junctions
  (broadcast / merge / merge_all / concat / zip / zip_with /
  zip_with_index / partition / balance), framing
  (`Framing::delimiter`, `Framing::length_field`), file IO,
  TCP IO, `KillSwitch` and `RestartSource` external control,
  explicit backpressure (`SourceQueue`, `Sink::queue`,
  `OverflowStrategy`), runnable graphs + materializer.

## Hosting and integration

### `rakka-coordination`, `rakka-discovery`, `rakka-di`, `rakka-hosting`

- **Aligned**: lease primitives, pluggable service discovery,
  dependency-injection container, builder API for system + config +
  DI wiring.

## Observability

### `rakka-telemetry`, `rakka-dashboard`

- **Aligned**: probe surface across actors, dead letters, cluster,
  sharding, persistence, remote, streams, distributed data;
  Prometheus exporter, OpenTelemetry exporter (`metrics-otel`,
  OTLP gRPC / HTTP / stdout), live web UI over the running system,
  cluster-mode aggregator that fans out across peers, react + vite
  + tailwind SPA embedded into the dashboard binary.

## Tooling

- `rakka-profiler` — cross-runtime profiler (Rust + Python),
  shared JSON schema, baseline numbers.
- `cargo xtask audit` — anti-pattern + LOC sentinel ledger with
  baseline regression check.
- `cargo xtask verify` — composite gate for releases.
- `cargo xtask bump` — version bump that walks workspace package
  + internal dep version pins.

## Python bindings

A separate facade — `pip install rakka` — that re-exposes every
subsystem above through PyO3 plus a GIL-isolation layer that has no
direct prior-art equivalent.

- `rakka._native.ActorSystem`, `Actor`, `Props`, `ActorRef`,
  `Context`, plus the `PyActor` shim, `pinned` and
  `subinterpreter-pool` dispatchers.
- `InterpreterInstance`, `InterpreterQuota`, `InterpreterMetrics`
  for explicit GIL strategy control.
- `rakka.testkit`, `rakka.cluster`, `rakka.cluster_tools`,
  `rakka.cluster_sharding`, `rakka.ddata`, `rakka.persistence`,
  `rakka.streams`, `rakka.coordination`, `rakka.discovery`,
  `rakka.di`, `rakka.hosting`.
- C-extension compatibility registry (`rakka.compat`) that
  surfaces subinterpreter / nogil safety per-extension.
- Native streams materializer integration (`run_collect`,
  `run_fold` over the Rust streams DSL) plus the legacy Python-only
  `map_reduce` helper.
- Profiler mirror in `rakka.profiler` with the same scenarios as
  the Rust binary.

See [`docs/python.md`](docs/python.md) for the GIL strategy guide.

## Forward-looking

The roadmap items below are *new*, not catch-up:

- **GPU dispatcher** — a `Dispatcher` implementation whose backend
  is a CUDA stream. The same `Mailbox<T>` contract; messages are
  coalesced into a host buffer, scheduled as a kernel, and the
  results feed back into the actor system as reply messages. The
  dispatch boundary is the unification point — see
  [`docs/actors-and-agentic-computing.md`](docs/actors-and-agentic-computing.md).
- **Heterogeneous serialization** — a serializer that lays out
  messages in accelerator-friendly tensor layouts when the
  destination is a GPU dispatcher.
- **Actor-graph integrations for agentic systems** — supervised
  agent state graphs as first-class actors, with the existing
  cluster + persistence + observability stack.

## See also

- [`docs/parity.md`](docs/parity.md) — depth grades by crate.
- [`docs/audit-2026-04.md`](docs/audit-2026-04.md) — measured depth
  baseline.
- [`docs/full-port-plan.md`](docs/full-port-plan.md) — long-form
  architectural plan.
- [`PORTING.md`](PORTING.md) — alignment ledger.
