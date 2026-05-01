# Full Akka.NET → Rust port (idiomatic Rust)

> **Scope chosen:** maximalist parity — all 15 phases land. **Initial
> priority:** after Phase 0/1 foundations, drive **Persistence (11)**
> and **Cluster + Remote (5, 6)** in parallel. See "Recommended
> execution order" near the bottom.

## Context

`rakka` advertises a "full port of Akka.NET" with all phases landed
(`PORTING_TODO.md`), 174+ Rust tests, and a `parity.md` table that says
`yes` for every crate. A 2026-04-30 depth audit (see "Findings" below)
shows the reality is much shallower:

| Subsystem              | rakka LOC | akka.net LOC | Coverage |
|------------------------|-----------|--------------|----------|
| rakka-core             | 3,769     | 67,348       | ~5%      |
| rakka-cluster          | 792       | 13,984       | ~6%      |
| rakka-cluster-sharding | 261       | 20,921       | ~1%      |
| rakka-streams          | 2,002     | 70,515       | ~3%      |
| rakka-persistence      | 4,081     | 40,909       | ~10%     |
| rakka-remote           | 2,269     | 42,871       | ~5%      |
| rakka-distributed-data | 420       | 9,035        | ~5%      |
| rakka-cluster-tools    | 191       | 7,561        | ~2.5%    |

Most crates implement the **happy path of public types** but skip the
**active protocol machinery** (leader election, gossip dissemination,
shard rebalance, recovery permitter, substream algebra, quarantine
tracking, real plugins under the persistence-storage crates, etc.). A
handful of `panic!` / `__placeholder__` / "stub" sentinels remain in
production paths.

Beyond depth, the audit also flagged where rakka transliterates .NET
patterns instead of using Rust idioms (`Box<dyn Any>` sender erasure,
pervasive `Any::downcast<T>()`, `RwLock<HashMap<_, _>>` hubs in lieu of
actor-owned state, closure-boxed `Props` factories, no compile-time
supervision contract).

**Goal of this plan.** Take rakka from "happy-path skeleton" to a true
port of Akka.NET that (a) behaves equivalently for every documented
public surface, (b) is structured around Rust idioms — not C# ones —
and (c) is verified by running the Akka.NET TCK suites and the
test-corpus translated from upstream where applicable.

This is a multi-quarter program. The phases below are scoped so that
each lands as a coherent, shippable PR with its own acceptance gate.

## Idiomatic Rust principles (applied throughout)

These are non-negotiable invariants that any new code must respect; the
Phase 0 sweep retrofits the existing tree.

1. **No `Box<dyn Any>` in any public API.** Sender identity uses a
   typed enum (`Sender::Local(UntypedActorRef)`, `::Remote(RemoteRef)`,
   `::None`). Cross-actor messaging stays type-checked end to end.
2. **No `Any::downcast<T>()` in hot paths.** Replace registries that
   look up by `TypeId` with typed indices (`SerializerRegistry<M>`,
   `ExtensionId<E>`), or use sealed enums.
3. **Actor state, not `RwLock<HashMap>`.** Anywhere two or more tasks
   share state today, model it as an actor (Replicator, Mediator,
   ShardCoordinator, ClusterDaemon, EndpointManager). Reserve
   `RwLock` for read-mostly leaf caches.
4. **No `panic!`/`unwrap()`/`unimplemented!()` in library code.** All
   crates compile with `#![deny(clippy::unwrap_used,
   clippy::expect_used, clippy::panic, clippy::todo,
   clippy::unimplemented)]`. Errors travel via `thiserror` enums.
5. **Async-first; never block the runtime.** Use `tokio::spawn_blocking`
   for filesystem/CPU work; never call `std::thread::sleep`,
   `block_on`, or `std::sync::Mutex` on an `await` path.
6. **Persistent / immutable structures for snapshots.** Use `imbl`
   (or `arc-swap` of `Arc<T>`) for membership, gossip, replicator
   state — never clone-and-mutate large `HashMap`s under a lock.
7. **Type-state for lifecycle.** Phantom-typed `Context<A, S>` so
   `pre_start → handle* → post_stop` ordering is enforced at compile
   time; APIs only valid during certain phases (e.g. `become`,
   `unstash`) are gated.
8. **Compile-time supervision contracts.** Each child relationship is
   declared via a `SupervisorOf<Child>` impl on the parent, so the
   compiler rejects spawning a child whose error type the parent
   does not handle.
9. **`tracing` everywhere.** Replace ad-hoc `eprintln!` /
   `println!` (audit found ~40) with `tracing::{trace,debug,info,warn,
   error}` spans that include `actor.path`, `actor.uid`,
   `system.name`.
10. **Sealed traits over open inheritance.** Marker traits like
    `Actor`, `Message`, `Serializer` are sealed; downstream crates
    extend through composition, not by impl-bombing the sealed marker.
11. **Errors evolve with `#[non_exhaustive]`.** All public enums add
    the attribute so adding variants is non-breaking.
12. **Feature flags for optional integrations.** Persistence backends,
    serializers, transports, telemetry exporters all gated behind
    cargo features so a slim build is possible.

A new doc, `docs/idiomatic-rust.md`, codifies these and is referenced
from every PR template.

## Findings summary (audit, 2026-04-30)

Saved verbatim into `docs/audit-2026-04.md` so future planning has a
known baseline. Highlights worth carrying forward:

- **rakka-core**: 6 routers (akka.net has 20+); dispatchers are a
  thin wrapper over `tokio::spawn`; `IO` is a 43-LOC TCP/UDP stub
  vs. upstream's ~6.4k LOC TcpManager/UdpManager state machines;
  Stash is a marker trait whose storage lives in `Context`; FSM
  is trait-only with no DSL macros; Coordinated Shutdown phases
  exist but most upstream phases are no-ops.
- **rakka-cluster**: gossip / membership / reachability /
  vector-clock data structures are present (and the 5 SBR
  strategies are coded), but **no leader-election state
  machine, no active gossip dissemination loop, no
  ClusterDaemon, no convergence detection, no real
  heartbeat-driven reachability**. SBR strategies are unreachable
  from membership today.
- **rakka-cluster-sharding**: 42-LOC `ShardCoordinator` with no
  persistence, no rebalance, no allocation strategy, no
  passivation, no handoff, no remember-entities. Not usable in
  production.
- **rakka-streams**: linear DSL is solid, but no `groupBy`,
  `splitWhen`, `groupedWithin`, real `mapAsync` boundary, no
  `Supervision.Decider`, no `SubFlow`/`SubSource`, no `Hub`,
  no `StreamRefs`, no `recover`/`watchTermination`/`partition`/
  `balance`/`intersperse`/`delay`/... (~35 operators).
- **rakka-persistence**: no `Eventsourced` base, no
  `ReceivePersistent`, no `PersistentFSM`, no `RecoveryPermitter`,
  no async snapshots; `PersistenceQuery` only does
  `events_by_persistence_id`. The TCK is 161 LOC vs upstream's
  3,764 LOC. The 6 storage backend crates are placeholders.
- **rakka-remote**: solid TCP stack and handshake; missing
  reader/writer split, quarantine lifecycle, TLS, chunking,
  send-queue backpressure, LRU caches, multi-hop routing.
  `panic!("unexpected pdu …")` and `__placeholder__` address
  must be removed.
- **rakka-distributed-data**: 5 CRDTs in a `RwLock<HashMap>`
  Replicator with no delta gossip, no durable store, no
  read/write consistency levels. Missing `ORMap`, `LWWMap`,
  `PNCounterMap`, `Flag`, `ORMultiMap`.
- **rakka-cluster-tools**: `DistributedPubSub`, `ClusterClient`,
  `ClusterSingletonProxy` are essentially empty.
- **rakka-cluster-metrics**: 57-LOC snapshot holder; no collection,
  no gossip, no adaptive routing.
- **rakka-config**: TOML-only; no HOCON, no `include`, no
  reference resolution, no env-var substitution.
- **rakka-testkit**: ~192 LOC; no `expectAllOf`, no time
  manipulation, no event-stream filter assertions, no
  multi-node spec adapter.

## Phased plan

Each phase has: deliverables, idiomatic-Rust focus, acceptance gate,
key files, and rough sizing. "S" = ≤2 weeks, "M" = ≤6 weeks, "L" =
≤12 weeks of focused work for one experienced engineer.

### Phase 0 — Foundations & invariants (S)

**Deliverables**

- `docs/idiomatic-rust.md` (the 12 principles above + examples).
- `docs/audit-2026-04.md` (verbatim audit; baseline for parity tracking).
- Workspace `Cargo.toml`: enable `[workspace.lints]` with the deny set
  from principle #4; allow per-crate exceptions only with comment.
- CI gate: `cargo clippy --workspace --all-features -- -D warnings`
  becomes blocking.
- New `cargo xtask audit` task: counts `unwrap`/`panic`/`todo`/
  `unimplemented`/`Box<dyn Any>`/`__placeholder__` per crate; fails
  CI on regression.
- Replace `eprintln!`/`println!` with `tracing` (~40 sites).
- Update `parity.md` to track *depth* (a, b, c, d grade) per
  subsystem, not just presence.

**Files to touch**: `Cargo.toml` (workspace lints), `xtask/src/*`
(new audit task), every crate's `lib.rs` (lint allow-list with
TODOs), `docs/parity.md`.

**Acceptance**: CI green with new lint set; baseline audit metric
recorded in `docs/reports/audit-2026-04.json`.

### Phase 1 — Sender/receiver typing & supervision contract (M)

This phase rips out the largest .NET impedance leak before everything
downstream depends on the new shapes.

**Deliverables**

- New `Sender` enum (`Local(UntypedActorRef)`, `Remote(RemoteRef)`,
  `None`) replacing `Box<dyn Any + Send>` in `MessageEnvelope` and
  `Context::sender`.
- `ActorRef<M>::tell_from(&self, msg: M, sender: Sender)` and
  `tell(msg)` shortcut where sender is inferred via task-local.
- `SupervisorOf<C: Actor>` trait on parent actors; `spawn_child`
  bound to `Self: SupervisorOf<C>`. Default blanket impl falls back
  to `OneForOneStrategy::default()` for backwards compat.
- Type-state `Context<A, Phase = Running>` with phase markers
  `Starting`, `Running`, `Stopping`. APIs gated per phase.
- `props!` macro replacing `Props::create(|| Foo { … })` factories.
- `#[derive(Receive)]` macro generating typed message dispatch
  (eliminates hand-written match-on-enum boilerplate).
- Migrate **every** in-tree consumer (testkit, examples, all
  crates). No public API breakage avoidance — bump major version.

**Files to touch**: `crates/rakka-core/src/actor/{actor_ref,
context,message_envelope,props,supervisor_strategy}.rs`,
`crates/rakka-macros/src/lib.rs`, every example & test.

**Acceptance**: workspace builds & tests pass; `xtask audit` shows 0
`Box<dyn Any>` and 0 `Any::downcast` in `rakka-core` public surface;
`docs/idiomatic-rust.md` examples compile via `doctest`.

### Phase 2 — Configuration parity (S→M)

**Deliverables**

- `rakka-config` HOCON parser (or vendored `hocon` crate, augmented).
- `include "file"` resolution.
- Reference substitution `${path.to.value}`.
- Env-var substitution `${?ENV_NAME}`.
- Backwards-compatible TOML loader (kept as the recommended format
  for greenfield apps; HOCON exists for migrators).
- `Config::from_akka_reference()` loads the upstream
  `reference.conf` files baked into `resources/akka-reference/`.

**Files**: `crates/rakka-config/src/{lib,hocon,resolver}.rs`,
`resources/akka-reference/*.conf`.

**Acceptance**: parity test suite reads every `reference.conf` from
upstream and compares `Config` round-trip; round-trips passes for
all 13 reference files.

### Phase 3 — rakka-core depth (L)

**Deliverables (split into sub-PRs)**

- 3.1 **Dispatchers**: pluggable `Dispatcher` trait;
  ship `default-dispatcher` (Tokio multi-thread), `pinned-dispatcher`
  (per-actor task), `single-thread-dispatcher`,
  `affinity-pool-dispatcher`. Throughput / mailbox-batch knobs.
- 3.2 **Mailboxes**: `UnboundedMailbox`, `BoundedMailbox`,
  `BoundedPriorityMailbox`, `UnboundedPriorityMailbox`,
  `BoundedDequeBasedMailbox` (for stash overflow). Mailbox is
  selected via `Props::with_mailbox(MailboxType::…)`.
- 3.3 **Routing**: bring count from 6 → 20+ matching upstream:
  `Random`, `RoundRobin`, `Smallest-Mailbox`, `Broadcast`,
  `ScatterGatherFirstCompletion`, `TailChopping`,
  `ConsistentHashing`, `BalancingPool`, plus `Pool`/`Group`
  variants for each, plus `FromConfig`. Resizable pools with
  `DefaultResizer`.
- 3.4 **Pattern**: `gracefulStop`, `pipe_to`, `retry`,
  `CircuitBreaker` (state machine + listeners), `BackoffSupervisor`
  with `onFailure`/`onStop` strategies and reset-jitter.
- 3.5 **EventStream**: predicate-based subscriptions, `DeadLetters`
  routing, `UnhandledMessage` channel, `Logger` actor wiring.
- 3.6 **Stash**: real bounded stash storage owned by the actor (not
  `Context`), with `unstash_all` ordering guarantees and
  overflow → DeadLetters.
- 3.7 **FSM**: macro `#[fsm]` generating state transitions,
  `state(S) -> S2 when …` DSL, timers, replies.
- 3.8 **CoordinatedShutdown**: real upstream phases
  (`before-service-unbind`, `service-unbind`, `service-requests-done`,
  `service-stop`, `before-cluster-shutdown`,
  `cluster-sharding-shutdown-region`, `cluster-leave`,
  `cluster-exiting`, `cluster-exiting-done`, `cluster-shutdown`,
  `before-actor-system-terminate`, `actor-system-terminate`).
  Tasks register with an `id`, declare `dependsOn`, and time out.
- 3.9 **IO**: full `TcpManager`, `UdpManager` state machines
  matching upstream's `Tcp.Bind`/`Tcp.Connected`/`Tcp.Write` API
  shape (idiomatically — typed enums, no `IActorRef` casts).

**Files**: `crates/rakka-core/src/dispatch/`,
`crates/rakka-core/src/actor/mailbox/`,
`crates/rakka-core/src/routing/`,
`crates/rakka-core/src/pattern/`,
`crates/rakka-core/src/event/`,
`crates/rakka-core/src/io/`,
`crates/rakka-core/src/fsm/`,
`crates/rakka-core/src/coordinated_shutdown.rs`.

**Acceptance**: coverage ratio rakka-core / akka.net climbs to ≥35%;
new sub-folders carry per-feature integration tests; FSM macro has
trybuild compile-fail tests.

### Phase 4 — rakka-testkit depth (S)

**Deliverables**

- `TestProbe` matchers: `expect_msg`, `expect_msg_pf`, `expect_msg_class`,
  `expect_no_msg`, `expect_terminated`, `expect_all_of`,
  `receive_while`, `receive_n`, `fish_for_message`.
- `EventFilter::error/warning/info/custom` with optional `occurrences`
  / `start`.
- Virtual time scheduler (`TestScheduler`) for deterministic timer
  tests.
- `MultiNodeSpec` harness: spawn N processes with shared barriers,
  used by cluster/sharding/persistence integration tests.

**Files**: `crates/rakka-testkit/src/{probe,event_filter,
test_scheduler,multinode}.rs`.

**Acceptance**: testkit's own self-tests (`cargo test -p
rakka-testkit`) cover every matcher; `MultiNodeSpec` runs the
existing remote integration test as a 3-node cluster.

### Phase 5 — rakka-remote depth (M)

**Deliverables**

- Reader / writer task split per peer (parallel inbound vs outbound).
- Quarantine lifecycle: `Quarantined → Tombstoned`, gossip-driven
  re-association, configurable purge interval.
- TLS via `rustls`; cert verification + SNI. Optional feature.
- Message chunking for payloads > `maximum-frame-size` (default 256
  KiB), reassembly on read.
- Send-queue with bounded back-pressure, `OverflowStrategy` mirroring
  the streams crate.
- LRU caches for `ActorPath` ↔ `RemoteRef` and serializer-id ↔
  manifest.
- Replace `panic!("unexpected pdu …")` with typed
  `RemoteError::UnknownPdu`; remove `__placeholder__` root address.
- `RemoteDeployer` ships fully-typed Props via a new `RemoteProps`
  trait + manifest registry (closes the dangling note in
  `PORTING_TODO.md`).
- Failure detector matches upstream shape: per-address phi-accrual
  with configurable `acceptable-heartbeat-pause`,
  `heartbeat-interval`, `threshold`, `min-std-deviation`.

**Files**: `crates/rakka-remote/src/{endpoint/{reader,writer,
quarantine},transport/tls,serializer/cache,deployer}.rs`.

**Acceptance**: existing `tests/two_process.rs` extended into
4-process suite; new TLS handshake test; chunking round-trips a
10-MiB payload; `xtask audit` shows 0 `panic!` in
`rakka-remote/src/`.

### Phase 6 — rakka-cluster depth (L)

**Deliverables**

- `ClusterDaemon` actor — single owner of mutable cluster state.
- Active gossip protocol: push/pull, periodic gossip tick,
  `GossipStatus`/`GossipEnvelope` PDUs over remote.
- Convergence detection (no member is unreachable from anyone) → mark
  `ConvergenceReached`.
- Leader election state machine — deterministic by member ordering;
  drives `Joining → Up`, `Leaving → Exiting → Removed`,
  `Down` transitions.
- Heartbeat sender / receiver actors; reachability driven by failure
  detector wired in Phase 5.
- Cluster events bus: `MemberUp`, `MemberLeft`, `UnreachableMember`,
  `ReachableMember`, `LeaderChanged`, `ClusterShuttingDown` published
  on `system.event_stream`.
- Wire SBR strategies into membership decisions (today they are
  unreachable from runtime).
- Node roles, weighted role selection.
- Multi-DC awareness (data-center role, cross-DC heartbeat slow path).

**Files**: `crates/rakka-cluster/src/{daemon,gossip_loop,
leader,heartbeat,events,sbr_runtime}.rs`.

**Acceptance**: `MultiNodeSpec` test boots 5 nodes, joins them, kills
2, verifies leader re-election and SBR decision (per strategy);
`docs/cluster.md` describes the protocol and shows convergence
under partition.

### Phase 7 — rakka-cluster-tools depth (M)

**Deliverables**

- `DistributedPubSub.Mediator`: per-node mediator actor; topic
  registration; `Publish`, `Subscribe`, `SubscribeAck`, `Send`,
  `SendToAll`; group-membership routing; gossip of topic state via
  Phase 6's protocol.
- `ClusterSingletonManager` with handover protocol on
  oldest-changed event; `ClusterSingletonProxy` with buffer-while-
  unreachable behaviour.
- `ClusterClient` + `ClusterReceptionist`: contact-point discovery,
  initial-contacts list, retry/backoff, `Send`/`Publish`/
  `SendToAll` from non-cluster nodes.

**Files**: `crates/rakka-cluster-tools/src/{pub_sub/{mediator,
topic,subscriber},singleton/{manager,proxy,handover},
client/{client,receptionist}}.rs`.

**Acceptance**: 3-node cluster pub/sub roundtrip; singleton
fail-over test (kill oldest → next-oldest takes over within 2s);
external client test.

### Phase 8 — rakka-distributed-data depth (M)

**Deliverables**

- Add CRDTs: `ORMap`, `LWWMap`, `PNCounterMap`, `Flag`, `ORMultiMap`,
  `LWWRegister` (already there but verify monotonic merge), `GCounter`
  delta-CRDT semantics.
- Delta-CRDT propagation: each CRDT exposes `delta()`; Replicator
  ships deltas instead of full state on update.
- Read/Write consistency levels: `local`, `from(n)`, `majority`,
  `all`, with timeouts.
- `Replicator` becomes a real actor (no `RwLock<HashMap>`); peers
  exchange via Phase 6's gossip transport.
- Durable storage trait + `lmdb` (or `redb`) backend for crash
  recovery.
- Subscriber API: `Replicator::Subscribe(key, subscriber)` with
  notification on change.

**Files**: `crates/rakka-distributed-data/src/{crdt/{or_map,
lww_map,pn_counter_map,flag,or_multi_map}.rs,replicator/{actor,
delta_propagation,consistency,durable}.rs,subscriber.rs}`.

**Acceptance**: 5-node convergence test for each CRDT; durable
store survives restart; `Subscribe` delivers diff events in
order.

### Phase 9 — rakka-cluster-sharding depth (L)

**Deliverables**

- `PersistentShardCoordinator` (event-sourced via
  rakka-persistence): allocation events, snapshot every N events.
- `DDataShardCoordinator` alternative (state in DistributedData
  via Phase 8) selectable by config.
- `ShardAllocationStrategy` trait + `LeastShardAllocationStrategy`
  default + `ConsistentHashingShardAllocationStrategy`.
- Rebalance algorithm: detect imbalance, request handoff, wait for
  shard to drain.
- Passivation: idle entity timeout sends `Passivate(stop_message)`
  → coordinator, entity stops, future messages buffer & restart.
- Remember-entities: persist active entity IDs so they restart on
  shard re-allocation.
- Handoff protocol: 3-phase (begin → stop → start-elsewhere) with
  message buffering on the source region.
- `ShardRegion::set_remote_forwarder` (already present) becomes the
  default path; local-only mode is a degenerate case.
- Multi-DC sharding (optional, gated behind feature).

**Files**: `crates/rakka-cluster-sharding/src/{coordinator/
{persistent,ddata,allocation,rebalance},shard/{passivation,
remember_entities,handoff},region/{forwarder,buffer}}.rs`.

**Acceptance**: `MultiNodeSpec` test: 3-node, 100 entities,
balanced; kill 1 node → entities migrate, no message loss;
remember-entities survives full-cluster restart; SQL persistence
backend used by the persistent coordinator.

### Phase 10 — rakka-cluster-metrics depth (S→M)

**Deliverables**

- Sample CPU / heap / disk via `sysinfo`.
- Gossip metrics through Phase 6's transport (separate metric topic).
- `AdaptiveLoadBalancingRouter` consuming the metric stream.
- Subscription API for telemetry (already wired into `rakka-telemetry`).

**Files**: `crates/rakka-cluster-metrics/src/{collector,
gossip,adaptive_router,subscriber}.rs`.

**Acceptance**: 3-node cluster sees neighbour metrics within 5s;
adaptive router skews traffic toward least-loaded node in a soak
test.

### Phase 11 — rakka-persistence depth (L)

**Deliverables**

- `Eventsourced` base trait + `#[derive(Eventsourced)]` macro:
  `command(C)` → `Vec<Event>`; `apply(E)` → state mutation;
  `recovery_completed`.
- `ReceivePersistent` (closure-style API for ad-hoc actors).
- `PersistentFSM` matching the Phase-3 `#[fsm]` macro but with
  event sourcing.
- `RecoveryPermitter`: bounds concurrent recoveries (config:
  `max-concurrent-recoveries`).
- Async snapshot save during normal operation; serialized async load
  during recovery.
- `PersistenceQuery` real streaming API (built on rakka-streams):
  `events_by_persistence_id(id, from, to)`,
  `current_events_by_persistence_id(id, from, to)`,
  `events_by_tag(tag, offset)`, `current_events_by_tag(tag, offset)`,
  `all_persistence_ids()`, `current_persistence_ids()`,
  `events_by_persistence_id_typed`, journal-side `Offset` types.
- Real plugin implementations under
  `rakka-persistence-{sql,redis,mongodb,cassandra,aws,azure}` (audit
  shows these are placeholder crates today). Each must:
    - implement `Journal` + `SnapshotStore` traits;
    - pass the upstream-translated TCK below;
    - run in CI's `persistence-integration` job.
- Expanded TCK matching upstream (3,764 LOC → port to Rust):
  `JournalSpec`, `JournalSerializationSpec`,
  `JournalPerfSpec` (criterion-driven), `SnapshotStoreSpec`,
  `EventsByTagSpec`, `AllEventsSpec`, plus the corresponding
  `Current*` variants.

**Files**: `crates/rakka-persistence/src/{eventsourced,
receive_persistent,persistent_fsm,recovery_permitter,async_snapshot}.rs`,
`crates/rakka-persistence-query/src/{stream,offset,journal_provider}.rs`,
`crates/rakka-persistence-tck/src/{journal_spec,journal_serialization,
journal_perf,snapshot_spec,events_by_tag_spec,all_events_spec}.rs`,
`crates/rakka-persistence-{sql,redis,mongodb,cassandra,aws,azure}/src/*`.

**Acceptance**: every backend crate passes the full TCK in CI;
`xtask parity` shows all six providers green; query layer streams
1M events per backend in the perf TCK without unbounded buffering.

### Phase 12 — rakka-streams depth (L)

**Deliverables (sub-PRs by operator family)**

- 12.1 **Substreams**: `groupBy`, `splitWhen`, `splitAfter`,
  `prefixAndTail`, `flatMapPrefix`. Introduces `SubFlow`/`SubSource`
  algebra; backed by per-key sub-graph materialization.
- 12.2 **Time-windowed**: `groupedWithin(n, dur)`,
  `batchWithin`, `idleTimeout`, `keepAlive`, `delay`,
  `delayWith`, `pulse`.
- 12.3 **Async-boundary stages**: explicit `.async()` + real
  `mapAsync(parallelism)` / `mapAsyncUnordered` with internal
  ordering buffer.
- 12.4 **Supervision**: `Supervision::Decider` per stage
  (`Stop`, `Resume`, `Restart`); `withAttributes(supervisionStrategy(…))`.
- 12.5 **Hub patterns**: `BroadcastHub`, `MergeHub`, `PartitionHub`
  with consumer-side and producer-side dynamic attach.
- 12.6 **Routing junctions**: `partition(n, fn)`, `balance(n)`,
  `unzip`, `unzipWith`, `concatAllSourceIfMissing`, `interleave`,
  `intersperse`, `extrapolate`, `expand`.
- 12.7 **Recovery**: `recover`, `recoverWith`,
  `recoverWithRetries`, `mapError`, `divertTo`, `wireTap`.
- 12.8 **Lifecycle**: `watchTermination`, `monitor`, `log`,
  `addAttributes`, `named`, `withMaterializerAttributes`.
- 12.9 **StreamRefs**: `SourceRef[T]`/`SinkRef[T]` shipped over
  remoting (Phase 5), enabling stream-of-stream across nodes.
- 12.10 **HTTP integration** as a separate crate
  `rakka-http` built on `hyper` (akka-http parity is its own
  multi-quarter effort; carved out so this phase isn't blocked).

**Files**: `crates/rakka-streams/src/{substream,timed,async_boundary,
supervision,hub,routing,recovery,lifecycle,stream_ref}.rs`.

**Acceptance**: per sub-PR, a property-based test (proptest) covers
the operator vs a reference Rust iterator/stream where one exists;
substream and StreamRef require multi-node integration tests.

### Phase 13 — Idiomatic-Rust cross-cutting pass (M)

This is a sweep across crates after Phases 1–12 land, retrofitting any
spots that still leak .NET shapes (audit will surface them):

- Replace remaining `RwLock<HashMap>` patterns with actors
  (e.g. `ServiceContainer`, `ExtensionRegistry` — but only if
  contention measurements justify it; Phase 0 lints will surface
  candidates).
- Convert remaining `Box<dyn Trait + Send + Sync>` collections to
  enum-dispatched alternatives where the variant set is closed.
- Adopt `imbl::HashMap` / `imbl::Vector` for hot snapshot paths
  (gossip, replicator, sharding allocation).
- GATs on `Actor` for borrowed-message handlers
  (`type Message<'a>` returning a deserialized view rather than
  an owned value) — only where benchmarks justify it.
- `#[non_exhaustive]` on every public enum.
- Sealed-trait pattern on `Actor`, `Message`, `Serializer`,
  `Transport`.

**Acceptance**: `xtask audit` baseline metrics improve in every
category; no public API uses `Box<dyn Any>`.

### Phase 14 — Documentation, examples, migration guide (M)

**Deliverables**

- `docs/migrating-from-akka-net.md`: idiomatic translation table
  (Props, ActorRef, IUntypedActorContext → Context, Receive →
  `#[derive(Receive)]`, Tell → `tell`, Ask → `ask`, …).
- `docs/architecture.md`: which Akka.NET concepts have direct
  analogues, which were re-shaped, which are intentionally absent
  (Hyperion, DotNetty, F# DSL — already documented in
  `PORTING.md`, expand here).
- Production-grade examples added under `examples/`:
    - `examples/sharded-keyvalue` — uses Phase 9 sharding +
      Phase 11 SQL persistence + Phase 8 DData;
    - `examples/event-sourced-banking` — Eventsourced API with
      snapshots and recovery;
    - `examples/cluster-pubsub-chat` — Phase 7 pub/sub across N
      nodes.
- `docs/parity.md` regenerated with per-subsystem **depth grade**
  (a/b/c/d/f) plus the % LOC ratio.

**Acceptance**: every example compiles and is exercised by an
integration test; `cargo doc --workspace --no-deps` runs
warning-free.

### Phase 15 — Verification, release pipeline & 1.0 candidate (M)

The release pipeline is now driven end-to-end by:

* **`cargo xtask bump <patch|minor|major|--pre <id>|--set <ver>>`** —
  single-source bump for `Cargo.toml` (workspace) + `pyproject.toml`
  + `Cargo.lock`. Round-trip verified.
* **`.github/workflows/version-bump.yml`** — every push to `main`
  picks a SemVer bump from Conventional-Commit subjects (or honors
  `Release-As: <ver>` overrides), runs the xtask, autocommits as
  `chore(release): vX.Y.Z`, and pushes a `vX.Y.Z` tag.
* **`.github/workflows/release.yml`** — fires on `v*` tags. Runs
  `cargo xtask verify`, builds release binaries
  (`rakka-dashboard`, `rakka-profiler`) for `x86_64-unknown-linux-gnu`
  and `aarch64-apple-darwin`, creates a GitHub Release with
  auto-generated notes, and publishes every publishable crate to
  crates.io in dependency order.
* **Claude `version-bump` skill** at
  `~/.claude/skills/version-bump/SKILL.md` — usable from any session
  where the user says "bump the version" / "cut a release".



**Deliverables**

- All persistence backends pass full TCK in CI (already gated by
  Phase 11).
- `MultiNodeSpec` suite covers cluster (Phase 6),
  cluster-tools (Phase 7), DData (Phase 8), sharding (Phase 9),
  remote (Phase 5).
- 24-hour soak test in CI nightly: 5-node cluster, 100k entities,
  rolling node restart every 30 minutes.
- Public API stability review; semver-checks via `cargo-semver-checks`.
- Tag `1.0.0-rc.1`; release order from `release.yml`.
- Final `docs/parity.md` with no `f` grades; `docs/audit-2026-04.md`
  re-run shows no regressions on its baseline metrics.

**Acceptance**: 7-day soak passes; semver-checks clean; `parity.md`
all `a` or `b`.

## Cross-cutting tracks (parallelizable)

- **Telemetry / dashboard / Python / profiler.** These phases are
  already shipped (P14 complete, Python phases done, profiler done).
  As Phases 1–12 add new subsystems, hooks into `rakka-telemetry`
  must be added in the same PR; Python bindings (`pycore`,
  `pycluster`, …) follow with a one-PR-per-phase delay so Python
  can keep up with the public Rust API. Phase P3 (`pyremote` codecs)
  is unblocked once Phase 5 lands; ship it alongside Phase 5.
- **Upstream sync.** `cargo xtask sync-upstream` runs quarterly;
  bump the per-crate `PORTING.md` row whenever a phase merges. The
  CI job already exists.
- **Akka.NET TCK port.** Bring upstream's xUnit specs into
  `rakka-persistence-tck`, `rakka-streams-testkit` (new crate) and
  `rakka-testkit` as Rust tests; this is per-phase work, not a
  separate phase.

## Recommended execution order (chosen)

Scope is **maximalist parity** — all 15 phases land. After the
foundation work, the priority is **Persistence (11)** and
**Cluster + Remote (5 → 6)** before the rest.

Concrete order with parallelization opportunities:

1. **Phase 0** — foundations & lints. Single PR. Blocks nothing
   but sets the bar.
2. **Phase 1** — sender/supervision rework. Single PR; touches
   every crate but is mechanical after the new `Sender`/
   `SupervisorOf` shapes are agreed. Blocks all later phases'
   API shapes.
3. **Phase 2** (HOCON) — small, parallel to Phase 1. Lands when
   ready; no consumer until tests opt in.
4. **Phase 4** (testkit) — runs in parallel with Phase 1; the
   `MultiNodeSpec` harness is needed by Phases 5/6/9/11.
5. **Phase 5** (remote depth) — first real-systems phase.
   Required by Phases 6, 7, 9. Pull the Python `pyremote` codecs
   (deferred P3) into the same release train.
6. **Phase 11** (persistence depth) — runs in parallel with
   Phase 5 from the start (independent code paths). Critical
   because it unblocks the persistent shard coordinator in
   Phase 9 and because the upstream TCK port is the longest tail.
   Sub-PR order: Eventsourced + RecoveryPermitter → query
   streaming → backend implementations (SQL → Redis → Mongo →
   Cassandra → AWS → Azure) → expanded TCK runs.
7. **Phase 6** (cluster depth) — starts as soon as Phase 5
   reader/writer split lands. Sub-PR order: ClusterDaemon →
   gossip loop → heartbeat/reachability → leader election →
   convergence → SBR runtime wiring → events bus.
8. **Phase 3** (rakka-core depth) — runs in parallel with
   Phase 6 once Phase 1 is in. Sub-PRs 3.1–3.9 can be picked
   up by separate contributors.
9. **Phase 7** (cluster-tools) — after Phase 6.
10. **Phase 8** (DData) — after Phase 6 (uses the gossip
    transport). Provides the DData shard coordinator option
    needed by Phase 9.
11. **Phase 9** (sharding) — after Phases 6, 8, 11.
12. **Phase 10** (cluster-metrics) — after Phase 6.
13. **Phase 12** (streams operators) — independent of the
    cluster track; can run in parallel from Phase 1 onward.
    StreamRefs (12.9) waits on Phase 5.
14. **Phase 13** (idiomatic-Rust sweep) — once Phases 1–12 are
    in, retrofit anything the audit still flags.
15. **Phase 14** (docs/migration/examples) — runs continuously
    from Phase 1; consolidated near the end for the
    migration guide.
16. **Phase 15** (verification, 1.0-rc) — gating phase; nothing
    after this.

Parallel-track timeline at 3 contributors:

| Quarter | Track A (cluster)            | Track B (persistence) | Track C (streams + core) |
|---------|------------------------------|-----------------------|--------------------------|
| Q1      | Phase 0, 1, 4 (shared)       | Phase 0, 1 (shared)   | Phase 0, 1, 2 (shared)   |
| Q2      | Phase 5                      | Phase 11 sub-PRs 1-3  | Phase 3.1–3.4, 12.1–12.3 |
| Q3      | Phase 6                      | Phase 11 sub-PRs 4-6  | Phase 3.5–3.9, 12.4–12.6 |
| Q4      | Phase 7, 8                   | Phase 11 TCK + perf   | Phase 12.7–12.9          |
| Q5      | Phase 9, 10                  | (support sharding)    | Phase 12.10 (rakka-http) |
| Q6      | Phase 13, 14, 15 (all hands) |                       |                          |

## Risks & sequencing constraints

- **Phase 1 is on the critical path.** Sender/supervision rework
  changes signatures across every other crate; do this before
  Phases 3–12 to avoid double work.
- **Phase 5 (remote) blocks Phases 6, 7, 9** — cluster gossip,
  pub/sub, and sharding handoff all need real reader/writer-split
  remoting.
- **Phase 6 (cluster) blocks Phases 7, 8, 9, 10** — gossip is the
  substrate for everything else.
- **Phase 11 (persistence) blocks Phase 9's persistent
  coordinator.** A DData coordinator can be the interim path
  if Phase 8 lands first.
- **HOCON parser scope creep** (Phase 2). Time-box; if a vendored
  parser is impractical, ship TOML-only with a translator script
  for upstream `reference.conf` files.
- **Akka-HTTP is a separate beast.** Phase 12.10 carves it out
  intentionally; do not block streams parity on HTTP parity.

## Verification (end-to-end)

- `cargo build --workspace --all-features`
- `cargo test --workspace --all-features`
- `cargo clippy --workspace --all-features -- -D warnings`
- `cargo xtask audit` (no regressions vs `docs/reports/audit-2026-04.json`)
- `cargo xtask parity` (no `f` depth grades after Phase 15)
- `cargo xtask multinode --suite all` (Phase-4 harness, runs
  cluster/sharding/pubsub/ddata/remote)
- `cargo xtask soak --hours 24` (Phase 15 nightly)
- Persistence integration matrix (existing `persistence-integration`
  CI job, expanded by Phase 11)
- Python: `pytest python/tests -v` after each Python PR
- Docs build: `mkdocs build --strict`

## Critical files (will be touched)

- `Cargo.toml` (workspace lints, new feature flags) — Phase 0.
- `crates/rakka-core/src/actor/{actor_ref,context,message_envelope,
  props,supervisor_strategy}.rs` — Phase 1.
- `crates/rakka-macros/src/lib.rs` (props!, derive(Receive),
  fsm!, derive(Eventsourced)) — Phases 1, 3, 11.
- `crates/rakka-config/src/{lib,hocon,resolver}.rs` — Phase 2.
- `crates/rakka-core/src/{dispatch,actor/mailbox,routing,pattern,
  event,io,fsm,coordinated_shutdown}/` — Phase 3.
- `crates/rakka-testkit/src/{probe,event_filter,test_scheduler,
  multinode}.rs` — Phase 4.
- `crates/rakka-remote/src/{endpoint,transport,serializer,deployer}/`
  — Phase 5.
- `crates/rakka-cluster/src/{daemon,gossip_loop,leader,heartbeat,
  events,sbr_runtime}.rs` — Phase 6.
- `crates/rakka-cluster-tools/src/{pub_sub,singleton,client}/`
  — Phase 7.
- `crates/rakka-distributed-data/src/{crdt,replicator,subscriber}.rs`
  — Phase 8.
- `crates/rakka-cluster-sharding/src/{coordinator,shard,region}/`
  — Phase 9.
- `crates/rakka-cluster-metrics/src/{collector,gossip,
  adaptive_router}.rs` — Phase 10.
- `crates/rakka-persistence{,-query,-tck,-sql,-redis,-mongodb,
  -cassandra,-aws,-azure}/src/*` — Phase 11.
- `crates/rakka-streams/src/{substream,timed,async_boundary,
  supervision,hub,routing,recovery,lifecycle,stream_ref}.rs`
  — Phase 12.
- `docs/{idiomatic-rust,audit-2026-04,migrating-from-akka-net,
  architecture,parity,full-port-plan}.md` — Phases 0, 14.
- `xtask/src/{audit,parity,multinode,soak}.rs` — Phases 0, 15.

## What's out of scope

- Wire compatibility with JVM/CLR Akka (already declared in
  `PORTING.md`).
- Hyperion CLR-binary serialization (placeholder crate stays a
  Serde/bincode shim).
- F# DSL (`Akka.FSharp`) — `rakka-macros` covers ergonomics.
- Akka-HTTP (carved out as `rakka-http`, separate effort).
- Aeron transport (akka.net itself doesn't ship it; Artery is JVM-only).
- Cluster-multi-hop gossip routing (deferred unless a use case
  emerges).

## Sizing summary

- Phase 0: S (≤2w)
- Phase 1: M (≤6w) — critical path
- Phase 2: S→M
- Phase 3: L (≤12w) — split into sub-PRs 3.1–3.9
- Phase 4: S
- Phase 5: M
- Phase 6: L — split by sub-system (daemon, gossip, leader, heartbeat)
- Phase 7: M
- Phase 8: M
- Phase 9: L — split (persistent coord, ddata coord, allocation,
  rebalance, passivation, remember-entities, handoff)
- Phase 10: S→M
- Phase 11: L — split per backend + TCK
- Phase 12: L — split per operator family (12.1–12.10)
- Phase 13: M
- Phase 14: M
- Phase 15: M

Total nominal effort: ≈18 engineer-months for a single experienced
contributor; ≈6 calendar months at 3 contributors with parallel
phases (3↔4, 8↔11, 12 sub-PRs in parallel after Phase 1 lands).
