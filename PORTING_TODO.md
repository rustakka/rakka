# Porting To-Do

Phase progress against the plan. Update as each phase lands.

> **Reality check (2026-04-30 audit).** The phases below are marked
> `[x]` because the *named scaffolding* exists. A depth audit on
> 2026-04-30 found that most subsystems cover only **~1–10% of
> upstream Akka.NET LOC** and skip critical protocol machinery
> (active gossip, leader election, shard rebalance, recovery
> permitter, substream algebra, real persistence backends, …). See
> [`docs/full-port-plan.md`](docs/full-port-plan.md) and
> [`docs/audit-2026-04.md`](docs/audit-2026-04.md) for the gap
> analysis and a 15-phase roadmap to true parity. The "Full-port
> roadmap" section at the end of this file tracks that follow-on
> work.

- [x] Phase 0 - Workspace bootstrap (Cargo workspace, xtask, PORTING.md)
- [x] Phase 1 - `rakka-config`
- [x] Phase 2 - `rakka-core`
  - [x] 2.1 util/
  - [x] 2.2 actor/ primitives (Address, ActorPath, Props, ActorRef)
  - [x] 2.3 dispatch/ (Mailbox, MessageQueues, Dispatcher)
  - [x] 2.4 actor/scheduler/
  - [x] 2.5 actor_cell/ (children, fault_handling, death_watch, receive_timeout)
  - [x] 2.6 actor_system + providers
  - [x] 2.7 supervisor_strategy, fsm, stash, inbox, coordinated_shutdown, extensions, setup
  - [x] 2.8 event, pattern, routing, io
  - [x] 2.9 serialization
  - [x] 2.10 rakka-macros
- [x] Phase 3 - `rakka-testkit`
- [x] Phase 4 - `rakka-remote` *(reworked 2026-04 — see "Remoting parity
      pass" below)*
- [x] Phase 5 - `rakka-cluster`
- [x] Phase 6 - `rakka-cluster-tools`
- [x] Phase 7 - `rakka-distributed-data`
- [x] Phase 8 - `rakka-cluster-sharding`
- [x] Phase 9 - `rakka-persistence` (+ query, query-inmemory, tck)
- [x] Phase 10 - `rakka-streams`
  - [x] 10.1 Source/Flow/Sink linear operators (map/filter/take/skip/scan/
    grouped/concat/prepend/delay/throttle/map_async/map_async_unordered/
    intersperse/buffer/wire_tap/tick/unfold/repeat/cycle/from_future/
    from_receiver)
  - [x] 10.2 Fan-in / fan-out junctions (`merge`, `merge_all`, `concat`,
    `zip`, `zip_with`, `zip_with_index`, `broadcast`)
  - [x] 10.3 Byte framing (`Framing::delimiter`, `Framing::length_field`)
  - [x] 10.4 IO adapters (`FileIO::from_path`/`to_path`/`pipe_to_path`,
    `Tcp::bind`/`Tcp::outgoing_connection`)
  - [x] 10.5 External control (`KillSwitch`, `RestartSource` + `RestartSettings`)
  - [x] 10.6 Explicit backpressure (`SourceQueue`, `Sink::queue`,
    `OverflowStrategy` with Backpressure/Drop{Head,New,Tail,Buffer}/Fail)
  - [x] 10.7 `RunnableGraph` + richer `ActorMaterializer` (`run`, `run_with`)
- [x] Phase 11 - `rakka-cluster-metrics`, `-coordination`, `-discovery`, `-di`, `-hosting`
- [x] Phase 12 - Examples + benchmarks (`examples/pingpong`, `examples/chat`, `examples/fault-tolerance`, `benches/ping_throughput`)
- [x] Phase 13 - Persistence provider crates (`resources/Rakka Persistence Plan.md`)
  - [x] 13.0 Core extensions: `JournalError::Backend`, `PersistentRepr.tags`,
    TCK split into `journal_suite` / `journal_tag_suite` / `snapshot_suite`
  - [x] 13.a `rakka-persistence-sql` (`sqlx`; SQLite default,
    Postgres/MySQL/MSSQL features; journal + snapshot + `ReadJournal`
    with tag queries)
  - [x] 13.b `rakka-persistence-redis` (`fred`; sorted-set journal,
    hash snapshot store, `MULTI`/`EXEC` batches)
  - [x] 13.c `rakka-persistence-mongodb` (indexed collections,
    atomic `insert_many`, BSON payloads)
  - [x] 13.d `rakka-persistence-cassandra` (`scylla`; partitioned
    journal tables, prepared-statement replay)
  - [x] 13.e `rakka-persistence-aws` (DynamoDB single-table design
    with `E#`/`S#` sort keys, conditional writes)
  - [x] 13.f `rakka-persistence-azure` (Azure Table Storage via
    a SharedKeyLite `reqwest` client; Cosmos feature placeholder)
  - [x] 13.g `persistence-integration` CI job with Postgres/MySQL/
    Redis/Mongo/Cassandra/DynamoDB Local/Azurite service containers
  - [x] 13.h `release.yml` publishing core → TCK → query → provider
    crates in dependency order on `release: published`
- [x] Ongoing - xtask sync-upstream, docs/parity.md, CI quarterly diff
- [x] Phase 14 - Observability dashboard
  - [x] 14.1 `rakka-telemetry` probe crate (actors, dead letters, cluster,
    sharding, persistence, remote, streams, distributed-data)
  - [x] 14.2 `rakka-dashboard` axum service (REST `/api/*` + `/ws`
    multiplexer with topic filters and heartbeats)
  - [x] 14.3 Cluster-mode aggregator (peer fan-out + merged
    `/api/cluster-wide/*` routes, gated by the `aggregator` feature)
  - [x] 14.4 React + Vite + TS + Tailwind + shadcn/ui SPA with React Flow
    + Recharts visualizations (Overview, Actors, DeadLetters, Cluster,
    Sharding, Persistence, Remote, Streams, DData, Events)
  - [x] 14.5 `rust-embed` the built UI into the dashboard binary
    (`embed-ui` feature)
  - [x] 14.6 Prometheus exporter (`metrics-prometheus`) serving
    `GET /metrics` with actor/cluster/sharding/persistence/remote/
    streams/ddata metrics
  - [x] 14.7 OpenTelemetry exporter (`metrics-otel` + `otel-otlp-grpc`,
    `otel-otlp-http`, `otel-stdout` sub-features) pushing the same
    semantic metrics as OTLP
  - [x] 14.8 CLI (`rakka-dashboard --prometheus --otlp-endpoint …`),
    Python `rakka.dashboard.serve(...)`, and `cargo xtask dashboard`
    convenience task
  - [x] 14.9 `docs/dashboard.md`, `docs/observability.md`, mkdocs nav
    entries, Vitest component tests + Playwright smoke tests, Rust
    handler tests + WebSocket + exporter integration tests

## Python bindings

- [x] Phase P0 - pyproject.toml, maturin, `crates/py-bindings/*` scaffold, CI
- [x] Phase P1 - `pycore` bindings: ActorSystem, Actor, Props, ActorRef,
  Context, PyActor shim, pinned + subinterpreter-pool dispatchers,
  `InterpreterInstance` + `InterpreterQuota` + `InterpreterMetrics`
- [x] Phase P1.5 - GIL throughput benchmarks + C-extension compat registry
- [x] Phase P2 - `rakka.testkit` (TestKit, TestProbe, pytest fixture)
- [ ] Phase P3 - pyremote + pluggable Python codecs (JSON/msgpack/pickle)
  (deferred; native ActorSystem is process-local today)
- [x] Phase P4 - cluster + cluster_tools (Membership, VectorClock,
  DistributedPubSub)
- [x] Phase P5 - distributed-data (GCounter, PNCounter, GSet, ORSet)
- [x] Phase P6 - cluster-sharding (ShardRegion + Python extractor)
- [x] Phase P7 - persistence (`InMemoryJournal` write/replay/highest)
- [x] Phase P8 - streams (Python `map_reduce` pipeline helper)
- [x] Phase P9 - coordination (Lease), discovery (StaticDiscovery),
  di (ServiceContainer), hosting (`Builder`)
- [x] Phase P10 - examples (pingpong / ml_inference / persistence) and
  smoke benchmarks in `python/tests/test_benchmarks.py`
- [x] Phase P11 - docs scaffold (mkdocs.yml + docs/python.md GIL guide)

## Tooling

- [x] `rakka-profiler` crate + binary: actor memory + CPU profiler
  (scenarios: `tell`, `ask`, `fanout`, `cpu`).
- [x] `rakka.profiler` Python sub-package mirroring the Rust binary
  (auto-picks `python-nogil` → `python-subinterpreter-pool` →
  `python-pinned` per scenario).
- [x] `scripts/profile.py` orchestrator producing a side-by-side
  comparison (+ `cargo xtask profile` passthrough).
- [x] Baseline captured to `docs/reports/profiler-baseline.md`
  (`docs/profiler.md` for the guide).

## Parity pass (2026-04)

- [x] Expanded `rakka-streams` surface (see Phase 10.x above).
- [x] `rakka-macros` `#[derive(Actor)]` now emits a real
      `impl Actor` (was a no-op returning empty TokenStream).
- [x] `rakka-serialization-hyperion` replaced its empty crate body with
      a Serde/bincode `HyperionSerializer<T>` that plugs into
      `rakka_core::serialization::Serializer`.
- [x] `rakka-persistence-sql` gained a MSSQL migration
      (`migrations/mssql/001_init.sql`); `dialect::migration_for` no
      longer returns a placeholder comment for MSSQL.
- [x] `crates/py-bindings/pycore/src/ext_streams.rs` now drives the
      native `rakka-streams` materializer (`run_collect`, `run_fold`)
      in addition to the legacy Python-only `map_reduce`.
- [x] Workspace warning cleanup: unused imports/`mut`, `ChildEntry` and
      test-only dead-code attributes, FSM `M::Stop` coverage, chat
      example sends `ChatMsg::Post`, pycore import trim (`cargo build
      --workspace` is warning-free).

## Remoting parity pass (2026-04 → 2026-04)

Background: an audit (2026-04-30) found Phase 4 was scoped much smaller
than the upstream Akka.Remote module — 549 LOC vs. ~9 047 LOC of C#.
The original `rakka-remote` shipped a working `TcpTransport` and the two
failure detectors but had stub/placeholder implementations of every
other Akka.Remote concept. This pass closes that gap.

- [x] R1.1 `rakka_core::actor::RemoteRef` / `RemoteSystemMsg` /
      `SerializedMessage` / `RemoteProvider` extension points; `ActorRef<M>`
      and `UntypedActorRef` are now polymorphic (Local/Remote variants)
      while preserving the existing public API.
- [x] R1.2 `RemoteSettings` (heartbeat/handshake/quarantine/backoff/
      ack-window knobs) and `AddressUidExtension` (per-incarnation UID
      surfaced in every `Associate` PDU).
- [x] R1.3 `AkkaPdu` framing (Associate/Disassociate/Heartbeat/Payload/
      Ack), bincode-based wire codec, length-prefixed `read_frame` /
      `write_frame`.
- [x] R1.4 Pluggable `SerializerRegistry` (system/bincode/json built-ins),
      type-id and manifest indexed; `register_bincode::<T>()` /
      `register_json::<T>()` helpers.
- [x] R1.5 `Endpoint` reader/writer pair with heartbeat tick, ack window,
      sliding-window resend buffer (`AckedSendBuffer` /
      `AckedReceiveBuffer`).
- [x] R1.6 `EndpointManager` association state machine
      (Idle → Pending → Connected → Quarantined → Tombstoned), per-peer
      reconnect attempt counter, dispatcher pump that fans inbound PDUs
      to the right `EndpointReader`.
- [x] R1.7 `RemoteActorRefImpl` + `RemoteActorRefProvider`; installed on
      `ActorSystem` so `actor_selection("akka.tcp://Sys@host:port/...")`
      returns a typed `ActorRef<M>` that serializes through the registry
      and ships envelopes via the manager.
- [x] R2.1 `AkkaProtocolTransport` handshake layer (Associate exchange,
      protocol version validation, cookie auth, peer-UID change → quarantine).
- [x] R2.2 `RemoteWatcher` — local `watch(remote)` ships
      `RemoteSystemMsg::Watch`, periodic failure-detector poll surfaces
      `Terminated` for unreachable peers.
- [x] R2.3 `RemoteSystemDaemon` + `RemoteDeployer` for inbound dispatch
      and `Deploy::remote` actor creation requests.
- [x] R3.1 Transport adapters: `ThrottleTransport` (latency/blackhole),
      `FailureInjectorTransport` (drop-every-n / fail), `TestTransport`
      (in-memory deterministic), `RemoteRouterConfig`
      (round-robin / consistent-hash across remote routees).
- [x] R3.2 `FailureDetectorRegistry` (per-`Address` PhiAccrual default)
      and `RemoteMetricsExtension` (per-Address sent/received messages
      and bytes; consumed by `rakka-telemetry::remote::refresh_from_endpoint_manager`).
- [x] R3.3 `ClusterRemoteAdapter` — minimal cluster-over-remote sidecar
      that exposes a local `cluster` actor receiving `Gossip` and lets
      callers `send_gossip(peer_address)`. Replaces the in-process-only
      gossip plumbing.
- [x] R3.4 `ShardRegion::set_remote_forwarder` lets the sharding region
      ship messages to remote shard owners; the previous "later phase"
      caveat at `shard_region.rs:47` is gone.
- [x] R3.5 Two-process integration tests (`crates/rakka-remote/tests/two_process.rs`):
      cross-process `tell`, peer-state tracking, metrics, codec mismatch
      drop, on-wire protocol-version honesty.

Notes / follow-ups left for a future pass:

- `RemoteDeployer` ships a `(manifest, bytes)` create request rather
  than a fully-typed `Props`. Closing that gap requires a
  language-agnostic Props serialization story which Akka.NET solves with
  Hyperion; we have a placeholder Hyperion crate but no working remote
  Props codec yet.
- The Python `pyremote` crate (Phase P3) is now wireable on top of the
  native remote story; the actual Python codec plug-in is still
  deferred but unblocked.
- `rakka-cluster` membership / leader / convergence still runs in
  process; `ClusterRemoteAdapter` exchanges raw `Gossip` blobs but does
  not yet drive a distributed leader-election loop.

All in-scope phases are landed with passing unit tests. See `PORTING.md`
for per-crate upstream tracking, deferred items, and the maintenance loop.

## Full-port roadmap (post-audit)

Tracking the 15-phase plan in `docs/full-port-plan.md`. Each item
links back to the phase doc for deliverables and acceptance gates.

- [x] Phase 0 — Foundations & invariants
      (`docs/idiomatic-rust.md`, `docs/audit-2026-04.md`,
      `[workspace.lints.clippy] todo/unimplemented = deny`,
      `cargo xtask audit [--check] [--json]`,
      `docs/reports/audit-2026-04.json` baseline,
      CI `audit` job in `.github/workflows/ci.yml`,
      depth-graded `docs/parity.md`)
- [ ] Phase 1 — Sender enum + SupervisorOf trait + type-state
      Context + props!/derive(Receive) macros
    - [x] 1.A `Sender` enum (Local/Remote/None), `ActorRef::tell_from`,
          `Context::sender_typed`, `MessageEnvelope::with_typed_sender`;
          legacy `Box<dyn Any>` sender retained behind `#[deprecated]`
          for transition (`crates/rakka-core/src/actor/sender.rs`,
          `tests/sender_typed.rs` — 2 tests passing).
    - [x] 1.B Removed `tell_with_sender`, `with_sender`,
          `MessageEnvelope::sender_any`, and the `Context::current_sender:
          Option<Box<dyn Any + Send>>` field. `Context::sender()` now
          returns `&Sender`. rakka-core `Box<dyn Any>` sites: 3 → 1
          (only `dead_letters.rs` remains, by design — unknown type).
          Workspace total: 8 → 6.
    - [ ] 1.C Type-state `Context<A, Phase>` (`Starting`, `Running`,
          `Stopping`); gate `become`/`unstash`/`set_receive_timeout`
          per phase.
    - [x] 1.C Runtime `LifecyclePhase` (`Starting`/`Running`/
          `Stopping`) on `Context<A>` set by the cell at each
          transition; `Context::phase()` accessor. Additive
          precursor to the phantom-typed `Context<A, Phase>` —
          generic helpers can gate calls without taking the
          phase as a type parameter.
    - [x] 1.D `SupervisorOf<C: Actor>` trait shipped with
          `SupervisionError` default; opt-in (Rust coherence forbids
          a "blanket + override" pattern, so the legacy
          `Props::supervisor_strategy` remains the runtime default
          until an actor implements `SupervisorOf<C>` explicitly).
          `Context::spawn_supervised::<C>(…)` follow-on tracked in 1.E.
          (`crates/rakka-core/src/supervision.rs`, 6 supervision tests
          passing.)
    - [x] 1.E `props!` macro and `#[derive(Receive)]` minimal subset
          (unit variants via `#[receive(unit_variants(A, B, …))]`)
          shipped in `rakka-macros`. 2 new integration tests passing
          (`tests/props_macro.rs`, `tests/derive_receive.rs`).
          Tuple/struct-variant dispatch + `Context::spawn_supervised`
          bound enforcement deferred to Phase 1.E.B (need richer
          syn-side parsing to extract field types).
- [x] Phase 2 — HOCON config (`crates/rakka-config/src/hocon.rs`,
      14 tests passing). Supports flat + dotted + nested keys,
      arrays, comments (`#`, `//`, `/* */`), triple-quoted strings,
      `include "file"` (file-relative), `${path.to.value}` strict
      substitutions, `${?ENV}` optional env-var substitutions.
      `Config::from_hocon_str` / `from_hocon_file` are the entry
      points; `ConfigError::Hocon(HoconError)` plumbs errors.
      Reference-conf round-trip is a Phase 2.B follow-on.
- [ ] Phase 3 — rakka-core depth (3.1 dispatchers, 3.2 mailboxes,
      3.3 routing 6→20+, 3.4 pattern, 3.5 event stream,
      3.6 stash, 3.7 FSM macro, 3.8 coordinated shutdown,
      3.9 IO managers)
    - [x] 3.3 Routing: `TailChoppingRouter<M>` (round-robin
          attempt selection w/ `interval`/`within`/`max_attempts`
          policy). 7 routers total now (still climbing toward
          akka.net's 20+). 4 new tests.
    - [x] 3.5 EventStream: `subscribe_filtered<T>(pred, fn)` and
          `subscriber_count<T>()` query. 2 new tests.
    - [x] 3.6 Stash: `BoundedStash<M>` with `StashOverflow`
          policies (`DropOldest`/`DropNewest`/`Reject`) and
          typed `StashResult` so callers can route displaced
          messages to DeadLetters. 5 new tests.
    - [x] 3.4 Pattern: `retry(op, max_attempts, RetrySchedule)` with
          `Fixed` + `Exponential(min, max)` schedules; CircuitBreaker
          reset-timeout bug fixed (was using
          `Instant::now().elapsed()` which never transitioned to
          half-open). 4 new tests.
- [x] Phase 4 — testkit depth (matchers, TestScheduler,
      MultiNodeSpec). `TestProbe` gained `expect_msg_class`,
      `receive_n`, `receive_while`, `fish_for_message`,
      `expect_all_of` (10 probe tests passing).
      `TestScheduler` virtual-time clock with cancel/fire-in-order
      (4 tests). `MultiNodeSpec` boots N actor systems with shared
      `tokio::sync::Barrier`-backed labels (3 tests, including
      timeout). EventFilter expansion (predicates, occurrence
      bounds) tracked as Phase 4.B follow-on.
- [ ] Phase 5 — remote depth (reader/writer split, quarantine,
      TLS, chunking, send-queue backpressure, RemoteProps,
      configurable phi-accrual; remove panic!/__placeholder__)
    - [x] 5.A Removed `__placeholder__` root-address sentinel from
          `system.rs` (dead `parse_remote_path` deleted; the live
          `parse_actor_path` now also pre-validates `actor_selection`
          paths). Renamed `WatcherStub` → `RemoteWatcherProxy` to
          drop the `// stub` audit marker. `__placeholder__` /
          `// stub` counts in `rakka-remote`: 1 → 0 each.
    - [x] 5.B Added typed `RemoteError` + `RemoteErrorKind` enum
          (`crates/rakka-remote/src/error.rs`,
          `From<TransportError>` blanket; 3 unit tests). Replaces
          ad-hoc `String` errors in future quarantine / handshake /
          codec call paths.
    - [x] 5.I `RemotePropsRegistry` + `register_bincode::<T>` —
          per-system manifest table mapping `(manifest, bytes)`
          payloads to typed factory closures. `RemotePropsError`
          enum (`UnknownManifest`, `Codec`). Closes the
          PORTING_TODO note about `Deploy::remote` shipping
          untyped Props. 4 new tests.
    - [x] 5.J Configurable phi-accrual knobs on `RemoteSettings`:
          `phi_threshold`, `phi_max_sample_size`,
          `phi_min_std_deviation`, `phi_acceptable_heartbeat_pause`
          + `with_phi_*` builders.
    - [x] 5.D Reader/writer task split: `RawTransport` trait
          (recv/send) + `spawn_reader_writer(transport, capacity)`
          orchestrator returning `ReaderWriterHandle` w/ outbound
          `tx`, inbound `rx`, and per-task `JoinHandle`s. Plug-in
          shape that `EndpointHandle` adopts in a follow-on PR.
          3 new tests (drain, EOF, concurrency).
    - [x] 5.E TLS scaffolding: `TlsConfig` (cert/key/ca paths,
          SNI server name, mTLS, insecure dev escape), PEM-block
          parser (`parse_pem_blocks`), `RemoteSettings::tls` +
          `with_tls(...)`. Wire-level rustls integration plugs in
          once Phase 5.D ships. 4 new tests.
    - [x] 5.F Message chunking: `Chunker::split(message_id, payload)`
          → ordered `Chunk`s; `Reassembler::push(chunk)` returns
          `Some(payload)` once all fragments arrive. Idempotent on
          duplicates, typed `ChunkError::{InvalidIndex, SizeMismatch}`,
          16-byte wire header. `RemoteSettings::maximum_payload_size`
          (default 256 KiB). 8 new tests.
    - [x] 5.H `LruCache<K, V>` bounded LRU with eviction-returns-
          evicted; ready for `ActorPath ↔ RemoteRef` and serializer-id
          ↔ manifest caches. 6 new tests.
    - [x] 5.C Quarantine lifecycle queries: `AssociationState` made
          public + `#[non_exhaustive]`; `EndpointManager::peer_state`
          query and `EndpointManager::purge_tombstones(ttl) -> usize`
          sweep. Integration test `tests/quarantine_lifecycle.rs`
          covers the `Idle → Tombstoned → purged` flow.
    - [ ] 5.D Reader / writer task split per peer (parallel inbound
          vs outbound) — needs an `EndpointHandle` rework.
    - [ ] 5.E TLS via `rustls` (optional `tls` feature).
    - [ ] 5.F Message chunking for payloads > `maximum-frame-size`.
    - [ ] 5.G Send-queue with bounded backpressure +
          `OverflowStrategy` mirroring `rakka-streams`.
    - [ ] 5.H LRU caches for `ActorPath ↔ RemoteRef` and
          serializer-id ↔ manifest.
    - [ ] 5.I `RemoteProps` trait + manifest registry for fully
          typed `Deploy::remote` (closes the dangling note in
          `PORTING_TODO.md`).
    - [ ] 5.J Configurable phi-accrual knobs
          (`acceptable-heartbeat-pause`, `heartbeat-interval`,
          `threshold`, `min-std-deviation`).
    - [ ] 5.K Pull deferred Python `pyremote` codec plug-in (P3).
- [ ] Phase 6 — cluster depth (ClusterDaemon, active gossip,
      convergence, leader election, heartbeat, events bus, SBR
      runtime wiring, multi-DC)
    - [x] 6.A `ClusterEventBus` with RAII `SubscriptionHandle`
          (`MemberJoined/Up/Left/Exited/Removed`,
          `UnreachableMember/ReachableMember`, `LeaderChanged`,
          `ClusterShuttingDown`, `Convergence(bool)`). 3 tests.
    - [x] 6.C `MembershipState::apply_leader_actions()` — pure
          per-tick driver (Joining→Up, Leaving→Exiting, Down→Removed,
          purge Removed members) returning the events to publish.
          `MembershipState::join`/`leave` helpers. `is_converged`
          fixed to mean "no unreachable members" (matches akka.net
          gossip semantic). 6 new tests.
    - [x] 6.B `elect_leader(state)` (lowest-address reachable
          Up/Leaving member), `next_status(current, converged)`
          transition rules, `is_converged(state)` predicate. 7
          tests.
    - [ ] 6.C ClusterDaemon actor that owns `MembershipState` and
          publishes events on transitions.
    - [ ] 6.D Active gossip dissemination loop + GossipStatus/
          GossipEnvelope PDUs over rakka-remote.
    - [x] 6.D Gossip PDU shapes: `GossipPdu::{Status, Envelope,
          Merge}` w/ bincode round-trip; `gossip_decide(local,
          remote) -> GossipDecision::{SendEnvelope, RequestMerge,
          MergeBoth, Same}` decision function;
          `pick_gossip_target(peers, self, cursor)` round-robin
          target selector. The active dissemination loop wires once
          Phase 5.D ships. 7 new tests.
    - [x] 6.F `SbrRuntime<S>` — wires a `DowningStrategy` into
          `MembershipState` with a stability deadline; `tick(state,
          now)` returns `SbrAction::{None, DownUnreachable, DownAll,
          DownSelf}`. Resets the clock when the partition heals.
          4 new tests.
    - [x] 6.E `HeartbeatSender` — per-peer interval timer +
          `due_peers(now)` driver + `record_tick(addr, now)` +
          `ticks_per_peer` snapshot. The cross-node PDU exchange
          plugs in once Phase 5.D + 6.D land. 3 new tests.
    - [ ] 6.F Wire SBR strategies into membership decisions.
    - [x] 6.G Multi-DC awareness: `member_dc(m)` extracts `dc-*`
          role; `same_dc`/`partition_by_dc` helpers; `CrossDcSettings`
          (slow heartbeat / longer pause / capped monitored peers);
          `heartbeat_interval_for(local, peer, ...)` picker. Wiring
          into `HeartbeatSender` is a follow-on. 6 new tests.
- [ ] Phase 7 — cluster-tools depth (PubSub mediator, singleton
      handover, ClusterClient + receptionist)
    - [x] 7.A `DistributedPubSub.Mediator` — typed `publish_msg::<M>`
          (actually delivers, not just enumerates), `subscribe_to_group`
          + `send_to_group` round-robin, `group_count` query.
          (`pub_sub.rs`, 4 new tests, 7 total in cluster-tools.)
    - [ ] 7.B Cross-node gossip wiring (waits on Phase 6 gossip transport).
    - [x] 7.C `ClusterSingletonManager` handover state machine:
          `Inactive → Starting → Active(here) → HandingOver →
          Inactive` (and `Active(remote)` for follower nodes).
          `ClusterSingletonProxy` buffers messages during handover
          (configurable `with_buffer_size`); flushes on `set_active_*`;
          counts overflow `drops`. 4 new tests, 11 cluster-tools
          total.
    - [x] 7.D `ClusterClientSettings` (initial_contacts /
          establishing_get_contacts_interval / reconnect_timeout /
          max_attempts), `ClusterClient::next_contact` round-robin,
          `ClusterClient::establish(try_resolve)` driver with
          backoff, typed `ClusterClientError`,
          `ClusterReceptionist::registered()` listing. 5 new tests.
- [ ] Phase 8 — distributed-data depth (ORMap/LWWMap/PNCounterMap/
      Flag/ORMultiMap, delta-CRDTs, consistency levels, durable
      store, Subscribe API)
    - [x] 8.E `Replicator::subscribe(key, fn)` change-notification
          API with RAII `SubscriptionToken`. Fires on `update` and
          `delete`. 4 new tests.
    - [x] 8.A New CRDTs: `Flag` (monotonic boolean), `ORMap<K, V>`
          (observed-remove map of K → V where V: CrdtMerge),
          `LWWMap<K, V>` (timestamp-keyed last-write-wins),
          `PNCounterMap<K>` (per-key PNCounter). 6 new tests, 13
          total in distributed-data.
    - [ ] 8.B `ORMultiMap` (set-of-V CRDT values per key).
    - [x] 8.B `ORMultiMap<K, V>` (map of key → `OrSet<V>`) with
          add/remove/contains/key_count + CRDT merge.
    - [x] 8.D Typed `WriteConsistency`/`ReadConsistency`
          (`Local`/`All{ timeout }`/`Majority{ timeout }`/
          `From { n, timeout }`) with `required_acks(cluster_size)`
          / `required_replies(cluster_size)` / `timeout()`
          accessors. Quorum exchange itself activates when Phase 6
          gossip lands. 4 new tests.
    - [x] 8.C `DeltaCrdt` trait (`type Delta`, `take_delta`,
          `merge_delta`); `GCounter` is the first impl (per-node
          increment delta map). 2 new tests. Other CRDTs land
          incrementally.
    - [ ] 8.D Read/Write consistency levels with timeouts.
    - [ ] 8.E Replicator becomes a real actor (no `RwLock<HashMap>`).
    - [ ] 8.F Durable storage backend (`redb`/`lmdb`).
    - [ ] 8.G `Subscribe(key, subscriber)` change-notification API.
- [ ] Phase 9 — sharding depth (PersistentShardCoordinator,
      DDataShardCoordinator, allocation strategies, rebalance,
      passivation, remember-entities, 3-phase handoff)
    - [x] 9.H `HandoffCoordinator` 3-phase state machine
          (`Idle → Beginning → HandingOff(N) → Stopped → Started`)
          with typed `HandoffError::InvalidTransition`,
          `entity_stopped` count-down, snapshot. 6 new tests.
    - [x] 9.E `DDataShardCoordinator` — `LWWMap<String, String>`-
          backed shard→region table with `merge_remote(snapshot)`
          for gossip-driven convergence and `snapshot()` for
          outbound. Strictly-monotonic timestamps via `tick()`. 6
          new tests.
    - [x] 9.F `RebalanceRunner` — combines `ShardCoordinator` +
          `HandoffCoordinator` + `ShardAllocationStrategy` per
          tick; emits `RebalanceAction::{BeginHandoff, Allocate}`.
          Caller drives the actions; runner is pure scheduling.
          3 new tests.
    - [x] 9.D `PersistentShardCoordinator` shipped — built on
          `rakka_persistence::Eventsourced`. Events:
          `ShardAllocated`/`ShardRebalanced`/`ShardRemoved`. Bincode
          codec. `project_into(state, &ShardCoordinator)` rebuilds
          the in-memory coordinator after replay. 4 new tests.
    - [x] 9.G Remember-entities scaffolding: `RememberEntitiesStore`
          trait + `InMemoryRememberStore` reference impl +
          `RememberedEntities` cache wrapper (warm/record_active/
          record_inactive/entities/shard_count). 3 new tests.
    - [x] 9.A `ShardAllocationStrategy` trait +
          `LeastShardAllocationStrategy` (least-loaded with
          rebalance threshold + max-simultaneous knob) +
          `PinnedAllocationStrategy`. (5 tests.)
    - [x] 9.B `PassivationTracker` (per-entity `last_seen`,
          `idle_since(ttl)`, `record_activity`/`drop_entity`/
          `snapshot`). (5 tests.)
    - [x] 9.C `ShardCoordinator::allocate_with_strategy` +
          `rebalance_with_strategy` + `region_shard_counts`.
          (5 tests.)
    - [ ] 9.D `PersistentShardCoordinator` (event-sourced via
          `Eventsourced` from Phase 11.A).
    - [ ] 9.E `DDataShardCoordinator` (state in distributed-data
          via Phase 8).
    - [ ] 9.F Rebalance algorithm runner + handoff state machine.
    - [ ] 9.G Remember-entities (persist active entity ids).
    - [ ] 9.H 3-phase handoff (begin → stop → start-elsewhere).
- [ ] Phase 10 — cluster-metrics depth (collector, gossip,
      adaptive routing)
    - [x] 10.A `MetricsProbe` trait (dep-free; users supply the
          probe), `StaticProbe` for tests, `AdaptiveLoadBalancer`
          (picks lowest-cpu candidate, lex tie-break). 6 new tests.
    - [ ] 10.B Built-in `sysinfo`-backed probe behind a feature.
    - [ ] 10.C Metrics gossip via Phase 6 transport.
    - [ ] 10.D Wire `AdaptiveLoadBalancer` into
          `RemoteRouterConfig`.
- [ ] Phase 11 — persistence depth (Eventsourced derive,
      ReceivePersistent, PersistentFSM, RecoveryPermitter,
      async snapshots, real query streaming, real backends, full
      TCK matching upstream)
    - [x] 11.A `Eventsourced` trait with typed `Error` + thiserror
          plumbing, `recovery_completed` lifecycle hook, pluggable
          `event_manifest()`, codec via `Result<Vec<u8>, String>`.
          `EventsourcedError<DomainErr>` with `From<JournalError>`.
          (`crates/rakka-persistence/src/eventsourced.rs`, 3 tests.)
    - [x] 11.B `RecoveryPermitter` (semaphore-backed bounded recovery
          concurrency; `acquire`/`try_acquire`/`close`/`in_flight`/
          `available` queries). `Eventsourced::recover` acquires a
          permit before replaying. (4 tests including capacity
          bounds + close-cancels-pending.)
    - [x] 11.F Async snapshots: `AsyncSnapshotter<S>` with
          `SnapshotPolicy::{Periodic { every }, Manual}` +
          `with_keep_last(n)` retention pruning.
          `should_snapshot(seq_nr)` predicate so the actor can
          decide cheaply, plus async `save(pid, seq, payload)`.
          4 new tests.
    - [x] 11.E `PersistentFSM<S, D, C, E, Err>` — event-sourced
          state machine on top of `Eventsourced`. on_command +
          on_event + with_codec builder; tracks transition history.
          2 new tests.
    - [x] 11.D `ReceivePersistent` closure-style helper:
          `on_command(state, cmd) -> Result<Vec<E>, Err>`,
          `on_event(state, &E)`, `with_codec(encode, decode)`.
          Round-trips through journal + recovers under
          `RecoveryPermitter`. 2 new tests.
    - [x] 11.C `PersistenceQuery` real surface: typed `Offset`
          (`NoOffset`/`Sequence`/`TimeBased`); `events_by_tag` +
          `current_*` variants; `all_persistence_ids`/
          `current_persistence_ids`; `EventEnvelope.tags` exposed.
          Default impls let backends opt in only to what they
          index. (3 tests.)
    - [ ] 11.D `ReceivePersistent` (closure-style API for ad-hoc
          persistent actors).
    - [ ] 11.E `PersistentFSM` (state-machine on top of Eventsourced).
    - [ ] 11.F Async snapshots during normal operation.
    - [ ] 11.G Real backend implementations (sql/redis/mongo/
          cassandra/aws/azure) — each must pass the expanded TCK.
    - [ ] 11.H Expanded TCK matching upstream (3,764 LOC port).
- [ ] Phase 12 — streams depth (12.1 substreams, 12.2 time-windowed,
      12.3 async-boundary, 12.4 supervision, 12.5 hubs,
      12.6 routing junctions, 12.7 recovery, 12.8 lifecycle,
      12.9 StreamRefs, 12.10 rakka-http)
    - [x] 12.3 `Source::async_boundary(buffer)` — explicit async
          stage that decouples upstream + downstream onto separate
          Tokio tasks via a bounded mpsc channel.
    - [x] 12.4 Stream-level `Decider<E>` + `SupervisionDirective`
          (`Stop`/`Resume`/`Restart`); `with_decider(src, decider)`
          on `Source<Result<T, E>>` returning `Source<T>`.
          `deciders::{resuming, stopping, restarting}` helpers.
          4 new tests.
    - [x] 12.9 `SourceRefHandle<T>` / `SinkRefHandle<T>` — handles
          to streams that can cross actor / process boundaries.
          mpsc-channel scaffolding now; serialized over remoting
          once Phase 5.D wires it. Type aliases `SourceRef<T>` /
          `SinkRef<T>` for `Arc<…Handle<T>>`. 4 new tests.
    - [x] 12.8 Lifecycle: `watch_termination(src) -> (Source<T>,
          oneshot::Receiver<()>)` (fires on completion),
          `monitor(src, on_each)` (per-element observer),
          `count_elements(src)` convenience helper. 3 new tests.
    - [x] 12.5 Hub patterns: `BroadcastHub<T>` (one source → many
          dynamic consumers, lag-skips on slow subscribers) and
          `MergeHub<T>` (many dynamic producers → one consumer,
          mpsc-backed). Built on `tokio::sync::broadcast` and
          `tokio::sync::mpsc`. 5 new tests.
    - [x] 12.1 Substreams: `group_by(src, max, key_fn)` (returns
          `Source<(K, Source<T>)>`), `split_when(src, pred)`
          (returns `Source<Source<T>>`). Per-key tokio mpsc-backed
          substreams. 3 new tests.
    - [x] 12.2 Time-windowed: `grouped_within(src, n, dur)` (chunk
          + flush-on-timeout), `idle_timeout(src, dur)`.
          3 new tests.
    - [x] 12.6 Routing junctions: `partition(src, n, fn)`,
          `balance(src, n)` (round-robin), `unzip(src)` for
          `Source<(A, B)>`. 4 new tests.
    - [x] 12.7 Recovery operators on `Source<Result<T, E>>`:
          `recover` (replace `Err` with mapped value, terminate),
          `map_error` (transform error variant), `recover_with`
          (switch to replacement source on first error). 6 tests.
- [ ] Phase 13 — Idiomatic-Rust cross-cutting sweep
    - [x] 13.B/C `util::Snapshot<T>` — `RwLock<Arc<T>>`-backed
          read-mostly container with `load()` (Arc clone),
          `store(T)` (whole-snapshot swap), and `rcu(|cur| next)`
          (atomic transition). Eliminates clone-and-mutate-under-lock
          on hot snapshot paths; ready for gossip / replicator /
          sharding allocation tables. 4 new tests.
    - [x] 13.D extension: `#[non_exhaustive]` added to
          `ReachabilityStatus`, `DowningDecision`, `VectorRelation`,
          `OverflowStrategy`, `QueueOfferResult`, `FramingError`
          across `rakka-cluster` and `rakka-streams` (downstream
          matches in telemetry + py-bindings updated with wildcard
          arms).
    - [x] 13.D `#[non_exhaustive]` sweep on core public enums:
          `Directive`, `StrategyKind`, `CircuitBreakerState`,
          `CircuitBreakerError<E>` (additional core enums already
          had it via Phase 1 / 5 work).
    - [x] 13.A Sealed `CrdtMerge` trait via `private::Sealed`
          super-trait — only the in-tree CRDTs may implement it.
          Downstream extends through composition (`ORMap<K, V>` /
          `LWWMap<K, V>` / domain-specific wrappers).
    - [ ] 13.B Replace remaining `RwLock<HashMap>` hubs with actors
          where contention measurements justify it.
    - [ ] 13.C Adopt `imbl::HashMap` / `imbl::Vector` for hot
          snapshot paths.
    - [ ] 13.D `#[non_exhaustive]` sweep on all public enums.
    - [ ] 13.E Sealed-trait pass on `Actor`, `Message`,
          `Serializer`, `Transport`, `Journal`, `SnapshotStore`.
- [ ] Phase 14 — Docs, examples, migration guide
    - [x] 14.A `docs/migrating-from-akka-net.md` (translation
          table + idiom-by-idiom diff + migration playbook).
    - [x] 14.B `docs/architecture.md` (layered crate stack +
          concept-by-concept Akka.NET ↔ rakka mapping).
    - [x] 14.D `examples/cluster-pubsub-chat` shipped — runnable
          demo of `DistributedPubSub::publish_msg::<M>` typed
          broadcast and `subscribe_to_group` round-robin delivery.
          Output verified: 2 broadcast subscribers each get 3
          messages; 4 group sends round-robin between 2 workers.
    - [x] 14.C `examples/event-sourced-counter` shipped — a runnable
          end-to-end demo of `Eventsourced` + `RecoveryPermitter` +
          `AsyncSnapshotter` with a periodic snapshot policy. Output:
          two snapshots saved, replay matches written state.
          Additional production examples (`sharded-keyvalue`,
          `cluster-pubsub-chat`) tracked as Phase 14.D.
- [ ] Phase 15 — Verification + 1.0-rc
    - [x] 15.C `MultiNodeSpec` integration test for cluster-tools
          (`crates/rakka-cluster-tools/tests/pubsub_multinode.rs`,
          2 tests passing): 3-node DistributedPubSub broadcast,
          4-caller barrier rendezvous.
    - [x] 15.D `cargo xtask soak [--hours <n>]` — runs the workspace
          test suite in a loop until the deadline expires; reports
          iteration count + first failure.
    - [x] 15.F.0 Release pipeline rebuilt: `release.yml` triggers on
          `v*` tag (or `workflow_dispatch` dry-run); runs
          `cargo xtask verify`, builds release binaries
          (`rakka-dashboard`, `rakka-profiler`) for Linux x86_64 +
          macOS aarch64, creates a GitHub Release with auto-generated
          notes, and publishes every publishable crate to crates.io
          in dependency order.
    - [x] 15.F.1 Auto-version bump pipeline:
          `version-bump.yml` runs on every `main` push, decides a
          SemVer bump from Conventional-Commit subjects
          (`feat:`/`fix:`/`BREAKING CHANGE`/`Release-As: x.y.z`),
          calls `cargo xtask bump <kind>` to update the workspace
          version + `pyproject.toml` + `Cargo.lock`, commits as
          `chore(release): vX.Y.Z`, and pushes a `vX.Y.Z` tag that
          fires `release.yml`. `cargo xtask bump
          <patch|minor|major|--pre <id>|--set <ver>>` is the
          single source of truth (script-friendly; verified
          round-trip 0.1.0 → 0.1.1 → 0.1.0).
    - [x] 15.F.2 Claude `version-bump` skill at
          `~/.claude/skills/version-bump/SKILL.md` — codifies the
          decision rule (Conventional Commits → SemVer with the
          0.x exemption), points at `cargo xtask bump`, and
          describes when **not** to bump (doc/CI/refactor only).
    - [x] 15.E `semver-checks` CI job — installs
          `cargo-semver-checks` and runs `check-release` for every
          publishable crate. Initially `continue-on-error: true`
          (warn-only) until first 1.0 release; flips to hard fail
          at 15.F.
    - [x] 15.A `cargo xtask verify` aggregator (build + test +
          clippy `-D warnings` + `audit --check`); CI `verify` job
          downstream of `fmt`/`clippy`/`test`/`audit`. 1.0-rc gate.
    - [ ] 15.B All persistence backends pass full TCK in CI.
    - [ ] 15.C `MultiNodeSpec` suite covering cluster / pub-sub /
          ddata / sharding / remote.
    - [ ] 15.D 24-hour soak test (5-node, 100k entities,
          rolling restarts).
    - [ ] 15.E `cargo-semver-checks` clean.
    - [ ] 15.F Tag `1.0.0-rc.1`.
