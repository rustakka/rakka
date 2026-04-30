# Porting To-Do

Phase progress against the plan. Update as each phase lands.

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
