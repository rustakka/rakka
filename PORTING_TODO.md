# Porting To-Do

Phase progress against the plan. Update as each phase lands.

- [x] Phase 0 - Workspace bootstrap (Cargo workspace, xtask, PORTING.md)
- [x] Phase 1 - `rustakka-config`
- [x] Phase 2 - `rustakka-core`
  - [x] 2.1 util/
  - [x] 2.2 actor/ primitives (Address, ActorPath, Props, ActorRef)
  - [x] 2.3 dispatch/ (Mailbox, MessageQueues, Dispatcher)
  - [x] 2.4 actor/scheduler/
  - [x] 2.5 actor_cell/ (children, fault_handling, death_watch, receive_timeout)
  - [x] 2.6 actor_system + providers
  - [x] 2.7 supervisor_strategy, fsm, stash, inbox, coordinated_shutdown, extensions, setup
  - [x] 2.8 event, pattern, routing, io
  - [x] 2.9 serialization
  - [x] 2.10 rustakka-macros
- [x] Phase 3 - `rustakka-testkit`
- [x] Phase 4 - `rustakka-remote`
- [x] Phase 5 - `rustakka-cluster`
- [x] Phase 6 - `rustakka-cluster-tools`
- [x] Phase 7 - `rustakka-distributed-data`
- [x] Phase 8 - `rustakka-cluster-sharding`
- [x] Phase 9 - `rustakka-persistence` (+ query, query-inmemory, tck)
- [x] Phase 10 - `rustakka-streams`
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
- [x] Phase 11 - `rustakka-cluster-metrics`, `-coordination`, `-discovery`, `-di`, `-hosting`
- [x] Phase 12 - Examples + benchmarks (`examples/pingpong`, `examples/chat`, `examples/fault-tolerance`, `benches/ping_throughput`)
- [x] Phase 13 - Persistence provider crates (`resources/Rustakka Persistence Plan.md`)
  - [x] 13.0 Core extensions: `JournalError::Backend`, `PersistentRepr.tags`,
    TCK split into `journal_suite` / `journal_tag_suite` / `snapshot_suite`
  - [x] 13.a `rustakka-persistence-sql` (`sqlx`; SQLite default,
    Postgres/MySQL/MSSQL features; journal + snapshot + `ReadJournal`
    with tag queries)
  - [x] 13.b `rustakka-persistence-redis` (`fred`; sorted-set journal,
    hash snapshot store, `MULTI`/`EXEC` batches)
  - [x] 13.c `rustakka-persistence-mongodb` (indexed collections,
    atomic `insert_many`, BSON payloads)
  - [x] 13.d `rustakka-persistence-cassandra` (`scylla`; partitioned
    journal tables, prepared-statement replay)
  - [x] 13.e `rustakka-persistence-aws` (DynamoDB single-table design
    with `E#`/`S#` sort keys, conditional writes)
  - [x] 13.f `rustakka-persistence-azure` (Azure Table Storage via
    a SharedKeyLite `reqwest` client; Cosmos feature placeholder)
  - [x] 13.g `persistence-integration` CI job with Postgres/MySQL/
    Redis/Mongo/Cassandra/DynamoDB Local/Azurite service containers
  - [x] 13.h `release.yml` publishing core → TCK → query → provider
    crates in dependency order on `release: published`
- [x] Ongoing - xtask sync-upstream, docs/parity.md, CI quarterly diff
- [x] Phase 14 - Observability dashboard
  - [x] 14.1 `rustakka-telemetry` probe crate (actors, dead letters, cluster,
    sharding, persistence, remote, streams, distributed-data)
  - [x] 14.2 `rustakka-dashboard` axum service (REST `/api/*` + `/ws`
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
  - [x] 14.8 CLI (`rustakka-dashboard --prometheus --otlp-endpoint …`),
    Python `rustakka.dashboard.serve(...)`, and `cargo xtask dashboard`
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
- [x] Phase P2 - `rustakka.testkit` (TestKit, TestProbe, pytest fixture)
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

- [x] `rustakka-profiler` crate + binary: actor memory + CPU profiler
  (scenarios: `tell`, `ask`, `fanout`, `cpu`).
- [x] `rustakka.profiler` Python sub-package mirroring the Rust binary
  (auto-picks `python-nogil` → `python-subinterpreter-pool` →
  `python-pinned` per scenario).
- [x] `scripts/profile.py` orchestrator producing a side-by-side
  comparison (+ `cargo xtask profile` passthrough).
- [x] Baseline captured to `docs/reports/profiler-baseline.md`
  (`docs/profiler.md` for the guide).

## Parity pass (2026-04)

- [x] Expanded `rustakka-streams` surface (see Phase 10.x above).
- [x] `rustakka-macros` `#[derive(Actor)]` now emits a real
      `impl Actor` (was a no-op returning empty TokenStream).
- [x] `rustakka-serialization-hyperion` replaced its empty crate body with
      a Serde/bincode `HyperionSerializer<T>` that plugs into
      `rustakka_core::serialization::Serializer`.
- [x] `rustakka-persistence-sql` gained a MSSQL migration
      (`migrations/mssql/001_init.sql`); `dialect::migration_for` no
      longer returns a placeholder comment for MSSQL.
- [x] `crates/py-bindings/pycore/src/ext_streams.rs` now drives the
      native `rustakka-streams` materializer (`run_collect`, `run_fold`)
      in addition to the legacy Python-only `map_reduce`.
- [x] Workspace warning cleanup: unused imports/`mut`, `ChildEntry` and
      test-only dead-code attributes, FSM `M::Stop` coverage, chat
      example sends `ChatMsg::Post`, pycore import trim (`cargo build
      --workspace` is warning-free).

Phase P3 (pyremote pluggable codecs) remains deferred until the native
remoting story crosses a process boundary.

All in-scope phases are landed with passing unit tests. See `PORTING.md`
for per-crate upstream tracking, deferred items, and the maintenance loop.
