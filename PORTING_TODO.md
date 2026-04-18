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
- [x] Phase 11 - `rustakka-cluster-metrics`, `-coordination`, `-discovery`, `-di`, `-hosting`
- [x] Phase 12 - Examples + benchmarks (`examples/pingpong`, `examples/chat`, `examples/fault-tolerance`, `benches/ping_throughput`)
- [x] Ongoing - xtask sync-upstream, docs/parity.md, CI quarterly diff

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

Test suite: 90 Rust tests + 29 Python tests passing. Phase P3 (remote +
pluggable codecs) is deferred until the native remoting story crosses a
process boundary.

All in-scope phases are landed with passing unit tests. See `PORTING.md`
for per-crate upstream tracking, deferred items, and the maintenance loop.
