# Depth program

The long-form roadmap from today's
[depth grades](parity.md) toward a runtime that's fully fleshed out
across every subsystem and ready to extend into the unified-compute
story
([`actors-and-agentic-computing.md`](actors-and-agentic-computing.md)).

This is a depth document, not a percent-complete tracker. A
subsystem is "done" when it's production-functional with conformance
test coverage, not when a checkbox is ticked.

## How depth gets measured

The discipline is empirical. Every subsystem reports through:

- **Audit metrics** (`cargo xtask audit`) — counts unwrap / panic /
  todo / placeholder / println sentinels; gated against the baseline
  in [`audit-2026-04.md`](audit-2026-04.md). Regressions fail CI.
- **Conformance tests** — `atomr-persistence-tck` for storage
  adapters; `atomr-streams` operator coverage; `atomr-cluster`
  multi-node specs.
- **Idiomatic invariants** — twelve principles in
  [`idiomatic-rust.md`](idiomatic-rust.md), each numbered so PRs and
  audit reports can cite them by name (P-1 through P-12).
- **Cross-runtime profiler** — same scenarios in Rust and Python,
  same JSON schema; baseline numbers in
  [`profiler.md`](profiler.md).

A subsystem moves from depth `c` to `b` when it covers the operators
its peers expect; from `b` to `a` when conformance + multi-node tests
exercise the protocol machinery, not just the public types.

## Foundations

### Core invariants (cross-cutting)

The twelve principles in [`idiomatic-rust.md`](idiomatic-rust.md)
apply to every PR. The most load-bearing ones for the depth program:

- **P-1 / P-2** — typed dispatch, no `Box<dyn Any>` on hot paths.
  The dispatch boundary needs to stay branch-free if we want
  accelerator backends to inherit the same contract.
- **P-3** — actor state, not `RwLock<HashMap>`. Coordinators
  (replicator, mediator, shard coordinator, cluster daemon, endpoint
  manager) are actors. That's how the same coordination protocol
  works whether they run on host threads or accelerator-resident
  dispatchers.
- **P-6** — immutable snapshots on hot read paths, so a host-side
  reader and an accelerator-side dispatcher can share a view without
  a lock.
- **P-7 / P-8** — type-state lifecycle and compile-time supervision
  contracts. These keep the actor model rigorous as the runtime
  fans out into multiple dispatch backends.

### Actor surface (`atomr-core`)

`c` → `b` (**shipped**). The post-2026-04 spec sweep covers stash
(bounded with overflow strategy), extensions, scheduler, lifecycle,
IO (`TcpManager` / `UdpManager` `Bind` + `Connect`), routing
(round-robin, random, consistent-hash, scatter-gather, tail-chopping,
broadcast, listener), serialization registry, and `ActorPath` +
`Address`. Dispatcher kinds (work-stealing, calling-thread, pinned,
single-thread via `DispatcherConfig`) plus bounded-overflow /
control-aware mailbox queues round out the `b` surface.

`b` to `a` is the harder pieces: full coordinated-shutdown phase
graph at parity with the wider actor-runtime feature surface,
reflection-style extension lookup that stays typed, FSM declarative
macro coverage of the same feature surface, and the future GPU
dispatcher (see "Forward-looking" below).

### Configuration (`atomr-config`)

Path from `b` to `a` is HOCON edge cases — substitution within
arrays, `?:` defaults, `+=` array append, deep `include` resolution
with relative-path roots — plus a typed-deserialize bridge that's
ergonomic for users who want strongly-typed config sections.

## Distribution

### Remote (`atomr-remote`)

Already at `b` with the major protocol pieces — TCP transport,
framed PDU codec, ack'd delivery, endpoint state machine, watcher,
system daemon, transport adapters, failure-detector registry, two-
process integration tests.

`b` to `a` is the operational surface:

- Typed `Props` over the wire. The deployer ships
  `(manifest, bytes)` today; closing this requires a
  language-agnostic `Props` codec.
- TLS hardening: certificate validation, mTLS, rotation hooks.
- Message chunking + reassembly tuned by an explicit knob.
- Send-queue backpressure tuned for sustained throughput.
- LRU caches sized for inflight envelope tracking.

### Cluster (`atomr-cluster`)

Already at `b` — membership, reachability, vector clock, five SBR
strategies, gossip PDU, heartbeat sender, SBR runtime, multi-DC
tagging. The cluster daemon owns active gossip dissemination and
leader-action ticks over a pluggable `GossipTransport`.

`b` to `a` is the deeper protocol work: distributed leader-election
handover over remote, member-up/leaving/exiting transitions
exercised under packet loss, multi-DC quorum semantics.

### Cluster-tools (`atomr-cluster-tools`)

`b`. Path to `a` is full singleton handover under network partition,
buffered-proxy semantics under reconnect storms, and stress-tested
distributed pub/sub across a multi-DC cluster.

### Cluster-sharding (`atomr-cluster-sharding`)

`b`. Path to `a`:

- Persistent coordinator under recovery permitter pressure.
- DData-backed coordinator with read/write quorums tuned per
  consistency level.
- Three-phase handoff under message-in-flight conditions verified
  by multi-node spec.
- Remember-entities cleanup on graceful and ungraceful shutdown.

### Cluster-metrics (`atomr-cluster-metrics`)

`d` → `b` (**shipped**): built-in `sysinfo`-backed probe (behind the
`sysinfo-probe` feature), `EWMA` smoothing, `MetricsSelector`
(`Cpu` / `Heap` / `Mix`), `WeightedRoutees`, and the existing
`AdaptiveLoadBalancer` together cover the `b` surface. Path to `a`:
metrics-gossip wiring at cluster scale and adaptive load-balancer
benchmarks.

### Distributed data (`atomr-distributed-data`)

`b`. Path to `a` is delta-CRDT propagation under churn, durable
store conformance, and consistency-level tests at multi-node scale.

## Persistence

### Core (`atomr-persistence`)

`b`. Path to `a` is recovery-permitter pressure tests, async
snapshotter retention semantics, persistent FSM coverage of the full
feature surface.

### Storage adapters

All adapters at `b`. The full TCK (including the replay edge-case
and extended snapshot suites) now runs per-backend in
`persistence-integration.yml`, with real-service jobs for Postgres,
MySQL, Redis, MongoDB, Cassandra, DynamoDB Local, Azurite, and the
redb-backed `atomr-distributed-data-lmdb` durable store. Path to `a`:

- Stress tests for ordering under failure (sequence skip on retry,
  recovery from torn writes, replay performance at scale).
- MSSQL real-service job (currently compile-checked only).

### Query

`c` → `b` (**shipped**): `events_by_tag`, `current_events_by_tag`,
`all_persistence_ids`, and `current_persistence_ids` are wired
through the journal trait, backed by the in-memory backend, and
covered by the persistence-query envelope spec. Path to `a`:
backend-side index hints, bounded streaming variants, and per-
backend ordering guarantees documented in the TCK.

## Reactive streams

### `atomr-streams`

`b`. Operator coverage is broad. Path to `a`:

- Substreams (`group_by`, `split_when`) under all the merge / take /
  drain edge cases.
- Hub patterns (`BroadcastHub`, `MergeHub`) with completion semantics
  that match the rest of the DSL.
- Stream refs across the cluster: `SourceRef` / `SinkRef` over
  remoting, with backpressure tunneled through the protocol.

## Hosting and integration

### `atomr-coordination`, `atomr-discovery`, `atomr-di`, `atomr-hosting`

`b`. Each gets to `a` when there's a non-trivial production-grade
backend (lease impl over a real coordination service, discovery
backend over DNS-SD or Kubernetes API, DI integration with a real
service framework).

## Observability

### `atomr-telemetry`, `atomr-dashboard`

`b`. Path to `a`:

- Cross-cluster aggregator stress-tested under fan-out at scale.
- Live in-UI sampling (heatmaps, flame graphs) over the WebSocket
  channel.
- Trace propagation via OpenTelemetry contexts woven through the
  remote PDU envelope.

## Tooling

### `atomr-profiler`

`b`. Path to `a` is more scenarios (steady-state cluster traffic,
sharded-entity throughput, persistent recovery), per-scenario
allocation tracking, profiling against the GPU dispatcher when it
lands.

### `cargo xtask`

`audit`, `verify`, `bump`, `parity`, `profile`, `dashboard`,
`sync-upstream` — all shipped. Depth additions: `audit` learning
about new sentinels, `parity` auto-generating depth grades from the
audit baseline + LOC ratios.

## Forward-looking

These aren't catch-up items. They are net-new directions that atomr
takes beyond the typical actor-runtime surface.

### GPU dispatcher

A `Dispatcher` implementation whose backend is a CUDA stream:

1. Accept envelopes destined for actors annotated as accelerator-
   resident.
2. Coalesce a window of compatible envelopes into a host buffer.
3. Submit a kernel and wait on a stream event.
4. Produce reply messages from the kernel result and feed them back
   through the envelope contract.

The supervision tree, the mailbox, the backpressure, the
observability hooks — all stay the same. The cost of moving a
workload onto an accelerator is `with_dispatcher("gpu")`, not a new
framework. See
[`actors-and-agentic-computing.md`](actors-and-agentic-computing.md)
for the full argument.

### Heterogeneous serialization

A serializer that lays out messages in accelerator-friendly tensor
layouts when the destination is a GPU dispatcher. Likely a feature
flag (`gpu-codec`) on the serializer registry; the wire format
stays serde / bincode for host-to-host traffic.

### Actor-graph integrations for agentic systems

Supervised agent state graphs as first-class actors, composed with
the existing cluster + persistence + observability stack. The graph
nodes are actors; the edges are typed messages; turn-taking is
explicit; failure is supervised.

## Acceptance philosophy

A depth bump lands as a coherent, shippable PR with its own
acceptance gate. The gate is the audit + the conformance suite, not
a date or a checkbox count. If a PR can't show the metric moving in
the right direction, it isn't done.

## See also

- [`audit-2026-04.md`](audit-2026-04.md) — empirical depth baseline.
- [`parity.md`](parity.md) — current depth grades.
- [`idiomatic-rust.md`](idiomatic-rust.md) — twelve invariants.
- [`actors-and-agentic-computing.md`](actors-and-agentic-computing.md)
  — the unified-compute thesis.
- [`alignment-ledger.md`](alignment-ledger.md) — crate-by-crate
  alignment of the runtime surface.
- [`depth-roadmap.md`](depth-roadmap.md) — depth roadmap by
  subsystem.
