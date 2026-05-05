# Alignment ledger

atomr is a native Rust actor runtime with first-class Python
bindings. It draws on the design vocabulary that decades of actor
runtimes have converged on — the module boundaries, the supervision
idioms, the persistence and clustering primitives, the testkit shape.
Keeping our crate boundaries recognizable across that vocabulary gives
us a benchmark for *what coverage looks like* in a mature platform.

This document is the alignment ledger. It is not a percent-complete
tracker. It is a map of which crate owns which subsystem, so that
contributors and reviewers can sanity-check that we haven't drifted
off the well-trodden trail of features that production users expect.

## Crate alignment

| atomr crate | Subsystem |
|---|---|
| `atomr-core` | actor system, supervision, dispatch, mailbox, FSM, event stream, coordinated shutdown, IO managers (TCP/UDP) |
| `atomr-config` | layered configuration (HOCON-style) |
| `atomr-testkit` | probes, virtual time, multi-node spec, event filters, out-of-process multi-node (`MultiNodeOopController` / `MultiNodeOopNode`) |
| `atomr-remote` | location-transparent messaging, framed PDU, ack'd delivery, watcher |
| `atomr-cluster` | membership, gossip, reachability, split-brain resolvers, leader handover |
| `atomr-cluster-tools` | singleton, distributed pub/sub, cluster client |
| `atomr-cluster-sharding` | shard regions, allocation, rebalance, remember-entities |
| `atomr-cluster-metrics` | adaptive load balancing |
| `atomr-distributed-data` | CRDT replicator (`OrMap`, `LWWMap`, `PNCounterMap`, `ORMultiMap`, subscribe) |
| `atomr-distributed-data-lmdb` | redb-backed `DurableStore` — single-writer / multi-reader / mmap, full DurableStore spec coverage |
| `atomr-persistence` | event sourcing, journals, snapshots, recovery permitter, persistent FSM, at-least-once delivery |
| `atomr-persistence-query` | tagged event streams over journals |
| `atomr-persistence-query-inmemory` | in-memory query journal |
| `atomr-persistence-{sql,redis,mongodb,cassandra,aws,azure}` | storage adapters (Postgres / MySQL / Redis / Mongo / Cassandra / DynamoDB / Azurite) |
| `atomr-persistence-tck` | conformance suite (journal + snapshot, replay edge cases, extended suites) |
| `atomr-serialization-hyperion` | Hyperion-compatible serializer surface |
| `atomr-streams` | typed reactive streams DSL (FlowOperator, Hub, SubStream, conflate/expand, merge_sorted/merge_prioritized, queue/restart) |
| `atomr-coordination` | lease primitives |
| `atomr-discovery` | service discovery |
| `atomr-di` | dependency-injection container |
| `atomr-hosting` | builder API for system + config + DI |

## Python bindings

The Python facade exposes the Rust crates above through PyO3 plus a
GIL-isolation layer (`InterpreterInstance`, `InterpreterQuota`,
`InterpreterMetrics`) that is atomr-native. See
[`docs/python.md`](docs/python.md).

| Python surface | Subsystem |
|---|---|
| `atomr._native.ActorSystem` | actor system |
| `atomr._native.Props` | actor configuration / construction |
| `atomr._native.ActorRef` | typed addressable reference |
| `atomr._native.testkit.*` | testkit |
| `atomr._native.cluster.*` | cluster (Member, Membership, VectorClock) |
| `atomr._native.cluster_tools.DistributedPubSub` | distributed pub/sub |
| `atomr._native.cluster_sharding.ShardRegion` | shard region |
| `atomr._native.ddata.*` | CRDT replicator |
| `atomr._native.persistence.InMemoryJournal` | in-memory journal |
| `atomr._native.coordination.InMemoryLease` | lease primitive |
| `atomr._native.discovery.StaticDiscovery` | service discovery |
| `atomr._native.di.ServiceContainer` | DI container |
| `atomr._native.hosting.ActorSystemBuilder` | hosting builder |

## Deliberate design choices

- **Wire format.** atomr uses Tokio + a serde / bincode framed PDU
  codec. The remote story is a clean native transport — see
  [`docs/remoting.md`](docs/remoting.md).
- **Typed refs.** `ActorRef<M>` is parameterized by message type and
  checked at compile time. There is no untyped reference you can pass
  around without the type info.
- **No reflection.** `Box<dyn Any>` is forbidden in public APIs.
  Serialization happens through typed codec registries, not
  reflective payload introspection.
- **Async-first.** Every `await` boundary uses tokio; there is no
  blocking inside `Actor::handle`. Blocking work goes onto a pinned
  dispatcher.
- **Sealed framework markers.** `Actor`, `Message`, `Serializer`, and
  similar markers are sealed so that downstream crates extend by
  composition, not by re-implementing the contract.

## Why this matters as atomr grows

The alignment ledger is a discipline, not a ceiling. As atomr grows —
GPU dispatchers, agent-graph integrations, native streaming codecs —
those new directions stand on top of the same module boundaries. The
boundary between `atomr-core` and `atomr-cluster` is the same boundary
mature systems have used for years; staying inside it keeps our
abstractions clean even when we're inventing.

## See also

- [`depth-roadmap.md`](depth-roadmap.md) — depth roadmap for each
  subsystem.
- [`parity.md`](parity.md) — current presence + depth grades.
- [`idiomatic-rust.md`](idiomatic-rust.md) — invariants that keep us
  honest as we extend.
