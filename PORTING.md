# Alignment with prior-art actor runtimes

rakka stands on its own as a native Rust actor runtime. It also draws
on the design vocabulary of decades of mature actor runtimes — the
module boundaries, the supervision idioms, the persistence and
clustering primitives, the testkit shape. Keeping our crate boundaries
recognizable to people coming from those runtimes makes the transition
cheap and gives us a benchmark for *what coverage looks like* in a
mature platform.

This document is the alignment ledger. It is not a percent-complete
tracker. It is a map of which crate corresponds to which prior-art
module, so that contributors and reviewers can sanity-check that we
haven't drifted off the well-trodden trail of features that
production users expect.

## Crate alignment

| rakka crate | Prior-art module shape |
|---|---|
| `rakka-core` | actor system, supervision, dispatch, mailbox, FSM, event stream, coordinated shutdown |
| `rakka-config` | layered configuration (HOCON-style) |
| `rakka-testkit` | probes, virtual time, multi-node spec, event filters |
| `rakka-remote` | location-transparent messaging, framed PDU, ack'd delivery, watcher |
| `rakka-cluster` | membership, gossip, reachability, split-brain resolvers |
| `rakka-cluster-tools` | singleton, distributed pub/sub, cluster client |
| `rakka-cluster-sharding` | shard regions, allocation, rebalance, remember-entities |
| `rakka-cluster-metrics` | adaptive load balancing |
| `rakka-distributed-data` | CRDT replicator |
| `rakka-persistence` | event sourcing, journals, snapshots, recovery permitter |
| `rakka-persistence-query` | tagged event streams over journals |
| `rakka-persistence-tck` | conformance suite |
| `rakka-streams` | typed reactive streams DSL |
| `rakka-coordination` | lease primitives |
| `rakka-discovery` | service discovery |
| `rakka-di` | dependency-injection container |
| `rakka-hosting` | builder API for system + config + DI |

## Python bindings

The Python facade exposes the Rust crates above through PyO3 plus a
GIL-isolation layer (`InterpreterInstance`, `InterpreterQuota`,
`InterpreterMetrics`) that is rakka-native — it has no direct prior-
art equivalent. See [`docs/python.md`](docs/python.md).

| Python surface | Aligned with |
|---|---|
| `rakka._native.ActorSystem` | actor system |
| `rakka._native.Props` | actor configuration / construction |
| `rakka._native.ActorRef` | typed addressable reference |
| `rakka._native.testkit.*` | testkit |
| `rakka._native.cluster.*` | cluster (Member, Membership, VectorClock) |
| `rakka._native.cluster_tools.DistributedPubSub` | distributed pub/sub |
| `rakka._native.cluster_sharding.ShardRegion` | shard region |
| `rakka._native.ddata.*` | CRDT replicator |
| `rakka._native.persistence.InMemoryJournal` | in-memory journal |
| `rakka._native.coordination.InMemoryLease` | lease primitive |
| `rakka._native.discovery.StaticDiscovery` | service discovery |
| `rakka._native.di.ServiceContainer` | DI container |
| `rakka._native.hosting.ActorSystemBuilder` | hosting builder |

## Deliberate divergences

These are places where rakka does *not* line up with prior art, on
purpose:

- **Wire format.** rakka uses Tokio + a serde / bincode framed PDU
  codec. There is no wire compatibility with JVM or CLR actor
  runtimes. The remote story is a clean native transport — see
  [`docs/remoting.md`](docs/remoting.md).
- **Typed refs.** `ActorRef<M>` is parameterized by message type and
  checked at compile time. There is no untyped `IActorRef` analogue
  that you can pass around without the type info.
- **No reflection.** `Box<dyn Any>` is forbidden in public APIs.
  Serialization happens through typed codec registries, not
  reflective payload introspection.
- **Async-first.** Every `await` boundary uses tokio; there is no
  blocking inside `Actor::handle`. Blocking work goes onto a pinned
  dispatcher.
- **Sealed framework markers.** `Actor`, `Message`, `Serializer`, and
  similar markers are sealed so that downstream crates extend by
  composition, not by re-implementing the contract.

## Why this matters even when rakka grows past prior art

The alignment ledger is a discipline, not a ceiling. As rakka grows —
GPU dispatchers, agent-graph integrations, native streaming codecs —
those new directions stand on top of the same module boundaries. The
boundary between `rakka-core` and `rakka-cluster` is the same boundary
mature systems have used for years; staying inside it keeps our
abstractions clean even when we're inventing.

## See also

- [`PORTING_TODO.md`](PORTING_TODO.md) — depth roadmap for each
  subsystem.
- [`docs/parity.md`](docs/parity.md) — current presence + depth
  grades.
- [`docs/idiomatic-rust.md`](docs/idiomatic-rust.md) — invariants that
  keep us honest as we extend.
