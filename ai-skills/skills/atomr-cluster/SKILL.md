---
name: atomr-cluster
description: Use when bringing up clustering, sharding, singleton, distributed pub/sub, or distributed-data (CRDT) features in a project that depends on atomr. Covers feature flag selection, seed-node bootstrap, MessageExtractor design for sharding, and DistributedPubSub patterns. Triggers when adding cluster membership, ShardRegion, ShardCoordinator, ClusterSingleton, or Replicator usage.
---

# Building atomr clusters

atomr's cluster stack is split across several crates so you only pay
for what you use. This skill helps you pick the right one and wire it
correctly.

## Pick the right feature flag

| Need | Feature | Direct crate |
|---|---|---|
| Cross-process messaging only | `remote` | `atomr-remote` |
| Membership, gossip, reachability | `cluster` | `atomr-cluster` |
| Singleton, distributed pub/sub, cluster client | `cluster-tools` | `atomr-cluster-tools` |
| Sharded entities (rebalance, remember-entities) | `cluster-sharding` | `atomr-cluster-sharding` |
| Adaptive load balancing | `cluster-metrics` | `atomr-cluster-metrics` |
| CRDTs (`Replicator`) | `distributed-data` | `atomr-distributed-data` |

Cluster-grade applications usually want
`features = ["cluster", "cluster-tools", "cluster-sharding", "persistence"]`
or simply the `cluster-app` bundle.

`cluster-sharding` transitively requires `persistence` and
`distributed-data` because the shard coordinator persists allocations
and uses CRDTs for remember-entities.

## Bootstrapping membership

A node needs:

1. A unique `ActorSystem` name shared by every node in the cluster.
2. A bind address for remoting.
3. At least one **seed node** to contact.

```rust
use atomr::prelude::*;

let system = ActorSystem::create("my-cluster", Config::reference()).await?;
// `Config::reference()` loads `reference.conf` from your project; it should
// contain a `cluster { seed-nodes = [...] }` block. See docs/remoting.md.
```

Seed-node order matters during cold start: the first listed seed is
expected to be reachable first.

## Sharding

`ShardRegion` is the entry point. You provide:

- A **`MessageExtractor`** that maps an incoming message to an entity
  id and a shard id.
- An **entity factory** producing the per-entity behavior.
- A **`ShardCoordinator`** that owns shard placement.

Pattern (from `examples/sharded-keyvalue/`):

```rust
struct KvExtractor;
impl MessageExtractor for KvExtractor {
    type Message = KvCmd;
    fn entity_id(&self, m: &Self::Message) -> String { /* … */ }
    fn shard_id(&self, m: &Self::Message)  -> String { /* bucket entities */ }
}

let region = ShardRegion::new("node-1", Arc::new(KvExtractor), coord, factory);
region.deliver(my_cmd);
```

Design rules of thumb:

- **Number of shards ≫ number of nodes.** A common ratio is ~10×.
  Too few shards prevent rebalance from smoothing load; too many add
  coordinator overhead.
- **`shard_id` must be deterministic and well-distributed.** Hash the
  entity id; don't use the first character unless the keyspace is
  uniform (the example does it for clarity, not for production).
- **Entity ids should be stable.** Rebalance moves entities between
  nodes; an id that changes per-node breaks identity.
- **Passivation.** Use `PassivationTracker` to identify idle entities
  and stop them; they'll be recreated on next message.

## Singleton

`ClusterSingleton` (in `atomr-cluster-tools`) elects exactly one
instance of an actor across the cluster, with handoff on member loss.
Use it for cluster-wide schedulers, leaders, or external resource
owners — not for per-entity work (use sharding for that).

## Distributed pub/sub

`DistributedPubSub` (from `atomr-cluster-tools`) gives you
topic-broadcast and group-routing semantics:

```rust
use atomr_cluster_tools::DistributedPubSub;

let bus = DistributedPubSub::new();
bus.subscribe("room1", subscriber_ref);
bus.publish_msg("room1", ChatMsg { /* … */ });

bus.subscribe_to_group("work-queue", "G1", worker_ref);
bus.send_to_group("work-queue", "G1", JobMsg { /* … */ });   // round-robin
```

See `examples/cluster-pubsub-chat/` for the full pattern.

## Distributed data (CRDTs)

When you need cluster-wide state without a coordinator (counters, sets,
maps), use `atomr-distributed-data::Replicator`. It exposes the usual
CRDTs (G-Counter, OR-Set, LWW-Register, etc.) and replicates writes
under tunable consistency (`Local`, `Majority`, `All`).

For replicas that must survive a node restart, use
`atomr-distributed-data-lmdb::RedbDurableStore` — a redb-backed
implementation of `DurableStore` (the analog of Akka.NET's
`Akka.DistributedData.LightningDB.LmdbDurableStore`). Wire it into the
`Replicator` config when you mark a key as `durable`.

## Leader handover

`atomr_cluster::LeaderHandover` is a watcher that compares successive
membership snapshots and emits a `LeaderHandoverEvent` whenever the
elected leader changes (or transitions in/out of `None`). Subscribe
when you need to flush, fence, or hand off cluster-singleton-style
work as leadership moves between nodes.

## Split-brain

Always configure a split-brain resolver in production. atomr ships
several strategies (`KeepMajority`, `StaticQuorum`, `KeepOldest`,
`DownAllWhenUnstable`). See `crates/atomr-cluster/src/sbr.rs` and
`docs/architecture.md`.

## Observability

The dashboard (`atomr-dashboard`) and telemetry exporters are the
fastest way to debug membership and rebalance issues. Expect to wire
both in any non-trivial cluster deployment.

## Canonical references

- `examples/cluster-pubsub-chat/` — `DistributedPubSub`
- `examples/sharded-keyvalue/` — `ShardRegion`, `MessageExtractor`,
  `PassivationTracker`
- `crates/atomr-cluster/src/lib.rs` — membership API
- `crates/atomr-cluster/src/sbr.rs` — split-brain resolvers
- `crates/atomr-cluster-sharding/src/lib.rs` — sharding API
- `crates/atomr-cluster-tools/src/lib.rs` — singleton, pub/sub
- `docs/remoting.md` — transport + delivery semantics
- `docs/architecture.md` — cluster internals

Spec parity test files cover the cluster surface in depth — when in
doubt about expected semantics, search for them by name: `VectorClock`,
`MemberOrdering`, `Reachability`, `ClusterEvent`, `GossipSpec`,
`SbrStrategy`, `Heartbeat`, `MembershipState`, `Singleton`,
`ClusterClient`, `PubSub`, sharding `allocation`/`handoff`,
`FailureDetector`, `Endpoint state`, CRDT laws, `OrMap`, `LWWMap`,
`PNCounterMap`, `ORMultiMap`, `Replicator` subscribe.

## Common mistakes

- **Forgetting `cluster` implies `remote`.** Wire your transport.
- **Hand-rolled hashing in `shard_id`.** Use a stable hasher; small
  changes in algorithm reshuffle every entity.
- **Using ask across the cluster on a hot path.** Reliable delivery +
  local replies are usually cheaper.
- **Skipping the SBR.** A partition without one will leave you with
  two leaders; pick a strategy that matches your durability story.
