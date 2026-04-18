# Upstream Akka.NET porting log

This file tracks the upstream Akka.NET commit that each rustakka crate was
last synchronized with. `cargo xtask sync-upstream` diffs the working tree
against these commits and reports which files need review.

| Crate | Upstream path | Last synced commit |
|-------|---------------|--------------------|
| rustakka-core | src/core/Akka | (initial) |
| rustakka-config | src/core/Akka/Configuration | (initial) |
| rustakka-testkit | src/core/Akka.TestKit | (initial) |
| rustakka-remote | src/core/Akka.Remote | (initial) |
| rustakka-cluster | src/core/Akka.Cluster | (initial) |
| rustakka-cluster-tools | src/contrib/cluster/Akka.Cluster.Tools | (initial) |
| rustakka-cluster-sharding | src/contrib/cluster/Akka.Cluster.Sharding | (initial) |
| rustakka-cluster-metrics | src/contrib/cluster/Akka.Cluster.Metrics | (initial) |
| rustakka-distributed-data | src/contrib/cluster/Akka.DistributedData | (initial) |
| rustakka-persistence | src/core/Akka.Persistence | (initial) |
| rustakka-persistence-query | src/core/Akka.Persistence.Query | (initial) |
| rustakka-persistence-tck | src/core/Akka.Persistence.TCK | (initial) |
| rustakka-streams | src/core/Akka.Streams | (initial) |
| rustakka-coordination | src/core/Akka.Coordination | (initial) |
| rustakka-discovery | src/core/Akka.Discovery | (initial) |
| rustakka-di | src/contrib/dependencyinjection/Akka.DependencyInjection | (initial) |
| rustakka-hosting | Akka.Hosting (external) | (initial) |

## Python bindings

The Python bindings live under `crates/py-bindings/*` and `python/`.
They are not a line-by-line port of any single Akka.NET component —
they expose the Rust crates listed above through PyO3 and add a
GIL-isolation layer (`InterpreterInstance`, `InterpreterQuota`,
`InterpreterMetrics`) that has no direct Akka.NET analog.

| Component | Upstream analog |
|-----------|-----------------|
| `rustakka._native.ActorSystem` | `Akka.Actor.ActorSystem` |
| `rustakka._native.Props` | `Akka.Actor.Props` |
| `rustakka._native.ActorRef` | `Akka.Actor.IActorRef` |
| `rustakka._native.testkit.*` | `Akka.TestKit` |
| `rustakka._native.cluster.*` | `Akka.Cluster` (Member, Membership, VectorClock) |
| `rustakka._native.cluster_tools.DistributedPubSub` | `Akka.Cluster.Tools.PublishSubscribe` |
| `rustakka._native.cluster_sharding.ShardRegion` | `Akka.Cluster.Sharding.ShardRegion` |
| `rustakka._native.ddata.*` | `Akka.DistributedData` CRDTs |
| `rustakka._native.persistence.InMemoryJournal` | `Akka.Persistence.Journal.MemoryJournal` |
| `rustakka._native.coordination.InMemoryLease` | `Akka.Coordination.Lease` |
| `rustakka._native.discovery.StaticDiscovery` | `Akka.Discovery.Config.ConfigServiceDiscovery` |
| `rustakka._native.di.ServiceContainer` | `Akka.DependencyInjection.DependencyResolver` |
| `rustakka._native.hosting.ActorSystemBuilder` | `Akka.Hosting.AkkaConfigurationBuilder` |

GIL/interpreter infrastructure is rustakka-specific and has no Akka.NET
equivalent; see [`docs/python.md`](docs/python.md).

## Skipped / deferred

- `Akka.FSharp` — no direct Rust analog; ergonomics captured in `rustakka-macros`.
- `Akka.MultiNode.*` test-adapter infrastructure — ported as `rustakka-testkit`
  multi-process harness instead.
- `Akka.Remote.Transport.DotNetty` — we use Tokio + Prost as a native Rust
  transport; no wire compatibility with JVM/CLR Akka by design.
- `Akka.Serialization.Hyperion` — Hyperion is CLR-specific; Rust uses Serde
  with JSON/bincode (and pluggable codecs). The `rustakka-serialization-hyperion`
  crate is a no-op placeholder kept only for layout parity.
- Python bindings: Phase P3 (`pyremote` + pluggable Python codecs) deferred
  until the native remote story crosses a process boundary.

## Maintenance process

The upstream akka.net tree is **not committed** to this repository. It is
cloned on demand into `./akka.net/` (gitignored) by
`scripts/sync-upstream.py`, which also produces the change-analysis
report.

1. Clone/fetch + diff locally:

   ```bash
   python scripts/sync-upstream.py               # clone if missing, fetch, diff HEAD~200..HEAD
   python scripts/sync-upstream.py --since <sha> # diff since the SHA tracked above
   python scripts/sync-upstream.py -o report.md  # write the report to a file

   # equivalent via the xtask alias
   cargo xtask sync-upstream -- --since <sha>
   ```

2. The quarterly GitHub Actions job (`upstream-diff` in
   `.github/workflows/ci.yml`) runs the same script with `--depth 1000`,
   publishes the report to the run's step summary, and uploads it as an
   artifact.

3. After porting an upstream change, bump the "Last synced commit" column
   above and commit — nothing in `./akka.net/` needs to be tracked.
