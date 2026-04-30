# Upstream Akka.NET porting log

This file tracks the upstream Akka.NET commit that each rakka crate was
last synchronized with. `cargo xtask sync-upstream` diffs the working tree
against these commits and reports which files need review.

| Crate | Upstream path | Last synced commit |
|-------|---------------|--------------------|
| rakka-core | src/core/Akka | (initial) |
| rakka-config | src/core/Akka/Configuration | (initial) |
| rakka-testkit | src/core/Akka.TestKit | (initial) |
| rakka-remote | src/core/Akka.Remote | (initial) |
| rakka-cluster | src/core/Akka.Cluster | (initial) |
| rakka-cluster-tools | src/contrib/cluster/Akka.Cluster.Tools | (initial) |
| rakka-cluster-sharding | src/contrib/cluster/Akka.Cluster.Sharding | (initial) |
| rakka-cluster-metrics | src/contrib/cluster/Akka.Cluster.Metrics | (initial) |
| rakka-distributed-data | src/contrib/cluster/Akka.DistributedData | (initial) |
| rakka-persistence | src/core/Akka.Persistence | (initial) |
| rakka-persistence-query | src/core/Akka.Persistence.Query | (initial) |
| rakka-persistence-tck | src/core/Akka.Persistence.TCK | (initial) |
| rakka-streams | src/core/Akka.Streams | (initial) |
| rakka-coordination | src/core/Akka.Coordination | (initial) |
| rakka-discovery | src/core/Akka.Discovery | (initial) |
| rakka-di | src/contrib/dependencyinjection/Akka.DependencyInjection | (initial) |
| rakka-hosting | Akka.Hosting (external) | (initial) |

## Python bindings

The Python bindings live under `crates/py-bindings/*` and `python/`.
They are not a line-by-line port of any single Akka.NET component —
they expose the Rust crates listed above through PyO3 and add a
GIL-isolation layer (`InterpreterInstance`, `InterpreterQuota`,
`InterpreterMetrics`) that has no direct Akka.NET analog.

| Component | Upstream analog |
|-----------|-----------------|
| `rakka._native.ActorSystem` | `Akka.Actor.ActorSystem` |
| `rakka._native.Props` | `Akka.Actor.Props` |
| `rakka._native.ActorRef` | `Akka.Actor.IActorRef` |
| `rakka._native.testkit.*` | `Akka.TestKit` |
| `rakka._native.cluster.*` | `Akka.Cluster` (Member, Membership, VectorClock) |
| `rakka._native.cluster_tools.DistributedPubSub` | `Akka.Cluster.Tools.PublishSubscribe` |
| `rakka._native.cluster_sharding.ShardRegion` | `Akka.Cluster.Sharding.ShardRegion` |
| `rakka._native.ddata.*` | `Akka.DistributedData` CRDTs |
| `rakka._native.persistence.InMemoryJournal` | `Akka.Persistence.Journal.MemoryJournal` |
| `rakka._native.coordination.InMemoryLease` | `Akka.Coordination.Lease` |
| `rakka._native.discovery.StaticDiscovery` | `Akka.Discovery.Config.ConfigServiceDiscovery` |
| `rakka._native.di.ServiceContainer` | `Akka.DependencyInjection.DependencyResolver` |
| `rakka._native.hosting.ActorSystemBuilder` | `Akka.Hosting.AkkaConfigurationBuilder` |

GIL/interpreter infrastructure is rakka-specific and has no Akka.NET
equivalent; see [`docs/python.md`](docs/python.md).

## Skipped / deferred

- `Akka.FSharp` — no direct Rust analog; ergonomics captured in `rakka-macros`.
- `Akka.MultiNode.*` test-adapter infrastructure — ported as `rakka-testkit`
  multi-process harness instead.
- `Akka.Remote.Transport.DotNetty` — we use Tokio + bincode as a native Rust
  transport (`rakka_remote::TcpTransport`); no wire compatibility with
  JVM/CLR Akka by design. See `docs/remoting.md`.
- `Akka.Serialization.Hyperion` — Hyperion is CLR-specific; Rust uses Serde
  with JSON/bincode (and pluggable codecs). The `rakka-serialization-hyperion`
  crate is a no-op placeholder kept only for layout parity.
- Python bindings: Phase P3 (`pyremote` + pluggable Python codecs) — the
  native remote story now crosses a process boundary (see
  `docs/remoting.md`), so this phase is unblocked. Implementation
  itself is deferred to a future pass.

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
