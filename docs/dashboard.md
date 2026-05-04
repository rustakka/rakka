# Telemetry Dashboard

The **atomr-dashboard** is an optional, self-contained HTTP +
WebSocket service that visualizes a live atomr node. It ships with a
React single-page application (Vite + Tailwind + shadcn/ui + React Flow
+ Recharts) that is embedded directly into the Rust binary when built
with `--features embed-ui`.

The dashboard is strictly opt-in: nothing in `atomr-core` starts it
by default. Enable it explicitly from Rust (`DashboardServer::start`),
from Python (`atomr.dashboard.serve`), or with the standalone
`atomr-dashboard` binary.

## Architecture

```
┌───────────────────────────┐
│        atomr node      │
│ ┌────────────┬──────────┐ │   WebSocket   /ws (filtered events)
│ │  probes    │ telemetry│ │──────────────────────────────────▶
│ │ (actors,   │   bus    │ │     REST     /api/* (snapshots)
│ │  cluster…) │ broadcast│ │──────────────────────────────────▶
│ └────────────┴──────────┘ │     Metrics  /metrics (Prometheus)
│   │       │      │        │──────────────────────────────────▶
│   ▼       ▼      ▼        │     OTLP     OpenTelemetry push
│  Prom    OTel  Dashboard  │──────────────────────────────────▶
└───────────────────────────┘
```

The **telemetry probes** live in the `atomr-telemetry` crate. They
hook every subsystem — actors, dead letters, cluster, sharding,
persistence, remote, streams, distributed-data — and publish
`TelemetryEvent`s onto a `tokio::sync::broadcast` bus. Anything that
subscribes (WebSocket clients, Prometheus exporter, OpenTelemetry
exporter, external aggregators) receives copies.

The **dashboard service** in `atomr-dashboard` layers an axum
`Router` over the telemetry bus:

- `GET /api/overview` — rollup vitals
- `GET /api/actors/tree`, `/api/actors`
- `GET /api/dead-letters?limit=…`
- `GET /api/cluster/state`, `/api/cluster/reachability`
- `GET /api/sharding`
- `GET /api/persistence`
- `GET /api/remote`, `/api/streams`, `/api/ddata`
- `GET /api/snapshot` — every probe in one payload
- `GET /ws` — filtered live event stream (`?topics=actors,cluster,…`)
- `GET /metrics` — Prometheus text exposition (behind feature flag)
- `GET /api/cluster-wide/*` — aggregated views fanned across peers
  (behind the `aggregator` feature)
- `GET /` — the embedded SPA (behind `embed-ui`)

## Viewing behavior across atomr crates

The dashboard is the **public face** of **telemetry hooks** in
`atomr-telemetry`, which is wired into the rest of the workspace so
you can see **one node’s** behavior as it spans multiple subsystems. You
are not looking at a single crate in isolation: the same
`TelemetryExtension` drives REST snapshots, the WebSocket event stream, and
optional cluster-wide fan-out.

| Area | What you see | Where it is instrumented |
|------|----------------|---------------------------|
| **Actors** | Tree, flat list, mailbox depth, spawn/stop | `atomr-core` + actor registry probe |
| **Dead letters** | Ring buffer of failed delivers | `ActorRef` / `DeadLetterFeed` |
| **Cluster** | Members, reachability, gossip | `atomr-cluster` state probe |
| **Sharding** | Regions, shard allocations | `atomr-cluster-sharding` |
| **Persistence** | Journals, persistence IDs, recent writes | `atomr-persistence` + `JournalAdmin` |
| **Remote** | Associations, byte counters | `atomr-remote` |
| **Streams** | Running/finished materialized graphs | `atomr-streams` |
| **Distributed data** | Keys, update counts | `atomr-distributed-data` replicator snapshot |

**How to use it:** after `TelemetryExtension` is installed, start
`DashboardServer` (or the standalone binary) and open the UI or call
`GET /api/snapshot` for a **single JSON** document that combines every
probe. Use `GET /ws?topics=…` for a **live, filtered** `TelemetryEvent`
stream. In **cluster** mode, `GET /api/cluster-wide/…` merges peer
dashboards so you can reason about a **fleet** of nodes without SSHing
into each host.

Exporters (Prometheus, OpenTelemetry) attach to the same bus, so
**metrics in your scraper** and **panels in the dashboard** describe the
same activity. For exporter setup, see [Observability](observability.md).

## Connection modes

1. **Local** — the service runs in the same process as the `ActorSystem`
   and reads its in-process `TelemetryExtension`. The default.
2. **Remote** — the browser points at any node's dashboard URL. No
   special configuration required.
3. **Cluster** — one node runs as the aggregator with `peers = [..]`;
   the extra `/api/cluster-wide/*` endpoints fan requests to peer
   dashboards using `reqwest` and merge their responses. Requires the
   `aggregator` cargo feature.

## Enabling the service

### From Rust

```rust
use atomr_dashboard::{DashboardConfig, DashboardMode, DashboardServer};
use atomr_telemetry::TelemetryExtension;

let telemetry = TelemetryExtension::new("worker-1", 1024).install(&system);
let server = DashboardServer::new(
    telemetry,
    DashboardConfig {
        bind: "127.0.0.1:9100".parse()?,
        mode: DashboardMode::Local,
        ..Default::default()
    },
);
let handle = server.start().await?;
// …
handle.shutdown().await;
```

### From Python

```python
from atomr import dashboard

handle = dashboard.serve(
    bind="127.0.0.1:9100",
    node="worker-1",
    exporters={
        "prometheus": True,
        "otlp": {
            "endpoint": "http://collector:4317",
            "protocol": "grpc",
            "service_name": "my-app",
        },
    },
)
# ... or as a context manager ...
with dashboard.serve(bind="127.0.0.1:0", node="dev") as h:
    print(h.address)
```

### Standalone binary

```bash
cargo run -p atomr-dashboard --features bin,embed-ui,aggregator,metrics-prometheus -- \
    --bind 127.0.0.1:9100 \
    --node worker-1 \
    --prometheus \
    --otlp-endpoint http://localhost:4317 \
    --otlp-protocol grpc
```

Or, using the workspace xtask:

```bash
cargo xtask dashboard -- --bind 127.0.0.1:9100 --node worker-1
```

## Exporters (Prometheus + OpenTelemetry)

Both exporters are **off by default** and require (a) the matching
cargo feature and (b) a runtime opt-in.

| Exporter | Telemetry feature       | Dashboard feature       |
|----------|-------------------------|--------------------------|
| Prometheus | `prometheus`          | `metrics-prometheus`     |
| OTel core  | `otel`                | `metrics-otel`           |
| OTLP gRPC  | `otel-otlp-grpc`      | `metrics-otel-grpc`      |
| OTLP HTTP  | `otel-otlp-http`      | `metrics-otel-http`      |
| OTel stdout| `otel-stdout`         | `metrics-otel-stdout`    |

### Prometheus

When enabled, the dashboard mounts `GET /metrics` serving the standard
text exposition. Metric names:

- `atomr_actors_spawned_total`, `atomr_actors_stopped_total`,
  `atomr_actors_live`
- `atomr_mailbox_depth{actor_path="…"}`
- `atomr_dead_letters_total`
- `atomr_cluster_members_up`, `atomr_cluster_unreachable`,
  `atomr_cluster_member_events_total{kind="…"}`
- `atomr_sharding_events_total{region,event}`,
  `atomr_sharding_allocations{region}`
- `atomr_persistence_events_written_total{journal}`,
  `atomr_persistence_last_sequence_nr{journal}`
- `atomr_remote_endpoints`,
  `atomr_remote_association_events_total{state}`,
  `atomr_remote_bytes{remote,direction}`
- `atomr_streams_running`, `atomr_streams_started_total`,
  `atomr_streams_finished_total`
- `atomr_ddata_updates_total{key}`

All metrics carry a constant `node="…"` label.

Example Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: atomr
    metrics_path: /metrics
    static_configs:
      - targets: ["worker-1.internal:9100", "worker-2.internal:9100"]
```

### OpenTelemetry

Pushes the same semantic metrics under OTel naming
(`atomr.actors.spawned`, `atomr.dead_letters`, etc.) to an OTLP
collector. Pick a transport at compile time and set a runtime config:

```toml
[exporters.otlp]
endpoint = "http://otel-collector:4317"
protocol = "grpc"              # or "http"
service_name = "atomr-app"
interval_secs = 15
traces = true                  # emit message-handle spans
[exporters.otlp.headers]
authorization = "Bearer ..."
```

For development/tests set `stdout = true` and build with
`--features metrics-otel-stdout` to pretty-print metrics to the console.

### Cardinality

All labels are low-cardinality by construction:

- `node`, `kind`, `event`, `region`, `state`, `direction` are bounded.
- `actor_path` is bounded by the live actor set and gets cleaned up on
  actor stop (the exporter removes its label series).
- `key` (ddata) and `journal` are user-chosen but typically small.

## UI layout

- **Overview** — vitals grid + sparklines
- **Actors** — React Flow tree + inspector drawer
- **Dead letters** — virtualized table with live-follow + filters
- **Cluster** — ring visual, reachability heatmap, gossip versions
- **Sharding**, **Persistence**, **Remote**, **Streams**, **DData** —
  per-subsystem dashboards
- **Events** — unified live feed from the WebSocket

Mobile breakpoints collapse the sidebar into a five-tab bottom bar and
stack dashboard cards into a single column; React Flow canvases become
full-viewport with floating filter sheets.

## Security

The dashboard binds to loopback by default. There is **no built-in
authentication or TLS**. To expose it to a network place it behind a
reverse proxy (nginx, Traefik, Envoy) that terminates TLS and enforces
authentication.
