# Telemetry Dashboard

The **rakka-dashboard** is an optional, self-contained HTTP +
WebSocket service that visualizes a live rakka node. It ships with a
React single-page application (Vite + Tailwind + shadcn/ui + React Flow
+ Recharts) that is embedded directly into the Rust binary when built
with `--features embed-ui`.

The dashboard is strictly opt-in: nothing in `rakka-core` starts it
by default. Enable it explicitly from Rust (`DashboardServer::start`),
from Python (`rakka.dashboard.serve`), or with the standalone
`rakka-dashboard` binary.

## Architecture

```
┌───────────────────────────┐
│        rakka node      │
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

The **telemetry probes** live in the `rakka-telemetry` crate. They
hook every subsystem — actors, dead letters, cluster, sharding,
persistence, remote, streams, distributed-data — and publish
`TelemetryEvent`s onto a `tokio::sync::broadcast` bus. Anything that
subscribes (WebSocket clients, Prometheus exporter, OpenTelemetry
exporter, external aggregators) receives copies.

The **dashboard service** in `rakka-dashboard` layers an axum
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

## Viewing behavior across rakka crates

The dashboard is the **public face** of **telemetry hooks** in
`rakka-telemetry`, which is wired into the rest of the workspace so
you can see **one node’s** behavior as it spans multiple subsystems. You
are not looking at a single crate in isolation: the same
`TelemetryExtension` drives REST snapshots, the WebSocket event stream, and
optional cluster-wide fan-out.

| Area | What you see | Where it is instrumented |
|------|----------------|---------------------------|
| **Actors** | Tree, flat list, mailbox depth, spawn/stop | `rakka-core` + actor registry probe |
| **Dead letters** | Ring buffer of failed delivers | `ActorRef` / `DeadLetterFeed` |
| **Cluster** | Members, reachability, gossip | `rakka-cluster` state probe |
| **Sharding** | Regions, shard allocations | `rakka-cluster-sharding` |
| **Persistence** | Journals, persistence IDs, recent writes | `rakka-persistence` + `JournalAdmin` |
| **Remote** | Associations, byte counters | `rakka-remote` |
| **Streams** | Running/finished materialized graphs | `rakka-streams` |
| **Distributed data** | Keys, update counts | `rakka-distributed-data` replicator snapshot |

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
use rakka_dashboard::{DashboardConfig, DashboardMode, DashboardServer};
use rakka_telemetry::TelemetryExtension;

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
from rakka import dashboard

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
cargo run -p rakka-dashboard --features bin,embed-ui,aggregator,metrics-prometheus -- \
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

- `rakka_actors_spawned_total`, `rakka_actors_stopped_total`,
  `rakka_actors_live`
- `rakka_mailbox_depth{actor_path="…"}`
- `rakka_dead_letters_total`
- `rakka_cluster_members_up`, `rakka_cluster_unreachable`,
  `rakka_cluster_member_events_total{kind="…"}`
- `rakka_sharding_events_total{region,event}`,
  `rakka_sharding_allocations{region}`
- `rakka_persistence_events_written_total{journal}`,
  `rakka_persistence_last_sequence_nr{journal}`
- `rakka_remote_endpoints`,
  `rakka_remote_association_events_total{state}`,
  `rakka_remote_bytes{remote,direction}`
- `rakka_streams_running`, `rakka_streams_started_total`,
  `rakka_streams_finished_total`
- `rakka_ddata_updates_total{key}`

All metrics carry a constant `node="…"` label.

Example Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: rakka
    metrics_path: /metrics
    static_configs:
      - targets: ["worker-1.internal:9100", "worker-2.internal:9100"]
```

### OpenTelemetry

Pushes the same semantic metrics under OTel naming
(`rakka.actors.spawned`, `rakka.dead_letters`, etc.) to an OTLP
collector. Pick a transport at compile time and set a runtime config:

```toml
[exporters.otlp]
endpoint = "http://otel-collector:4317"
protocol = "grpc"              # or "http"
service_name = "rakka-app"
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
