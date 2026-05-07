# Observability Exporters

## Telemetry pipeline

`atomr-telemetry` installs **per-subsystem probes** (see the [Dashboard
view across crates](dashboard.md#viewing-behavior-across-atomr-crates)
table) that publish typed events to a `TelemetryBus`. The optional
**atomr-dashboard** subscribes to the same data for a **web UI and REST
API**; this document is about the **exporters** that turn that bus into
Prometheus and OpenTelemetry for long-term store and alert routing.

The telemetry bus inside `atomr-telemetry` can optionally push data
to external observability backends. Two exporters ship in-tree:

- **Prometheus** — pull-model `/metrics` scrape endpoint on the
  dashboard service.
- **OpenTelemetry** — push-model OTLP (gRPC or HTTP) or stdout
  exporter for metrics (and traces, when enabled).

Both are strictly opt-in: you must compile the matching cargo feature
**and** flip the runtime config. See [Dashboard](./dashboard.md#exporters-prometheus-opentelemetry)
for the full feature matrix and metric reference.

## Turning on Prometheus

1. Build the dashboard with the `metrics-prometheus` feature:
   ```bash
   cargo build -p atomr-dashboard --features bin,metrics-prometheus
   ```
2. Either pass `--prometheus` to the CLI, set
   ```toml
   [exporters.prometheus]
   enabled = true
   namespace = "atomr"
   ```
   in your dashboard config, or pass
   ```python
   dashboard.serve(exporters={"prometheus": True})
   ```
   from Python.
3. Scrape with:
   ```yaml
   scrape_configs:
     - job_name: atomr
       metrics_path: /metrics
       static_configs:
         - targets: ["worker-1:9100"]
   ```

## Turning on OpenTelemetry

1. Pick a transport and compile it in:
   - `metrics-otel-grpc` — OTLP via tonic (default port 4317)
   - `metrics-otel-http` — OTLP via reqwest (default port 4318)
   - `metrics-otel-stdout` — pretty-print to stdout (dev/tests)
2. Configure:
   ```toml
   [exporters.otlp]
   endpoint = "http://otel-collector:4317"
   protocol = "grpc"
   service_name = "atomr-app"
   interval_secs = 15
   traces = true
   [exporters.otlp.headers]
   authorization = "Bearer ..."
   ```
3. Or from Python:
   ```python
   dashboard.serve(
       exporters={
           "otlp": {
               "endpoint": "http://otel-collector:4317",
               "protocol": "grpc",
               "service_name": "atomr-app",
               "interval_secs": 15,
           },
       },
   )
   ```

## Metric reference

Every exporter emits the same semantic metrics under the naming
convention native to that backend. See the
[dashboard docs](./dashboard.md#prometheus) for the full table. Labels
are low-cardinality by design — the `actor_path` label on the mailbox
gauge is scoped to the live actor set and removed on stop.

## Cardinality guidance

- Do not proxy user-supplied strings into label values.
- The `journal` and `ddata.key` labels are application-chosen. If you
  spin up thousands, consider pre-aggregating before emission.
- `service.name` / `service.instance.id` resource attributes (OTLP) are
  set from the `node` label and the `service_name` config.
- Use an OTLP collector with a `metricstransform` processor if you need
  to drop or rewrite label values before reaching the backend.
