"""Optional telemetry dashboard.

Thin Python facade over ``rustakka._native.dashboard``. Starts the axum
HTTP/WebSocket service that backs the React UI, optionally enabling the
Prometheus ``/metrics`` scrape endpoint and/or pushing OpenTelemetry
metrics/traces over OTLP.

Example:

    from rustakka import dashboard

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
    try:
        ...
    finally:
        handle.shutdown()

``exporters`` entries are silently ignored when the underlying wheel was
built without the matching cargo feature (``metrics-prometheus`` /
``metrics-otel*``). Use ``pip install rustakka[observability]`` once the
extras are published, or build from source with the matching features
enabled.
"""
from __future__ import annotations

from typing import Any

from . import _native

_sub = _native.dashboard


def serve(
    bind: str = "127.0.0.1:9100",
    node: str = "local",
    peers: list[str] | None = None,
    exporters: dict[str, Any] | None = None,
):
    """Start the dashboard service.

    Args:
        bind: ``host:port`` to listen on. Defaults to loopback only.
        node: Node label attached to telemetry metrics and events.
        peers: Optional list of peer dashboard URLs. When non-empty the
            service runs in cluster-aggregator mode and fans out to the
            peers on ``/api/cluster-wide/*`` routes.
        exporters: Optional dict selecting external observability
            backends. Recognized keys:

            * ``"prometheus"`` — ``True`` / ``False`` or a dict
              ``{"enabled": True, "namespace": "rustakka"}``
            * ``"otlp"`` — dict describing an OpenTelemetry OTLP exporter.
              Required key: ``"endpoint"``. Optional keys: ``"protocol"``
              (``"grpc"`` / ``"http"``), ``"service_name"``,
              ``"interval_secs"``, ``"headers"``, ``"resource_attributes"``,
              ``"stdout"``.

    Returns:
        A ``DashboardHandle`` usable as a context manager. Call
        ``handle.shutdown()`` (or exit the ``with`` block) to stop the
        server gracefully.
    """
    return _sub.serve(bind=bind, node=node, peers=peers, exporters=exporters)


DashboardHandle = _sub.DashboardHandle

__all__ = ["serve", "DashboardHandle"]
