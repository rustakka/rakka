"""Optional telemetry dashboard.

Thin Python facade over ``atomr._native.dashboard``. Starts the axum
HTTP/WebSocket service that backs the React UI, optionally enabling the
Prometheus ``/metrics`` scrape endpoint and/or pushing OpenTelemetry
metrics/traces over OTLP.

Example:

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
    try:
        ...
    finally:
        handle.shutdown()

``exporters`` entries are silently ignored when the underlying wheel was
built without the matching cargo feature (``metrics-prometheus`` /
``metrics-otel*``). Use ``pip install atomr[observability]`` once the
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
    system: Any | None = None,
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
              ``{"enabled": True, "namespace": "atomr"}``
            * ``"otlp"`` — dict describing an OpenTelemetry OTLP exporter.
              Required key: ``"endpoint"``. Optional keys: ``"protocol"``
              (``"grpc"`` / ``"http"``), ``"service_name"``,
              ``"interval_secs"``, ``"headers"``, ``"resource_attributes"``,
              ``"stdout"``.
        system: Optional :class:`atomr.ActorSystem` to attach to. When
            provided, the dashboard renders live data from that system
            (actor tree, dead letters, cluster, sharding, persistence,
            etc.). Without it the dashboard runs against an empty
            telemetry extension.

    Returns:
        A ``DashboardHandle`` usable as a context manager. Call
        ``handle.shutdown()`` (or exit the ``with`` block) to stop the
        server gracefully.
    """
    return _sub.serve(
        bind=bind,
        node=node,
        peers=peers,
        exporters=exporters,
        system=system,
    )


DashboardHandle = _sub.DashboardHandle


def start_demo_graph(system: Any, name: str) -> int:
    """Register a fake running stream graph on ``system``'s telemetry probe.

    Pure-Python streams runs don't yet feed the dashboard's Streams page —
    use this (paired with :func:`finish_demo_graph`) to populate it from
    a script. Requires ``system`` to have telemetry installed; the easiest
    way is to start the dashboard with ``serve(..., system=system)`` first.
    """
    return _sub.start_demo_graph(system, name)


def finish_demo_graph(system: Any, graph_id: int) -> None:
    """Companion to :func:`start_demo_graph`. ``graph_id`` is what
    :func:`start_demo_graph` returned."""
    _sub.finish_demo_graph(system, graph_id)


__all__ = ["serve", "DashboardHandle", "start_demo_graph", "finish_demo_graph"]
