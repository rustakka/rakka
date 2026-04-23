"""Smoke test for rustakka.dashboard.

Starts the service bound to an ephemeral port, hits a handful of REST
endpoints with the stdlib HTTP client, and shuts down.
"""
from __future__ import annotations

import json
import urllib.request

from rustakka import dashboard


def _fetch_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=5) as resp:  # noqa: S310
        assert resp.status == 200, resp.status
        return json.loads(resp.read().decode("utf-8"))


def test_dashboard_serves_overview_snapshot():
    handle = dashboard.serve(bind="127.0.0.1:0", node="py-test")
    try:
        addr = handle.address
        base = f"http://{addr}"
        overview = _fetch_json(f"{base}/api/overview")
        assert overview["node"] == "py-test"
        assert "actor_count" in overview
        assert "dead_letter_count" in overview
        snap = _fetch_json(f"{base}/api/snapshot")
        assert snap["node"] == "py-test"
        assert "actors" in snap
    finally:
        handle.shutdown()


def test_dashboard_context_manager():
    with dashboard.serve(bind="127.0.0.1:0", node="py-ctx") as handle:
        assert handle.address.startswith("127.0.0.1:")
