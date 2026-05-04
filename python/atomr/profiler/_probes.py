"""Resource probes + dispatcher autoselection."""

from __future__ import annotations

import os
import platform
from typing import Optional, Tuple

import atomr
from atomr import InterpreterQuota


def rss_bytes() -> Optional[int]:
    try:
        with open("/proc/self/status", "r", encoding="ascii") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        return None
    return None


def peak_rss_bytes() -> Optional[int]:
    try:
        with open("/proc/self/status", "r", encoding="ascii") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        return None
    return None


def cpu_time_ns() -> int:
    """User+system CPU time for the whole process (Python best-effort)."""
    t = os.times()
    return int((t.user + t.system + t.children_user + t.children_system) * 1e9)


def host_tag() -> str:
    return (
        f"{platform.system().lower()}/{platform.machine()} "
        f"cpus={os.cpu_count() or 0} py={platform.python_version()}"
    )


Dispatch = Tuple[str, str, int, Optional[InterpreterQuota]]


def best_dispatcher_for(scenario: str) -> Dispatch:
    """Return (dispatcher, role_label, pool_count, quota) for ``scenario``.

    Latency-sensitive scenarios prefer ``python-pinned``. CPU-bound ones
    pick whichever parallel dispatcher this CPython build supports.
    """
    if scenario in ("tell", "ask", "fanout"):
        return ("python-pinned", "latency", 1, None)

    if atomr.nogil_supported():
        return (
            "python-nogil",
            "nogil-cpu",
            max(2, os.cpu_count() or 2),
            InterpreterQuota(max_actors=64, max_handler_ms=1000),
        )

    if atomr.subinterpreters_supported():
        count = max(2, min(os.cpu_count() or 2, 8))
        return (
            "python-subinterpreter-pool",
            "subi-cpu",
            count,
            InterpreterQuota(max_actors=64, max_handler_ms=1000),
        )

    return ("python-pinned", "fallback-cpu", 1, None)
