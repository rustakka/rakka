"""High-level Python facade over ``atomr._native.streams``.

Two layers are exposed:

1. **Typed DSL on arbitrary Python objects** — :class:`Source`, :class:`Sink`,
   :class:`Flow`, :class:`RunnableGraph`, :class:`KillSwitch`,
   :class:`SourceQueue`, :class:`SinkQueue`, :class:`BroadcastHub`,
   :class:`MergeHub`. Build a graph with the builder methods, then call
   ``graph.run()`` (asyncio awaitable) or ``graph.run_blocking()``.

2. **Legacy i64 helpers** — kept for backward compatibility:
   :func:`map_reduce`, :func:`run_collect`, :func:`run_fold`,
   :func:`via_keep_alive`, :func:`via_initial_delay`, :func:`via_conflate`,
   :func:`via_expand`, :func:`merge_sorted_`, :func:`merge_prioritized_`,
   :func:`via_split_after_count`, :func:`via_prefix_and_tail`,
   :func:`via_recover_with_retries`, :func:`via_select_error`.

Stream callbacks acquire the GIL inline on the materializer dispatcher.
Element drops happen inside ``Python::with_gil`` via the Rust
``SendPyAny`` newtype so that ``filter`` / ``take`` / ``KillSwitch`` do not
panic on Drop.
"""
from __future__ import annotations

from typing import Any, Iterable, List, Optional

from . import _native

_sub = _native.streams

# Typed DSL classes.
Source = _sub.Source
Sink = _sub.Sink
Flow = _sub.Flow
RunnableGraph = _sub.RunnableGraph
KillSwitch = _sub.KillSwitch
SourceQueue = _sub.SourceQueue
SinkQueue = _sub.SinkQueue
BroadcastHub = _sub.BroadcastHub
MergeHub = _sub.MergeHub

# Legacy helpers.
map_reduce = _sub.map_reduce
run_collect = _sub.run_collect
run_fold = _sub.run_fold
via_keep_alive = _sub.via_keep_alive
via_initial_delay = _sub.via_initial_delay
via_conflate = _sub.via_conflate
via_expand = _sub.via_expand
merge_sorted_ = _sub.merge_sorted_
merge_prioritized_ = _sub.merge_prioritized_
via_split_after_count = _sub.via_split_after_count
via_prefix_and_tail = _sub.via_prefix_and_tail
via_recover_with_retries = _sub.via_recover_with_retries
via_select_error = _sub.via_select_error


# --- High-level conveniences ----------------------------------------------

def pipeline(
    items: Iterable[Any],
    *flows: "Flow",
    sink: Optional["Sink"] = None,
) -> "RunnableGraph":
    """Compose ``Source.from_iter(items).via(*flows).to(sink or Sink.collect())``.

    Equivalent to::

        src = Source.from_iter(items)
        for f in flows:
            src = src.via(f)
        return src.to(sink or Sink.collect())
    """
    src = Source.from_iter(items)
    for f in flows:
        src = src.via(f)
    return src.to(sink if sink is not None else Sink.collect())


async def run_pipeline(
    items: Iterable[Any],
    *flows: "Flow",
    sink: Optional["Sink"] = None,
) -> Any:
    """Awaitable convenience: build & run a one-shot pipeline."""
    graph = pipeline(items, *flows, sink=sink)
    return await graph.run()


def collect(items: Iterable[Any], *flows: "Flow") -> List[Any]:
    """Synchronously run ``items`` through the given flows into a list."""
    graph = pipeline(items, *flows, sink=Sink.collect())
    return graph.run_blocking()


__all__ = [
    # typed DSL
    "Source",
    "Sink",
    "Flow",
    "RunnableGraph",
    "KillSwitch",
    "SourceQueue",
    "SinkQueue",
    "BroadcastHub",
    "MergeHub",
    # conveniences
    "pipeline",
    "run_pipeline",
    "collect",
    # legacy
    "map_reduce",
    "run_collect",
    "run_fold",
    "via_keep_alive",
    "via_initial_delay",
    "via_conflate",
    "via_expand",
    "merge_sorted_",
    "merge_prioritized_",
    "via_split_after_count",
    "via_prefix_and_tail",
    "via_recover_with_retries",
    "via_select_error",
]
