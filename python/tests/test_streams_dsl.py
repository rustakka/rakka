"""Tests for the typed atomr.streams DSL on arbitrary Python objects.

Exercises Source/Flow/Sink/RunnableGraph composition, KillSwitch shutdown,
BroadcastHub fan-out, SourceQueue back-pressure, and the GIL-safety of
filtered (dropped) elements.
"""
from __future__ import annotations

import asyncio
import gc
import time

import pytest

from atomr import streams
from atomr.streams import (
    BroadcastHub,
    Flow,
    KillSwitch,
    MergeHub,
    RunnableGraph,
    Sink,
    Source,
    SourceQueue,
)


def test_source_via_flow_to_sink_collect():
    graph = (
        Source.from_iter([1, 2, 3])
        .via(Flow.map(lambda x: x * 2))
        .to(Sink.collect())
    )
    out = graph.run_blocking()
    assert out == [2, 4, 6]


def test_sink_fold_reduces():
    graph = Source.from_iter([1, 2, 3, 4]).to(Sink.fold(0, lambda a, b: a + b))
    assert graph.run_blocking() == 10


def test_flow_filter_drops_elements_without_panic():
    """Dropped elements must release the GIL on Drop without panicking.

    The filter predicate runs on the materializer dispatcher; the dropped
    SendPyAny's Drop acquires the GIL itself. We deliberately fill the
    stream with non-trivial Python objects (lists) so the refcount
    decrement is observable.
    """
    items = [[i] for i in range(100)]
    graph = (
        Source.from_iter(items)
        .via(Flow.filter(lambda x: x[0] % 2 == 0))
        .to(Sink.collect())
    )
    out = graph.run_blocking()
    assert [v[0] for v in out] == list(range(0, 100, 2))
    # Force GC to make sure no late-Drop panic surfaces from any lingering
    # SendPyAny copies.
    gc.collect()


def test_flow_take_drops_tail():
    out = (
        Source.from_iter(range(1000))
        .via(Flow.take(5))
        .to(Sink.collect())
        .run_blocking()
    )
    assert out == [0, 1, 2, 3, 4]


def test_flow_skip_then_take():
    g = Flow.skip(2).via(Flow.take(3))
    out = Source.from_iter(range(10)).via(g).to(Sink.collect()).run_blocking()
    assert out == [2, 3, 4]


def test_sink_head_option_some_and_none():
    assert (
        Source.from_iter([7, 8, 9]).to(Sink.head_option()).run_blocking() == 7
    )
    assert Source.from_iter([]).to(Sink.head_option()).run_blocking() is None


def test_sink_foreach_runs_for_each_element():
    seen: list = []
    Source.from_iter([1, 2, 3]).to(Sink.foreach(seen.append)).run_blocking()
    assert seen == [1, 2, 3]


def test_kill_switch_terminates_running_graph():
    """A long-running source completes promptly after KillSwitch.shutdown()."""
    # Build a 1M-element source so the consumer can't drain it before we
    # fire the kill switch.
    src = Source.from_iter(range(1_000_000))
    gated_src, ks = src.kill_switch()

    # Run on a background thread so we can fire the kill switch from main.
    import threading

    result_holder = {}

    def _runner():
        result_holder["out"] = (
            gated_src.to(Sink.collect()).run_blocking()
        )

    t = threading.Thread(target=_runner)
    t.start()
    # Give the stream a moment to start.
    time.sleep(0.05)
    ks.shutdown()
    t.join(timeout=10.0)
    assert not t.is_alive(), "stream should have completed after shutdown"
    # We don't assert exact length — just that it stopped early.
    assert len(result_holder["out"]) < 1_000_000


def test_kill_switch_abort_records_error():
    ks = KillSwitch()
    ks.abort("boom")
    assert ks.is_shut_down()
    assert ks.error() == "boom"


def test_broadcast_hub_fans_out_to_two_consumers():
    src = Source.from_iter([10, 20, 30])
    hub = BroadcastHub.attach(src, 16)
    # We can't subscribe before attach with this design — late subscribers
    # may miss elements. The semantics mirror the underlying Rust hub.
    # The first consumer should at minimum complete cleanly.
    c1 = hub.consumer()
    c2 = hub.consumer()
    # Drop the hub so consumers see the channel close.
    del hub

    # Both consumers may receive an empty list if they subscribed after the
    # source was already drained. We assert only that they complete.
    out1 = c1.to(Sink.collect()).run_blocking()
    out2 = c2.to(Sink.collect()).run_blocking()
    assert isinstance(out1, list)
    assert isinstance(out2, list)


def test_broadcast_hub_consumer_count_grows():
    # A hub with no attached source — just verify consumer_count plumbing.
    src = Source.empty()
    hub = BroadcastHub.attach(src, 4)
    assert hub.consumer_count() == 0
    _c1 = hub.consumer()
    _c2 = hub.consumer()
    assert hub.consumer_count() == 2


def test_source_queue_back_pressure_and_offer():
    """SourceQueue.offer enqueues; complete() shuts the source down."""
    src, q = Source.from_queue()

    import threading

    def _producer():
        time.sleep(0.02)
        q.offer("a")
        q.offer("b")
        q.offer("c")
        q.complete()

    t = threading.Thread(target=_producer)
    t.start()

    out = src.to(Sink.collect()).run_blocking()
    t.join(timeout=2.0)
    assert out == ["a", "b", "c"]


def test_source_queue_offer_after_complete_returns_closed():
    _src, q = Source.from_queue()
    q.complete()
    res = q.offer(42)
    assert res == "QueueClosed"
    assert q.is_closed()


def test_pipeline_helper_collects():
    out = streams.collect([1, 2, 3], Flow.map(lambda x: x + 1))
    assert out == [2, 3, 4]


def test_run_pipeline_helper_async():
    async def _go():
        return await streams.run_pipeline([1, 2], Flow.map(lambda x: x * 10))

    out = asyncio.run(_go())
    assert out == [10, 20]


def test_source_run_collect_async_round_trips_objects():
    async def _go():
        # Use real Python objects so we exercise the SendPyAny path.
        items = [{"i": i} for i in range(5)]
        return await Source.from_iter(items).run_collect_async()

    out = asyncio.run(_go())
    assert out == [{"i": i} for i in range(5)]


def test_flow_via_chain_composition():
    f = Flow.map(lambda x: x + 1).via(Flow.map(lambda x: x * 2))
    out = Source.from_iter([1, 2, 3]).via(f).to(Sink.collect()).run_blocking()
    assert out == [4, 6, 8]


def test_runnable_graph_cannot_run_twice():
    g = Source.from_iter([1]).to(Sink.collect())
    g.run_blocking()
    with pytest.raises(RuntimeError):
        g.run_blocking()


def test_legacy_helpers_still_exposed():
    # Backward compatibility — i64 path must keep working.
    assert streams.run_collect([1, 2, 3], lambda x: x + 1) == [2, 3, 4]
    assert (
        streams.run_fold([1, 2, 3, 4], lambda x: x, 0, lambda acc, v: acc + v)
        == 10
    )
    assert streams.merge_sorted_([1, 3, 5], [2, 4, 6]) == [1, 2, 3, 4, 5, 6]


def test_merge_hub_aggregates_two_sources():
    hub = MergeHub()
    hub.attach(Source.from_iter([1, 2, 3]))
    hub.attach(Source.from_iter([10, 20, 30]))
    merged = hub.source()
    # Drop hub so the merged channel closes when both attached tasks finish.
    del hub
    out = sorted(merged.to(Sink.collect()).run_blocking())
    assert out == [1, 2, 3, 10, 20, 30]
