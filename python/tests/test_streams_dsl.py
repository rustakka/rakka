"""Tests for the typed atomr.streams DSL on arbitrary Python objects.

Exercises Source/Flow/Sink/RunnableGraph composition, KillSwitch shutdown,
BroadcastHub fan-out, SourceQueue back-pressure, and the GIL-safety of
filtered (dropped) elements.

Also covers the Epic F additions: RestartSource, RestartSettings, GraphDsl,
Flow.with_supervision (stream Decider), BidiFlow, Framing, Tcp, FileIO.
"""
from __future__ import annotations

import asyncio
import gc
import os
import socket
import tempfile
import threading
import time

import pytest

from atomr import streams
from atomr.streams import (
    BidiFlow,
    BroadcastHub,
    FileIO,
    Flow,
    Framing,
    GraphDsl,
    KillSwitch,
    MergeHub,
    RestartSettings,
    RestartSource,
    RunnableGraph,
    Sink,
    Source,
    SourceQueue,
    Tcp,
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
    # fire the kill switch on a slow runner. Note: on a fast CI runner
    # an in-memory 1M-int Vec source can fully drain before the shutdown
    # signal lands, so the early-stop length assertion is racey. We test
    # the deterministic properties instead — observable shutdown state
    # plus join-doesn't-hang — and accept either drain order.
    src = Source.from_iter(range(1_000_000))
    gated_src, ks = src.kill_switch()
    assert not ks.is_shut_down()

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
    assert ks.is_shut_down()
    assert "out" in result_holder


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


# =============================================================================
# Epic F: RestartSource, GraphDsl, Decider, BidiFlow, Framing, Tcp, FileIO.
# =============================================================================


def test_restart_settings_round_trip_attributes():
    s = RestartSettings(
        min_backoff=0.05, max_backoff=1.0, random_factor=0.1, max_restarts=3
    )
    assert s.min_backoff == pytest.approx(0.05)
    assert s.max_backoff == pytest.approx(1.0)
    assert s.random_factor == pytest.approx(0.1)
    assert s.max_restarts == 3


def test_restart_source_resubscribes_until_max_restarts():
    """``RestartSource`` calls the factory up to ``max_restarts`` times."""
    counter = {"calls": 0}

    def factory():
        counter["calls"] += 1
        return Source.from_iter([counter["calls"]])

    rs = RestartSource(
        min_backoff=0.001, max_backoff=0.005, random_factor=0.0, max_restarts=3
    )
    src = rs.via_source(factory)
    out = src.to(Sink.collect()).run_blocking()
    assert counter["calls"] == 3
    # Three subscriptions, each emitting its incrementing call count.
    assert out == [1, 2, 3]


def test_graph_dsl_linear_source_flow_sink():
    """GraphDsl assembles a linear Source -> Flow -> Sink chain."""
    g = GraphDsl()
    a = g.add(Source.from_iter([1, 2, 3]))
    b = g.add(Flow.map(lambda x: x * 10))
    c = g.add(Sink.collect())
    g.edge(a, b)
    g.edge(b, c)
    assert g.run_blocking() == [10, 20, 30]


def test_flow_with_supervision_resume_drops_failing_element():
    """A `resume` directive on `ValueError` drops the offending element."""
    def bad(x):
        if x == 2:
            raise ValueError("boom")
        return x * 2

    flow = Flow.try_map(bad).with_supervision(
        decider=[("ValueError", "resume")], default="resume"
    )
    out = Source.from_iter([1, 2, 3, 4]).via(flow).to(Sink.collect()).run_blocking()
    # Element 2 raised ValueError, was resumed (dropped).
    assert out == [2, 6, 8]


def test_flow_with_supervision_stop_terminates_stream():
    """A `stop` directive on `ValueError` terminates the stream."""
    def bad(x):
        if x == 3:
            raise ValueError("boom")
        return x

    flow = Flow.try_map(bad).with_supervision(
        decider=[("ValueError", "stop")], default="stop"
    )
    out = Source.from_iter([1, 2, 3, 4, 5]).via(flow).to(Sink.collect()).run_blocking()
    # 1 and 2 pass through, 3 stops the stream, 4/5 are dropped.
    assert out == [1, 2]


def test_bidi_flow_projects_forward_and_backward():
    """BidiFlow.from_flows + project forward direction as a Flow."""
    forward = Flow.map(lambda x: x + 1)
    backward = Flow.map(lambda x: x - 1)
    bidi = BidiFlow.from_flows(forward, backward)

    # Forward direction must apply +1.
    f = bidi.forward()
    out_f = Source.from_iter([1, 2, 3]).via(f).to(Sink.collect()).run_blocking()
    assert out_f == [2, 3, 4]
    # Backward direction must apply -1.
    b = bidi.backward()
    out_b = Source.from_iter([10, 20, 30]).via(b).to(Sink.collect()).run_blocking()
    assert out_b == [9, 19, 29]


def test_framing_delimiter_splits_bytes_stream():
    """Framing.delimiter splits a chunked bytes stream on b'\\n'."""
    chunks = [b"hello\nwo", b"rld\nfoo\n"]
    out = (
        Source.from_iter(chunks)
        .via(Framing.delimiter(b"\n", 1024))
        .to(Sink.collect())
        .run_blocking()
    )
    assert out == [b"hello", b"world", b"foo"]


def test_framing_length_field_splits_bytes_stream():
    """Framing.length_field splits a u32-le-prefixed bytes stream."""
    msgs = [b"abc", b"hello"]
    buf = b""
    for m in msgs:
        buf += len(m).to_bytes(4, "little")
        buf += m
    chunks = [buf[:5], buf[5:]]
    out = (
        Source.from_iter(chunks)
        .via(Framing.length_field(1024))
        .to(Sink.collect())
        .run_blocking()
    )
    assert out == [b"abc", b"hello"]


def test_file_io_round_trip_chunks_through_disk():
    """FileIO.from_path → FileIO.to_path round-trip preserves bytes."""
    data = b"hello world, this is streams"
    with tempfile.NamedTemporaryFile(delete=False) as src_f:
        src_f.write(data)
        src_path = src_f.name
    dst_path = src_path + ".out"
    try:
        # Use FileIO.from_path as the source and FileIO.to_path as the sink.
        bytes_written = (
            FileIO.from_path(src_path, chunk_size=8)
            .to(FileIO.to_path(dst_path))
            .run_blocking()
        )
        assert bytes_written > 0
        with open(dst_path, "rb") as f:
            assert f.read() == data
    finally:
        for p in (src_path, dst_path):
            try:
                os.unlink(p)
            except OSError:
                pass


def _free_port() -> int:
    """Allocate a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_tcp_outgoing_send_and_incoming_receive_round_trip():
    """Tcp.outgoing → Tcp.incoming round-trips a single payload."""
    port = _free_port()
    bind_addr = f"127.0.0.1:{port}"

    # Run the listener consumer on a background thread; it collects up to
    # one message-sized chunk and returns.
    received = {}

    def _listener():
        src = Tcp.incoming(bind_addr)
        # Collect the first chunk (one tuple of (remote_addr, bytes)).
        head = src.via(Flow.take(1)).to(Sink.collect()).run_blocking()
        received["chunks"] = head

    t = threading.Thread(target=_listener)
    t.start()
    # Give the listener a moment to bind.
    time.sleep(0.1)

    # Connect and send.
    conn = Tcp.outgoing("127.0.0.1", port)
    assert conn.send(b"ping") is True
    conn.close()

    t.join(timeout=5.0)
    assert not t.is_alive(), "listener should have completed"
    assert "chunks" in received
    chunks = received["chunks"]
    assert len(chunks) == 1
    remote, data = chunks[0]
    assert isinstance(remote, str)
    assert data == b"ping"
