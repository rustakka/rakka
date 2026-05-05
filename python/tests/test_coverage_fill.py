"""Smoke tests for the spec-parity wave bindings added to py-bindings.

These exercise the new Python surface area that mirrors recently-added
Rust APIs: PruningState, WriteAggregator, ReadAggregator, RedbDurableStore,
LeaderHandover, MemberWeaklyUp, Ewma / MetricsSelector / WeightedRoutees,
AggregateDiscovery, MultiNodeOop barriers, expect_msg_eq matchers,
ClusterSingletonManager state machine, ClusterClientSettings,
DispatcherConfig, BoundedStash, ControlAwareQueue, ResizerConfig,
DeadLetterFilter, FsmBuilder, telemetry topics, and Config.extract.
"""

from __future__ import annotations

import os
import tempfile

import atomr


# -- Distributed-data ------------------------------------------------------


def test_orset_elements_iter():
    s = atomr.ddata.ORSet()
    s.add("a")
    s.add("b")
    assert sorted(s.elements()) == ["a", "b"]


def test_gset_elements_iter():
    g = atomr.ddata.GSet()
    g.add("x")
    g.add("y")
    assert sorted(g.elements()) == ["x", "y"]


def test_pruning_state_lifecycle():
    p = atomr.ddata.PruningState()
    p.initialize("nodeA", "owner1")
    assert p.is_pruned("nodeA")
    assert p.owner("nodeA") == "owner1"
    assert p.phase("nodeA") == "initialized"
    advanced = p.mark_performed("nodeA", obsolete_at=10)
    assert advanced is True
    assert p.phase("nodeA") == "performed"
    # gc with current_round below obsolete_at keeps the marker
    assert p.gc(5) == 0
    # current_round >= obsolete_at removes the marker (gc retains while
    # `obsolete_at > current_round`)
    assert p.gc(10) == 1
    assert not p.is_pruned("nodeA")


def test_write_aggregator_quorum():
    agg = atomr.ddata.WriteAggregator(target=2)
    assert agg.target == 2
    assert not agg.is_satisfied()
    agg.ack()
    assert not agg.is_satisfied()
    agg.ack()
    assert agg.is_satisfied()


def test_read_aggregator_quorum():
    agg = atomr.ddata.ReadAggregator(target=3)
    agg.reply()
    agg.reply()
    assert not agg.is_satisfied()
    agg.reply()
    assert agg.is_satisfied()


# -- Durable redb store ---------------------------------------------------


def test_redb_durable_store_roundtrip():
    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, "ddata.redb")
        store = atomr.ddata_lmdb.RedbDurableStore(path)
        store.persist("counter", b"hello")
        assert bytes(store.load("counter")) == b"hello"
        assert store.keys() == ["counter"]
        store.delete_marker("counter")
        assert store.load("counter") is None


def test_redb_durable_store_tmp():
    s = atomr.ddata_lmdb.RedbDurableStore.tmp()
    s.persist("k", b"v")
    assert bytes(s.load("k")) == b"v"


# -- Cluster ---------------------------------------------------------------


def test_member_weakly_up_factory():
    m = atomr.cluster.member_weakly_up("nodeX", ["role-a"])
    assert m.status == "weaklyup"
    assert "role-a" in m.roles


def test_member_age_ordering():
    a = atomr.cluster.Member("a")
    b = atomr.cluster.Member("b")
    # Equal up_number: tie-break on address
    assert atomr.cluster.Member.age_ordering(a, b) <= 0
    assert atomr.cluster.Member.age_ordering(b, a) >= 0


def test_leader_handover_event_emitted():
    state = atomr.cluster.MembershipState()
    state.add_or_update(atomr.cluster.Member("a").with_status("up"))
    h = atomr.cluster.LeaderHandover()
    ev = h.observe(state)
    assert ev is not None
    assert ev.from_ is None
    assert ev.to is not None
    # Repeat — no change.
    assert h.observe(state) is None


# -- Cluster metrics ------------------------------------------------------


def test_ewma_smooths_signal():
    e = atomr.cluster_metrics.Ewma(initial=0.0, alpha=0.5)
    for _ in range(20):
        e.update(1.0)
    assert e.value > 0.99


def test_ewma_from_half_life():
    e = atomr.cluster_metrics.Ewma.from_half_life(0.0, 4.0)
    for _ in range(4):
        e.update(1.0)
    assert e.value >= 0.5


def test_metrics_selector_picks_lower_load():
    cpu = atomr.cluster_metrics.MetricsSelector("cpu")
    light = atomr.cluster_metrics.NodeMetrics("a", 0, 0.1, 0, 1)
    heavy = atomr.cluster_metrics.NodeMetrics("b", 0, 0.9, 0, 1)
    assert cpu.weight(light) > cpu.weight(heavy)


def test_weighted_routees_picks_higher_weight_more_often():
    metrics = atomr.cluster_metrics.ClusterMetrics()
    metrics.publish(atomr.cluster_metrics.NodeMetrics("fast", 0, 0.1, 0, 1))
    metrics.publish(atomr.cluster_metrics.NodeMetrics("slow", 0, 0.9, 0, 1))
    sel = atomr.cluster_metrics.MetricsSelector("cpu")
    routees = atomr.cluster_metrics.WeightedRoutees(metrics, sel)
    fast_count = sum(
        1 for i in range(100) if routees.pick(["fast", "slow"], i / 100.0) == "fast"
    )
    assert fast_count > 60


def test_apply_pdu_push():
    metrics = atomr.cluster_metrics.ClusterMetrics()
    sample = atomr.cluster_metrics.NodeMetrics("x", 7, 0.5, 1, 2)
    atomr.cluster_metrics.apply_pdu(metrics, "push", [sample])
    got = metrics.get("x")
    assert got is not None
    assert got.timestamp == 7


# -- Cluster tools (Singleton, ClusterClientSettings, Receptionist) -------


def test_singleton_state_transitions():
    mgr = atomr.cluster_tools.ClusterSingletonManager(buffer_size=4)
    assert mgr.state == "inactive"
    mgr.begin_starting()
    assert mgr.state == "starting"
    mgr.begin_handover()
    assert mgr.state == "handing_over"
    mgr.clear()
    assert mgr.state == "inactive"
    assert mgr.buffered == 0


def test_cluster_client_settings_chain():
    s = atomr.cluster_tools.ClusterClientSettings(["a:9000"], 3)
    s2 = s.with_max_attempts(7).with_initial_contacts(["b:1", "c:2"])
    # Construction is enough — fields are private but the chain succeeds.
    assert s2 is not None


def test_cluster_receptionist_initially_empty():
    rec = atomr.cluster_tools.ClusterReceptionist()
    assert rec.registered() == []
    assert rec.has("svc") is False


# -- Discovery ------------------------------------------------------------


def test_aggregate_discovery_falls_through():
    empty = atomr.discovery.StaticDiscovery()
    full = atomr.discovery.StaticDiscovery()
    full.register("svc", "10.0.0.1", 8080)
    agg = atomr.discovery.AggregateDiscovery([empty, full])
    assert agg.provider_count() == 2
    res = agg.lookup("svc")
    assert len(res) == 1
    assert res[0] == ("10.0.0.1", 8080)


def test_aggregate_discovery_empty_when_no_match():
    empty = atomr.discovery.StaticDiscovery()
    agg = atomr.discovery.AggregateDiscovery([empty])
    assert agg.lookup("missing") == []


# -- Streams new operators ------------------------------------------------


def test_streams_merge_sorted():
    out = atomr.streams.merge_sorted_([1, 3, 5], [2, 4, 6])
    assert out == [1, 2, 3, 4, 5, 6]


def test_streams_merge_prioritized_drains_both_sides():
    out = atomr.streams.merge_prioritized_([1, 1, 1], 1, [2, 2, 2], 1)
    assert sorted(out) == [1, 1, 1, 2, 2, 2]


def test_streams_initial_delay_is_pass_through():
    out = atomr.streams.via_initial_delay([1, 2, 3], 0.0)
    assert out == [1, 2, 3]


def test_streams_keep_alive_pass_through():
    # No idle gap when the source is fully buffered → no fillers.
    out = atomr.streams.via_keep_alive([1, 2, 3], 1.0, 99)
    assert out == [1, 2, 3]


def test_streams_conflate_collapses_to_running_sum():
    out = atomr.streams.via_conflate([1, 2, 3, 4], lambda acc, x: acc + x)
    # Conflation behaviour is timing-sensitive but the sum is preserved.
    assert sum(out) == 10


def test_streams_expand_drains_extrapolation_after_upstream():
    out = atomr.streams.via_expand([1, 2], lambda x: [x * 10, x * 100])
    # Each upstream element flows through; extrapolation only fires after
    # upstream completes, then drains the iterator once.
    assert out[:2] == [1, 2]
    assert 20 in out or 200 in out


def test_streams_split_after_emits_substreams():
    n = atomr.streams.via_split_after_count([1, 2, 3, 1, 2, 3], lambda x: x == 3)
    assert n >= 1


def test_streams_prefix_and_tail():
    prefix, tail_count = atomr.streams.via_prefix_and_tail([1, 2, 3, 4, 5], 2)
    assert prefix == [1, 2]
    assert tail_count == 3


def test_streams_recover_with_retries():
    # 3 OKs + one error; replacement runs once, replacing the error.
    items = [(1, False), (2, False), (0, True), (3, False)]
    out = atomr.streams.via_recover_with_retries(items, [99], attempts=1)
    assert 99 in out


def test_streams_select_error():
    items = [(1, None), (0, "boom")]
    out = atomr.streams.via_select_error(items, lambda label: label.upper())
    assert out == [1]


# -- Persistence ----------------------------------------------------------


def test_persistence_events_by_tag():
    j = atomr.persistence.InMemoryJournal()
    j.write("p1", 1, b"red", ["red", "warm"])
    j.write("p1", 2, b"blue", ["blue", "cool"])
    j.write("p2", 1, b"red2", ["red"])
    red = j.events_by_tag("red")
    assert len(red) == 2
    pids = sorted({e[0] for e in red})
    assert pids == ["p1", "p2"]


def test_persistence_all_persistence_ids():
    j = atomr.persistence.InMemoryJournal()
    j.write("aa", 1, b"x")
    j.write("bb", 1, b"y")
    ids = sorted(j.all_persistence_ids())
    assert ids == ["aa", "bb"]


# -- Testkit matchers + multi-node OOP ------------------------------------


def test_multinode_oop_barrier_meets():
    import threading

    ctrl = atomr.testkit.MultiNodeOopController(2)
    addr = ctrl.local_addr

    errors = []

    def run():
        try:
            n = atomr.testkit.MultiNodeOopNode.connect(addr)
            n.barrier("phase-1")
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    t1 = threading.Thread(target=run)
    t2 = threading.Thread(target=run)
    t1.start()
    t2.start()
    t1.join(timeout=5)
    t2.join(timeout=5)
    ctrl.shutdown()
    assert errors == [], errors


# -- Telemetry ------------------------------------------------------------


def test_telemetry_all_topics_listed():
    topics = atomr.telemetry.all_topics()
    for required in ("actors", "cluster", "ddata", "persistence", "streams"):
        assert required in topics


def test_telemetry_bus_construction():
    bus = atomr.telemetry.TelemetryBus(capacity=128)
    assert bus.receiver_count() == 0
    sub = bus.subscribe_topic("actors")
    # Without anyone publishing, next() returns None on timeout.
    assert sub.next(timeout_secs=0.1) is None


# -- Config.extract -------------------------------------------------------


def test_config_extract_returns_python_tree():
    text = """
[atomr]
node = "self"
depth = 7
flags = ["a", "b"]
[atomr.nested]
inner = true
"""
    cfg = atomr.Config.from_toml(text)
    sub = cfg.extract("atomr.nested")
    assert sub == {"inner": True}
    full = cfg.extract("atomr")
    assert full["node"] == "self"
    assert full["depth"] == 7
    assert full["flags"] == ["a", "b"]


def test_config_extract_root():
    cfg = atomr.Config.from_toml('[a]\nb = "c"\n')
    full = cfg.extract_root()
    assert full == {"a": {"b": "c"}}


# -- Core extras ----------------------------------------------------------


def test_dispatcher_config_round_trip():
    d = atomr.core.DispatcherConfig(throughput=20, throughput_deadline_secs=0.5)
    assert d.throughput == 20
    assert d.throughput_deadline_secs == 0.5


def test_overflow_strategy_named():
    s = atomr.core.OverflowStrategy("drop_head")
    assert s.name == "drop_head"


def test_bounded_stash_drop_oldest():
    pol = atomr.core.StashOverflow("drop_oldest")
    s = atomr.core.BoundedStash(2, pol)
    assert s.stash("a") == ("stashed", 1)
    assert s.stash("b") == ("stashed", 2)
    kind, displaced = s.stash("c")
    assert kind == "dropped_oldest"
    assert displaced == "a"
    assert s.unstash_all() == ["b", "c"]


def test_bounded_stash_reject():
    pol = atomr.core.StashOverflow("reject")
    s = atomr.core.BoundedStash(1, pol)
    s.stash("a")
    kind, msg = s.stash("b")
    assert kind == "rejected"
    assert msg == "b"


def test_control_aware_queue_drains_control_first():
    q = atomr.core.ControlAwareQueue()
    q.push_user("user-1")
    q.push_control("ctl-1")
    assert q.pop() == "ctl-1"
    assert q.pop() == "user-1"
    assert q.is_empty()


def test_resizer_config_grows_under_pressure():
    r = atomr.core.ResizerConfig()
    delta = r.compute_delta(2, 2)
    assert delta > 0


def test_dead_letter_filter_accepts():
    f = atomr.core.DeadLetterFilter(accept_no_recipient=True, accept_dropped=False, accept_suppressed=True)
    assert f.accepts("no_recipient") is True
    assert f.accepts("dropped") is False
    assert f.accepts("suppressed") is True


def test_fsm_builder_transitions():
    def in_idle(state, data, msg):
        if msg == "go":
            return ("running", data + 1)
        return None

    def in_running(state, data, msg):
        if msg == "stop":
            return ("idle", data)
        return None

    transitions = []
    fsm = (
        atomr.core.FsmBuilder()
        .start_with("idle", 0)
        .when_state("idle", in_idle)
        .when_state("running", in_running)
        .on_transition(lambda f, t: transitions.append((f, t)))
        .build()
    )
    assert fsm.state == "idle"
    fsm.handle("go")
    assert fsm.state == "running"
    assert fsm.data == 1
    fsm.handle("stop")
    assert fsm.state == "idle"
    assert ("idle", "running") in transitions
    assert ("running", "idle") in transitions
