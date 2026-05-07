"""Surface-level checks for every extension submodule."""

import atomr


def test_ddata_gcounter_merge():
    a = atomr.ddata.GCounter()
    b = atomr.ddata.GCounter()
    a.increment("n1", 5)
    b.increment("n1", 3)
    b.increment("n2", 7)
    a.merge(b)
    assert a.value() == 5 + 7


def test_ddata_pncounter():
    c = atomr.ddata.PNCounter()
    c.increment("n1", 10)
    c.decrement("n1", 3)
    assert c.value() == 7


def test_ddata_gset_merges_union():
    a = atomr.ddata.GSet()
    b = atomr.ddata.GSet()
    a.add("x")
    b.add("y")
    a.merge(b)
    assert a.contains("x") and a.contains("y")


def test_ddata_orset_add_remove():
    s = atomr.ddata.ORSet()
    s.add("k")
    assert s.contains("k")
    s.remove("k")
    assert not s.contains("k")


def test_persistence_journal_roundtrip():
    j = atomr.persistence.InMemoryJournal()
    j.write("pid1", 1, b"a")
    j.write("pid1", 2, b"b")
    seen = j.replay("pid1")
    assert [bytes(p) for p in seen] == [b"a", b"b"]
    assert j.highest_sequence_nr("pid1") == 2


def test_coordination_lease_acquire_release():
    l = atomr.coordination.InMemoryLease()
    assert l.acquire("owner1") is True
    assert l.check() == "owner1"
    l.release("owner1")
    assert l.check() is None


def test_discovery_static():
    d = atomr.discovery.StaticDiscovery()
    d.register("svc", "1.2.3.4", 8080)
    targets = d.lookup("svc")
    assert len(targets) == 1
    host, port = targets[0]
    assert host == "1.2.3.4"
    assert port == 8080


def test_di_container():
    c = atomr.di.ServiceContainer()
    c.register("greeting", "hello")
    assert c.resolve("greeting") == "hello"
    assert "greeting" in c.keys()


def test_pubsub_local():
    ps = atomr.cluster_tools.DistributedPubSub()
    seen = []
    ps.subscribe("t", lambda m: seen.append(m))
    ps.publish("t", {"n": 1})
    ps.publish("t", {"n": 2})
    assert seen == [{"n": 1}, {"n": 2}]


def test_cluster_membership():
    state = atomr.cluster.MembershipState()
    m = atomr.cluster.Member("node1")
    state.add_or_update(m)
    assert state.member_count() == 1


def test_vector_clock_compare():
    a = atomr.cluster.VectorClock()
    b = atomr.cluster.VectorClock()
    a.tick("A")
    b.tick("A")
    assert a.compare(b) == "same"
    a.tick("A")
    assert b.compare(a) == "before"


def test_streams_map_reduce():
    total = atomr.streams.map_reduce(
        range(5), lambda x: x * 2, lambda acc, x: acc + x, 0
    )
    assert total == 0 + 2 + 4 + 6 + 8


def test_sharding_routes_to_entity():
    """Phase 6 ShardRegion: messages route by entity_id to a real
    PyActor child. The legacy in-memory dict ShardRegion was replaced;
    this test now exercises the live atomr-cluster-sharding wiring."""
    import time

    from atomr import Actor, ActorSystem, props
    from atomr.cluster_sharding import ShardRegion, ShardingSettings

    seen = {}

    class Entity(Actor):
        def __init__(self):
            self.n = 0

        async def handle(self, ctx, msg):
            self.n += 1
            seen.setdefault(ctx.path, []).append((msg, self.n))

    def extractor(msg):
        eid = str(msg["id"])
        return (eid, str(hash(eid) % 16), msg["payload"])

    sys = ActorSystem.create_blocking("sharding-ext-test")
    try:
        region = ShardRegion.start(
            sys,
            type_name="ext",
            entity_props=props(Entity),
            message_extractor=extractor,
            settings=ShardingSettings(),
        )
        region.tell({"id": 1, "payload": "x"})
        region.tell({"id": 1, "payload": "y"})
        region.tell({"id": 2, "payload": "z"})

        deadline = time.time() + 2.0
        while region.entity_count() < 2 and time.time() < deadline:
            time.sleep(0.02)
        assert region.entity_count() == 2
    finally:
        sys.terminate_blocking()
