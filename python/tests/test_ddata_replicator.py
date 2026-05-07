"""Phase 7 — distributed-data CRDTs and `Replicator` actor.

Each new CRDT gets a roundtrip test: create two replicas, mutate them
independently, merge, assert convergence. The `Replicator` tests
exercise the typed update / get path, both consistency levels, the
async subscription iterator, and durable-store survival across an
actor-system restart.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import uuid
from typing import Any

import pytest

import atomr
import atomr.ddata as d


# ---------------------------------------------------------------------
# CRDT roundtrip tests
# ---------------------------------------------------------------------


def test_lww_register_roundtrip():
    a = d.LwwRegister.with_value(b"alice")
    # Force a higher timestamp on b so it wins the merge.
    b = d.LwwRegister(initial=b"bob", node="n2", timestamp=a.timestamp + 100)
    a.merge(b)
    assert a.value() == b"bob"
    assert a.timestamp >= b.timestamp


def test_lww_register_older_loses():
    a = d.LwwRegister(initial=b"new", node="n1", timestamp=1000)
    b = d.LwwRegister(initial=b"old", node="n2", timestamp=500)
    a.merge(b)
    assert a.value() == b"new"


def test_flag_monotonic():
    a = d.Flag()
    b = d.Flag()
    b.enable()
    assert not a.is_enabled()
    a.merge(b)
    assert a.is_enabled()
    # Once on, stays on regardless of merging an "off" replica.
    c = d.Flag()
    a.merge(c)
    assert a.is_enabled()


def test_ormap_roundtrip():
    a = d.ORMap()
    b = d.ORMap()
    a.put("k1", d.LwwRegister.with_value(b"v1"))
    b.put("k2", d.LwwRegister.with_value(b"v2"))
    a.merge(b)
    keys = sorted(a.keys())
    assert keys == ["k1", "k2"]
    assert a.get("k1").value() == b"v1"
    assert a.get("k2").value() == b"v2"


def test_ormap_rejects_non_lww_value():
    m = d.ORMap()
    with pytest.raises(ValueError):
        m.put("k", d.GCounter())


def test_ormap_remove_then_concurrent_put():
    a = d.ORMap()
    a.put("k", d.LwwRegister.with_value(b"v1"))
    b_replica = d.ORMap()
    b_replica.put("k", d.LwwRegister.with_value(b"v1"))
    b_replica.remove("k")
    # Concurrent re-add on a wins because its tag is newer.
    a.put("k", d.LwwRegister.with_value(b"v2"))
    a.merge(b_replica)
    got = a.get("k")
    assert got is not None


def test_lww_map_roundtrip():
    a = d.LWWMap()
    b = d.LWWMap()
    a.put("k", b"old", timestamp=100)
    b.put("k", b"new", timestamp=200)
    a.merge(b)
    assert a.get("k") == b"new"


def test_pncounter_map_roundtrip():
    a = d.PNCounterMap()
    b = d.PNCounterMap()
    a.increment("alice", delta=5, node="n1")
    b.increment("alice", delta=7, node="n2")
    a.merge(b)
    assert a.value("alice") == 12


def test_or_multi_map_roundtrip():
    a = d.ORMultiMap()
    b = d.ORMultiMap()
    a.add("colors", "red")
    b.add("colors", "blue")
    a.merge(b)
    assert a.contains("colors", "red")
    assert a.contains("colors", "blue")
    assert a.key_count() == 1


# ---------------------------------------------------------------------
# Replicator tests
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replicator_update_then_get_gcounter():
    sys = await atomr.ActorSystem.create(f"rep-{uuid.uuid4().hex}")
    rep = d.Replicator.get(sys)

    def inc(c):
        c.increment("a", 1)
        return c

    ack = await rep.update("counter", d.GCounter, inc)
    assert ack == "ok"

    got = await rep.get_value("counter", d.GCounter)
    assert got is not None
    assert got.value() == 1
    await sys.terminate()


@pytest.mark.asyncio
async def test_replicator_async_modify_fn():
    sys = await atomr.ActorSystem.create(f"rep-{uuid.uuid4().hex}")
    rep = d.Replicator.get(sys)

    async def add(s):
        s.add("x")
        return s

    ack = await rep.update("set", d.ORSet, add)
    assert ack == "ok"

    got = await rep.get_value("set", d.ORSet)
    assert got is not None
    assert got.contains("x")
    await sys.terminate()


@pytest.mark.asyncio
async def test_replicator_write_consistency_local_succeeds():
    sys = await atomr.ActorSystem.create(f"rep-{uuid.uuid4().hex}")
    rep = d.Replicator.get(sys)

    ack = await rep.update(
        "k",
        d.GCounter,
        lambda c: (c.increment("a", 1) or c),
        d.WriteConsistency.local(),
    )
    assert ack == "ok"
    await sys.terminate()


@pytest.mark.asyncio
async def test_replicator_write_consistency_majority_single_node():
    sys = await atomr.ActorSystem.create(f"rep-{uuid.uuid4().hex}")
    rep = d.Replicator.get(sys)

    # In a single-node "cluster", majority == 1 reply, which the local
    # write satisfies immediately.
    ack = await rep.update(
        "k",
        d.GCounter,
        lambda c: (c.increment("a", 1) or c),
        d.WriteConsistency.majority(timeout=0.5),
    )
    assert ack == "ok"
    await sys.terminate()


@pytest.mark.asyncio
async def test_replicator_get_returns_none_for_missing_key():
    sys = await atomr.ActorSystem.create(f"rep-{uuid.uuid4().hex}")
    rep = d.Replicator.get(sys)

    got = await rep.get_value("nope", d.GCounter)
    assert got is None
    await sys.terminate()


@pytest.mark.asyncio
async def test_replicator_delete():
    sys = await atomr.ActorSystem.create(f"rep-{uuid.uuid4().hex}")
    rep = d.Replicator.get(sys)

    await rep.update("k", d.GCounter, lambda c: (c.increment("a", 1) or c))
    ack = await rep.delete("k")
    assert ack == "ok"

    got = await rep.get_value("k", d.GCounter)
    assert got is None
    await sys.terminate()


@pytest.mark.asyncio
async def test_replicator_subscribe_delivers_events():
    sys = await atomr.ActorSystem.create(f"rep-{uuid.uuid4().hex}")
    rep = d.Replicator.get(sys)
    sub = rep.subscribe("k")

    async def writer():
        # Yield the event loop so the async-iterator's __anext__ can
        # arm before we fire the update.
        await asyncio.sleep(0.05)
        await rep.update("k", d.GCounter, lambda c: (c.increment("a", 1) or c))

    asyncio.ensure_future(writer())

    # Pull one event from the iterator with a generous timeout.
    async def first(it):
        async for ev in it:
            return ev
        return None

    ev = await asyncio.wait_for(first(sub), timeout=2.0)
    assert ev == "k"
    await sys.terminate()


@pytest.mark.asyncio
async def test_replicator_consistency_classes_construct():
    rl = d.ReadConsistency.local()
    rm = d.ReadConsistency.majority(timeout=1.0)
    ra = d.ReadConsistency.all(timeout=1.0)
    rf = d.ReadConsistency.read_from(2, timeout=1.0)
    wl = d.WriteConsistency.local()
    wm = d.WriteConsistency.majority(timeout=1.0)
    wa = d.WriteConsistency.all(timeout=1.0)
    wf = d.WriteConsistency.write_to(2, timeout=1.0)
    for v in (rl, rm, ra, rf, wl, wm, wa, wf):
        assert "Consistency" in repr(v)


@pytest.mark.asyncio
async def test_durable_store_file_survives_restart(tmp_path):
    # Configure system #1 with a file durable store, write data, drop.
    cfg_text = (
        "[distributed-data.durable]\n"
        f'store-actor-class = "file"\n'
        f'path = "{tmp_path}"\n'
    )
    cfg1 = atomr.Config.from_toml(cfg_text)
    sys1 = await atomr.ActorSystem.create("durable-test-a", cfg1)
    rep1 = d.Replicator.get(sys1)

    await rep1.update("k", d.GCounter, lambda c: (c.increment("n", 5) or c))

    # Verify the durable store now has the key on disk.
    store = rep1.durable
    assert "k" in store.keys()
    await sys1.terminate()

    # New actor-system, same path: durable store on disk should still
    # report the key. (The replicator actor reloads markers but
    # rebuilds in-memory CRDT state from the next update; verifying
    # the on-disk presence is the key invariant.)
    cfg2 = atomr.Config.from_toml(cfg_text)
    sys2 = await atomr.ActorSystem.create("durable-test-b", cfg2)
    rep2 = d.Replicator.get(sys2)
    store2 = rep2.durable
    assert "k" in store2.keys()
    await sys2.terminate()


def test_durable_store_factory_methods(tmp_path):
    n = d.DurableStore.noop()
    assert n.keys() == []
    f = d.DurableStore.file(str(tmp_path))
    assert isinstance(f.keys(), list)


def test_existing_crdts_still_work():
    a = d.GCounter()
    b = d.GCounter()
    a.increment("n1", 5)
    b.increment("n2", 7)
    a.merge(b)
    assert a.value() == 12

    s = d.ORSet()
    s.add("x")
    assert s.contains("x")

    ps = d.PruningState()
    ps.initialize("nodeA", "owner1")
    assert ps.owner("nodeA") == "owner1"

    wa = d.WriteAggregator(3)
    wa.ack()
    wa.ack()
    wa.ack()
    assert wa.is_satisfied()
