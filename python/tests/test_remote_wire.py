"""Epic A — wire-level remote-tell over auto-allocated TCP sockets.

Two `ActorSystem`s run on `127.0.0.1:0` (kernel-allocated ports). The
`Cluster.with_tcp_transport` builder starts each daemon over real TCP.
Messages travel through the network stack and are decoded on the
receiving side via the codec registry.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass

import pytest

from atomr import Actor, ActorSystem, props
from atomr.cluster import Cluster


@dataclass
class Beep:
    n: int

    def to_dict(self):
        return {"n": self.n}

    @classmethod
    def from_dict(cls, d):
        return cls(n=d["n"])


def _encoder(obj):
    return json.dumps(obj.to_dict()).encode("utf-8")


def _decoder(blob):
    return Beep.from_dict(json.loads(blob.decode("utf-8")))


class Recorder(Actor):
    def __init__(self):
        self.received = []

    async def handle(self, ctx, message):
        if isinstance(message, Beep):
            self.received.append(message)
        return message


def _register_codec(sys):
    sys.register_codec(
        "json",
        _encoder,
        _decoder,
        manifests=["test_remote_wire.Beep"],
    )


def _wait_for(predicate, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def test_tcp_transport_resolves_auto_allocated_port():
    """`with_tcp_transport(":0")` returns a self_address with a real
    nonzero port."""
    sys_a = ActorSystem.create_blocking("WireRA")
    try:
        cluster = Cluster.with_tcp_transport(sys_a, "127.0.0.1:0")
        addr = cluster.self_address
        # Format: akka.tcp://WireRA@127.0.0.1:<port>
        assert addr.startswith("akka.tcp://WireRA@127.0.0.1:")
        port = int(addr.rsplit(":", 1)[1])
        assert port > 0
    finally:
        sys_a.terminate_blocking()


def test_tcp_transport_invalid_bind_addr_raises():
    sys_a = ActorSystem.create_blocking("WireBad")
    try:
        with pytest.raises(Exception):
            Cluster.with_tcp_transport(sys_a, "not-a-socket")
    finally:
        sys_a.terminate_blocking()


def test_tcp_wire_level_tell_delivers_to_remote_actor():
    """Two ActorSystems on auto-allocated TCP ports exchange a
    `Beep(42)` message. The receiving system's recorder observes it
    after wire-level transit."""
    sys_a = ActorSystem.create_blocking("TcpA")
    sys_b = ActorSystem.create_blocking("TcpB")
    try:
        _register_codec(sys_a)
        _register_codec(sys_b)

        rec = Recorder()
        ref_b_local = sys_b.actor_of(props(lambda: rec), "rec")

        cluster_a = Cluster.with_tcp_transport(sys_a, "127.0.0.1:0")
        cluster_b = Cluster.with_tcp_transport(sys_b, "127.0.0.1:0")

        # Mint a remote-shaped ref on A pointing at B's TCP address.
        # `with_path` reuses the local mailbox channel; the routing
        # logic in `tell_remote` consults the path string and dispatches
        # via the cluster transport when the address differs from
        # local.
        b_path = f"{cluster_b.self_address}/user/rec"
        ref_remote = ref_b_local.with_path(b_path)
        assert ref_remote.path == b_path

        sys_a.tell_remote(ref_remote, Beep(n=42))

        ok = _wait_for(lambda: len(rec.received) >= 1, timeout=5.0)
        assert ok, f"recorder did not receive over TCP; received={rec.received}"
        assert rec.received[0].n == 42
    finally:
        sys_a.terminate_blocking()
        sys_b.terminate_blocking()


def test_tcp_wire_level_two_way_exchange():
    """Both nodes send a Beep to the other; each recorder sees its
    peer's payload independently."""
    sys_a = ActorSystem.create_blocking("BiA")
    sys_b = ActorSystem.create_blocking("BiB")
    try:
        _register_codec(sys_a)
        _register_codec(sys_b)

        rec_a = Recorder()
        rec_b = Recorder()
        ref_a_local = sys_a.actor_of(props(lambda: rec_a), "rec")
        ref_b_local = sys_b.actor_of(props(lambda: rec_b), "rec")

        cluster_a = Cluster.with_tcp_transport(sys_a, "127.0.0.1:0")
        cluster_b = Cluster.with_tcp_transport(sys_b, "127.0.0.1:0")

        ref_b_remote = ref_b_local.with_path(f"{cluster_b.self_address}/user/rec")
        ref_a_remote = ref_a_local.with_path(f"{cluster_a.self_address}/user/rec")

        sys_a.tell_remote(ref_b_remote, Beep(n=1))
        sys_b.tell_remote(ref_a_remote, Beep(n=2))

        ok = _wait_for(lambda: rec_a.received and rec_b.received, timeout=5.0)
        assert ok, f"rec_a={rec_a.received} rec_b={rec_b.received}"
        assert rec_a.received[0].n == 2
        assert rec_b.received[0].n == 1
    finally:
        sys_a.terminate_blocking()
        sys_b.terminate_blocking()
