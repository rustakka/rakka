"""Epic A — multi-node `tell_remote` over the in-process cluster
transport (`Cluster.with_test_transport`). Two `ActorSystem`s share an
in-memory `ClusterRegistry`; messages travel through the channel-backed
`InProcessClusterTransport`, get decoded on the receiving side via the
codec registry, and dispatched to the local actor.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass

import pytest

from atomr import Actor, ActorSystem, props
from atomr.cluster import Cluster, ClusterRegistry


@dataclass
class Greeting:
    text: str

    def to_dict(self):
        return {"text": self.text}

    @classmethod
    def from_dict(cls, d):
        return cls(text=d["text"])


def _encoder(obj):
    return json.dumps(obj.to_dict()).encode("utf-8")


def _decoder(blob):
    return Greeting.from_dict(json.loads(blob.decode("utf-8")))


class Recorder(Actor):
    """Actor that stores every Greeting it receives."""

    def __init__(self):
        self.received = []

    async def handle(self, ctx, message):
        if isinstance(message, Greeting):
            self.received.append(message)
        return message


def _register_codec(sys):
    sys.register_codec(
        "json",
        _encoder,
        _decoder,
        manifests=["test_cluster_transport.Greeting"],
    )


def _wait_for(predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def test_test_transport_round_trip_between_two_systems():
    """Two ActorSystems sharing one ClusterRegistry can exchange
    Greeting messages end-to-end via the in-process transport.
    """
    registry = ClusterRegistry()
    sys_a = ActorSystem.create_blocking("A")
    sys_b = ActorSystem.create_blocking("B")
    try:
        _register_codec(sys_a)
        _register_codec(sys_b)

        # Spawn the recorder on B and capture its Python-side instance
        # so we can read the received list.
        recorder_state = Recorder()

        def make_recorder():
            return recorder_state

        ref_b = sys_b.actor_of(props(make_recorder), "rec")

        # Bring up clusters on both nodes joined to the same registry.
        cluster_a = Cluster.with_test_transport(sys_a, registry)
        cluster_b = Cluster.with_test_transport(sys_b, registry)
        # Both addresses are the local "akka://A" / "akka://B".
        assert cluster_a.self_address == "akka://A"
        assert cluster_b.self_address == "akka://B"

        # From A, build a "remote" ref pointing at B's recorder by
        # cooking a path on the B address.
        # We piggy-back on actor_of's local ref but rewrite the path —
        # the transport routes purely on path string, so a hand-built
        # PyActorRef-like object works as long as the address resolves
        # to a remote system. Easiest: spawn a *placeholder* actor on
        # A with the same name, then construct an address-rewritten
        # ref from B's perspective. For now use the simpler approach:
        # `tell_remote` accepts a PyActorRef. We need a ref whose path
        # starts with `akka://B`. We do that by constructing an actor
        # on B and passing it across — a Py actor handle is just a
        # Python object that stores `inner` + path, and we can use the
        # native `tell_remote` from sys_a with that ref.
        sys_a.tell_remote(ref_b, Greeting(text="hello-from-A"))

        # Wait for the recorder to receive it.
        ok = _wait_for(lambda: len(recorder_state.received) >= 1, timeout=3.0)
        assert ok, f"recorder did not receive message; received={recorder_state.received}"
        assert recorder_state.received[0].text == "hello-from-A"
    finally:
        sys_a.terminate_blocking()
        sys_b.terminate_blocking()


def test_test_transport_two_way_traffic():
    """Both sides exchange messages; each recorder receives its peer's
    greeting independently.
    """
    registry = ClusterRegistry()
    sys_a = ActorSystem.create_blocking("Alpha")
    sys_b = ActorSystem.create_blocking("Beta")
    try:
        _register_codec(sys_a)
        _register_codec(sys_b)

        rec_a = Recorder()
        rec_b = Recorder()
        ref_on_a = sys_a.actor_of(props(lambda: rec_a), "rec")
        ref_on_b = sys_b.actor_of(props(lambda: rec_b), "rec")

        Cluster.with_test_transport(sys_a, registry)
        Cluster.with_test_transport(sys_b, registry)

        sys_a.tell_remote(ref_on_b, Greeting(text="A→B"))
        sys_b.tell_remote(ref_on_a, Greeting(text="B→A"))

        ok = _wait_for(
            lambda: rec_a.received and rec_b.received,
            timeout=3.0,
        )
        assert ok, f"rec_a={rec_a.received} rec_b={rec_b.received}"
        assert rec_a.received[0].text == "B→A"
        assert rec_b.received[0].text == "A→B"
    finally:
        sys_a.terminate_blocking()
        sys_b.terminate_blocking()


def test_test_transport_local_path_uses_in_process_pipeline():
    """A `tell_remote` whose target lives on the *same* system bypasses
    the wire and decodes immediately.
    """
    registry = ClusterRegistry()
    sys_a = ActorSystem.create_blocking("Solo")
    try:
        _register_codec(sys_a)
        Cluster.with_test_transport(sys_a, registry)

        rec = Recorder()
        ref = sys_a.actor_of(props(lambda: rec), "rec")
        sys_a.tell_remote(ref, Greeting(text="self-hello"))

        ok = _wait_for(lambda: len(rec.received) >= 1, timeout=2.0)
        assert ok
        assert rec.received[0].text == "self-hello"
    finally:
        sys_a.terminate_blocking()


def test_test_transport_unknown_manifest_raises_at_send_site():
    """If the *sender* hasn't registered a codec for the message class,
    `tell_remote` raises before touching the wire.
    """
    registry = ClusterRegistry()
    sys_a = ActorSystem.create_blocking("Sender")
    sys_b = ActorSystem.create_blocking("Receiver")
    try:
        # Only B has the codec; A doesn't.
        _register_codec(sys_b)

        rec = Recorder()
        ref_b = sys_b.actor_of(props(lambda: rec), "rec")
        Cluster.with_test_transport(sys_a, registry)
        Cluster.with_test_transport(sys_b, registry)

        with pytest.raises(Exception):  # AtomrError
            sys_a.tell_remote(ref_b, Greeting(text="will-fail"))
    finally:
        sys_a.terminate_blocking()
        sys_b.terminate_blocking()


def test_test_transport_send_without_transport_raises_for_remote_path():
    """Without `with_test_transport`, sending to a remote-shaped path
    raises (no transport configured)."""
    sys_a = ActorSystem.create_blocking("Lonely")
    sys_b = ActorSystem.create_blocking("Faraway")
    try:
        _register_codec(sys_a)
        # B has a real ref, but its address is `akka://Faraway` —
        # remote from A's perspective. No transport has been
        # configured on A.
        rec = Recorder()
        ref_b = sys_b.actor_of(props(lambda: rec), "rec")

        with pytest.raises(Exception):
            sys_a.tell_remote(ref_b, Greeting(text="will-fail"))
    finally:
        sys_a.terminate_blocking()
        sys_b.terminate_blocking()
