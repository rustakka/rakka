"""In-memory loopback test for atomr.remote_serial.

Wires two `RemoteSystem`s together via `SerialTransport.duplex_pair()`
(which creates a `tokio::io::duplex` byte pipe in Rust and gives back
the two endpoints as Python `SerialTransport` objects). Sends a Chat
message and a Ping from B → A, and verifies A's Peer actor receives
both. No real serial hardware required.
"""

from __future__ import annotations

import asyncio
import json

import pytest

import atomr
from atomr.remote_serial import RemoteSystem, SerialTransport, decode_json, encode_json

MANIFEST = "LinkMsg"


class CapturingPeer(atomr.Actor):
    """Pushes every received bytes payload into the asyncio Queue
    given to its constructor — same pattern the demo uses.

    `loop` is captured by the caller on the asyncio thread; the actor
    factory runs on a Tokio worker so `asyncio.get_event_loop()` here
    would fail.
    """

    def __init__(self, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop) -> None:
        self.queue = queue
        self.loop = loop

    async def handle(self, ctx, msg: bytes) -> None:
        self.loop.call_soon_threadsafe(self.queue.put_nowait, msg)


@pytest.mark.asyncio
async def test_chat_and_ping_roundtrip_over_duplex():
    sys_a = await atomr.ActorSystem.create("A")
    sys_b = await atomr.ActorSystem.create("B")
    transport_a, transport_b = SerialTransport.duplex_pair("A", "B")

    remote_a = await RemoteSystem.start_serial(sys_a, transport_a)
    remote_b = await RemoteSystem.start_serial(sys_b, transport_b)

    remote_a.register_bytes_codec(MANIFEST)
    remote_b.register_bytes_codec(MANIFEST)

    queue: asyncio.Queue = asyncio.Queue()
    loop = asyncio.get_event_loop()
    peer = sys_a.actor_of(
        atomr.props(CapturingPeer, factory=lambda: CapturingPeer(queue, loop)),
        "peer",
    )
    remote_a.expose_actor(peer, MANIFEST)

    target_path = f"{remote_a.local_address}/user/peer"
    target = remote_b.actor_selection(target_path, MANIFEST)
    assert target is not None, f"actor_selection({target_path}) returned None"

    # 1) Chat round-trip
    target.tell(encode_json({"type": "Chat", "body": "hello over usb"}))
    msg = await asyncio.wait_for(queue.get(), timeout=5.0)
    decoded = decode_json(msg)
    assert decoded == {"type": "Chat", "body": "hello over usb"}

    # 2) Ping round-trip
    target.tell(encode_json({
        "type": "Ping",
        "seq": 42,
        "sent_at": 1.0,
        "from_addr": remote_b.local_address,
    }))
    msg = await asyncio.wait_for(queue.get(), timeout=5.0)
    decoded = decode_json(msg)
    assert decoded["type"] == "Ping"
    assert decoded["seq"] == 42
    assert decoded["from_addr"] == remote_b.local_address

    await remote_a.shutdown()
    await remote_b.shutdown()


def test_list_devices_returns_list():
    """Should return a list (possibly empty) without raising on any platform."""
    devices = SerialTransport.list_devices()
    assert isinstance(devices, list)
    for entry in devices:
        assert isinstance(entry, tuple)
        assert len(entry) == 2
        assert isinstance(entry[0], str)
        assert isinstance(entry[1], str)
