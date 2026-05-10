"""USB / serial remoting for Python actors.

Thin asyncio-friendly wrapper over the PyO3 types defined in
``atomr._native``. The wire model is **pre-encoded bytes**: Python
serializes its messages (typically via :func:`json.dumps`), the
:class:`SerialTransport` ships them, and the receiving side delivers
the same bytes to the local actor's mailbox as a Python ``bytes``
object.

Typical use::

    import asyncio, json, atomr
    from atomr.remote_serial import RemoteSystem, SerialTransport

    class Peer(atomr.Actor):
        async def handle(self, ctx, msg):  # msg is `bytes`
            data = json.loads(msg)
            if data["type"] == "Chat":
                print("[in]", data["body"])

    async def main():
        sys = await atomr.ActorSystem.create("A")
        transport = SerialTransport("A", "/dev/ttyACM0")
        remote = await RemoteSystem.start_serial(sys, transport)
        remote.register_bytes_codec("LinkMsg")

        peer = sys.actor_of(atomr.props(Peer), "peer")
        remote.expose_actor(peer, "LinkMsg")

        print("local:", remote.local_address)
        await asyncio.Event().wait()

    asyncio.run(main())
"""

from __future__ import annotations

import json as _json
from typing import Any, Mapping

from . import _native

SerialTransport = _native.SerialTransport
RemoteSystem = _native.RemoteSystem
RemoteActorRef = _native.RemoteActorRef


def encode_json(obj: Mapping[str, Any] | list | str | int | float | bool | None) -> bytes:
    """Encode a JSON-compatible Python value as the bytes payload that
    :meth:`RemoteActorRef.tell` expects. Equivalent to
    ``json.dumps(obj).encode()`` but kept here so call sites can read
    as ``rs.encode_json(...)`` rather than reaching for ``json``.
    """
    return _json.dumps(obj).encode()


def decode_json(payload: bytes) -> Any:
    """Inverse of :func:`encode_json`. Use inside an actor's ``handle``
    to decode the incoming :class:`bytes` payload back to a Python value.
    """
    return _json.loads(payload)


__all__ = [
    "SerialTransport",
    "RemoteSystem",
    "RemoteActorRef",
    "encode_json",
    "decode_json",
]
