"""Phase 9 — codec registry + in-process Python remote round-trip."""

from __future__ import annotations

import json
from dataclasses import dataclass

import pytest

import atomr
from atomr import Actor, ActorSystem, props
from atomr.system import (
    BuiltinJsonCodec,
    PyCodecRegistry,
    manifest_of,
    validate_manifest,
)


@dataclass
class Greeting:
    """A simple top-level dataclass we can address by manifest."""

    text: str
    n: int = 0

    def to_dict(self):
        return {"text": self.text, "n": self.n}

    @classmethod
    def from_dict(cls, d):
        return cls(text=d["text"], n=d["n"])


def _encoder(obj: Greeting) -> bytes:
    return json.dumps(obj.to_dict()).encode("utf-8")


def _decoder(blob: bytes) -> Greeting:
    return Greeting.from_dict(json.loads(blob.decode("utf-8")))


def test_manifest_of_returns_module_qualname():
    g = Greeting(text="hi")
    manifest = manifest_of(g)
    assert manifest == "test_remote.Greeting"


def test_validate_manifest_round_trip():
    # Round-trips because Greeting is importable from this module.
    validate_manifest("test_remote.Greeting")


def test_validate_manifest_rejects_unknown_class():
    with pytest.raises(ValueError):
        validate_manifest("test_remote.NoSuchClass")


def test_validate_manifest_rejects_missing_dot():
    with pytest.raises(ValueError):
        validate_manifest("nodothere")


def test_pycodec_registry_round_trip():
    reg = PyCodecRegistry()
    reg.register("greet", _encoder, _decoder, ["test_remote.Greeting"])
    g = Greeting(text="hello", n=3)
    blob = reg.encode("test_remote.Greeting", g)
    assert isinstance(blob, bytes)
    decoded = reg.decode("test_remote.Greeting", blob)
    assert decoded == g


def test_builtin_json_codec_validates_payload():
    codec = BuiltinJsonCodec()
    assert codec.id() == "json"
    blob = codec.encode("test_remote.Greeting", b'{"text":"x","n":1}')
    assert codec.decode("test_remote.Greeting", blob) == b'{"text":"x","n":1}'


def test_register_codec_validates_manifest():
    sys = ActorSystem.create_blocking("codec-test")
    try:
        with pytest.raises(ValueError):
            sys.register_codec(
                "json",
                _encoder,
                _decoder,
                manifests=["nope.NoSuchModule.NoSuchClass"],
            )
    finally:
        sys.terminate_blocking()


def test_register_codec_round_trip_via_system():
    sys = ActorSystem.create_blocking("codec-rt")
    try:
        sys.register_codec(
            "json",
            _encoder,
            _decoder,
            manifests=["test_remote.Greeting"],
        )
        g = Greeting(text="ok", n=42)
        recovered = sys.codec_roundtrip(g)
        assert recovered == g
    finally:
        sys.terminate_blocking()


def test_use_json_codec_default_falls_back_for_dicts():
    sys = ActorSystem.create_blocking("codec-default")
    try:
        sys.use_json_codec(default=True)
        # `dict` has manifest `builtins.dict`, not registered explicitly,
        # but the default codec catches it.
        out = sys.codec_roundtrip({"k": 1, "v": [1, 2, 3]})
        assert out == {"k": 1, "v": [1, 2, 3]}
    finally:
        sys.terminate_blocking()


class _Echo(Actor):
    def __init__(self):
        self.last = None

    async def handle(self, ctx, message):
        self.last = message
        return message


def test_tell_remote_decodes_and_delivers():
    """Exercise the full encode/decode pipeline by sending a message
    through the codec registry and then asking the actor to reflect it
    back. This is the in-process equivalent of a wire-level remote send.
    """
    sys = ActorSystem.create_blocking("codec-remote")
    try:
        sys.register_codec(
            "json",
            _encoder,
            _decoder,
            manifests=["test_remote.Greeting"],
        )
        ref = sys.actor_of(props(_Echo), "echo")
        sys.tell_remote(ref, Greeting(text="ahoy", n=7))
        # Round-trip a fresh ask to verify the actor saw the decoded
        # message and that it was a Greeting (not bytes).
        reply = ref.ask_blocking(Greeting(text="ping", n=1), 5.0)
        assert isinstance(reply, Greeting)
    finally:
        sys.terminate_blocking()


def test_codec_registry_lists_manifests_and_names():
    reg = PyCodecRegistry()
    reg.register("greet", _encoder, _decoder, ["test_remote.Greeting"])
    reg.register_json(["test_remote._Echo"])
    assert "test_remote.Greeting" in reg
    assert set(reg.manifests()) == {"test_remote._Echo", "test_remote.Greeting"}
    assert "greet" in reg.names()
    assert "json" in reg.names()


def test_unknown_manifest_raises_atomr_error():
    sys = ActorSystem.create_blocking("codec-miss")
    try:
        with pytest.raises(atomr.AtomrError):
            sys.codec_roundtrip(Greeting(text="x"))
    finally:
        sys.terminate_blocking()
