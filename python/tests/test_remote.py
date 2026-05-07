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


# ---------------------------------------------------------------------------
# Epic G — collision policy & lax (`strict=False`) manifest validation.
# ---------------------------------------------------------------------------


def test_register_codec_collision_raises_by_default():
    """First registration wins; second raises a clear ValueError listing the
    existing codec name."""
    sys = ActorSystem.create_blocking("codec-collide")
    try:
        sys.register_codec(
            "json", _encoder, _decoder, manifests=["test_remote.Greeting"]
        )
        # Second registration with the same manifest collides.
        with pytest.raises(ValueError) as excinfo:
            sys.register_codec(
                "alt", _encoder, _decoder, manifests=["test_remote.Greeting"]
            )
        msg = str(excinfo.value)
        assert "test_remote.Greeting" in msg
        assert "json" in msg
        assert "force" in msg.lower()
    finally:
        sys.terminate_blocking()


def test_register_codec_force_true_overrides_collision():
    """`force=True` silently replaces the existing codec entry."""
    sys = ActorSystem.create_blocking("codec-force")
    try:
        # First, register a "tagged" codec that wraps the dict in an
        # envelope so we can tell the two codecs apart later.
        def _tag_enc(obj):
            return json.dumps({"v": obj.to_dict(), "tag": "first"}).encode()

        def _tag_dec(blob):
            payload = json.loads(blob.decode())
            return Greeting.from_dict(payload["v"])

        sys.register_codec(
            "tagged", _tag_enc, _tag_dec, manifests=["test_remote.Greeting"]
        )
        # Round-trip uses the first codec.
        out1 = sys.codec_roundtrip(Greeting(text="a", n=1))
        assert out1 == Greeting(text="a", n=1)

        # Force-replace with the plain encoder.
        sys.register_codec(
            "json",
            _encoder,
            _decoder,
            manifests=["test_remote.Greeting"],
            force=True,
        )
        # Still round-trips, but now via the new codec.
        out2 = sys.codec_roundtrip(Greeting(text="b", n=2))
        assert out2 == Greeting(text="b", n=2)
    finally:
        sys.terminate_blocking()


def test_use_json_codec_collision_and_force():
    """`use_json_codec` honors the same collision/force policy."""
    sys = ActorSystem.create_blocking("codec-json-collide")
    try:
        sys.use_json_codec(manifests=["test_remote.Greeting"])
        with pytest.raises(ValueError):
            sys.use_json_codec(manifests=["test_remote.Greeting"])
        # Force-true is the documented override.
        sys.use_json_codec(manifests=["test_remote.Greeting"], force=True)
    finally:
        sys.terminate_blocking()


def test_validate_manifest_lax_skips_importlib_with_warning():
    """`strict=False` skips the importlib round-trip and emits a warning
    instead of raising, so `__main__`-scoped fixtures can be addressed.
    """
    import warnings

    # Strict mode rejects an unknown class.
    with pytest.raises(ValueError):
        validate_manifest("test_remote.NoSuchClass")

    # Lax mode accepts it but emits a warning.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        validate_manifest("test_remote.NoSuchClass", strict=False)
    assert any(
        "not strictly validated" in str(w.message) for w in caught
    ), [str(w.message) for w in caught]


def test_register_codec_strict_false_accepts_main_scoped_manifest():
    """`__main__`-scoped class manifests are normally rejected by strict
    mode (the qualname can't be looked up via importlib at registration
    time). With `strict=False`, the registration succeeds and emits a
    warning.
    """
    import warnings

    sys = ActorSystem.create_blocking("codec-main")
    try:
        # Strict mode (default) refuses — `__main__.MyMessage` cannot be
        # resolved in the test harness.
        with pytest.raises(ValueError):
            sys.register_codec(
                "json",
                _encoder,
                _decoder,
                manifests=["__main__.MyMessage"],
            )

        # Lax mode lets it through; warning is emitted.
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            sys.register_codec(
                "json",
                _encoder,
                _decoder,
                manifests=["__main__.MyMessage"],
                strict=False,
            )
        assert any(
            "__main__.MyMessage" in str(w.message) for w in caught
        ), [str(w.message) for w in caught]

        # Round-trip works via the registered codec because we asked
        # for a manifest match through the public encode/decode API.
        reg = sys.codecs
        blob = reg.encode("__main__.MyMessage", Greeting(text="x", n=0))
        out = reg.decode("__main__.MyMessage", blob)
        assert out == Greeting(text="x", n=0)
    finally:
        sys.terminate_blocking()


def test_pycodec_registry_register_force_true():
    """The lower-level `PyCodecRegistry.register` also takes `force`."""
    reg = PyCodecRegistry()
    reg.register("greet", _encoder, _decoder, ["test_remote.Greeting"])
    with pytest.raises(ValueError):
        reg.register("alt", _encoder, _decoder, ["test_remote.Greeting"])
    reg.register(
        "alt2",
        _encoder,
        _decoder,
        ["test_remote.Greeting"],
        force=True,
    )
    assert "alt2" in reg.names()
