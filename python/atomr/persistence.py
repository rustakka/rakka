"""Python persistence facade — Phase 4 of the Python bindings expansion.

Exports the Rust building blocks from :mod:`atomr._native.persistence`
plus the :class:`EventSourcedActor` base class users subclass to write
event-sourced actors in Python.

Two-callback model
~~~~~~~~~~~~~~~~~~

A subclass declares:

* a ``persistent_id`` class attribute (str);
* :meth:`initial_state` returning a fresh state object;
* :meth:`command_handler(state, ctx, cmd) -> List[Effect]` (async);
* :meth:`event_handler(state, event, recovery_mode=False) -> state`
  (sync, **must be pure**).

Event handlers run **both** during recovery (``recovery_mode=True``) and
during live writes (``recovery_mode=False``). Authors must ensure the
handler does no side effects when ``recovery_mode`` is true; the framework
provides the flag but cannot enforce purity.

Effects
~~~~~~~

The command handler returns a list of :class:`Effect` values. Available
constructors:

* :meth:`Effect.persist(event)` — persist one event then apply it.
* :meth:`Effect.persist_all(events)` — persist a batch atomically.
* :meth:`Effect.snapshot(every=None)` — emit a snapshot, or set a cadence.
* :meth:`Effect.reply(value)` — reply to the sender of the current command.
* :meth:`Effect.stop()` — stop the actor after the current command.
* :meth:`Effect.none()` — sentinel no-op.

Codec registration
~~~~~~~~~~~~~~~~~~

Events are serialised through the Phase-9 codec registry on the
``ActorSystem``. Before spawning an event-sourced actor that persists
*custom* event classes, the user must register a codec for each event's
``module.qualname`` manifest. For ``dict`` events with a ``"type"`` key,
the framework uses the type string as the manifest and a JSON codec is
sufficient — call ``system.use_json_codec(default=True)`` to install it.
"""
from __future__ import annotations

import inspect
import json
from typing import Any, Callable, Iterable, List, Optional

from . import _native

_sub = _native.persistence

# -- Re-exports from the native module --------------------------------------

InMemoryJournal = _sub.InMemoryJournal
InMemorySnapshotStore = _sub.InMemorySnapshotStore
RecoveryPermitter = _sub.RecoveryPermitter
Effect = _sub.Effect


# -- Helpers ----------------------------------------------------------------


def _manifest_of(event: Any) -> str:
    """Derive the codec manifest for an event.

    For dict events with a ``"type"`` key, the type string is the
    manifest (so ``{"type": "Incremented", "by": 1}`` round-trips
    through a JSON codec keyed on ``"Incremented"``). For all other
    objects, ``module.qualname`` is the manifest.
    """
    if isinstance(event, dict):
        t = event.get("type")
        if isinstance(t, str):
            return t
        return "dict"
    cls = type(event)
    return f"{cls.__module__}.{cls.__qualname__}"


def _default_json_encoder(obj: Any) -> bytes:
    return json.dumps(obj).encode("utf-8")


def _default_json_decoder(blob: bytes) -> Any:
    return json.loads(blob.decode("utf-8"))


# -- EventSourcedActor base class -------------------------------------------


class EventSourcedActor:
    """Base class for Python event-sourced actors.

    Subclasses **must** set :attr:`persistent_id`, override
    :meth:`initial_state`, :meth:`command_handler`, and
    :meth:`event_handler`.

    Spawning::

        from atomr import props as build_props
        from atomr.persistence import (
            EventSourcedActor, InMemoryJournal, InMemorySnapshotStore,
        )

        journal = InMemoryJournal()
        snapshots = InMemorySnapshotStore()

        class CounterActor(EventSourcedActor):
            persistent_id = "counter-1"
            ...

        # Custom factory captures shared journal + snapshot store:
        def make():
            return CounterActor(journal=journal, snapshot_store=snapshots)

        ref = system.actor_of(build_props(CounterActor, factory=make), "c")

    The shared :class:`InMemoryJournal` survives :func:`ActorSystem.terminate`
    only if the Python instance is held outside the system, which is the
    point of the dependency-injection pattern above: the journal can be
    re-attached to a fresh system for recovery testing.
    """

    persistent_id: str = ""

    def __init__(
        self,
        *,
        journal: Optional[Any] = None,
        snapshot_store: Optional[Any] = None,
        snapshot_every: Optional[int] = None,
        recovery_permitter: Optional[Any] = None,
        codec_registry: Optional[Any] = None,
    ) -> None:
        if not isinstance(self.persistent_id, str) or not self.persistent_id:
            raise ValueError(
                f"{type(self).__qualname__}: persistent_id class attribute "
                "must be a non-empty string"
            )
        self._journal = journal if journal is not None else InMemoryJournal()
        self._snapshot_store = snapshot_store
        self._snapshot_every = snapshot_every
        self._codec_registry = codec_registry
        self._recovery_permitter = recovery_permitter
        self._state: Any = None
        self._sequence_nr: int = 0
        self._last_snapshot_seq: int = 0
        # Set during pre_start; reset on post_stop.
        self._recovered: bool = False

    # ---- Subclass hooks -------------------------------------------------

    def initial_state(self) -> Any:
        """Return a fresh state object (called once before recovery)."""
        return None

    async def command_handler(self, state: Any, ctx: Any, cmd: Any) -> List[Effect]:
        """Translate a command into a list of :class:`Effect`."""
        raise NotImplementedError(
            f"{type(self).__qualname__}: command_handler must be overridden"
        )

    def event_handler(
        self, state: Any, event: Any, recovery_mode: bool = False
    ) -> Any:
        """Apply ``event`` to ``state`` and return the new state.

        Must be pure — same input → same output. Side effects (logging,
        emitting metrics, calling other actors) are forbidden during
        ``recovery_mode=True`` because they would replay on every
        crash. The framework passes the flag but cannot enforce purity.
        """
        raise NotImplementedError(
            f"{type(self).__qualname__}: event_handler must be overridden"
        )

    def snapshot_state(self, state: Any) -> Any:
        """Return a snapshot-able view of ``state``. Default: identity."""
        return state

    def restore_state(self, snapshot: Any) -> Any:
        """Restore live state from the value :meth:`snapshot_state` returned."""
        return snapshot

    # ---- Codec helpers --------------------------------------------------

    def _encode_event(self, event: Any) -> tuple[bytes, str]:
        manifest = _manifest_of(event)
        registry = self._codec_registry
        if registry is not None:
            try:
                blob = bytes(registry.encode(manifest, event))
                return blob, manifest
            except Exception:
                # Fall through to default JSON for dict events.
                pass
        if isinstance(event, dict) or _is_jsonable(event):
            return _default_json_encoder(event), manifest
        raise RuntimeError(
            f"no codec registered for manifest `{manifest}` and event is not "
            "JSON-serializable; register a codec via "
            "`system.register_codec(...)` or `system.use_json_codec(default=True)`"
        )

    def _decode_event(self, blob: bytes, manifest: str) -> Any:
        registry = self._codec_registry
        if registry is not None:
            try:
                return registry.decode(manifest, blob)
            except Exception:
                pass
        # Default: assume JSON.
        try:
            return _default_json_decoder(blob)
        except Exception as e:
            raise RuntimeError(
                f"cannot decode event with manifest `{manifest}`: {e}"
            ) from e

    def _encode_snapshot(self, snapshot: Any) -> bytes:
        if self._codec_registry is not None:
            manifest = _manifest_of(snapshot)
            try:
                return bytes(self._codec_registry.encode(manifest, snapshot))
            except Exception:
                pass
        if isinstance(snapshot, (dict, list, tuple, str, int, float, bool, type(None))):
            return _default_json_encoder(snapshot)
        raise RuntimeError(
            "snapshot value is not JSON-serializable and no codec is "
            "registered; register a codec for the snapshot type or "
            "override `snapshot_state` to return a JSON-able view."
        )

    def _decode_snapshot(self, blob: bytes) -> Any:
        return _default_json_decoder(blob)

    # ---- Lifecycle hooks (used by the Rust shim) -----------------------

    async def pre_start(self, ctx: Any) -> None:
        """Recover from snapshot + journal before the first command."""
        if self._recovery_permitter is not None:
            self._recovery_permitter.acquire_blocking()
        self._state = self.initial_state()
        from_seq = 1
        # 1. Load the latest snapshot if available.
        if self._snapshot_store is not None:
            loaded = self._snapshot_store.load(self.persistent_id)
            if loaded is not None:
                snap_seq, snap_blob = loaded
                snapshot = self._decode_snapshot(bytes(snap_blob))
                self._state = self.restore_state(snapshot)
                self._sequence_nr = int(snap_seq)
                self._last_snapshot_seq = int(snap_seq)
                from_seq = int(snap_seq) + 1
        # 2. Replay journal events from `from_seq` onwards.
        events = self._journal.replay_events(self.persistent_id, from_seq)
        for seq_nr, payload, manifest, _tags in events:
            event = self._decode_event(bytes(payload), manifest)
            self._state = self.event_handler(self._state, event, recovery_mode=True)
            self._sequence_nr = max(self._sequence_nr, int(seq_nr))
        self._recovered = True
        await self.recovery_completed(self._state, self._sequence_nr)

    async def recovery_completed(self, state: Any, highest_seq: int) -> None:
        """Optional hook fired once recovery finishes."""
        return None

    async def post_stop(self, ctx: Any) -> None:
        """Release any held resources."""
        return None

    async def pre_restart(self, ctx: Any, reason: BaseException, message: Any) -> None:
        return None

    async def post_restart(self, ctx: Any, reason: BaseException) -> None:
        return None

    # ---- Main message dispatch -----------------------------------------

    async def handle(self, ctx: Any, message: Any) -> Any:
        """Route ``message`` through ``command_handler`` then apply effects."""
        effects = await self.command_handler(self._state, ctx, message)
        if effects is None:
            return None
        if isinstance(effects, Effect):
            effects = [effects]
        return await self._apply_effects(list(effects), ctx)

    async def _apply_effects(self, effects: List[Effect], ctx: Any) -> Any:
        last_reply: Any = None
        stop_after = False
        for eff in effects:
            kind = eff.kind
            if kind == "persist":
                events = list(eff.events or [])
                if not events:
                    continue
                await self._persist_events(events)
            elif kind == "snapshot":
                if eff.every is not None:
                    self._snapshot_every = int(eff.every) if int(eff.every) > 0 else None
                else:
                    await self._save_snapshot()
            elif kind == "reply":
                last_reply = eff.reply_value
            elif kind == "stop":
                stop_after = True
            elif kind == "none":
                continue
            else:  # pragma: no cover - guarded by Effect constructors
                raise RuntimeError(f"unknown Effect kind: {kind}")
        if stop_after and ctx is not None and hasattr(ctx, "stop_self"):
            try:
                ctx.stop_self()
            except Exception:
                pass
        return last_reply

    async def _persist_events(self, events: Iterable[Any]) -> None:
        for ev in events:
            blob, manifest = self._encode_event(ev)
            self._sequence_nr += 1
            self._journal.write_event(
                self.persistent_id, self._sequence_nr, blob, manifest
            )
            self._state = self.event_handler(
                self._state, ev, recovery_mode=False
            )
            if (
                self._snapshot_every
                and self._snapshot_store is not None
                and self._sequence_nr - self._last_snapshot_seq >= int(self._snapshot_every)
            ):
                await self._save_snapshot()

    async def _save_snapshot(self) -> None:
        if self._snapshot_store is None:
            return
        snap = self.snapshot_state(self._state)
        blob = self._encode_snapshot(snap)
        self._snapshot_store.save(self.persistent_id, self._sequence_nr, blob)
        self._last_snapshot_seq = self._sequence_nr

    # ---- Read-only accessors used by tests -----------------------------

    @property
    def state(self) -> Any:
        return self._state

    @property
    def sequence_nr(self) -> int:
        return self._sequence_nr


def _is_jsonable(obj: Any) -> bool:
    try:
        json.dumps(obj)
        return True
    except (TypeError, ValueError):
        return False


__all__ = [
    "Effect",
    "EventSourcedActor",
    "InMemoryJournal",
    "InMemorySnapshotStore",
    "RecoveryPermitter",
]
