"""Thin wrappers over :mod:`rakka._native` that keep the public API stable
even as the Rust internals evolve."""

from __future__ import annotations

from typing import Any, Callable, Optional

from . import _native

ActorSystem = _native.ActorSystem
Props = _native.Props
ActorRef = _native.ActorRef
Config = _native.Config
Context = _native.Context


def props(
    actor_cls: type,
    *,
    dispatcher: str = "python-pinned",
    interpreter_role: str = "default",
    mailbox: Optional[str] = None,
    factory: Optional[Callable[[], Any]] = None,
) -> Props:
    """Convenience builder — `factory` defaults to `actor_cls`'s no-arg ctor."""
    if factory is None:
        factory = actor_cls  # type: ignore[assignment]
    return Props.create(factory, dispatcher, interpreter_role, mailbox)


__all__ = ["ActorSystem", "Props", "ActorRef", "Config", "Context", "props"]
