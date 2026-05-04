"""hosting facade — ActorSystemBuilder wrapper with Pythonic chaining."""

from __future__ import annotations

from typing import Callable, List, Optional, Tuple

from . import _native

ActorSystemBuilder = _native.hosting.ActorSystemBuilder


class Builder:
    """Python-friendly wrapper that accumulates config, interpreter pools,
    and on-start callbacks before building a single :class:`ActorSystem`.
    """

    def __init__(self, name: str) -> None:
        self._name = name
        self._config = None
        self._pools: List[Tuple[str, str, int, object]] = []
        self._on_start: List[Callable] = []

    def with_config(self, config) -> "Builder":
        self._config = config
        return self

    def configure_interpreter(
        self,
        label: str,
        dispatcher: str = "python-pinned",
        count: int = 1,
        quota: Optional["_native.InterpreterQuota"] = None,
    ) -> "Builder":
        self._pools.append((label, dispatcher, count, quota))
        return self

    def on_start(self, callback: Callable) -> "Builder":
        self._on_start.append(callback)
        return self

    def build(self):
        sys = _native.ActorSystem.create_blocking(self._name, self._config)
        for label, dispatcher, count, quota in self._pools:
            sys.configure_interpreter(label, dispatcher, count, quota)
        for cb in self._on_start:
            cb(sys)
        return sys


__all__ = ["Builder", "ActorSystemBuilder"]
