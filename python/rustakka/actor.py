"""Python-side :class:`Actor` base class.

Subclasses implement :meth:`handle`. Hooks ``pre_start``/``post_stop`` are
optional and may be either regular or async methods — the Rust shim
awaits coroutines on the interpreter's thread.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class Actor(ABC):
    """Base class for Python actors hosted inside a Rust ActorSystem."""

    @abstractmethod
    async def handle(self, ctx: Any, message: Any) -> Any:  # pragma: no cover - abstract
        ...

    async def pre_start(self, ctx: Any) -> None:  # noqa: D401
        """Called exactly once before the first message is handled."""
        return None

    async def post_stop(self, ctx: Any) -> None:
        """Called exactly once after the actor has stopped."""
        return None

    async def pre_restart(self, ctx: Any, reason: BaseException, message: Any) -> None:
        return None

    async def post_restart(self, ctx: Any, reason: BaseException) -> None:
        return None
