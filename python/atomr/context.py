"""Context-related Python facades.

Re-exports the native ``Context`` and ``Cancelable`` classes alongside any
ergonomic helpers we layer on top. Most users won't import from this module
directly — the actor's ``handle(self, ctx, msg)`` already receives a
:class:`Context` instance, and ``ctx.schedule_once(...)`` etc. return a
:class:`Cancelable` object.
"""

from __future__ import annotations

from . import _native

Context = _native.Context
Cancelable = _native.Cancelable

__all__ = ["Context", "Cancelable"]
