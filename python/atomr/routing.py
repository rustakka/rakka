"""Routers, exposed as ``Props`` factories.

Each constructor returns a ``Props`` whose ``actor_of`` produces a
pure-Rust router that owns N children built from ``child_props`` and
forwards every message according to the chosen logic.

The Rust extension also installs the constructors as classmethods on
``Props`` itself, so both call styles work::

    pool = Props.round_robin(child_props, n=3)
    # equivalently:
    from atomr.routing import round_robin
    pool = round_robin(child_props, n=3)

Consistent-hash routers require the explicit key form::

    ref.tell_with_key(message, key=42)

A plain ``tell`` against a consistent-hash router drops the message
with a warning; an ``ask`` raises an :class:`AtomrError`.
"""

from __future__ import annotations

from typing import Any

from . import _native
from .system import Props

broadcast = _native.routing._broadcast
round_robin = _native.routing._round_robin
random = _native.routing._random
consistent_hash = _native.routing._consistent_hash
smallest_mailbox = _native.routing._smallest_mailbox
tail_chopping = _native.routing._tail_chopping
scatter_gather = _native.routing._scatter_gather
backoff = _native.routing._backoff


__all__ = [
    "broadcast",
    "round_robin",
    "random",
    "consistent_hash",
    "smallest_mailbox",
    "tail_chopping",
    "scatter_gather",
    "backoff",
]
