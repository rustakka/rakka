"""Supervision strategies for Python actors (Phase 2).

This module re-exports the native ``SupervisorStrategy`` builder and
adds a small Python-side ergonomics layer:

* :class:`Directive` — string enum of the four directives the decider
  may return.
* :class:`Terminated` — the user-message that an actor receives when
  one of its watched peers stops.
* Helper functions that accept Python exception **classes** instead of
  string class paths so callers don't have to spell out
  ``builtins.ValueError`` themselves.

The native strategy builder accepts a list of
``(class_path, directive)`` pairs because the Rust decider runs
without the GIL and cannot call back into Python. The compiled decider
receives the panic payload's ``module + "." + qualname`` string
(produced by :mod:`atomr` when a Python actor's handler raises), so
class-name matching is exact.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Optional, Tuple, Type, Union

from . import _native

# Native re-export — users can call this directly.
SupervisorStrategy = _native.SupervisorStrategy


class Directive:
    """String constants matching the Rust `Directive` enum.

    The native strategy builder accepts the lowercase string names; this
    class is a typing-only convenience.
    """

    RESUME = "resume"
    RESTART = "restart"
    STOP = "stop"
    ESCALATE = "escalate"

    ALL = ("resume", "restart", "stop", "escalate")


@dataclass(frozen=True)
class Terminated:
    """User-message delivered by the runtime when a watched actor stops.

    Build via :func:`atomr.Context.watch` (Phase 1 / Phase 2). The
    ``path`` field is the full string path of the terminated actor
    (e.g. ``"akka://S/user/parent/child"``).
    """

    path: str


def class_path(exc: Type[BaseException]) -> str:
    """Return ``"<module>.<qualname>"`` for an exception class.

    Used to translate Python exception classes into the wire format
    consumed by the native decider.
    """
    if not isinstance(exc, type):
        raise TypeError(f"expected exception class, got {exc!r}")
    module = getattr(exc, "__module__", "builtins")
    qualname = getattr(exc, "__qualname__", exc.__name__)
    return f"{module}.{qualname}"


_DeciderRules = Union[
    Mapping[Type[BaseException], str],
    Iterable[Tuple[Union[Type[BaseException], str], str]],
]


def _normalize_decider(decider: Optional[_DeciderRules]) -> Optional[list[tuple[str, str]]]:
    if decider is None:
        return None
    pairs: list[tuple[str, str]] = []
    if isinstance(decider, Mapping):
        items: Iterable[Tuple[Union[Type[BaseException], str], str]] = decider.items()
    else:
        items = decider  # type: ignore[assignment]
    for k, v in items:
        if v not in Directive.ALL:
            raise ValueError(
                f"unknown directive {v!r}; expected one of {Directive.ALL}"
            )
        if isinstance(k, str):
            pairs.append((k, v))
        elif isinstance(k, type) and issubclass(k, BaseException):
            pairs.append((class_path(k), v))
        else:
            raise TypeError(
                f"decider key must be an exception class or class-path string, got {k!r}"
            )
    return pairs


def one_for_one(
    decider: Optional[_DeciderRules] = None,
    *,
    default: Optional[str] = None,
    max_retries: Optional[int] = None,
    within_seconds: Optional[float] = None,
) -> "SupervisorStrategy":
    """Build a `OneForOne` :class:`SupervisorStrategy`.

    Each child is supervised independently — a single child's failure
    does not affect its siblings.

    ``decider`` accepts either a mapping of exception class → directive
    (e.g. ``{ValueError: "restart", RuntimeError: "stop"}``) or an
    iterable of ``(class_or_path, directive)`` tuples. Directives are
    one of ``"resume" | "restart" | "stop" | "escalate"``. ``default``
    applies when no rule matches; if omitted, ``"restart"`` is used
    (matching Rust's ``OneForOneStrategy::default``).
    """
    return SupervisorStrategy.one_for_one(
        _normalize_decider(decider),
        default,
        max_retries,
        within_seconds,
    )


def all_for_one(
    decider: Optional[_DeciderRules] = None,
    *,
    default: Optional[str] = None,
    max_retries: Optional[int] = None,
    within_seconds: Optional[float] = None,
) -> "SupervisorStrategy":
    """Build an `AllForOne` :class:`SupervisorStrategy`.

    A single child's failure restarts (or otherwise affects) all
    siblings. Most useful for tightly-coupled child groups whose state
    must remain consistent.
    """
    return SupervisorStrategy.all_for_one(
        _normalize_decider(decider),
        default,
        max_retries,
        within_seconds,
    )


def stopping() -> "SupervisorStrategy":
    """Stop the failing child on every error (no retries)."""
    return SupervisorStrategy.stopping()


def escalating() -> "SupervisorStrategy":
    """Escalate every failure to the parent's supervisor."""
    return SupervisorStrategy.escalating()


__all__ = [
    "SupervisorStrategy",
    "Directive",
    "Terminated",
    "class_path",
    "one_for_one",
    "all_for_one",
    "stopping",
    "escalating",
]
