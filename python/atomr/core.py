"""core facade over atomr._native.core.

Exposes secondary atomr-core types — DispatcherConfig, BoundedStash,
ControlAwareQueue, ResizerConfig, DeadLetterFilter, OverflowStrategy /
StashOverflow enums, and a Python-driven FsmBuilder/Fsm pair.
"""
from . import _native

_sub = _native.core
globals().update({k: getattr(_sub, k) for k in dir(_sub) if not k.startswith('_')})
__all__ = [k for k in dir(_sub) if not k.startswith('_')]
