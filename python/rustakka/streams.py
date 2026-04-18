"""streams facade over rustakka._native.streams."""
from . import _native

_sub = _native.streams
globals().update({k: getattr(_sub, k) for k in dir(_sub) if not k.startswith('_')})
__all__ = [k for k in dir(_sub) if not k.startswith('_')]
