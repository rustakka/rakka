"""coordination facade over rustakka._native.coordination."""
from . import _native

_sub = _native.coordination
globals().update({k: getattr(_sub, k) for k in dir(_sub) if not k.startswith('_')})
__all__ = [k for k in dir(_sub) if not k.startswith('_')]
