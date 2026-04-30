"""discovery facade over rakka._native.discovery."""
from . import _native

_sub = _native.discovery
globals().update({k: getattr(_sub, k) for k in dir(_sub) if not k.startswith('_')})
__all__ = [k for k in dir(_sub) if not k.startswith('_')]
