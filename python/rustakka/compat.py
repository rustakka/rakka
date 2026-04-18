"""C-extension compatibility registry."""

from . import _native

declare_compat = _native.declare_compat
compat_flags = _native.compat_flags
compat_list = _native.compat_list

__all__ = ["declare_compat", "compat_flags", "compat_list"]
