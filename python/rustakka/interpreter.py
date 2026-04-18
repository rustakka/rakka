"""Interpreter configuration and capability probes."""

from . import _native

InterpreterQuota = _native.InterpreterQuota
subinterpreters_supported = _native.subinterpreters_supported
nogil_supported = _native.nogil_supported

__all__ = ["InterpreterQuota", "subinterpreters_supported", "nogil_supported"]
