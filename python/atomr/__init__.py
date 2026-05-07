"""atomr — high-level Python API for the Rust actor framework.

The heavy lifting happens in the native extension :mod:`atomr._native`;
this package provides ergonomic wrappers, type stubs, and the `Actor`
base class that Python users subclass.
"""

from . import _native  # noqa: F401

from .actor import Actor
from .system import ActorSystem, Props, ActorRef, Config, Context, props
from .context import Cancelable
from .errors import (
    AtomrError,
    ActorSystemError,
    SpawnError,
    AskError,
    InterpreterOverloaded,
    InterpreterCompatError,
)
from .interpreter import InterpreterQuota, subinterpreters_supported, nogil_supported
from .compat import declare_compat, compat_flags, compat_list

from . import testkit
from . import cluster
from . import cluster_metrics
from . import cluster_tools
from . import cluster_sharding
from . import core
from . import ddata
from . import ddata_lmdb
from . import persistence
from . import streams
from . import coordination
from . import discovery
from . import di
from . import hosting
from . import telemetry

__version__ = _native.__version__

__all__ = [
    "Actor",
    "ActorSystem",
    "Props",
    "ActorRef",
    "Config",
    "Context",
    "Cancelable",
    "props",
    "InterpreterQuota",
    "subinterpreters_supported",
    "nogil_supported",
    "declare_compat",
    "compat_flags",
    "compat_list",
    "AtomrError",
    "ActorSystemError",
    "SpawnError",
    "AskError",
    "InterpreterOverloaded",
    "InterpreterCompatError",
    "testkit",
    "cluster",
    "cluster_metrics",
    "cluster_tools",
    "cluster_sharding",
    "core",
    "ddata",
    "ddata_lmdb",
    "persistence",
    "streams",
    "coordination",
    "discovery",
    "di",
    "hosting",
    "telemetry",
]
