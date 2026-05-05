"""ddata_lmdb facade over atomr._native.ddata_lmdb.

Provides a redb-backed durable store for `atomr-distributed-data`.
akka.net analog: `Akka.DistributedData.LightningDB.LmdbDurableStore`.
"""
from . import _native

_sub = _native.ddata_lmdb
globals().update({k: getattr(_sub, k) for k in dir(_sub) if not k.startswith('_')})
__all__ = [k for k in dir(_sub) if not k.startswith('_')]
