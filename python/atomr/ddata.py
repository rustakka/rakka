"""Distributed data — CRDTs and a Replicator actor.

The CRDTs (`GCounter`, `PNCounter`, `GSet`, `ORSet`, `LwwRegister`,
`Flag`, `ORMap`, `LWWMap`, `PNCounterMap`, `ORMultiMap`) merge
deterministically when two replicas exchange state.

The :class:`Replicator` actor owns a per-system CRDT store. Use
``Replicator.get(system)`` to obtain a singleton handle, then ``await``
on :meth:`Replicator.update` / :meth:`Replicator.get_value` /
:meth:`Replicator.delete` for typed access.

`update(key, initial=GCounter, modify_fn=fn, write_consistency=...)`
hands `modify_fn` the current CRDT (or a fresh one if absent), expects
the caller to mutate it in place (or return a new instance), and
merges the result back into the replicator. `modify_fn` may be sync
or `async`.

Nested CRDT support is intentionally limited: ORMap values are
restricted to ``LwwRegister`` instances. User-defined Python CRDTs
cannot serve as ORMap values because the Rust merge runs server-side
without the GIL.
"""

from __future__ import annotations

from . import _native

_sub = _native.ddata

# CRDTs.
GCounter = _sub.GCounter
PNCounter = _sub.PNCounter
GSet = _sub.GSet
ORSet = _sub.ORSet
LwwRegister = _sub.LwwRegister
Flag = _sub.Flag
ORMap = _sub.ORMap
LWWMap = _sub.LWWMap
PNCounterMap = _sub.PNCounterMap
ORMultiMap = _sub.ORMultiMap

# Replicator + consistency types.
Replicator = _sub.Replicator
ReplicatorSubscription = _sub.ReplicatorSubscription
ReadConsistency = _sub.ReadConsistency
WriteConsistency = _sub.WriteConsistency
DurableStore = _sub.DurableStore

# Helpers.
PruningState = _sub.PruningState
WriteAggregator = _sub.WriteAggregator
ReadAggregator = _sub.ReadAggregator
pruning_phases = _sub.pruning_phases

__all__ = [
    "GCounter",
    "PNCounter",
    "GSet",
    "ORSet",
    "LwwRegister",
    "Flag",
    "ORMap",
    "LWWMap",
    "PNCounterMap",
    "ORMultiMap",
    "Replicator",
    "ReplicatorSubscription",
    "ReadConsistency",
    "WriteConsistency",
    "DurableStore",
    "PruningState",
    "WriteAggregator",
    "ReadAggregator",
    "pruning_phases",
]
