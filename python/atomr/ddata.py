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

Nested CRDT support: ``ORMap`` is type-homogeneous in its value type.
Construct a typed map with one of the ``of_*`` factory methods:

* ``ORMap.of_lww_register()`` — values are ``LwwRegister`` (default;
  the bare ``ORMap()`` constructor is equivalent and preserved for
  backwards compat).
* ``ORMap.of_pn_counter()`` — values are ``PNCounter``.
* ``ORMap.of_flag()`` — values are ``Flag``.
* ``ORMap.of_g_set()`` — values are ``GSet``.
* ``ORMap.of_lww_map()`` — values are ``LWWMap``.

All values within one ORMap must share the same CRDT type. Mixing
variants in ``put`` raises :class:`ValueError`; merging two ORMaps
with different value types also raises :class:`ValueError`. The
``Replicator.update`` path currently round-trips ``ORMap`` only when
its value type is ``LwwRegister`` — use ``of_lww_register()`` (or
the bare constructor) for replicated maps. User-defined Python CRDTs
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
