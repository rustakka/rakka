"""Cluster-sharding facade over :mod:`atomr._native.cluster_sharding`.

A `ShardRegion` distributes Python entity actors across the cluster.
Single-node sharding works fully today; multi-node rebalance is best-
effort until the real gossip transport lands (see Phase 9 of the
binding roadmap).

Typical usage::

    from atomr import ActorSystem, props
    from atomr.cluster_sharding import ShardRegion, ShardingSettings

    system = ActorSystem.create_blocking("orders")

    def extractor(msg):
        # msg is `{"entity": "alice", "op": "incr"}`.
        return (msg["entity"], str(hash(msg["entity"]) % 16), msg)

    region = ShardRegion.start(
        system,
        type_name="counters",
        entity_props=props(CounterActor),
        message_extractor=extractor,
        settings=ShardingSettings(
            allocation_strategy="least-shards",
            passivation_idle_timeout=30.0,
            remember_entities=True,
        ),
    )
    region.tell({"entity": "alice", "op": "incr"})

The extractor may instead be supplied as three callables — pass
`shard_id_extractor` and (optionally) `unwrap_extractor` alongside
`message_extractor`, in which case `message_extractor` is treated as
the entity-id extractor.
"""
from __future__ import annotations

from . import _native

ShardRegion = _native.cluster_sharding.ShardRegion
ShardingSettings = _native.cluster_sharding.ShardingSettings

__all__ = [
    "ShardRegion",
    "ShardingSettings",
]
