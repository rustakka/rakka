"""Cluster control plane — high-level Python facade over the active
:class:`atomr._native.cluster.Cluster` object.

This module re-exports the read-only cluster data structures (`Member`,
`MembershipState`, `VectorClock`, …) and adds Python-side dataclasses for
each cluster event variant. The native ``Cluster.subscribe`` returns
``dict``-shaped events; helpers in this module convert them into the
typed dataclasses so client code can pattern-match cleanly.

Typical usage::

    from atomr import ActorSystem
    from atomr.cluster import Cluster, MemberUp

    sys = ActorSystem.create_blocking("my-cluster")
    cluster = Cluster.get(sys)
    await cluster.join_seed_nodes(["akka://my-cluster"])
    async for event in cluster.subscribe(["MemberUp"]):
        if isinstance(event, MemberUp):
            print(f"member {event.member.address} is up")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Iterable, List, Mapping, Optional

from . import _native

_sub = _native.cluster

# ---------------------------------------------------------------------------
# Re-exports of the read-only data types from the native submodule.
# ---------------------------------------------------------------------------
Member = _sub.Member
MembershipState = _sub.MembershipState
VectorClock = _sub.VectorClock
LeaderHandover = _sub.LeaderHandover
LeaderHandoverEvent = _sub.LeaderHandoverEvent
member_weakly_up = _sub.member_weakly_up
ClusterRegistry = _sub.ClusterRegistry


# ---------------------------------------------------------------------------
# Event dataclasses. The native subscribe() yields raw dicts; we lift them
# to typed dataclasses on the consuming side so user code can pattern-match.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MemberInfo:
    """Lightweight value snapshot of a cluster member."""

    address: str
    status: str
    up_number: int
    roles: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class _MemberEvent:
    member: MemberInfo


@dataclass(frozen=True)
class MemberJoined(_MemberEvent):
    """Emitted when a member is added in the `joining` state."""


@dataclass(frozen=True)
class MemberWeaklyUp(_MemberEvent):
    """Emitted when a member is marked `weakly_up` before convergence."""


@dataclass(frozen=True)
class MemberUp(_MemberEvent):
    """Emitted when a member transitions to `up`."""


@dataclass(frozen=True)
class MemberLeft(_MemberEvent):
    """Emitted when a member voluntarily leaves the cluster."""


@dataclass(frozen=True)
class MemberExited(_MemberEvent):
    """Emitted when a member completes the leaving handshake."""


@dataclass(frozen=True)
class MemberDowned(_MemberEvent):
    """Emitted when a member is forcibly downed (alias of UnreachableMember
    + Down transition).
    """


@dataclass(frozen=True)
class MemberRemoved:
    """Emitted when a member is removed from the cluster (final state)."""

    member: MemberInfo
    previous_status: str


@dataclass(frozen=True)
class UnreachableMember(_MemberEvent):
    """Emitted when the failure detector flags a member as unreachable."""


@dataclass(frozen=True)
class ReachableMember(_MemberEvent):
    """Emitted when a previously unreachable member becomes reachable."""


@dataclass(frozen=True)
class LeaderChanged:
    """Emitted whenever the elected leader changes."""

    from_address: Optional[str]
    to_address: Optional[str]


@dataclass(frozen=True)
class ClusterShuttingDown:
    """Emitted when the local cluster begins shutdown."""


@dataclass(frozen=True)
class Convergence:
    """Emitted when the gossip view converges (`True`) or fails to (`False`)."""

    converged: bool


def _member_from_dict(d: Mapping[str, Any]) -> MemberInfo:
    return MemberInfo(
        address=d.get("address", ""),
        status=d.get("status", ""),
        up_number=int(d.get("up_number", 0)),
        roles=list(d.get("roles", [])),
    )


def event_from_dict(d: Mapping[str, Any]) -> Any:
    """Convert a raw ``dict`` event from :class:`Cluster.subscribe` into one
    of the typed dataclasses above. Unknown event kinds are returned as
    raw dicts.
    """
    kind = d.get("kind")
    if kind == "MemberJoined":
        return MemberJoined(member=_member_from_dict(d["member"]))
    if kind == "MemberWeaklyUp":
        return MemberWeaklyUp(member=_member_from_dict(d["member"]))
    if kind == "MemberUp":
        return MemberUp(member=_member_from_dict(d["member"]))
    if kind == "MemberLeft":
        return MemberLeft(member=_member_from_dict(d["member"]))
    if kind == "MemberExited":
        return MemberExited(member=_member_from_dict(d["member"]))
    if kind == "MemberRemoved":
        return MemberRemoved(
            member=_member_from_dict(d["member"]),
            previous_status=d.get("previous_status", "removed"),
        )
    if kind == "UnreachableMember":
        return UnreachableMember(member=_member_from_dict(d["member"]))
    if kind == "ReachableMember":
        return ReachableMember(member=_member_from_dict(d["member"]))
    if kind == "LeaderChanged":
        return LeaderChanged(
            from_address=d.get("from_address"),
            to_address=d.get("to_address"),
        )
    if kind == "ClusterShuttingDown":
        return ClusterShuttingDown()
    if kind == "Convergence":
        return Convergence(converged=bool(d.get("converged", False)))
    return d


# ---------------------------------------------------------------------------
# Cluster — async wrapper around _native.cluster.Cluster.
# ---------------------------------------------------------------------------

# Supported SBR strategy names. Mirrored from atomr_pycluster but kept
# inline here so this module is self-contained.
SBR_STRATEGIES = (
    "keep-majority",
    "static-quorum",
    "keep-oldest",
    "down-all",
    "lease-majority",
)


class _SubscriptionWrapper:
    """Async iterator wrapper that lifts native dict events to dataclasses."""

    __slots__ = ("_native_sub",)

    def __init__(self, native_sub: Any) -> None:
        self._native_sub = native_sub

    @property
    def dropped_events(self) -> int:
        """Number of events dropped because the bounded channel was full."""
        return int(self._native_sub.dropped_events)

    @property
    def filter(self) -> Optional[List[str]]:
        """Event-type filter passed to ``Cluster.subscribe``, or ``None`` for
        unfiltered subscriptions.
        """
        return self._native_sub.filter

    def close(self) -> None:
        """Eagerly close the subscription. Subsequent iterations raise
        ``StopAsyncIteration``.
        """
        self._native_sub.close()

    def __aiter__(self) -> "_SubscriptionWrapper":
        return self

    async def __anext__(self) -> Any:
        raw = await self._native_sub.__anext__()
        return event_from_dict(raw)


class Cluster:
    """Active cluster control plane — Python wrapper around
    :class:`atomr._native.cluster.Cluster`.

    Use :meth:`Cluster.get` to fetch the singleton for an
    :class:`atomr.ActorSystem`. The first call lazily starts the
    cluster daemon; subsequent calls return the same instance.
    """

    __slots__ = ("_native",)

    def __init__(self, native: Any) -> None:
        self._native = native

    @classmethod
    def get(cls, system: Any) -> "Cluster":
        """Return (or lazily create) the cluster singleton for ``system``.

        Without a prior call to :meth:`with_test_transport` or
        :meth:`with_tcp_transport`, the underlying transport is a
        no-op (single-node mode).
        """
        sys_native = getattr(system, "_native", system)
        return cls(_sub.Cluster.get(sys_native))

    @classmethod
    def with_test_transport(
        cls,
        system: Any,
        registry: ClusterRegistry,
        advertised_address: Optional[str] = None,
    ) -> "Cluster":
        """Configure an in-process cluster transport bound to ``registry``.

        Multiple :class:`ActorSystem`s sharing the same
        :class:`ClusterRegistry` reach each other deterministically via
        in-memory channels — useful for multi-node tests in a single
        Python process.

        ``advertised_address`` overrides the address the local node
        announces to peers. Defaults to ``akka://<system_name>``.
        """
        sys_native = getattr(system, "_native", system)
        return cls(_sub.Cluster.with_test_transport(sys_native, registry, advertised_address))

    @classmethod
    def with_tcp_transport(
        cls,
        system: Any,
        bind_addr: str,
        advertised_host: Optional[str] = None,
    ) -> "Cluster":
        """Configure a TCP cluster transport bound to ``bind_addr``.

        Pass ``"127.0.0.1:0"`` to auto-allocate a port; the resolved
        address is observable via :attr:`Cluster.self_address`.
        """
        sys_native = getattr(system, "_native", system)
        return cls(_sub.Cluster.with_tcp_transport(sys_native, bind_addr, advertised_host))

    @property
    def self_address(self) -> str:
        """Address of this node as a string."""
        return self._native.self_address

    @property
    def leader(self) -> Optional[str]:
        """Address of the elected leader, or ``None`` if there is none yet."""
        return self._native.leader

    def member_count(self) -> int:
        """Number of currently-known members."""
        return int(self._native.member_count())

    def membership_snapshot(self) -> Any:
        """Return a snapshot of the current :class:`MembershipState`."""
        return self._native.membership_snapshot()

    async def join_seed_nodes(self, seed_nodes: Iterable[str], timeout: float = 30.0) -> None:
        """Register seed-node addresses, ensure self is a member, and resolve
        once self reaches ``Up``. Raises :class:`AtomrError` on timeout.
        """
        await self._native.join_seed_nodes(list(seed_nodes), timeout)

    async def leave(self, timeout: float = 30.0) -> None:
        """Mark the local node as ``Leaving`` and wait for ``Removed``."""
        await self._native.leave(timeout)

    async def down(self, address: str) -> None:
        """Mark the given address as ``Down``."""
        await self._native.down(address)

    def subscribe(
        self,
        event_types: Optional[Iterable[str]] = None,
        capacity: int = 1024,
    ) -> AsyncIterator[Any]:
        """Subscribe to cluster events.

        ``event_types`` is an optional iterable of class names to filter on
        (e.g. ``["MemberUp", "MemberRemoved"]``). When ``None``, every
        event is delivered.

        Returns an async iterator (`__aiter__/__anext__`) backed by a
        bounded mpsc channel. Overflowing events are dropped; the
        iterator's ``dropped_events`` property exposes the running
        counter.
        """
        types = list(event_types) if event_types is not None else None
        sub = self._native.subscribe(types, capacity)
        return _SubscriptionWrapper(sub)


__all__ = [
    # Read-only data types.
    "Member",
    "MembershipState",
    "VectorClock",
    "LeaderHandover",
    "LeaderHandoverEvent",
    "member_weakly_up",
    # Active control plane.
    "Cluster",
    "ClusterRegistry",
    "SBR_STRATEGIES",
    # Event dataclasses.
    "MemberInfo",
    "MemberJoined",
    "MemberWeaklyUp",
    "MemberUp",
    "MemberLeft",
    "MemberExited",
    "MemberDowned",
    "MemberRemoved",
    "UnreachableMember",
    "ReachableMember",
    "LeaderChanged",
    "ClusterShuttingDown",
    "Convergence",
    "event_from_dict",
]
