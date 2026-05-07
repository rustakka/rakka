"""Typed Python facade over the native :class:`atomr._native.Context`.

The native class is constructed by Rust on every dispatched message; we
re-export it here with type stubs for IDE / mypy support. Users do not
construct :class:`Context` themselves — they receive one as the first
argument to :meth:`Actor.handle`.

Example::

    class Greeter(Actor):
        async def handle(self, ctx, message):
            if message == "ping":
                ctx.tell  # not on Context — use ctx.self_ref or ctx.sender
                if ctx.sender is not None:
                    ctx.sender.tell("pong")

The members below mirror the Rust :rust:`Context<A>` API surface where
that surface is meaningful at the Python boundary; mutations are
applied at end-of-message (Akka semantics) so reads inside the same
handler see the pre-mutation state.
"""

from __future__ import annotations

from typing import Any, Optional, TYPE_CHECKING

from . import _native

# Re-export the native class. Casts below give static type checkers
# something to grip onto.
Context = _native.Context

if TYPE_CHECKING:
    from .system import ActorRef, Props

    class Context:  # type: ignore[no-redef]
        """Per-message handle into the actor's runtime state.

        Instances are created by Rust on each dispatch and discarded
        when the handler returns; saving a reference and using it
        outside the originating handler raises :class:`RuntimeError`.
        """

        @property
        def self_ref(self) -> "ActorRef":
            """`ActorRef` to this actor."""
            ...

        @property
        def path(self) -> str:
            """Full actor path, e.g. ``"akka://sys/user/foo"``."""
            ...

        @property
        def sender(self) -> Optional["ActorRef"]:
            """Sender of the current message, if it was set via
            :meth:`ActorRef.tell_with_sender`."""
            ...

        async def spawn(self, props: "Props", name: str) -> "ActorRef":
            """Spawn a child actor under this actor.

            Children inherit this actor's ``interpreter_role`` for cache
            locality; override via ``props.with_interpreter_role(...)``.
            Returns the new child's :class:`ActorRef`.
            """
            ...

        def stop_child(self, name: str) -> None:
            """Stop a child by name. No-op if the name is unknown."""
            ...

        def stop_self(self) -> None:
            """Stop this actor after the current handler returns."""
            ...

        def stash(self, msg: Any) -> None:
            """Stash ``msg`` for a later :meth:`unstash_all`. Stash
            order is preserved."""
            ...

        def unstash_all(self) -> None:
            """Re-deliver every stashed message back to self. Order is
            preserved (FIFO)."""
            ...

        def set_receive_timeout(self, seconds: Optional[float] = None) -> None:
            """Configure the idle-receive timeout. ``None`` clears it."""
            ...

        def schedule_once(
            self,
            delay_secs: float,
            msg: Any,
            target: Optional["ActorRef"] = None,
        ) -> None:
            """Deliver ``msg`` to ``target`` (or self) after ``delay_secs``."""
            ...

        def schedule_periodically(
            self,
            initial_secs: float,
            interval_secs: float,
            msg: Any,
            target: Optional["ActorRef"] = None,
        ) -> None:
            """Deliver ``msg`` to ``target`` (or self) every ``interval_secs``,
            starting after ``initial_secs``."""
            ...

        def become_(self, new_handler: Any) -> None:
            """Replace the active handler with the async callable
            ``new_handler(ctx, msg)``. Subsequent messages dispatch
            through ``new_handler`` until :meth:`unbecome` is called."""
            ...

        def unbecome(self) -> None:
            """Restore the actor's default :meth:`handle` method."""
            ...


__all__ = ["Context"]
