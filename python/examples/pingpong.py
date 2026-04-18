"""Minimal ping/pong demo.

Run with ``python -m examples.pingpong`` after ``maturin develop --release``.
"""

from __future__ import annotations

import time

from rustakka import Actor, ActorSystem, props


class Ping(Actor):
    async def handle(self, ctx, msg):
        return f"pong:{msg}"


def main() -> None:
    system = ActorSystem.create_blocking("pingpong-example")
    try:
        ref = system.actor_of(props(Ping), "ping")
        start = time.perf_counter()
        for i in range(1000):
            ref.ask_blocking(i, 5.0)
        dt = time.perf_counter() - start
        print(f"1,000 ask round-trips in {dt*1000:.1f} ms ({1000/dt:,.0f}/s)")
    finally:
        system.terminate_blocking()


if __name__ == "__main__":
    main()
