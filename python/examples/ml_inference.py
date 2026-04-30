"""ML-style inference workload, spread across a subinterpreter pool.

This demonstrates GIL isolation: each interpreter runs on its own OS
thread with its own GIL, so CPU-bound Python handlers actually run in
parallel as long as the underlying C extensions are subinterpreter-safe.
"""

from __future__ import annotations

import math
import time

from rakka import Actor, ActorSystem, InterpreterQuota, props


class Predictor(Actor):
    """Pretend-ML actor — does a fixed-cost Python computation."""

    async def handle(self, ctx, vec):
        # Intentionally CPU-heavy pure-Python to exercise the GIL.
        acc = 0.0
        for x in vec:
            acc += math.sin(x) * math.cos(x)
        return acc


def main() -> None:
    system = ActorSystem.create_blocking("ml-example")
    try:
        quota = InterpreterQuota(
            max_actors=16,
            max_handler_ms=500,
            module_allowlist=["math", "rakka"],
            import_policy="eager",
        )
        system.configure_interpreter(
            "ml-inference", "python-subinterpreter-pool", 4, quota
        )

        refs = [
            system.actor_of(
                props(
                    Predictor,
                    dispatcher="python-subinterpreter-pool",
                    interpreter_role="ml-inference",
                ),
                f"predictor-{i}",
            )
            for i in range(4)
        ]

        vec = list(range(2000))
        start = time.perf_counter()
        results = []
        for i in range(16):
            r = refs[i % len(refs)].ask_blocking(vec, 10.0)
            results.append(r)
        dt = time.perf_counter() - start
        print(f"16 predictions in {dt*1000:.1f} ms ({16/dt:,.1f}/s)")
        print(f"first result: {results[0]:.4f}")
    finally:
        system.terminate_blocking()


if __name__ == "__main__":
    main()
