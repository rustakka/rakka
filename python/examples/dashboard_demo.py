"""atomr dashboard demo (Python).

Spins up a small actor topology, populates the streams + dead-letter
probes, and starts the embedded web dashboard. Browse to the printed
URL to see the live actor tree, dead letters, and stream graphs.

Run::

    maturin develop --release --manifest-path crates/py-bindings/pycore/Cargo.toml
    python -m examples.dashboard_demo

Press Ctrl-C to stop.
"""
from __future__ import annotations

import signal
import threading
import time

from atomr import Actor, ActorSystem, dashboard, props


# ---------------------------------------------------------------------------
#  Actor topology
# ---------------------------------------------------------------------------


class Worker(Actor):
    """Counts the `Inc` messages it receives."""

    def __init__(self) -> None:
        self.n = 0

    async def handle(self, ctx, message):
        if message == "Inc":
            self.n += 1


class Boss(Actor):
    """Lazily spawns three Workers on the first `Tick`, fans `Inc` out on
    every subsequent `Tick`, and retires `worker-2` on `Retire` so later
    ticks turn into dead letters on the dashboard's DeadLetters page.

    Note: spawning happens in `handle` rather than `pre_start` because the
    Python binding currently passes `ctx=None` to `pre_start`."""

    def __init__(self) -> None:
        self.workers: list = []  # list[(name, ActorRef)]

    async def handle(self, ctx, message):
        if not self.workers:
            for i in range(3):
                name = f"worker-{i}"
                ref = await ctx.spawn(props(Worker), name)
                self.workers.append((name, ref))
        if message == "Tick":
            for _, ref in self.workers:
                ref.tell("Inc")
        elif isinstance(message, tuple) and message[0] == "Retire":
            ctx.stop_child(message[1])
            # Keep the ref in self.workers so subsequent Ticks still try
            # to deliver to it — that's what produces the dead letters.


# ---------------------------------------------------------------------------
#  Background drivers
# ---------------------------------------------------------------------------


def run_ticker(boss, stop_event: threading.Event) -> None:
    """Tick every 2s for ~25s, then back off so the DeadLetters page
    caps at a small bounded total instead of growing forever."""
    deadline = time.monotonic() + 25.0
    while not stop_event.is_set() and time.monotonic() < deadline:
        boss.tell("Tick")
        stop_event.wait(2.0)


def run_retirer(boss, stop_event: threading.Event) -> None:
    """Retire workers one at a time so the DeadLetters page shows
    variety in recipients, not a flood from a single worker."""
    schedule = [(6.0, "worker-2"), (14.0, "worker-1"), (22.0, "worker-0")]
    start = time.monotonic()
    for at, name in schedule:
        wait = max(0.0, (start + at) - time.monotonic())
        if stop_event.wait(wait):
            return
        boss.tell(("Retire", name))
        print(f"[demo] retired {name} — its ref now produces dead letters")


def run_streams(system, stop_event: threading.Event) -> None:
    """Register pretend running stream graphs so the Streams page lights up.
    Pure-Python streams runs don't yet feed the telemetry probe — this
    uses `dashboard.start_demo_graph`/`finish_demo_graph` so the demo
    has the same Streams visualisation as the Rust version."""
    round_no = 0
    while not stop_event.is_set():
        round_no += 1
        name = f"py-tick-collector-{round_no}"
        gid = dashboard.start_demo_graph(system, name)
        print(f"[demo] stream graph {name!r} started (id={gid})")
        # Pretend the graph runs for ~2s.
        if stop_event.wait(2.0):
            dashboard.finish_demo_graph(system, gid)
            return
        dashboard.finish_demo_graph(system, gid)
        if stop_event.wait(1.0):
            return


# ---------------------------------------------------------------------------
#  Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    system = ActorSystem.create_blocking("dashboard-demo-py")
    handle = None
    stop_event = threading.Event()
    threads: list[threading.Thread] = []
    try:
        # Start the dashboard *before* spawning anything. `serve(system=...)`
        # installs the telemetry extension on the system; spawns that
        # happen before the install aren't recorded in the actor registry,
        # so the Actors page would otherwise look empty.
        handle = dashboard.serve(
            bind="127.0.0.1:9100",
            node="demo-node-1",
            system=system,
        )

        boss = system.actor_of(props(Boss), "boss")

        # Background drivers — daemon threads so an unhandled signal doesn't
        # leave them dangling.
        for fn, args in (
            (run_ticker, (boss, stop_event)),
            (run_retirer, (boss, stop_event)),
            (run_streams, (system, stop_event)),
        ):
            t = threading.Thread(target=fn, args=args, daemon=True)
            t.start()
            threads.append(t)

        addr = handle.address
        print()
        print("┌─────────────────────────────────────────────────────────────")
        print("│ atomr dashboard demo (Python)")
        print(f"│   UI:        http://{addr}/")
        print(f"│   API:       http://{addr}/api/snapshot")
        print(f"│   actors:    http://{addr}/api/actors/tree")
        print(f"│   dead lts:  http://{addr}/api/dead-letters")
        print(f"│   streams:   http://{addr}/api/streams")
        print(f"│   ws stream: ws://{addr}/ws")
        print("│ Ctrl-C to stop.")
        print("└─────────────────────────────────────────────────────────────")
        print()

        signal.signal(signal.SIGINT, lambda *_: stop_event.set())
        signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
        while not stop_event.is_set():
            time.sleep(0.5)
    finally:
        stop_event.set()
        for t in threads:
            t.join(timeout=1.0)
        if handle is not None:
            handle.shutdown()
        system.terminate_blocking()


if __name__ == "__main__":
    main()
