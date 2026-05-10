"""USB link probe — Python flavor.

Mirror of the Rust binary at `examples/usb-link-probe/src/main.rs`,
but driven from Python on top of the PyO3-bound Rust transport +
`RemoteSystem`. Same three subcommands; same chat + ping/pong stats
behavior.

Wire format: JSON dicts under the `LinkMsg` manifest. Both ends must
be Python (the Rust binary uses bincode and is not wire-compatible).

Run:

    python probe.py list-devices
    python probe.py listen  --device /dev/ttyACM0
    python probe.py connect --device COM3 --peer akka.serial://A@/dev/ttyACM0:0
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import signal
import sys
import time
from collections import deque
from typing import Deque, Optional

import atomr
from atomr.remote_serial import RemoteSystem, SerialTransport, decode_json, encode_json

MANIFEST = "LinkMsg"
WINDOW = 1024


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------


class Stats:
    """Rolling RTT recorder. Mirrors the Rust `stats.rs`."""

    def __init__(self) -> None:
        self.sent = 0
        self.recvd = 0
        self.samples: Deque[float] = deque(maxlen=WINDOW)  # seconds

    def record_sent(self) -> None:
        self.sent += 1

    def record_recv(self, rtt_seconds: float) -> None:
        self.recvd += 1
        self.samples.append(rtt_seconds)

    def snapshot(self) -> dict:
        loss = 0.0 if self.sent == 0 else (self.sent - min(self.recvd, self.sent)) / self.sent * 100.0
        return {
            "sent": self.sent,
            "recvd": self.recvd,
            "loss_pct": loss,
            "p50": _percentile(self.samples, 0.50),
            "p95": _percentile(self.samples, 0.95),
            "p99": _percentile(self.samples, 0.99),
        }


def _percentile(samples, p: float) -> Optional[float]:
    if not samples:
        return None
    sorted_samples = sorted(samples)
    rank = max(1, int(p * len(sorted_samples) + 0.999999))  # nearest-rank ceil
    return sorted_samples[min(rank, len(sorted_samples)) - 1]


def _fmt_ms(d: Optional[float]) -> str:
    return f"{d * 1000:.1f}ms" if d is not None else "—"


# ---------------------------------------------------------------------------
# Peer actor — receives bytes, queues for the io-loop
# ---------------------------------------------------------------------------


class Peer(atomr.Actor):
    """Thin shim that forwards every received byte payload into the
    asyncio queue owned by the io-loop. Defining the reactive logic
    in the actor would force us to bridge asyncio↔actor every step;
    keeping it in the io-loop is simpler.

    `loop` must be captured on the asyncio thread before spawning —
    the factory runs on a Tokio worker so `asyncio.get_event_loop()`
    inside `__init__` would fail.
    """

    def __init__(self, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop) -> None:
        self.queue = queue
        self.loop = loop

    async def handle(self, ctx, msg: bytes) -> None:
        # Cross-thread enqueue — actor handle runs on a Tokio worker, the
        # consumer is on the asyncio main thread.
        self.loop.call_soon_threadsafe(self.queue.put_nowait, msg)


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------


async def cmd_list_devices(_args) -> int:
    devices = SerialTransport.list_devices()
    if not devices:
        print("(no serial ports found)")
        return 0
    for name, kind in devices:
        print(f"{name}  {kind}")
    return 0


async def cmd_endpoint(args, peer_addr_initial: Optional[str]) -> int:
    sys_obj = await atomr.ActorSystem.create(args.system)
    transport = SerialTransport(args.system, args.device, args.baud)
    remote = await RemoteSystem.start_serial(sys_obj, transport)
    remote.register_bytes_codec(MANIFEST)

    queue: asyncio.Queue = asyncio.Queue()
    loop = asyncio.get_event_loop()
    peer = sys_obj.actor_of(
        atomr.props(Peer, factory=lambda: Peer(queue, loop)),
        "peer",
    )
    remote.expose_actor(peer, MANIFEST)

    my_addr = remote.local_address
    print(f"local address: {my_addr}")
    if peer_addr_initial:
        print(f"peer address:  {peer_addr_initial}")
    else:
        print("peer address:  (waiting for incoming Ping to learn it)")
    print("(type lines, Ctrl-C to exit)")

    stats = Stats()
    peer_addr = peer_addr_initial  # mutable closure-captured-by-list trick below
    peer_addr_holder = [peer_addr]
    target_holder: list[Optional[object]] = [None]

    def resolve_target():
        addr = peer_addr_holder[0]
        if addr is None:
            return None
        if target_holder[0] is None:
            target_holder[0] = remote.actor_selection(f"{addr}/user/peer", MANIFEST)
        return target_holder[0]

    stop_event = asyncio.Event()

    async def inbound_task():
        while not stop_event.is_set():
            try:
                msg_bytes = await asyncio.wait_for(queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            try:
                data = decode_json(msg_bytes)
            except Exception as e:
                print(f"[err] failed to decode inbound: {e}", file=sys.stderr)
                continue
            kind = data.get("type")
            if kind == "Chat":
                print(f"[in]  {data.get('body', '')}")
            elif kind == "Ping":
                from_addr = data.get("from_addr")
                if from_addr and peer_addr_holder[0] is None:
                    peer_addr_holder[0] = from_addr
                    target_holder[0] = None  # force re-resolve
                target = resolve_target()
                if target is not None:
                    target.tell(encode_json({
                        "type": "Pong",
                        "seq": data["seq"],
                        "sent_at": data["sent_at"],
                    }))
            elif kind == "Pong":
                rtt = max(0.0, time.time() - data["sent_at"])
                stats.record_recv(rtt)

    async def stdin_task():
        loop = asyncio.get_event_loop()
        # `sys.stdin.readline` blocks; offload to a worker thread.
        while not stop_event.is_set():
            line = await loop.run_in_executor(None, sys.stdin.readline)
            if not line:
                break
            body = line.rstrip("\n")
            target = resolve_target()
            if target is None:
                print("[..]  peer not associated yet, dropping line", file=sys.stderr)
                continue
            target.tell(encode_json({"type": "Chat", "body": body}))
            print(f"[out] {body}")

    async def ping_task():
        seq = 0
        while not stop_event.is_set():
            await asyncio.sleep(args.ping_interval)
            target = resolve_target()
            if target is None:
                continue
            target.tell(encode_json({
                "type": "Ping",
                "seq": seq,
                "sent_at": time.time(),
                "from_addr": my_addr,
            }))
            stats.record_sent()
            seq += 1

    async def stats_task():
        # Skip the immediate first tick — nothing useful yet.
        await asyncio.sleep(args.stats_interval)
        while not stop_event.is_set():
            snap = stats.snapshot()
            print(
                f"stats: sent={snap['sent']} recvd={snap['recvd']} "
                f"loss={snap['loss_pct']:.1f}% "
                f"p50={_fmt_ms(snap['p50'])} "
                f"p95={_fmt_ms(snap['p95'])} "
                f"p99={_fmt_ms(snap['p99'])}"
            )
            await asyncio.sleep(args.stats_interval)

    tasks = [
        asyncio.create_task(t())
        for t in (inbound_task, stdin_task, ping_task, stats_task)
    ]

    # Ctrl-C handling: signal the asyncio loop to stop_event, let
    # tasks finish naturally rather than abort mid-tell.
    def _on_sigint():
        print("(ctrl-c received, shutting down)")
        stop_event.set()

    loop = asyncio.get_event_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, _on_sigint)
    except NotImplementedError:
        # Windows asyncio has no add_signal_handler — fall back to KeyboardInterrupt.
        pass

    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        stop_event.set()

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await remote.shutdown()
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


_DURATION = re.compile(r"^(\d+)(ms|s|m)$")


def _parse_duration(s: str) -> float:
    m = _DURATION.match(s)
    if not m:
        raise argparse.ArgumentTypeError(f"bad duration `{s}` (use `1s`, `500ms`, `2m`)")
    n, unit = int(m.group(1)), m.group(2)
    return {"ms": n / 1000, "s": float(n), "m": float(n * 60)}[unit]


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="probe.py", description="USB link probe (Python)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list-devices", help="enumerate serial ports")

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--device", required=True, help="serial device path (e.g. /dev/ttyACM0, COM3)")
    common.add_argument("--baud", type=int, default=115_200)
    common.add_argument("--system", default="A", help="ActorSystem name (becomes part of address)")
    common.add_argument("--ping-interval", type=_parse_duration, default=1.0, help="Ping cadence (default 1s)")
    common.add_argument("--stats-interval", type=_parse_duration, default=5.0, help="Stats overlay cadence (default 5s)")

    listen = sub.add_parser("listen", parents=[common], help="open device and wait for a connect peer")
    _ = listen
    connect = sub.add_parser("connect", parents=[common], help="open device and associate to peer's address")
    connect.add_argument("--peer", required=True, help="peer's printed address (akka.serial://...)")
    return p


async def _main_async() -> int:
    args = _build_parser().parse_args()
    if args.cmd == "list-devices":
        return await cmd_list_devices(args)
    if args.system == "A" and args.cmd == "connect":
        args.system = "B"  # match the Rust default mapping
    return await cmd_endpoint(args, peer_addr_initial=getattr(args, "peer", None))


def main() -> int:
    try:
        return asyncio.run(_main_async())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
