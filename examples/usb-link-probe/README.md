# usb-link-probe

Cross-OS diagnostic + interactive demo for `atomr-remote-serial` over
a USB cable. Plug a Linux box into a Windows box (or any pair of
{Linux, macOS, Windows}), run one binary on each side, and verify the
link with two signals at once:

* **Chat** — type a line on either end; it appears on the other.
  Operator-friendly proof the link works.
* **Stats overlay** — rolling sent / received / loss% / p50/p95/p99
  RTT line, computed from a periodic ping/pong probe. Objective proof
  the link works.

Companion to `examples/usb-cable-link/`, which is a simpler echo demo
referenced from `docs/remoting.md`. This one adds discovery, clap CLI,
cross-OS-friendly defaults, bidirectional traffic, and stats.

## Hardware setup

You need a byte pipe between the two machines. The two common shapes:

* **CDC-ACM USB serial (Tier 2 in `docs/remoting.md`)** — one side is
  a Linux board configured as a USB gadget exposing `/dev/ttyGS0`,
  the other side is any host with USB. Cheap; works on any OS as the
  host.
* **USB-to-serial null-modem dongles** — two USB-to-serial dongles
  cabled together with a null-modem RS-232 cable. Slower (typical
  115200 baud) but works between two non-gadget machines.

See `docs/remoting.md` § *USB cable mode* for the design overview,
and **[`SETUP.md`](SETUP.md) for per-platform plumbing** —
required packages, user permissions, drivers, USB-gadget config,
finding the right device path, and per-OS troubleshooting.

## Build (Rust path)

```
cargo build --release -p example-usb-link-probe
```

The binary is `target/release/usb-link-probe(.exe)`. Copy it to both
machines.

## Python flavor

The same demo is available as a Python script at `python/probe.py`.
It uses the PyO3 bindings in `atomr.remote_serial` (`SerialTransport`,
`RemoteSystem`, `RemoteActorRef`) on top of the same Rust transport
+ `RemoteSystem` the binary uses, so the underlying byte path is
identical — only the chat / ping / stats logic moves from Rust to
Python.

Setup (once per machine):

```
pip install atomr        # production
# OR, from a checkout:
maturin develop --release
```

Then on each side:

```
python examples/usb-link-probe/python/probe.py list-devices
python examples/usb-link-probe/python/probe.py listen  --device /dev/ttyACM0
python examples/usb-link-probe/python/probe.py connect --device COM3 \
    --peer akka.serial://A@/dev/ttyACM0:0
```

Wire format: JSON dicts under the `LinkMsg` manifest (the demo's
`encode_json`/`decode_json` helpers handle this). **The Python demo
is not wire-compatible with the Rust binary**, which uses bincode —
both sides of a given session must run the same flavor. The
`atomr-remote-serial` transport itself is identical either way.

## Discover your serial device

```
usb-link-probe list-devices
```

Lists every serial port the OS knows about. Cross-platform via
`tokio_serial::available_ports()`. Sample output:

```
/dev/ttyACM0  UsbPort(UsbPortInfo { vid: 2341, pid: 0043, ... })
COM3          UsbPort(UsbPortInfo { vid: 0403, pid: 6001, ... })
```

If the demo's own enumeration disagrees with what you expect, the
per-OS commands also work:

* **Linux:** `ls /dev/ttyACM* /dev/ttyUSB* /dev/ttyGS* 2>/dev/null`
* **macOS:** `ls /dev/cu.*`
* **Windows (cmd):** `mode` — or open Device Manager → *Ports
  (COM & LPT)*.

## Run a Linux ↔ Windows session

**On the Linux side (the `listen` peer):**

```
usb-link-probe listen --device /dev/ttyACM0
```

It prints its address, e.g.:

```
local address: akka.serial://A@/dev/ttyACM0:0
peer address:  (waiting for incoming Ping to learn it)
(type lines, Ctrl-C to exit)
```

**On the Windows side (the `connect` peer):**

```
usb-link-probe.exe connect --device COM3 --peer akka.serial://A@/dev/ttyACM0:0
```

Substitute the address the listen side actually printed. Within a
second or two you should see the `connect` side start ticking pings
and the `listen` side learn the peer address. Type a line on either
end:

```
> hello from windows
[out] hello from windows
```

…and on the Linux side:

```
[in]  hello from windows
```

Then the listen side starts pinging too, and both sides print a
stats overlay every five seconds:

```
stats: sent=42 recvd=41 loss=2.4% p50=2.1ms p95=8.1ms p99=12.4ms
```

To swap roles (Windows listening, Linux connecting), just swap the
subcommand on each side and update `--peer`.

## Reading the stats overlay

| Field    | Meaning |
|---------:|---------|
| `sent`   | Lifetime ping count this side has emitted. |
| `recvd`  | Lifetime pong count this side has matched against an outstanding ping. |
| `loss`   | `(sent - recvd) / sent` over the lifetime — useful at-a-glance, but slow to recover from a brief outage. |
| `p50/p95/p99` | Nearest-rank percentiles over the **last 1024 RTT samples**. The window slides, so a settled link converges quickly even after a hiccup. |

What the numbers should look like over a healthy USB-CDC link: loss%
at 0.0, p50 sub-5ms, p99 under 20ms. Persistent non-zero loss means
the cable, driver, or baud is misconfigured; a creeping p99 means the
peer is backpressuring (often: the operator is hammering stdin faster
than the link can drain).

## Useful flags

| Flag | Default | What it controls |
|---|---|---|
| `--baud <N>` | `115200` | Ignored for true USB-CDC; relevant for USB-to-serial dongles. |
| `--system <NAME>` | `A` (listen) / `B` (connect) | Address prefix; show up in `akka.serial://<NAME>@…`. Pick distinct names if you're running both sides on the same host. |
| `--ping-interval <DUR>` | `1s` | How often to emit a Ping. Use `100ms` to stress the link. |
| `--stats-interval <DUR>` | `5s` | How often to print the overlay. Use `1s` for tighter feedback. |

Durations accept `ms` / `s` / `m` units.

## Troubleshooting

For OS-level issues (driver missing, `Permission denied`, `Access is
denied`, ModemManager fighting you for the port, COM-port
re-numbering, USB autosuspend) see **[`SETUP.md`](SETUP.md)** —
each per-OS section ends with a troubleshooting table.

Demo-specific symptoms:

* **No traffic in either direction, but no errors either** — baud
  mismatch on USB-to-serial dongles. Force the same `--baud` on both
  sides. True USB-CDC ignores the baud setting, but most dongles
  enforce it.
* **`peer not associated yet, dropping line`** — only the `listen`
  side prints this, and only before the first incoming Ping. If it
  persists, the `connect` side isn't reaching you — check the
  `--peer` address matches what `listen` printed *exactly* (the
  device path on the listen side is part of the address).
* **`actor_selection returned None`** — the `--peer` address is
  malformed. The expected shape is `akka.serial://<system>@<device>:0`.

## Tests

```
cargo test -p example-usb-link-probe
```

Runs the in-memory protocol test (two `RemoteSystem`s wired through a
`tokio::io::duplex` pair) and the stats unit tests. No real serial
device required, so this works in CI on every platform.

The cross-OS hardware verification is operator-run by design — there
is no GitHub-hosted Windows runner with a USB peer to talk to.
