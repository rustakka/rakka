# Remoting

`atomr-remote` lets two `ActorSystem`s on different processes (or
machines) exchange messages. It covers:

- length-prefixed binary framing (the framed-PDU codec)
- handshake / heartbeat / ack / disassociate PDUs (the protocol
  transport)
- Tokio TCP transport, in-memory `TestTransport`, throttle &
  failure-injector adapters
- per-association `Endpoint` reader/writer with sliding-window ack'd
  delivery
- `EndpointManager` state machine
  (Idle → Pending → Connected → Quarantined → Tombstoned)
- pluggable `SerializerRegistry` (bincode default, JSON optional,
  per-type registration)
- `RemoteActorRefProvider` that plugs into `ActorSystem.actor_selection`
- `RemoteWatcher` for cross-process death watch
- `RemoteSystemDaemon` + `RemoteDeployer` for inbound dispatch and
  remote actor creation
- per-`Address` failure detector registry and metrics extension
- `RemoteRouterConfig` for distributing routees across remote nodes

## Quick start

```rust,no_run
use atomr_core::prelude::*;
use atomr_remote::{RemoteSettings, RemoteSystem};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Greeting { Hello(String) }

struct Greeter;

#[async_trait]
impl Actor for Greeter {
    type Msg = Greeting;
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: Greeting) {
        match msg { Greeting::Hello(name) => println!("hi, {name}!") }
    }
}

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
// On node A:
let sys_a = ActorSystem::create("A", atomr_config::Config::reference()).await?;
let remote_a = RemoteSystem::start(
    sys_a.clone(),
    "127.0.0.1:7000".parse()?,
    RemoteSettings::default(),
).await?;
remote_a.register_bincode::<Greeting>();
let greeter = sys_a.actor_of(Props::create(|| Greeter), "greeter")?;
remote_a.expose_actor(greeter);

// On node B (could be in a different process / machine):
let sys_b = ActorSystem::create("B", atomr_config::Config::reference()).await?;
let remote_b = RemoteSystem::start(
    sys_b.clone(),
    "127.0.0.1:7001".parse()?,
    RemoteSettings::default(),
).await?;
remote_b.register_bincode::<Greeting>();

let greeter_remote: ActorRef<Greeting> = remote_b
    .actor_selection::<Greeting>("atomr.tcp://A@127.0.0.1:7000/user/greeter")
    .expect("remote selection");
greeter_remote.tell(Greeting::Hello("world".into()));
# Ok(()) }
```

## Required steps

1. **Bind a `RemoteSystem`** on every node that should be reachable.
2. **Register a codec for every message type** that crosses the wire on
   *both* sides: `remote.register_bincode::<MyMsg>()` (or
   `register_json::<MyMsg>()`). Receiving side drops envelopes whose
   manifest is unknown — this is intentional, surfaced in the logs.
3. **`expose_actor`** any local actor that should be addressable from
   peers. The actor's path becomes its remote address: an actor
   registered as `actor_of(props, "echo")` is reachable at
   `atomr.tcp://Sys@host:port/user/echo`.
4. **`actor_selection::<M>(path)`** to obtain a typed `ActorRef<M>` on
   the calling side. The returned ref has all the regular
   `tell`/`ask_with` ergonomics; messages are serialized and shipped via
   the underlying `EndpointManager`.

## Architecture

```
                         +-----------------------+
            sys.tell --> |  ActorRef<M>          |
                         +-----------+-----------+
                                     |
                       (Local)       |       (Remote)
                                     |
                         +-----------v-----------+
                         |  RemoteActorRefImpl   |
                         |  serialize via codec  |
                         +-----------+-----------+
                                     |
                         +-----------v-----------+
                         |  EndpointManager      |
                         |  - assoc state SM     |
                         |  - dispatch inbound   |
                         |  - failure detectors  |
                         +-----------+-----------+
                                     |
                         +-----------v-----------+
                         |  Endpoint(reader,     |
                         |          writer)      |
                         |  - heartbeat ticks    |
                         |  - ack window         |
                         |  - resend buffer      |
                         +-----------+-----------+
                                     |
                         +-----------v-----------+
                         |  ProtocolTransport    |
                         |  - Associate handshake|
                         |  - Heartbeat / Ack    |
                         |  - Disassociate       |
                         +-----------+-----------+
                                     |
                         +-----------v-----------+
                         |  Transport (TCP /     |
                         |  Test / Throttle /    |
                         |  FailureInjector)     |
                         +-----------------------+
```

## Settings

`RemoteSettings::default()` is conservative and suitable for development.
Production deployments should tune (at minimum):

| field | default | notes |
|---|---|---|
| `max_frame_size` | 4 MiB | upper bound on a single PDU |
| `heartbeat_interval` | 1 s | writer emits `Heartbeat` when idle |
| `heartbeat_timeout` | 10 s | failure detector trip threshold |
| `handshake_timeout` | 15 s | waiting for `Associate` reply |
| `quarantine_duration` | 60 s | quarantine window after UID mismatch |
| `ack_window` | 1 000 | per-endpoint sliding window size |
| `require_cookie` | None | optional handshake cookie |

## Adapters

`ThrottleTransport`, `FailureInjectorTransport`, and `TestTransport`
wrap any other `Transport` and are useful for tests:

```rust,ignore
let raw: Arc<dyn Transport> = Arc::new(TcpTransport::new("A", "127.0.0.1:0".parse()?));
let throttled = ThrottleTransport::new(raw.clone(), ThrottleMode::Latency(Duration::from_millis(50)));
let dropping = FailureInjectorTransport::new(throttled, InjectionMode::DropEvery(3));
let remote = RemoteSystem::start_with_transport(sys, dropping, RemoteSettings::default()).await?;
```

## USB cable mode

Two hosts physically connected by a USB cable can exchange actor
messages without going over the network. There are two ways to do
this; both avoid IP egress, both ship with `atomr` today.

### Tier 1 — CDC-NCM gadget (zero new code)

If at least one side is running Linux with USB gadget capability
(typical: Raspberry Pi Zero W, BeagleBone, any board with USB OTG;
also full PCs in DRP role), bring up a CDC-NCM gadget so the cable
appears as a USB-Ethernet adapter on both sides:

```bash
# Linux gadget side. Use the `usb-gadget` crate or the kernel's
# configfs interface to register a CDC-NCM function. Then bring up
# the resulting `usb0` interface with a link-local IPv4:
ip addr add 169.254.0.1/16 dev usb0
ip link set usb0 up

# Linux/macOS/Windows host side: kernel auto-detects the netif
# (usb0 / enX0 / Ethernet 4). Assign a peer IP:
ip addr add 169.254.0.2/16 dev usb0
ip link set usb0 up
```

Now `RemoteSystem::start(sys, "169.254.0.1:2552".parse()?, settings)`
on one side and `RemoteSystem::start(sys, "169.254.0.2:2552".parse()?, settings)`
on the other. The existing `TcpTransport` works unchanged. Highest
leverage if you can run a Linux kernel on at least one side.

### Tier 2 — `SerialTransport`

When TCP-over-USB-Ethernet isn't an option (no Linux on either side
running gadget mode, or you want to skip the IP layer entirely),
`atomr-remote-serial` provides a `Transport` over a USB CDC-ACM
serial endpoint (`/dev/ttyACM0` on Linux/macOS hosts, `COMx` on
Windows, `/dev/ttyGS0` on the Linux gadget side, or
`/dev/cu.usbmodemXXXX` on macOS):

```rust,ignore
use std::sync::Arc;
use atomr_remote::{RemoteSettings, RemoteSystem};
use atomr_remote_serial::SerialTransport;

let transport = Arc::new(SerialTransport::new("SystemA", "/dev/ttyACM0"));
let remote = RemoteSystem::start_with_transport(sys, transport, RemoteSettings::default()).await?;
```

Each side configures its own local device path. The transport
auto-reconnects on cable wiggles or gadget reboots via
`ReconnectPolicy` (50ms→5s exponential backoff by default).

`SerialTransport::with_streams(name, reader, writer, max_frame)` is
the same transport over caller-supplied byte halves — useful for
testing with `tokio::io::duplex`, or for layering the Akka protocol
over a Unix socket, an SSH-tunneled stream, or any other byte pipe.

A worked example with both sides ships in
`examples/usb-cable-link/`. Run `cable-side-a /dev/ttyACM0` on one
machine, then `cable-side-b /dev/ttyACM0 'akka.serial://A@/dev/ttyGS0:0'`
on the other.

For a richer cross-OS diagnostic — clap CLI, `list-devices`
discovery, bidirectional chat, and a rolling sent / loss% / RTT
overlay — see `examples/usb-link-probe/` (its README walks through a
Linux ↔ Windows session end to end).

### Choosing between Tier 1 and Tier 2

| Concern                     | Tier 1 (CDC-NCM + TCP)    | Tier 2 (`SerialTransport`)        |
|-----------------------------|---------------------------|-----------------------------------|
| New code                    | None                      | One extra crate dep               |
| OS support                  | Linux/macOS/Windows       | Linux/macOS/Windows               |
| Requires gadget mode        | Yes (one side Linux)      | Yes (one side Linux for ACM)      |
| IP routing / DHCP / MTU     | You configure             | None — bytes only                 |
| Auto-reconnect              | Via `EndpointManager`     | Inside the transport (sub-second) |
| Per-frame overhead          | TCP + framing + bincode   | bincode + 4-byte length prefix    |
| Multiple peers per side     | Yes (one IP per peer)     | One peer per device path          |

Pick Tier 1 when both sides already speak IP cleanly. Pick Tier 2
when you want a smaller surface (no kernel-level netif setup, no
`ip addr add` dance, no firewall implications) or when one side
exposes only a serial endpoint (an embedded board, an
`embassy-usb` device, etc.).

## Cluster integration

`atomr-cluster` ships a `ClusterRemoteAdapter` that bootstraps a
`RemoteSystem`, exposes a local `cluster` actor for inbound `Gossip`
messages, and provides `send_gossip(peer)` for outbound dissemination.
See `crates/atomr-cluster/src/remote_adapter.rs`.

## Sharding integration

`ShardRegion::set_remote_forwarder` lets the sharding region ship
messages to remote shard owners via `RemoteSystem::actor_selection`.

## Caveats / non-goals

- **Wire compatibility with other actor runtimes is not a goal.**
  The PDU encoding is bincode-framed; clusters from other runtimes
  cannot join an `atomr` cluster.
- **Remote `Props` deployment** ships a `(manifest, bytes)` create
  request rather than a fully-typed `Props`. The remote daemon must have
  a route registered for that manifest.
- **TLS** is not yet wired in. Bind `TcpTransport` behind an existing
  TLS-terminating proxy if you need encryption today; first-class TLS
  support is on the roadmap.
