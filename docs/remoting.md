# Remoting

`rakka-remote` lets two `ActorSystem`s on different processes (or
machines) exchange messages. It covers:

- length-prefixed binary framing (`AkkaPdu`)
- handshake / heartbeat / ack / disassociate PDUs
  (`AkkaProtocolTransport`)
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
use rakka_core::prelude::*;
use rakka_remote::{RemoteSettings, RemoteSystem};
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
let sys_a = ActorSystem::create("A", rakka_config::Config::reference()).await?;
let remote_a = RemoteSystem::start(
    sys_a.clone(),
    "127.0.0.1:7000".parse()?,
    RemoteSettings::default(),
).await?;
remote_a.register_bincode::<Greeting>();
let greeter = sys_a.actor_of(Props::create(|| Greeter), "greeter")?;
remote_a.expose_actor(greeter);

// On node B (could be in a different process / machine):
let sys_b = ActorSystem::create("B", rakka_config::Config::reference()).await?;
let remote_b = RemoteSystem::start(
    sys_b.clone(),
    "127.0.0.1:7001".parse()?,
    RemoteSettings::default(),
).await?;
remote_b.register_bincode::<Greeting>();

let greeter_remote: ActorRef<Greeting> = remote_b
    .actor_selection::<Greeting>("akka.tcp://A@127.0.0.1:7000/user/greeter")
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
   `akka.tcp://Sys@host:port/user/echo`.
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
                         |  AkkaProtocolTransport|
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

## Cluster integration

`rakka-cluster` ships a `ClusterRemoteAdapter` that bootstraps a
`RemoteSystem`, exposes a local `cluster` actor for inbound `Gossip`
messages, and provides `send_gossip(peer)` for outbound dissemination.
See `crates/rakka-cluster/src/remote_adapter.rs`.

## Sharding integration

`ShardRegion::set_remote_forwarder` lets the sharding region ship
messages to remote shard owners via `RemoteSystem::actor_selection`.

## Caveats / non-goals

- **Wire compatibility with JVM/CLR Akka is not a goal.** The PDU
  encoding is bincode, not Akka's protobuf. JVM and CLR clusters cannot
  join a `rakka` cluster.
- **Remote `Props` deployment** ships a `(manifest, bytes)` create
  request rather than a fully-typed `Props`. The remote daemon must have
  a route registered for that manifest.
- **TLS** is not yet wired in. Bind `TcpTransport` behind an existing
  TLS-terminating proxy if you need encryption today; first-class TLS
  support is on the roadmap.
