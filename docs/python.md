# atomr Python bindings

`atomr` ships first-class Python bindings that let you author actors in
Python while keeping the Rust scheduler, mailbox, supervision,
clustering, persistence, distributed data, and streams machinery below.
The native extension is built with [PyO3] + [maturin]; the Python
facade lives in `python/atomr/`. Tests: 270 passing.

## Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install maturin pytest pytest-asyncio msgpack
maturin develop --release
```

Supported Python: 3.10+ (abi3). 3.12 enables subinterpreters; 3.13t
(PEP 703 free-threaded) enables the `python-nogil` dispatcher.

## Hello, actor

```python
from atomr import Actor, ActorSystem, props

class Greeter(Actor):
    async def handle(self, ctx, msg):
        return f"hello, {msg}"

system = ActorSystem.create_blocking("app")
ref = system.actor_of(props(Greeter), "greeter")
print(ref.ask_blocking("world", timeout=5.0))   # -> "hello, world"
system.terminate_blocking()
```

## Module layout

The Python package mirrors the Rust workspace:

| Rust crate                     | Python module                |
|--------------------------------|------------------------------|
| `atomr-core`                | `atomr` (Actor, Props, Context, SupervisorStrategy, …) |
| `atomr-testkit`             | `atomr.testkit`           |
| `atomr-cluster`             | `atomr.cluster`           |
| `atomr-cluster-tools`       | `atomr.cluster_tools`     |
| `atomr-cluster-sharding`    | `atomr.cluster_sharding`  |
| `atomr-cluster-metrics`     | `atomr.cluster_metrics`   |
| `atomr-distributed-data`    | `atomr.ddata`             |
| `atomr-distributed-data-lmdb` | `atomr.ddata_lmdb`      |
| `atomr-persistence`         | `atomr.persistence`       |
| `atomr-streams`             | `atomr.streams`           |
| `atomr-coordination`        | `atomr.coordination`      |
| `atomr-discovery`           | `atomr.discovery`         |
| `atomr-di`                  | `atomr.di`                |
| `atomr-hosting`             | `atomr.hosting`           |
| `atomr-telemetry`           | `atomr.telemetry`         |
|  *(patterns, routers)*      | `atomr.pattern`, `atomr.routing` |

## Actor lifecycle — full Context API

`handle(ctx, msg)` receives a real `Context` object that mirrors the
Rust `Context<A>`. Mutations are queued via an op channel and applied
at end-of-handler against the live actor cell — same Akka semantics.

```python
class Worker(Actor):
    async def pre_start(self, ctx): ...
    async def handle(self, ctx, msg):
        # Spawn a child:
        child = ctx.spawn(props(Helper), "helper")
        # Address introspection:
        ctx.path                     # "akka://app/user/worker"
        ctx.self_ref                 # ActorRef pointing at self
        ctx.sender                   # Optional[ActorRef] of the sender
        # Death-watch:
        ctx.watch(child)             # delivers atomr.Terminated(path) on stop
        ctx.unwatch(child)
        # Stash / unstash:
        ctx.stash(msg)
        ctx.unstash_all()
        # Become / unbecome (behavior switch):
        ctx.become(other_handler)
        ctx.unbecome()
        # Timers — return a Cancelable:
        token = ctx.schedule_once(0.5, "tick")
        ctx.schedule_periodically(initial=1.0, interval=0.1, msg="poll")
        ctx.schedule_with_fixed_delay(initial=1.0, delay=0.1, msg="beat")
        token.cancel()               # idempotent
        # Lifecycle control:
        ctx.set_receive_timeout(5.0) # delivers ReceiveTimeout
        ctx.stop_self()
        ctx.stop_child("helper")
```

`ActorRef` itself exposes `tell`, `tell_with_sender`, `tell_with_key`,
`ask` (asyncio), `ask_blocking`, `stop`, `is_terminated`, and `path`.

## Supervision

`Props` accepts a configurable strategy. Deciders compile to a Rust
closure that matches the panicking exception's class path — supervision
runs without the GIL.

```python
from atomr import SupervisorStrategy, Directive

strategy = SupervisorStrategy.one_for_one(
    decider=[
        ("builtins.ValueError", "restart"),
        ("builtins.RuntimeError", "stop"),
    ],
    default="restart",
    max_retries=5,
    within_seconds=30.0,
)
ref = system.actor_of(
    props(MyActor).with_supervisor_strategy(strategy),
    "my",
)

# Or the convenience shortcut for retry budget only:
ref = system.actor_of(
    props(MyActor).with_supervisor_budget(max_retries=2, within_seconds=10.0),
    "my",
)
```

`max_retries` / `within_seconds` are enforced by `actor_cell` — once
the budget is exhausted the actor escalates per the strategy
(typically stops). `Directive` constants: `RESUME`, `RESTART`, `STOP`,
`ESCALATE`. `atomr.Terminated(path)` is delivered to watchers when a
watched actor stops.

## Patterns and routers

```python
from atomr import props
from atomr.pattern import CircuitBreaker, RetrySchedule, retry, pipe_to
from atomr.routing import (
    broadcast, round_robin, random, consistent_hash,
    smallest_mailbox, tail_chopping, scatter_gather, backoff,
)

# Routers as Props factories — routing in Rust, no GIL on the hop:
pool = round_robin(props(Worker), n=4)
broad = broadcast(props(Listener), n=3)
hashed = consistent_hash(props(Entity), n=8)
small  = smallest_mailbox(props(Worker), n=4)
chop   = tail_chopping(props(Slow), n=3, interval_secs=0.05, within_secs=1.0)
sg     = scatter_gather(props(Service), n=4, within_secs=0.5)

# Backoff supervisor — one child, exponential restart:
sup = backoff(props(Flaky), min_backoff=0.1, max_backoff=10.0, random_factor=0.2)

# Resilience patterns:
cb = CircuitBreaker(max_failures=5, call_timeout=1.0, reset_timeout=10.0)
result = await cb.call_async(coro)        # opens after 5 consecutive failures

schedule = RetrySchedule.exponential(0.1, 5.0)
result = await retry(coro_factory, max_attempts=4, schedule=schedule)

await pipe_to(future, target_ref)         # send the future's result to target

# Consistent-hash routing requires explicit keys:
hashed.tell_with_key(msg, key=hash(msg.entity_id))
```

## Distributed actors — clustering and remoting

Two `ActorSystem`s on different processes (or in-process for tests)
form a cluster, exchange messages over a real transport, and observe
membership events through an async iterator.

```python
from atomr import ActorSystem
from atomr.cluster import Cluster, ClusterRegistry

# Real TCP loopback transport — auto-allocate ports.
sys_a = ActorSystem.create_blocking("A")
ca = Cluster.with_tcp_transport(sys_a, "127.0.0.1:0")
print(ca.self_address)         # "akka.tcp://A@127.0.0.1:42573"

sys_b = ActorSystem.create_blocking("B")
cb = Cluster.with_tcp_transport(sys_b, "127.0.0.1:0")

# Or in-process channel transport for deterministic tests:
registry = ClusterRegistry()
Cluster.with_test_transport(sys_x, registry)
Cluster.with_test_transport(sys_y, registry)

# Observe membership events:
sub = ca.subscribe(["MemberUp", "MemberRemoved", "MemberDowned"])
async for event in sub:
    ...

# Control plane:
await ca.join_seed_nodes([cb.self_address], timeout=10.0)
await ca.leave(timeout=10.0)
ca.down(other_addr)               # transitions remote member to Down
ca.member_count()
ca.membership_snapshot()
```

### SBR (split-brain resolver) configuration

Pass via `Config`:

```toml
[cluster.sbr]
strategy = "keep-majority"   # keep-majority | static-quorum
                             # | keep-oldest | down-all | lease-majority
quorum-size = 3              # for static-quorum
down-if-alone = true         # for keep-oldest
stable-after = 20            # seconds
```

### Wire-level remote tell

Codecs are registered per system; manifests are
`module.qualname` strings derived from the message class.

```python
import json

class Greeting:
    def __init__(self, text): self.text = text
    def to_dict(self): return {"text": self.text}
    @classmethod
    def from_dict(cls, d): return cls(text=d["text"])

sys_a.register_codec(
    "json",
    encoder=lambda obj: json.dumps(obj.to_dict()).encode(),
    decoder=lambda blob: Greeting.from_dict(json.loads(blob)),
    manifests=["myapp.Greeting"],
)

# Send to a remote actor through the configured transport:
sys_a.tell_remote(remote_ref, Greeting("hi"))
```

`register_codec(..., force=True)` overrides existing registrations.
`register_codec(..., strict=False)` skips importlib validation for
`__main__`-scoped or inline message classes (warns at registration).

`system.use_json_codec(default=True)` installs a catch-all JSON codec
for any unmatched manifest.

## Cluster sharding

A `ShardRegion` routes messages to entity actors by `(entity_id,
shard_id)`. Entities are spawned lazily, may be passivated when idle,
and survive region restarts via `RememberEntities`.

```python
from atomr.cluster_sharding import ShardRegion, ShardingSettings

def extractor(msg):
    eid = str(msg["entity"])
    return (eid, str(hash(eid) % 16), msg)

region = ShardRegion.start(
    system,
    type_name="counters",
    entity_props=props(CounterEntity),
    message_extractor=extractor,
    settings=ShardingSettings(
        allocation_strategy="least-shards",   # or "pinned"
        rebalance_threshold=1,
        max_simultaneous_rebalance=3,
        passivation_idle_timeout=30.0,
        remember_entities=True,
        number_of_shards=16,
    ),
)
region.tell({"entity": "alice", "op": "incr"})
region.request_passivation("alice")
region.entity_count()
```

For multi-node sharding, give each node its own region with the same
`type_name` and have all nodes join the same cluster transport.
Sharded entities should typically be event-sourced so their state
survives migration (see Persistence below).

## Event-sourced actors

```python
from atomr.persistence import EventSourcedActor, Effect, InMemoryJournal

class Counter(EventSourcedActor):
    persistent_id = "counter-1"

    def initial_state(self):
        return {"count": 0}

    async def command_handler(self, state, ctx, cmd):
        if cmd["op"] == "incr":
            return [Effect.persist({"type": "Incremented", "by": cmd["by"]})]
        if cmd["op"] == "snap":
            return [Effect.snapshot()]
        if cmd["op"] == "get":
            return [Effect.reply_message(state["count"])]
        return []

    def event_handler(self, state, event, recovery_mode=False):
        if event["type"] == "Incremented":
            state["count"] += event["by"]
        return state

# Register the dict-event JSON codec once per system:
system.use_json_codec(default=True)

journal = InMemoryJournal()
ref = system.actor_of(
    props(Counter, factory=lambda: Counter(journal=journal)),
    "counter",
)
```

`Effect` constructors:

- `Effect.persist(event)` — append to journal then run `event_handler`.
- `Effect.persist_all([events])` — atomic batch.
- `Effect.snapshot(every=None)` — flush via `AsyncSnapshotter`; with
  `every=N` snapshots after every N persisted events.
- `Effect.reply_message(value)` — send `value` back to the asker.
  Read via `effect.value`.
- `Effect.stop()` — terminate after applying queued effects.
- `Effect.none()` — explicit no-op.

Recovery runs in `pre_start`: load latest snapshot, replay events
through `event_handler(state, event, recovery_mode=True)`. The
`recovery_mode` flag lets you suppress side effects during replay.

## Distributed data — CRDTs and Replicator

```python
from atomr.ddata import (
    GCounter, PNCounter, GSet, ORSet, LwwRegister, Flag,
    ORMap, LWWMap, PNCounterMap, ORMultiMap,
    Replicator, ReadConsistency, WriteConsistency,
    DurableStore,
)

# Local CRDTs:
c = GCounter()
c.increment("nodeA", 1)
c.merge(other_replica)          # convergent

# Replicator-coordinated, async:
rep = Replicator.get(system)
ack = await rep.update(
    "shared-counter",
    GCounter,
    lambda c: (c.increment("A", 1) or c),
    WriteConsistency.majority(timeout=2.0),
)
val = await rep.get_value("shared-counter", GCounter)

# Subscribe to changes:
sub = rep.subscribe("shared-counter")
async for key in sub:
    ...

# ORMap of nested CRDTs (factory selects the value type):
m = ORMap.of_pn_counter()
m.put("alice", PNCounter())
```

Consistency variants: `ReadConsistency.{local, majority(t), all(t),
read_from(n, t)}` and `WriteConsistency.{local, majority(t), all(t),
write_to(n, t)}`.

Durable backing:

```toml
[distributed-data.durable]
store-actor-class = "noop" | "file" | "lmdb"
path = "/var/atomr/dd"
```

Or programmatically: `Replicator.with_durable_store(system,
DurableStore.file("/path"))`.

## Streams DSL

Typed reactive streams over arbitrary Python objects. Callbacks acquire
the GIL inline; heavy work belongs in `Flow.map_async`. Element drops
are GIL-safe via the `SendPyAny` newtype.

```python
from atomr.streams import (
    Source, Flow, Sink, RunnableGraph, KillSwitch,
    BroadcastHub, MergeHub, SourceQueue, SinkQueue,
    GraphDsl, BidiFlow, Framing, Tcp, FileIO,
    RestartSource, RestartSettings,
)

# Linear pipeline:
total = await (
    Source.from_iter([1, 2, 3, 4])
        .via(Flow.map(lambda x: x * 2))
        .via(Flow.filter(lambda x: x > 2))
        .to(Sink.fold(0, lambda a, b: a + b))
        .run()
)

# Async map with parallelism:
flow = Flow.map_async(coro_fn, parallelism=8)

# Supervision (compiled decider, no GIL on the hop):
flow = Flow.try_map(risky_fn).with_supervision(
    [("builtins.ValueError", "resume")], default="stop",
)

# Restartable source:
restartable = RestartSource(min_backoff=0.1, max_backoff=5.0,
                            random_factor=0.2, max_restarts=5)
src = restartable.via_source(lambda: Source.from_iter(range(10)))

# Hubs:
hub = BroadcastHub.attach(source, buffer_size=16)
consumer1 = hub.consumer()
consumer2 = hub.consumer()

# IO adapters (bytes streams):
tcp = Tcp.outgoing("127.0.0.1", 9000).via(
    Framing.delimiter(b"\n", max_frame_length=1024)
)
sink = Sink.fold(b"", lambda a, b: a + b)

# Kill switch:
src, ks = source.kill_switch()
ks.shutdown()

# Graph DSL (linear builder):
g = GraphDsl()
a = g.add(source); b = g.add(flow); c = g.add(sink)
g.edge(a, b); g.edge(b, c)
g.run_blocking()
```

## Testing

```python
from atomr.testkit import testkit, TestKit, TestProbe, TestScheduler, fish_for_message, EventFilter

# pytest fixture:
def test_my_actor(testkit):
    probe = testkit.probe()
    ref.tell({"hi": probe.ref_})
    msg = await probe.expect_message(timeout=1.0)
    assert msg == "expected"

# Virtual time:
sched = TestScheduler()
token = sched.schedule_after(0.5, "tick")
await sched.advance(0.5)               # token fires deterministically

# Predicate matching:
msg = await fish_for_message(probe, lambda m: m.startswith("ok"), timeout=1.0)

# Event-stream traps:
filt = EventFilter(probe, cls_path="myapp.Heartbeat", message_regex=r"ping")
await filt.await_count(3, timeout=2.0)
```

## GIL tuning guide

The framework offers four dispatcher shapes. Pick one per workload.

### `python-pinned` (default)

One interpreter, one OS thread, one GIL. Best latency for small actor
graphs where handlers are short and the bulk of the work is I/O or
delegated to Rust.

```python
system.configure_interpreter("default", "python-pinned")
```

### `python-subinterpreter-pool` (recommended for CPU-bound)

N subinterpreters on N OS threads. Each interpreter has its own GIL, so
CPU-bound Python handlers actually run in parallel (assuming the C
extensions you import are subinterpreter-safe; see the compatibility
registry below).

```python
from atomr import InterpreterQuota

system.configure_interpreter(
    "ml-inference",
    "python-subinterpreter-pool",
    count=4,
    quota=InterpreterQuota(
        max_actors=32,
        max_handler_ms=250,
        memory_soft_limit_bytes=2 * 1024**3,
        module_allowlist=["numpy", "torch", "atomr"],
        import_policy="eager",
    ),
)
```

### `python-nogil`

Free-threaded CPython 3.13+ (PEP 703). Single interpreter, but no GIL;
`count` becomes the number of OS worker threads. Only useful if your
deployment runs a no-GIL CPython build — check with
`atomr.nogil_supported()`.

### `python-subprocess`

Each interpreter runs in a separate OS process. Strongest isolation —
used for untrusted handlers or hard memory caps.

### Quotas

`InterpreterQuota` exposes the same knobs on every dispatcher:

| knob                       | purpose                                   |
|----------------------------|-------------------------------------------|
| `max_actors`               | reject new spawns when the pool is full   |
| `max_mailbox_total`        | back-pressure: reject `tell` past budget  |
| `memory_soft_limit_bytes`  | log/restart when RSS exceeds the budget   |
| `cpu_share`                | advisory scheduler hint                   |
| `max_handler_ms`           | flag long-running handlers in metrics     |
| `module_allowlist/denylist`| enforced by the C-ext compat gate at boot |
| `import_policy`            | `lazy` (default) or `eager` warm-up       |

### Metrics

```python
for pool in atomr._native.interpreter_metrics():
    print(pool["label"], pool["kind"], pool["messages_handled"])
```

Fields: `actors_hosted`, `messages_handled`, `gil_hold_ns_total`,
`mailbox_depth_total`, `handler_panics`, `long_handlers`.

### C-extension compatibility registry

Before spawning an interpreter pool we consult the compatibility
registry. Defaults ship for stdlib, `numpy`, `msgpack`, `pydantic`, etc.
Operators or library authors can declare their own:

```python
import atomr

atomr.declare_compat(
    "my_fast_lib",
    subinterpreter_safe=True,
    nogil_safe=False,
    notes="verified against release 1.4",
)
```

Handlers that try to import a module flagged as unsafe for the
selected dispatcher raise `atomr.InterpreterCompatError` — see
`atomr.compat_list()` for the current registry contents.

## Hosting

```python
from atomr.hosting import Builder

system = (
    Builder()
        .with_config(Config.from_toml("config.toml"))
        .configure_interpreter("ml", "python-subinterpreter-pool", count=4)
        .on_start(lambda sys: setup_actors(sys))
        .build()
)
```

## Examples

- `python/examples/pingpong.py` — smoke test + throughput.
- `python/examples/ml_inference.py` — subinterpreter pool.
- `python/examples/persistence_counter.py` — Rust journal from Python.

## Profiling

A cross-runtime profiler runs the same scenarios in Rust and Python,
emitting a shared JSON schema:

```bash
python -m atomr.profiler --scenario all --format md
python -m atomr.profiler --scenario cpu --messages 5000 --format json -o cpu.json
```

For a side-by-side Rust + Python table, run `python scripts/profile.py`
from the repo root. Full guide: [`profiler.md`](profiler.md).

## API surface summary

```python
# Core
atomr.Actor                           # subclass; implement async def handle(ctx, msg)
atomr.ActorSystem                     # .create / .create_blocking / .actor_of /
                                       # .terminate / .when_terminated /
                                       # .codecs / .register_codec / .use_json_codec /
                                       # .tell_remote / .codec_roundtrip
atomr.Props, atomr.props()            # (factory, dispatcher, interpreter_role, mailbox)
                                       # .with_supervisor_strategy / .with_supervisor_budget /
                                       # .with_dispatcher / .with_interpreter_role / .with_mailbox
atomr.ActorRef                        # .tell / .tell_with_sender / .tell_with_key /
                                       # .ask / .ask_blocking / .stop / .is_terminated / .path
atomr.Context                         # .self_ref / .path / .sender / .spawn / .stop_child /
                                       # .watch / .unwatch / .stash / .unstash_all /
                                       # .stop_self / .set_receive_timeout /
                                       # .schedule_once / .schedule_periodically /
                                       # .schedule_with_fixed_delay / .become / .unbecome
atomr.SupervisorStrategy              # .one_for_one / .all_for_one / .stopping / .escalating
atomr.Directive                       # RESUME / RESTART / STOP / ESCALATE
atomr.Terminated                      # delivered to watchers when watched actor stops
atomr.Config                          # .from_toml / .from_dict / .empty / .extract
atomr.Cancelable                      # .cancel / .is_cancelled

# Interpreter
atomr.InterpreterQuota
atomr.subinterpreters_supported() / nogil_supported()
atomr.declare_compat / compat_flags / compat_list

# Resilience patterns + routers
atomr.pattern.CircuitBreaker / CircuitBreakerOpen / RetrySchedule / retry / pipe_to
atomr.routing.broadcast / round_robin / random / consistent_hash /
              smallest_mailbox / tail_chopping / scatter_gather / backoff

# Test kit
atomr.testkit.TestKit / TestProbe / testkit (pytest fixture) /
              MultiNodeOopController / MultiNodeOopNode / within /
              TestScheduler / fish_for_message / EventFilter

# Cluster
atomr.cluster.Cluster                 # .with_tcp_transport / .with_test_transport / .get
                                       # .self_address / .join_seed_nodes / .leave / .down /
                                       # .subscribe / .member_count / .membership_snapshot / .leader
atomr.cluster.ClusterRegistry         # in-process transport bus
atomr.cluster.Member / MembershipState / VectorClock / LeaderHandover /
              MemberUp / MemberDowned / MemberRemoved / UnreachableMember / LeaderChanged

# Cluster sharding
atomr.cluster_sharding.ShardRegion / ShardingSettings

# Cluster tools / metrics
atomr.cluster_tools.DistributedPubSub / ClusterSingletonManager / ClusterReceptionist
atomr.cluster_metrics.NodeMetrics / ClusterMetrics / Ewma / MetricsSelector /
              WeightedRoutees / AdaptiveLoadBalancer

# Distributed data
atomr.ddata.GCounter / PNCounter / GSet / ORSet / LwwRegister / Flag /
            ORMap / LWWMap / PNCounterMap / ORMultiMap /
            Replicator / ReadConsistency / WriteConsistency /
            DurableStore / PruningState / WriteAggregator / ReadAggregator
atomr.ddata_lmdb.RedbDurableStore

# Persistence
atomr.persistence.EventSourcedActor / Effect / InMemoryJournal /
                  InMemorySnapshotStore / RecoveryPermitter

# Streams
atomr.streams.Source / Flow / Sink / RunnableGraph / KillSwitch /
              BroadcastHub / MergeHub / SourceQueue / SinkQueue / QueueOfferResult /
              GraphDsl / BidiFlow / Framing / Tcp / FileIO /
              RestartSource / RestartSettings /
              # plus the i64 helpers from the original Phase 8 wave

# Other
atomr.coordination.InMemoryLease
atomr.discovery.StaticDiscovery / AggregateDiscovery
atomr.di.ServiceContainer
atomr.hosting.Builder / ActorSystemBuilder
atomr.telemetry.TelemetryBus / TopicSubscriber
atomr.core.DispatcherConfig / BoundedStash / ControlAwareQueue / ResizerConfig /
            DeadLetterFilter / OverflowStrategy / StashOverflow / FsmBuilder / Fsm
```

[PyO3]: https://pyo3.rs
[maturin]: https://www.maturin.rs
