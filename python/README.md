# atomr — Python bindings

First-class Python bindings for the Rust [`atomr`](../) actor
framework. Write actors in Python, run them under the Rust scheduler
with full supervision, clustering, sharding, persistence, distributed
data, streams, and remoting — and pick a dispatcher that matches your
workload's GIL tolerance.

## Install (development)

```bash
python -m venv .venv && source .venv/bin/activate
pip install maturin pytest pytest-asyncio msgpack
maturin develop --release
```

If your host lacks Python dev headers and you can't `sudo apt install
python3-dev`, use the bundled helper instead:

```bash
source scripts/dev-env.sh        # creates .venv, installs deps, exports PYO3_CONFIG_FILE
maturin develop --release
```

See `.cargo/pyo3-config.txt.example` for the custom PyO3 config
template. Nothing in `.venv/`, `.venv-build/`, or `.cargo/pyo3-config.txt`
is committed — each developer builds their own.

Supported Python: 3.10+ (abi3). 3.12 enables subinterpreters;
3.13 free-threaded (PEP 703) enables the `python-nogil` dispatcher.

## Hello, actor

```python
from atomr import Actor, ActorSystem, props

class Greeter(Actor):
    async def handle(self, ctx, msg):
        return f"hello, {msg}"

system = ActorSystem.create_blocking("app")
ref = system.actor_of(props(Greeter), "greeter")
print(ref.ask_blocking("world", timeout=5.0))
system.terminate_blocking()
```

`handle(ctx, msg)` receives a real `Context` — children, watch, stash,
timers, become, sender, `stop_self`, all available. See [the Python
guide](../docs/python.md) for the full surface.

## Package layout

```
python/
├── atomr/                Python facade — import this
│   ├── __init__.py             re-exports Actor / ActorSystem / SupervisorStrategy / …
│   ├── actor.py                Actor base class
│   ├── system.py               ActorSystem, Props, ActorRef, props()
│   ├── context.py              Context, Cancelable
│   ├── supervision.py          SupervisorStrategy, Directive, Terminated
│   ├── pattern.py              CircuitBreaker, RetrySchedule, retry, pipe_to
│   ├── routing.py              broadcast, round_robin, consistent_hash, …
│   ├── errors.py               AtomrError, InterpreterOverloaded, …
│   ├── interpreter.py          InterpreterQuota + capability probes
│   ├── compat.py               C-extension compatibility registry
│   ├── core.py                 DispatcherConfig, BoundedStash, FsmBuilder, …
│   ├── testkit.py              TestKit, TestProbe, TestScheduler, EventFilter,
│   │                           fish_for_message, MultiNodeOopController, pytest fixture
│   ├── cluster.py              Cluster (TCP + test transport), ClusterRegistry,
│   │                           Member, MembershipState, VectorClock,
│   │                           MemberUp/Downed/Removed event dataclasses
│   ├── cluster_tools.py        DistributedPubSub, ClusterSingletonManager
│   ├── cluster_sharding.py     ShardRegion (with passivation, remember-entities)
│   ├── cluster_metrics.py      NodeMetrics, AdaptiveLoadBalancer, MetricsSelector
│   ├── ddata.py                GCounter, PNCounter, GSet, ORSet, LwwRegister, Flag,
│   │                           ORMap, LWWMap, PNCounterMap, ORMultiMap,
│   │                           Replicator, Read/WriteConsistency, DurableStore
│   ├── ddata_lmdb.py           RedbDurableStore
│   ├── persistence.py          EventSourcedActor, Effect, InMemoryJournal,
│   │                           InMemorySnapshotStore, RecoveryPermitter
│   ├── streams.py              Source/Flow/Sink/RunnableGraph DSL, KillSwitch,
│   │                           BroadcastHub/MergeHub, GraphDsl, BidiFlow,
│   │                           Framing, Tcp, FileIO, RestartSource
│   ├── coordination.py         InMemoryLease
│   ├── discovery.py            StaticDiscovery, AggregateDiscovery
│   ├── di.py                   ServiceContainer
│   ├── hosting.py              Builder, ActorSystemBuilder
│   └── telemetry.py            TelemetryBus, TopicSubscriber
├── tests/                      pytest suite (270 tests)
└── examples/                   runnable examples
    ├── pingpong.py
    ├── ml_inference.py         subinterpreter pool demo
    └── persistence_counter.py
```

## Distributed actors

Two `ActorSystem`s on different processes (or in-process) form a
cluster, exchange messages over a real transport, and observe
membership through an async iterator.

```python
from atomr import ActorSystem
from atomr.cluster import Cluster, ClusterRegistry

# Real TCP loopback (auto-allocate port):
sys_a = ActorSystem.create_blocking("A")
ca = Cluster.with_tcp_transport(sys_a, "127.0.0.1:0")
print(ca.self_address)        # akka.tcp://A@127.0.0.1:42573

# Or in-process channel transport for deterministic tests:
registry = ClusterRegistry()
Cluster.with_test_transport(sys_a, registry)
Cluster.with_test_transport(sys_b, registry)

# Send across nodes:
sys_a.register_codec("json", encode, decode, manifests=["app.MyMsg"])
sys_a.tell_remote(remote_ref, MyMsg(...))
```

See [`../docs/python.md`](../docs/python.md) for the full distributed
actors guide (SBR config, codec registration, cluster events,
sharding, replicator, multi-node tests).

## Supervision

```python
from atomr import SupervisorStrategy

strategy = SupervisorStrategy.one_for_one(
    decider=[("builtins.ValueError", "restart")],
    default="stop",
    max_retries=3,
    within_seconds=10.0,
)
ref = system.actor_of(
    props(MyActor).with_supervisor_strategy(strategy),
    "my",
)
```

Retry budgets are enforced by `actor_cell` — exhaustion escalates to
the parent (or stops the actor at the root).

## Patterns and routers

```python
from atomr.routing import round_robin, consistent_hash, backoff
from atomr.pattern import CircuitBreaker, retry, RetrySchedule

pool = round_robin(props(Worker), n=4)            # routing in Rust, no GIL
hashed = consistent_hash(props(Entity), n=8)
hashed.tell_with_key(msg, key=hash(entity_id))    # explicit key required

sup = backoff(props(Flaky), min_backoff=0.1, max_backoff=10.0, random_factor=0.2)

cb = CircuitBreaker(max_failures=5, call_timeout=1.0, reset_timeout=10.0)
result = await cb.call_async(coro)

await retry(coro_factory, max_attempts=4,
            schedule=RetrySchedule.exponential(0.1, 5.0))
```

## Event sourcing

```python
from atomr.persistence import EventSourcedActor, Effect, InMemoryJournal

class Counter(EventSourcedActor):
    persistent_id = "counter-1"

    def initial_state(self):
        return {"count": 0}

    async def command_handler(self, state, ctx, cmd):
        if cmd["op"] == "incr":
            return [Effect.persist({"type": "Incremented", "by": cmd["by"]})]
        if cmd["op"] == "get":
            return [Effect.reply_message(state["count"])]
        return []

    def event_handler(self, state, event, recovery_mode=False):
        if event["type"] == "Incremented":
            state["count"] += event["by"]
        return state

system.use_json_codec(default=True)
ref = system.actor_of(props(Counter), "counter")
```

## Distributed data

```python
from atomr.ddata import GCounter, Replicator, WriteConsistency

rep = Replicator.get(system)
await rep.update(
    "shared",
    GCounter,
    lambda c: (c.increment("A", 1) or c),
    WriteConsistency.majority(timeout=2.0),
)
val = await rep.get_value("shared", GCounter)
```

Supported CRDTs: `GCounter`, `PNCounter`, `GSet`, `ORSet`,
`LwwRegister`, `Flag`, `ORMap` (of any built-in CRDT), `LWWMap`,
`PNCounterMap`, `ORMultiMap`. Durable backing via
`DurableStore.{noop, file}` or the redb-backed `RedbDurableStore`.

## Sharding

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
        allocation_strategy="least-shards",
        passivation_idle_timeout=30.0,
        remember_entities=True,
    ),
)
region.tell({"entity": "alice", "op": "incr"})
```

## Streams

```python
from atomr.streams import Source, Flow, Sink

total = await (
    Source.from_iter([1, 2, 3, 4])
        .via(Flow.map(lambda x: x * 2))
        .to(Sink.fold(0, lambda a, b: a + b))
        .run()
)
```

DSL surface: `Source` / `Flow` / `Sink` / `RunnableGraph` /
`GraphDsl` / `BidiFlow` / `KillSwitch` / `BroadcastHub` / `MergeHub` /
`SourceQueue` / `SinkQueue` / `RestartSource` / `RestartSettings` /
`Framing` / `Tcp` / `FileIO`. Stream supervision via
`Flow.with_supervision(decider)`.

## GIL dispatchers

| dispatcher | parallelism | best for |
|---|---|---|
| `python-pinned` (default) | 1 interpreter, 1 thread | low-latency, I/O-bound |
| `python-subinterpreter-pool` | N interpreters, N threads, N GILs | CPU-bound Python, subinterpreter-safe C ext |
| `python-nogil` | 1 interpreter, no GIL (3.13t) | CPU-bound on free-threaded Python |
| `python-subprocess` | N processes | untrusted handlers, hard RSS caps |

Capability probes:

```python
import atomr
atomr.subinterpreters_supported()   # True on CPython 3.12+
atomr.nogil_supported()             # True on CPython 3.13t
```

### Quotas per interpreter pool

```python
from atomr import InterpreterQuota

system.configure_interpreter(
    "ml-inference",
    "python-subinterpreter-pool",
    count=4,
    quota=InterpreterQuota(
        max_actors=32,
        max_mailbox_total=10_000,
        memory_soft_limit_bytes=2 * 1024**3,
        cpu_share=0.5,
        max_handler_ms=250,
        module_allowlist=["numpy", "torch", "atomr"],
        import_policy="eager",
    ),
)
```

### Metrics

```python
for pool in atomr._native.interpreter_metrics():
    print(pool["label"], pool["kind"], pool["messages_handled"])
```

Fields: `actors_hosted`, `messages_handled`, `gil_hold_ns_total`,
`mailbox_depth_total`, `handler_panics`, `long_handlers`.

### C-extension compatibility

```python
import atomr
atomr.declare_compat(
    "my_fast_lib",
    subinterpreter_safe=True,
    nogil_safe=False,
    notes="verified against release 1.4",
)
```

## Profiling

A `atomr.profiler` sub-package mirrors the Rust `atomr-profiler`
binary:

```bash
python -m atomr.profiler --scenario all --format md
python -m atomr.profiler --scenario cpu --messages 5000 --format json -o cpu.json
```

It autoconfigures the fastest dispatcher per scenario. For a
side-by-side Rust + Python table, run `python scripts/profile.py` from
the repo root. Full guide in
[`../docs/profiler.md`](../docs/profiler.md).

## Testing

```python
from atomr.testkit import testkit, TestScheduler, fish_for_message

def test_my_actor(testkit):
    probe = testkit.probe()
    # ... send via probe.ref_ and consume probe.messages() ...

# Virtual time:
sched = TestScheduler()
token = sched.schedule_after(0.5, "tick")
await sched.advance(0.5)
```

Run the full suite:

```bash
pytest python/tests -v
```

Multi-node TCP and in-process tests live in
`python/tests/test_cluster_multinode.py`,
`test_sharding_multinode.py`, `test_ddata_multinode.py`,
`test_remote_wire.py`, and `test_cluster_transport.py`.

## More

See [`../docs/python.md`](../docs/python.md) for the full guide:
distributed actors, supervision, patterns, sharding, event sourcing,
distributed data, streams, GIL tuning, and the complete API surface
reference.
