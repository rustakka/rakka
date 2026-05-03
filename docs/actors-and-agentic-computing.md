# Actors, agentic systems, and unified compute

This note is for architects deciding *why* a native actor runtime in
Rust matters next to ad-hoc microservices, raw thread pools, or
"agents" that are only a prompt plus an HTTP loop. The argument has
three parts: actors as a programming model, actors as a substrate for
agentic systems, and actors as the boundary that lets a program span
CPU and accelerator without splitting in two.

## One model, many scales

Actors combine **encapsulated state** and **message-driven behavior**
in a single, cheap primitive. You don't need a separate "service" for
every class of work and a different concurrency story for every
layer.

- **On one host**, actors map onto CPU cores through schedulers,
  dispatchers, and non-blocking I/O. Work is decomposed into units
  that are independently schedulable but structurally coordinated
  (supervision, routing, backpressure) instead of one shared-memory
  graph.
- **Across hosts**, the same addressing and messaging model extends:
  remote transport, cluster membership, and sharding aren't a second
  product bolted on — they're the continuation of the same "talk to
  a ref" story at datacenter scale.
- **Across compute substrates**, the same dispatch primitive extends
  again. A message *is* the dispatch unit. The runtime can route it
  to a CPU mailbox today and a CUDA-backed dispatcher tomorrow with
  no change to the message contract.

That continuity — one model from thread to region to GPU stream — is
what makes the platform compelling for teams tired of re-learning
distribution and heterogeneous compute in every new framework.

## Why agentic systems want this shape

"Agentic" systems are usually described in terms of autonomous
components with goals, tools, memory, and collaboration. The actor
model isn't identical to any one agent framework, but it shapes the
platform the way those frameworks need:

| Agentic concern | Actor mechanism |
|---|---|
| **Autonomy** | Each actor runs its own logic and state; you interact by sending messages, not by poking shared globals. |
| **Identity and lifecycle** | Actors are named, supervised entities. Failure is recoverable: restart, escalate, or route to a death-letter path. |
| **Cooperation** | Conversations are explicit (`ask` / `pipe-to`, sharded entities, event streams) instead of hidden callbacks. |
| **Scoping risk** | Blast radius is bounded: bad behavior is isolated to an actor and its children unless you *choose* to share more. |
| **Cross-machine collaboration** | Location-transparent refs and cluster primitives express *who* without rewriting the program as a tangle of REST clients. |

Agents as a product concept and actors as a runtime primitive align
naturally — you get structure for the chaos of many concurrent,
semi-independent processes that reason, call tools, and coordinate.

## Why heterogeneous compute wants this shape

Modern workloads no longer live entirely on the CPU.

- **GPU territory:** inference, embedding, scoring, vector search,
  simulation, large reductions over tensors.
- **CPU territory:** orchestration, control flow, persistence, I/O,
  protocol negotiation, supervision.

Today's stacks bridge the two with ad-hoc batching layers, queues,
and serialization shims invented per project. The model fights you
the moment a workload moves between sides: who owns the buffer? Who
applies backpressure? What happens if the GPU is at capacity? When
the kernel crashes, what gets restarted, and who is told?

The actor model already encodes the right boundary. A message is
delivered to an addressable destination through a typed mailbox; the
delivery is supervised; the destination's failure is observable. That
boundary is identical whether the destination runs on a tokio worker
or on a CUDA stream.

The runtime can offer a small set of dispatcher kinds with the same
contract:

- **CPU dispatcher** — the default; tokio worker pool, work-stealing.
- **Pinned dispatcher** — single-OS-thread, for blocking work.
- **GPU dispatcher** (planned) — a stream-bound dispatcher that
  accepts messages destined for accelerator memory, batches them on
  the host side, schedules a kernel, and produces results back into
  the actor system.

To the rest of the program, the change is one line of configuration:
`with_dispatcher("gpu")`. The supervision tree, the mailbox, the
backpressure, the observability hooks — they all remain.

That's the unified-compute thesis: don't write two programs glued at
the seam. Write one program whose dispatch can target either side
explicitly, with the same primitives the rest of the system already
uses.

## Why Rust earns the granularity

The unified-compute argument only pays off if per-message overhead
stays low. Rust earns that granularity:

- **Zero-cost abstractions** mean the actor envelope, the dispatcher
  hop, the supervision check don't accrete hidden allocations.
- **Ownership** means concurrency safety is checked at compile time,
  so the runtime can hand a message between threads without coarse
  locks.
- **Predictable resource use** means a single host can sustain
  millions of fine-grained actors without ceremony — the same
  granularity that makes "one actor per agent" or "one actor per
  shard entity" or "one actor per inflight tool call" tractable.

That precision is also what lets the runtime push backpressure,
mailboxes, and supervision into primitives that don't need to be
rebuilt at every layer above. The same `Mailbox<T>` that a CPU
dispatcher pulls from is the same shape a GPU dispatcher would pull
from.

## Determinism and non-determinism, honestly

A single actor can be written as a state machine or reducer: process
mailbox messages in order, update private state, emit effects. That
gives a deterministic local story for that actor.

Persistence and event sourcing (where you opt into them) make replay
a first-class story: a journal defines a linear history for an
entity. That's how you turn "what state should this agent be in?"
from a question into a guarantee.

The system as a whole is partially ordered, not totally ordered, and
the runtime is honest about that. Multi-sender ordering, network
latency, and scheduler decisions don't promise global ordering; you
add explicit coordination (cluster singleton, lease, sharded entity)
when you need it. Time and external systems inject variability; the
runtime gives you timeouts, watch, and supervision so you handle that
explicitly rather than pretending the world is purely functional.

A healthy mental model: per-actor sequential processing gives a local
deterministic story; the system is concurrent and partially ordered
unless you add coordination. That's exactly the landscape most
agentic and distributed applications live in.

## What rakka adds in this picture

- **Native efficiency**: predictable cost per message, fine-grained
  per-actor footprint, no heavyweight runtime to fight.
- **Conceptual clarity**: state lives in one place (the actor);
  communication is message-shaped, so diagrams, observability, and
  code line up.
- **Cluster-grade primitives** without a separate distribution
  story: remoting, sharding, replicated data, persistence-at-scale,
  reactive streams.
- **Python on top** for iteration speed, with the GIL and
  interpreter model made explicit so you can mix native Rust actors
  and hosted Python actors in one architecture with eyes open.
- **A path to GPU dispatch** that doesn't require rewriting the
  program — just routing a class of messages through a dispatcher
  whose backend lives on the accelerator.

The claim isn't a silver bullet. The claim is plainer: a mature
actor platform is a strong place to run agentic, concurrent, and
heterogeneous-compute design — as native code, with one model, from
one core to many machines to many compute substrates.

## See also

- [Architecture](architecture.md) — the dispatch / supervision / I/O
  layout, including the hooks where heterogeneous backends slot in.
- [Idiomatic Rust principles](idiomatic-rust.md) — invariants that
  preserve granularity.
- [Python bindings](python.md) — interpreter strategies and quotas.
- [Dashboard](dashboard.md) — live cross-subsystem visibility.
- [Observability](observability.md) — metrics and tracing exporters.
- [Repository README](https://github.com/rustakka/rakka) — quick start.
