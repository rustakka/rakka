# Native actors, agentic systems, and distributed execution

This note is meant for architects and product leads who are deciding *why*
a native Akka-style runtime matters next to ad hoc microservices, raw
thread pools, or “agents” that are only a prompt plus an HTTP loop. It
applies to the **Akka family** (JVM Akka, Akka.NET) and to **rustakka**:
the ideas are the same; rustakka is an **idiomatic, native Rust** port of
the Akka.NET module layout, with **optional Python** on top.

## The core value: one model, two scales

**Actors** combine **encapsulated state** and **message-driven behavior** in
a single, cheap primitive. You do not need a separate “service” for every
class of work and a different concurrency story for every layer.

- **On one host**, actors map naturally onto **CPU cores** through
  schedulers, dispatchers, and non-blocking I/O. Work is decomposed into
  units that are **independently schedulable** but **structurally
  coordinated** (supervision, routing, back-pressure) instead of one giant
  shared memory graph.
- **Across hosts**, the **same** addressing and messaging model **extends**:
  remote transport, cluster membership, and sharding are not a second
  product bolted on—they are the continuation of the same “talk to a
  ref” story at datacenter scale.

That continuity—**one conceptual model** from thread to region—is what
makes the stack compelling for people who are tired of re-learning
distribution in every new framework.

## Why this aligns with “agentic” systems

**Agentic** systems are usually described in terms of autonomous
components (goals, tools, memory, collaboration). The actor model is not
identical to any one LLM agent framework, but it **shapes the platform**
the way those frameworks need:

| Agentic idea | Actor analogue |
|--------------|------------------|
| **Autonomy** | Each actor runs its own logic and state; you interact by **sending messages**, not by poking shared globals. |
| **Identity and lifecycle** | Actors are **named, supervised entities**. Failure is recoverable: restart, escalate, or route to a death-letter path. |
| **Cooperation** | Conversations are **explicit** (ask/pipe-to, sharded entities, event streams) instead of hidden callbacks. |
| **Scoping risk** | Blast radius is bounded: bad behavior is isolated to an actor and its children unless you *choose* to share more. |
| **Cross-machine collaboration** | **Location transparent** refs and cluster primitives express “who” without rewriting the program as a tangle of REST clients. |

So: agents *as a product concept* and actors *as a runtime primitive* are
a strong fit—you get **structure** for the chaos of many concurrent,
semi-independent processes.

## Layered agentic stack: LangGraph, graphs, and practices

Many LLM agent stacks separate **orchestration** (who runs next), **state**
(what the session remembers), and **execution** (where code runs). The
[LangGraph](https://github.com/langchain-ai/langgraph) style encodes
workflows as **agent state graphs**—nodes, edges, and transitions that
lend themselves to a **supervised, message-based** host.

Two companion crates in the **rustakka ecosystem** (not the Akka.NET port
itself) are meant to be composed with this repository:

- **`rustakka-langgraph`** — **LangGraph agent state graphs** on top of
  rustakka: graph nodes as actors, edges as message routes, and explicit
  turn-taking so the same *graph* you design for an agent team runs with
  cluster-aware refs and back-pressure, not a single-process toy loop.
- **`rustakka-agents`** — **agentic patterns and practices above the graph
  layer**: how to name actors, model tools, combine humans-in-the-loop,
  test agent behavior with the testkit, and run operational
  **supervision + persistence** for long-lived agent sessions. It assumes
  you may already be using a LangGraph-shaped graph; it adds the *layer*
  of conventions, safety rails, and integration guidance so production
  agentics does not drown in ad hoc process wiring.

**rustakka** (this tree) is still the **runtime and distribution**
substrate. **`rustakka-telemetry`** and **`rustakka-dashboard`** then give
you **cross-crate visibility** (see the [Dashboard](dashboard.md)) so you
can see those graphs and the rest of the system in one place.

## Determinism and non-determinism, honestly

**Determinism** in the sense that matters for engineering:

- A single actor can be written as a **state machine** or reducer:
  process mailbox messages in order, update private state, emit effects.
  That is **deterministic with respect to message order** for that actor
  when the handler is written that way.
- **Persistence and event sourcing** (where supported) make **replay** a
  first-class story: a journal defines a **linear history** for an entity.

**Non-determinism** is not denied—it is the real world:

- **Ordering** from multiple senders, network latency, and scheduling
  means global order is *not* free; the model **expects** you to use
  explicit protocols when you need a total order or consensus.
- **Time** and **external systems** (APIs, disks, other clusters) inject
  variability; the runtime gives you **timeouts, watch, and supervision**
  so you **handle** that instead of pretending it is purely functional.

A healthy mental model: **per-actor, sequential processing of the mailbox**
gives a **local** deterministic story; the **system** is concurrent and
**partially** ordered unless you add coordination. That is exactly the
landscape most agentic and distributed applications live in.

## Native efficiency and conceptual clarity

**Native** execution (here: Rust, Tokio, direct serialization) matters for:

- **Predictable cost per message** when you run hot paths at scale.
- **Clear failure domains**: OS threads, back-pressure, and resource limits
  combine with the actor model instead of fighting a heavyweight runtime.

**Conceptual clarity** comes from the **shape** of the program:

- State lives **in one place** (the actor), not in a dozen “manager”
  singletons.
- Communication is **message-shaped**, which makes system diagrams and
  runtime **observability** (mailboxes, [telemetry across crates](dashboard.md#viewing-behavior-across-rustakka-crates), sharding) line up with the
  code you wrote.

## rustakka in this picture

- **Parity of concepts** with Akka.NET: same module boundaries, similar
  APIs, so **skills transfer** and upstream evolution can be tracked.
- **Rust-native** subsystems: configuration, remoting, persistence
  providers, and streams follow idiomatic **Rust and async** practices.
- **Python** where you want iteration speed, with the **GIL and
  interpreter model** made explicit in the documentation—so you can **mix
  “native” Rust actors and “hosted” Python actors** in one architecture
  with eyes open.

The goal is not to claim a single silver bullet, but to state plainly:
**a mature actor platform is a strong place to run agentic, concurrent,
and distributed design**—as native code, with a clear model, on one
machine or many.

## See also

- [Python bindings](python.md) — GIL, dispatchers, and tuning.
- [Dashboard](dashboard.md) — **telemetry visualization hooks**; live API + Web UI across actors, cluster, sharding, persistence, remote, streams, and distributed data.
- [Observability](observability.md) — metrics and OpenTelemetry.
- [Repository README](../README.md) — quick start and crate map.
- **`rustakka-langgraph`**, **`rustakka-agents`** — companion ecosystem crates (LangGraph state graphs, then agentic practices above the graph). Build and publish alongside this repo.
