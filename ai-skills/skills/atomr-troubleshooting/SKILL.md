---
name: atomr-troubleshooting
description: Use when diagnosing failures in a project that depends on atomr. Covers compile-time errors (missing feature flags, wrong prelude), runtime issues (mailbox stalls, restart loops, ask timeouts), and cluster-level problems (split-brain, unreachable members). Triggers on atomr error messages, missing-symbol errors, hangs, or repeated restarts.
---

# Troubleshooting atomr

A diagnostic checklist organized by symptom. For deep dives into a
subsystem, hand off to the matching skill (`atomr-cluster`,
`atomr-persistence`, `atomr-python`).

## Compile-time

### "cannot find type `X` in crate `atomr`"

You almost certainly need a feature flag on the umbrella crate.
Subsystems are gated; flipping the feature also re-exports the crate
under a stable namespace.

| Type / module | Feature | Re-exported as |
|---|---|---|
| `Actor`, `ActorRef`, `Props`, `Context` | (always available, in `atomr::prelude`) | `atomr::core` |
| `TestKit`, `TestProbe` | `testkit` | `atomr::testkit` |
| `Cluster*` | `cluster` (or `cluster-tools` / `cluster-sharding`) | `atomr::cluster*` |
| `PersistentActor`, `Eventsourced` | `persistence` | `atomr::persistence` |
| `Source`, `Flow`, `Sink` | `streams` | `atomr::streams` |
| `Telemetry` exporters | `telemetry` | `atomr::telemetry` |

```toml
atomr = { version = "0.2", features = ["cluster", "persistence"] }
```

If you depend on a subsystem crate directly (e.g. `atomr-persistence`)
you skip the umbrella's feature gating but lose the unified namespace.

### "trait `Actor` is not implemented" / "missing `type Msg`"

`#[async_trait]` is required because `Actor::handle` is `async fn`.
Either:

```rust
use atomr::prelude::*;     // re-exports `async_trait`

#[async_trait]
impl Actor for MyActor { /* … */ }
```

…or import it directly: `use async_trait::async_trait;`.

### "the trait `Send` is not implemented"

Your `Msg` type owns something non-`Send` (e.g. `Rc`, raw pointers,
`MutexGuard` held across an await). Switch to `Arc`/`Mutex` from
`std::sync` or `parking_lot`, or restructure so locks aren't held
across `.await` points.

## Runtime

### Actor stops responding (mailbox stall)

The actor is `await`ing inside `handle` and the mailbox is queued
behind that future. Fix:

- Move the long-running work to a child actor and `tell` results back.
- Use `pattern::pipe_to` to fold a future's result back as a self-tell.
- Move blocking work to `tokio::task::spawn_blocking`.

Never call `ask` from inside `handle` on the same actor graph — it can
deadlock.

### Restart loop (`pre_restart` printed repeatedly)

The supervisor's restart budget keeps re-creating an actor that panics
during `pre_start`. Either:

- Fix the failing initialization (often: a missing config, a closed
  channel, an unavailable dependency).
- Tighten the strategy with `OneForOneStrategy::new().with_max_retries(N)`
  so the parent eventually escalates.
- Switch the decider to `Directive::Stop` for the offending error
  instead of `Restart`.

### `ask` times out

Common causes, in order:

1. The actor never replies — check that the handler actually sends to
   the reply oneshot.
2. The actor is blocked on a long await (see "mailbox stall" above).
3. The timeout is too aggressive for CI — bump it.
4. The actor was stopped or restarted between send and reply — replies
   from the old instance are dropped.

### Messages silently dropped

- An `ActorRef` outliving its actor is valid but `tell` becomes a
  no-op. Wire `system.event_stream()` or use `Context::watch` to learn
  about termination.
- For remote/cluster refs, check that delivery semantics are what you
  expect: atomr-remote provides at-most-once by default; opt into
  reliable delivery if you need it.

## Cluster

For full cluster troubleshooting, hand off to `atomr-cluster`. Quick
checks:

- **Members stuck in `Joining`.** Seed nodes unreachable, or auth
  mismatch. Check the `cluster` config block and the gossip transport
  logs.
- **Split-brain.** Configure an SBR (split-brain resolver). See
  `crates/atomr-cluster/src/sbr.rs` and `docs/architecture.md`.
- **Sharding stuck rebalancing.** Coordinator persistence broken — see
  `atomr-persistence` skill.

## Telemetry as a debugging tool

Enable `telemetry` and wire an OTLP exporter; the runtime emits spans
for mailbox enqueue/dequeue, supervision decisions, and gossip
transitions. See `docs/observability.md`.

The dashboard (`atomr-dashboard`, feature `dashboard`) gives a live
view of the actor tree, mailbox depths, and recent supervision events
— often the fastest way to localize a problem.

## Canonical references

- `docs/architecture.md` — runtime structure
- `docs/observability.md` — tracing + metrics
- `docs/dashboard.md` — live UI
- `docs/remoting.md` — delivery semantics
- `crates/atomr-core/src/supervision.rs` — supervision strategies
