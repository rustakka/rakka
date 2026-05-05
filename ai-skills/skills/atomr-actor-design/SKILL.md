---
name: atomr-actor-design
description: Use when authoring or modifying an actor in a project that depends on atomr. Covers Msg type design, the Actor trait's lifecycle hooks, ask vs tell, supervision strategies, and FSM patterns. Triggers on writing or editing an `impl Actor`, choosing a Msg enum, picking a supervision strategy, or wiring `Props`/`ActorSystem`.
---

# Authoring atomr actors

This skill helps you write idiomatic actors against `atomr` (umbrella
crate `atomr` on crates.io, imported as `atomr`).

## Mental model

A atomr actor is **state + behavior + a typed mailbox**. Each actor:

1. owns its state (no shared mutability),
2. declares one associated `Msg` type (typically an enum),
3. processes messages serially via `async fn handle(&mut self, ctx, msg)`,
4. is addressed by an `ActorRef<Msg>` returned from
   `ActorSystem::actor_of(...)`.

Tell-style sends are fire-and-forget; ask-style sends return a future
resolved by the actor's reply. Failure is supervised by the parent —
the actor itself does not catch panics.

## The Actor trait

Defined in `atomr_core::actor::traits`; available from the prelude.

```rust
use atomr::prelude::*;

#[derive(Default)]
struct Greeter { count: u64 }

#[async_trait]
impl Actor for Greeter {
    type Msg = String;

    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: String) {
        self.count += 1;
        println!("hi {msg} (#{count})", count = self.count);
    }

    // Optional lifecycle hooks (defaults are no-ops):
    async fn pre_start(&mut self, _ctx: &mut Context<Self>) {}
    async fn post_stop(&mut self, _ctx: &mut Context<Self>) {}
    async fn pre_restart(&mut self, _ctx: &mut Context<Self>, _err: &str) {}
    async fn post_restart(&mut self, _ctx: &mut Context<Self>, _err: &str) {}

    fn supervisor_strategy(&self) -> SupervisorStrategy {
        SupervisorStrategy::default()
    }
}
```

Keep in mind:
- `Actor: Sized + Send + 'static` and `Msg: Send + 'static`.
- `handle` is `async`. Long-running work blocks this actor's mailbox —
  spawn a child or pipe a future via `pattern::pipe_to` for parallelism.
- Lifecycle hooks let you re-establish resources after a restart.
  `pre_restart` runs on the dying instance; `post_restart` runs on the
  fresh one. Clean up handles in `post_stop`.

## Designing the Msg type

- **Prefer one `enum` per actor.** It documents the protocol and lets
  the compiler enforce exhaustiveness in `handle`.
- **Make replies addressable.** For ask-style flows, include a reply
  channel inside the variant, or use `pattern::ask` which handles the
  oneshot for you.
- **Keep messages owned and `Send`.** Avoid borrowed data; an
  `ActorRef` lives across threads.
- **Don't smuggle behavior via closures.** Messages should be data;
  behavior belongs in `handle`. This keeps messages serializable for
  remoting/clustering.

```rust
enum Cmd {
    Get { key: String, reply: tokio::sync::oneshot::Sender<Option<String>> },
    Put { key: String, value: String },
}
```

## Spawning an actor

```rust
let system = ActorSystem::create("app", Config::empty()).await?;
let greeter = system.actor_of(Props::create(Greeter::default), "greeter")?;
greeter.tell("world".to_string());
system.terminate().await;
```

`Props::create(F)` takes a factory `F: Fn() -> A` so the supervisor can
re-instantiate the actor on restart. Don't capture mutable state in the
closure — capture `Arc`-shared dependencies instead.

## Tell vs ask

- **`actor_ref.tell(msg)`** — fire-and-forget, infallible, fastest path.
  Use this for the vast majority of message flows.
- **`pattern::ask(actor_ref, msg, timeout)`** — request/response. Returns
  a future resolved by the actor's reply, or a timeout error. The
  actor must be authored to send a reply (often via a oneshot inside
  the message).

Don't `ask` from inside `handle` — it blocks the mailbox. Use
`pattern::pipe_to` to fold an `ask`'s future back into a self-tell.

## Supervision

The default strategy is one-for-one with restart-on-panic. Customize
in `supervisor_strategy`:

```rust
use atomr::prelude::*;
use std::time::Duration;

fn supervisor_strategy(&self) -> SupervisorStrategy {
    OneForOneStrategy::new()
        .with_max_retries(3)
        .with_within(Duration::from_secs(60))
        .with_decider(|err| {
            if err.contains("transient") { Directive::Restart } else { Directive::Stop }
        })
        .into()
}
```

Directives: `Resume` (drop the message, keep state), `Restart` (rebuild
state via the factory), `Stop` (shut the child down), `Escalate` (defer
the decision to the grandparent).

## FSM actors

For protocols with explicit states, prefer the `fsm_macro` (or the
hand-written FSM pattern in `atomr_core::actor`) over branching on a
state field inside `handle`. See `crates/atomr-core/src/fsm_macro.rs`
and `crates/atomr-persistence/src/persistent_fsm.rs` for the pattern.

## When to reach beyond core

| You need… | Reach for… |
|---|---|
| State that survives restart (event sourcing) | `atomr-persistence` — see `atomr-persistence` skill |
| Cross-process / cross-host messaging | `atomr-remote` |
| Membership, sharding, pub/sub | `atomr-cluster*` — see `atomr-cluster` skill |
| Reactive pipelines | `atomr-streams` |
| Deterministic tests | `atomr-testkit` — see `atomr-testing` skill |

## IO managers

For TCP/UDP work, use the IO manager actors rather than touching
sockets directly. `TcpManager` accepts a `Bind { addr }` command for
listeners and a `Connect { addr }` command for outbound connections;
`TcpManager::connect(addr)` is a convenience helper that wraps the
latter. Both return an `ActorRef` for the connection actor that owns
the socket and forwards `Received` / `Closed` messages.

## Canonical references

- `crates/atomr-core/src/actor/traits.rs` — `Actor` trait definition
- `crates/atomr-core/src/supervision.rs` — strategies and directives
- `crates/atomr-core/src/lib.rs` — prelude contents
- `examples/pingpong/` — minimal tell-loop
- `examples/fault-tolerance/` — restart semantics + lifecycle hooks
- `docs/idiomatic-rust.md` — design choices behind the API
- `docs/architecture.md` — runtime structure

## Common mistakes

- **Awaiting long futures inside `handle`.** Mailbox stalls. Use
  `pattern::pipe_to` to fold the result back as a future message.
- **Capturing mutable state in `Props::create`.** Restart re-runs the
  factory; mutable captures violate that contract.
- **Borrowed data in `Msg`.** `Msg: Send + 'static` — own your bytes.
- **Catching panics in `handle`.** Let the supervisor decide.
- **Using `ask` where `tell` would do.** `ask` allocates a oneshot per
  call; on hot paths a tell + reply-via-message is cheaper.
