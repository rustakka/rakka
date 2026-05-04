---
name: atomr-testing
description: Use when writing tests against atomr actors using atomr-testkit. Covers TestKit, TestProbe (expect_msg / receive_n / expect_no_msg / expect_msg_pf), and the virtual-time TestScheduler. Triggers when authoring `#[tokio::test]` cases that exercise actors, mailboxes, or scheduled timers.
---

# Testing atomr actors

`atomr-testkit` provides the deterministic scaffolding that lets you
test actor systems without sleeping or racing. Reach for it whenever a
test exercises an `ActorRef`.

Enable it via the umbrella feature:

```toml
[dev-dependencies]
atomr = { version = "0.2", features = ["testkit"] }
# or directly:
atomr-testkit = "0.1"
```

## TestKit + TestProbe

`TestProbe<M>` is an actor whose mailbox you assert against directly.
Hand its `actor_ref()` to the system-under-test as if it were any other
collaborator; the probe records every message it receives so the test
body can pull them out.

```rust
use std::time::Duration;
use atomr::prelude::*;
use atomr::testkit::{TestKit, TestProbe};

#[tokio::test]
async fn greeter_replies() {
    let system = ActorSystem::create("test", Config::empty()).await.unwrap();
    let kit = TestKit::new(/* … */);
    let mut probe: TestProbe<String> = kit.probe("greeter-replies");

    // System under test: an actor that forwards a String to `probe`.
    let greeter = system.actor_of(
        Props::create({
            let to = probe.actor_ref().clone();
            move || ForwardActor { to: to.clone() }
        }),
        "greeter",
    ).unwrap();

    greeter.tell("hi".to_string());

    let msg = probe.expect_msg(Duration::from_secs(1)).await.unwrap();
    assert_eq!(msg, "hi");
}
```

### Probe API at a glance

| Method | What it does |
|---|---|
| `expect_msg(timeout)` | Pop the next message; error on timeout. |
| `expect_msg_pf(timeout, pred)` | Pop the next message and require `pred(&msg)`. |
| `expect_msg_class(timeout, extract)` | Pop and run a destructuring closure. |
| `expect_no_msg(timeout)` | Assert nothing arrives within `timeout`. |
| `receive_n(n, timeout)` | Collect `n` messages or fail. |

Always pass a finite timeout. CI is slow; pick something forgiving
(e.g. `Duration::from_secs(2)`) rather than `Duration::from_millis(50)`.

## Virtual time with `TestScheduler`

`TestScheduler` replaces wall-clock scheduling with a manually-driven
clock. Use it whenever the code under test calls `schedule_after`,
debounces, or runs periodic work — wall-clock `tokio::time::sleep` in
tests is slow and flaky.

```rust
use std::time::Duration;
use atomr::testkit::TestScheduler;

#[tokio::test]
async fn debounce_fires_after_quiet_period() {
    let sched = TestScheduler::new();
    let token = sched.schedule_after(Duration::from_millis(500), || {
        // … callback
    });

    assert!(!sched.fired(token));
    sched.advance(Duration::from_millis(499)).await;
    assert!(!sched.fired(token));
    sched.advance(Duration::from_millis(1)).await;
    assert!(sched.fired(token));
}
```

Cancel pending work with `sched.cancel(token)`. Inspect outstanding
work with `sched.pending()`.

## Multi-node specs

For distributed tests (membership, sharding, persistence-tck) reach for
`atomr_testkit::MultiNodeSpec`. It boots multiple nodes in one test and
exposes barriers and event filters. See the integration tests in
`crates/atomr-cluster/tests/` and `crates/atomr-persistence-tck/` for
working examples.

## Conformance suites

If you implement a `Journal` or `SnapshotStore`, run it through
`atomr-persistence-tck`'s `journal_suite`, `journal_tag_suite`, and
`snapshot_suite`. They are the source of truth for what "compatible
with atomr-persistence" means.

## Canonical references

- `crates/atomr-testkit/src/probe.rs` — `TestProbe` API
- `crates/atomr-testkit/src/test_kit.rs` — `TestKit::probe`
- `crates/atomr-testkit/src/test_scheduler.rs` — `TestScheduler`
- `crates/atomr-testkit/src/multinode.rs` — `MultiNodeSpec`

## Common mistakes

- **Wall-clock `sleep` in actor tests.** Replace with `TestScheduler`
  or with `expect_msg_pf` that polls until a predicate matches.
- **Trapping the probe inside a closure that's never dropped.**
  Probes need to outlive the assertions; bind them as `let mut probe = …`
  in the test body, not inside a `Props::create` closure that gets
  re-run on restart.
- **Asserting "nothing happens" via a 10ms `expect_no_msg`.** The
  scheduler may not have ticked yet; use longer windows (250ms+) or
  drive the scheduler explicitly via `TestScheduler::advance`.
- **Dropping the `ActorSystem` before assertions complete.** Keep the
  system alive until the last assertion, then `system.terminate().await`.
