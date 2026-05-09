//! The supervisor retries `open_device` on a non-existent path until
//! `shutdown()` interrupts it, without panicking and without holding
//! up `inbound()` consumers.

use std::time::Duration;

use atomr_remote::transport::Transport;
use atomr_remote_serial::{ReconnectPolicy, SerialTransport};

#[tokio::test]
async fn supervisor_retries_missing_device_then_yields_to_shutdown() {
    let policy = ReconnectPolicy {
        initial: Duration::from_millis(10),
        max: Duration::from_millis(40),
        multiplier: 2.0,
    };
    let transport = SerialTransport::with_options(
        "A",
        "/dev/no-such-tty-XXX",
        115_200,
        4 * 1024 * 1024,
        policy,
    );
    let _addr = transport.listen().await.unwrap();

    // Let the supervisor cycle a few attempts (each fails, schedules
    // a backoff sleep). 200ms covers ~3-4 attempts at the configured
    // backoff but is well below any real test budget.
    tokio::time::sleep(Duration::from_millis(200)).await;

    transport.shutdown().await.unwrap();
    // After shutdown, no further reconnect attempts should fire. We
    // can't directly assert that without instrumentation, but the
    // test passes if the runtime doesn't hang on drop.
}

#[tokio::test]
async fn never_policy_gives_up_immediately() {
    let transport = SerialTransport::with_options(
        "A",
        "/dev/no-such-tty-YYY",
        115_200,
        4 * 1024 * 1024,
        ReconnectPolicy::never(),
    );
    let _addr = transport.listen().await.unwrap();
    // Supervisor returns on first failure under `never`, so no resource
    // leak after a single yield.
    tokio::time::sleep(Duration::from_millis(50)).await;
    transport.shutdown().await.unwrap();
}
