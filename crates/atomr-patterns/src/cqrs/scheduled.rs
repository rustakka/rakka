//! Scheduled commands helper.
//!
//! [`schedule_command`] dispatches a command to a [`crate::Repository`]
//! after a delay. Useful for aggregates that need a "fire reminder in
//! N minutes" behaviour without dragging in the full
//! [`atomr_core::actor::scheduler::Scheduler`] surface.

use std::sync::Arc;
use std::time::Duration;

use crate::ddd::Repository;

/// Send `cmd` to `repo` after `delay`. Returns immediately; the
/// dispatch happens in a detached tokio task. Errors from the repo
/// are logged at `warn` level — callers who care about the outcome
/// should construct their own scheduling.
pub fn schedule_command<R: Repository + 'static>(
    repo: Arc<R>,
    delay: Duration,
    cmd: <R::Aggregate as atomr_persistence::Eventsourced>::Command,
) where
    <R::Aggregate as atomr_persistence::Eventsourced>::Error: std::fmt::Display,
{
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        if let Err(e) = repo.send(cmd).await {
            tracing::warn!(error = %e, "scheduled command failed");
        }
    });
}
