//! Re-export of the `ask` pattern.
//!
//! The primary API lives on [`crate::actor::ActorRef::ask_with`]; this free
//! function is a convenience wrapper so users can write
//! `ask(&target, |reply| Msg::Query(reply), Duration::from_secs(1)).await`.

use std::time::Duration;

use tokio::sync::oneshot;

use crate::actor::{ActorRef, AskError};

pub async fn ask<M: Send + 'static, R: Send + 'static, F>(
    target: &ActorRef<M>,
    build: F,
    timeout: Duration,
) -> Result<R, AskError>
where
    F: FnOnce(oneshot::Sender<R>) -> M,
{
    target.ask_with(build, timeout).await
}
