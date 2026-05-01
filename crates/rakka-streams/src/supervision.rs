//! Stream-level supervision deciders.
//!
//! Phase 12.4 of `docs/full-port-plan.md`. Akka.NET / Akka Streams
//! parity: `Supervision.Decider` + `withAttributes(supervisionStrategy(…))`.
//!
//! Stream operators on `Source<Result<T, E>>` consult a [`Decider`]
//! to decide what to do on each `Err`:
//!
//! * `Stop` — terminate the stream (and propagate the error to the
//!   downstream `Sink::collect_with_status` if used).
//! * `Resume` — drop the failing element and continue.
//! * `Restart` — drop element and conceptually reset operator state
//!   (we surface this as `Resume` for stateless operators).
//!
//! `with_decider(src, decider)` returns a `Source<T>` (Result-stripped)
//! by applying the decider to each `Err` element and emitting only
//! the `Ok` payloads downstream.

use crate::source::Source;
use futures::stream::StreamExt;

/// What a [`Decider`] returns for an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SupervisionDirective {
    /// Stop the stream — caller should treat as terminal.
    Stop,
    /// Drop the failing element; continue with the next.
    Resume,
    /// Drop the failing element; conceptually reset operator state.
    Restart,
}

/// A decider is a closure mapping `&E → SupervisionDirective`.
pub type Decider<E> = std::sync::Arc<dyn Fn(&E) -> SupervisionDirective + Send + Sync>;

/// Conventional decider helpers.
pub mod deciders {
    use super::{Decider, SupervisionDirective};
    use std::sync::Arc;

    /// Always `Resume` — never lets a single bad element kill the
    /// stream.
    pub fn resuming<E: Send + Sync + 'static>() -> Decider<E> {
        Arc::new(|_| SupervisionDirective::Resume)
    }

    /// Always `Stop` — first error tears the stream down (akka.net
    /// default).
    pub fn stopping<E: Send + Sync + 'static>() -> Decider<E> {
        Arc::new(|_| SupervisionDirective::Stop)
    }

    /// Always `Restart`.
    pub fn restarting<E: Send + Sync + 'static>() -> Decider<E> {
        Arc::new(|_| SupervisionDirective::Restart)
    }
}

/// Apply `decider` to each error in `src`, emitting only the
/// surviving `Ok` payloads.
pub fn with_decider<T, E>(src: Source<Result<T, E>>, decider: Decider<E>) -> Source<T>
where
    T: Send + 'static,
    E: Send + 'static,
{
    let inner = src.into_boxed();
    let mut stopped = false;
    let stream = inner
        .take_while(move |item| {
            let cont = !stopped;
            if let Err(e) = item {
                if let SupervisionDirective::Stop = decider(e) {
                    stopped = true;
                }
            }
            futures::future::ready(cont)
        })
        .filter_map(|item| futures::future::ready(item.ok()));
    Source { inner: stream.boxed() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::Sink;

    #[tokio::test]
    async fn resuming_decider_drops_errors() {
        let s: Source<Result<i32, &'static str>> =
            Source::from_iter(vec![Ok(1), Err("bad"), Ok(2), Err("worse"), Ok(3)]);
        let out = with_decider(s, deciders::resuming());
        let collected = Sink::collect(out).await;
        assert_eq!(collected, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn stopping_decider_terminates_at_first_error() {
        let s: Source<Result<i32, &'static str>> =
            Source::from_iter(vec![Ok(1), Ok(2), Err("boom"), Ok(99)]);
        let out = with_decider(s, deciders::stopping());
        let collected = Sink::collect(out).await;
        assert_eq!(collected, vec![1, 2]);
    }

    #[tokio::test]
    async fn restarting_decider_behaves_like_resume_for_stateless() {
        let s: Source<Result<i32, &'static str>> =
            Source::from_iter(vec![Err("x"), Ok(7), Err("y"), Ok(8)]);
        let out = with_decider(s, deciders::restarting());
        let collected = Sink::collect(out).await;
        assert_eq!(collected, vec![7, 8]);
    }

    #[tokio::test]
    async fn custom_decider_can_inspect_error() {
        use std::sync::Arc;
        let decider: Decider<i32> = Arc::new(|e: &i32| {
            if *e < 0 { SupervisionDirective::Stop } else { SupervisionDirective::Resume }
        });
        let s: Source<Result<i32, i32>> =
            Source::from_iter(vec![Ok(1), Err(5), Ok(2), Err(-1), Ok(99)]);
        let out = with_decider(s, decider);
        let collected = Sink::collect(out).await;
        assert_eq!(collected, vec![1, 2]);
    }
}
