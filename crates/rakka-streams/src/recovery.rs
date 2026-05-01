//! Recovery operators on `Source<Result<T, E>>`.
//!
//! Phase 12.7 of `docs/full-port-plan.md`. Akka.NET / Akka Streams
//! parity: `Recover`, `RecoverWith`, `RecoverWithRetries`, `MapError`.
//!
//! These operators are exposed as free functions that take a
//! `Source<Result<T, E>>` and return a transformed `Source`. They
//! sit alongside the linear-operator surface on `Source<T>` rather
//! than directly on `Source` because Result-shaped sources need
//! their own combinator semantics.

use crate::source::Source;
use futures::stream::StreamExt;

/// Replace any `Err(e)` with `f(e)` and continue, mapping the stream
/// to `T` (success values are unwrapped). The first error stops the
/// upstream — subsequent elements are dropped.
///
/// Akka.NET: `Source.Recover<T>(Func<Exception, Option<T>>)`.
pub fn recover<T, E, F>(src: Source<Result<T, E>>, mut f: F) -> Source<T>
where
    T: Send + 'static,
    E: Send + 'static,
    F: FnMut(E) -> Option<T> + Send + 'static,
{
    let inner = src.into_boxed();
    let mut errored = false;
    let stream = inner
        .take_while(move |item| {
            let cont = !errored;
            if matches!(item, Err(_)) {
                errored = true;
            }
            futures::future::ready(cont)
        })
        .filter_map(move |item| {
            futures::future::ready(match item {
                Ok(v) => Some(v),
                Err(e) => f(e),
            })
        });
    Source { inner: stream.boxed() }
}

/// Map the error variant via `f`. Both `Ok` and `Err` continue
/// downstream; only the `Err` payload type changes.
///
/// Akka.NET: `Source.SelectError`.
pub fn map_error<T, E1, E2, F>(src: Source<Result<T, E1>>, mut f: F) -> Source<Result<T, E2>>
where
    T: Send + 'static,
    E1: Send + 'static,
    E2: Send + 'static,
    F: FnMut(E1) -> E2 + Send + 'static,
{
    let stream = src.into_boxed().map(move |item| match item {
        Ok(v) => Ok(v),
        Err(e) => Err(f(e)),
    });
    Source { inner: stream.boxed() }
}

/// Replace the upstream's tail with `replacement` upon the first
/// `Err(_)`. Pre-error `Ok(_)` values flow through unchanged.
///
/// Akka.NET: `Source.RecoverWithRetries(maxAttempts, …)` with
/// `maxAttempts = 1` (multi-attempt retry waits on the
/// `RestartSource` machinery — Phase 12 follow-on).
pub fn recover_with<T, E>(
    src: Source<Result<T, E>>,
    replacement: Source<T>,
) -> Source<T>
where
    T: Send + 'static,
    E: Send + 'static,
{
    use futures::stream;
    let mut tripped = false;
    let mut replacement_opt = Some(replacement);
    let inner = src.into_boxed();
    let stream = inner
        .flat_map(move |item| {
            if tripped {
                return stream::empty().boxed();
            }
            match item {
                Ok(v) => stream::iter(std::iter::once(v)).boxed(),
                Err(_) => {
                    tripped = true;
                    if let Some(rep) = replacement_opt.take() {
                        rep.into_boxed()
                    } else {
                        stream::empty().boxed()
                    }
                }
            }
        });
    Source { inner: stream.boxed() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::Sink;

    #[tokio::test]
    async fn recover_replaces_error_with_value_and_terminates() {
        let s: Source<Result<i32, &'static str>> =
            Source::from_iter(vec![Ok(1), Ok(2), Err("oops"), Ok(99)]);
        let recovered = recover(s, |_e| Some(0));
        let collected = Sink::collect(recovered).await;
        assert_eq!(collected, vec![1, 2, 0]);
    }

    #[tokio::test]
    async fn recover_with_none_drops_error() {
        let s: Source<Result<i32, &'static str>> =
            Source::from_iter(vec![Ok(1), Err("e"), Ok(2)]);
        let recovered = recover(s, |_| None);
        let collected = Sink::collect(recovered).await;
        assert_eq!(collected, vec![1]);
    }

    #[tokio::test]
    async fn recover_passes_through_when_no_error() {
        let s: Source<Result<i32, &'static str>> =
            Source::from_iter(vec![Ok(1), Ok(2), Ok(3)]);
        let recovered = recover(s, |_| Some(0));
        let collected = Sink::collect(recovered).await;
        assert_eq!(collected, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn map_error_changes_error_type() {
        #[derive(Debug, PartialEq)]
        struct Wrapped(String);
        let s: Source<Result<i32, &'static str>> =
            Source::from_iter(vec![Ok(1), Err("boom")]);
        let mapped = map_error(s, |e| Wrapped(e.to_string()));
        let collected = Sink::collect(mapped).await;
        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0], Ok(1));
        assert_eq!(collected[1], Err(Wrapped("boom".into())));
    }

    #[tokio::test]
    async fn recover_with_switches_to_replacement_on_error() {
        let s: Source<Result<i32, &'static str>> =
            Source::from_iter(vec![Ok(1), Ok(2), Err("e"), Ok(99)]);
        let replacement: Source<i32> = Source::from_iter(vec![100, 200]);
        let recovered = recover_with(s, replacement);
        let collected = Sink::collect(recovered).await;
        assert_eq!(collected, vec![1, 2, 100, 200]);
    }

    #[tokio::test]
    async fn recover_with_passes_through_when_no_error() {
        let s: Source<Result<i32, &'static str>> =
            Source::from_iter(vec![Ok(1), Ok(2)]);
        let replacement: Source<i32> = Source::from_iter(vec![100]);
        let recovered = recover_with(s, replacement);
        let collected = Sink::collect(recovered).await;
        assert_eq!(collected, vec![1, 2]);
    }
}
