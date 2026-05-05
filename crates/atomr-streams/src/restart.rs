//! Restart combinators — re-run the inner graph on failure/completion.

use std::time::Duration;

use futures::stream::StreamExt;

use crate::source::Source;

#[derive(Debug, Clone, Copy)]
pub struct RestartSettings {
    pub min_backoff: Duration,
    pub max_backoff: Duration,
    pub random_factor: f64,
    pub max_restarts: Option<usize>,
}

impl Default for RestartSettings {
    fn default() -> Self {
        Self {
            min_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            random_factor: 0.0,
            max_restarts: Some(5),
        }
    }
}

pub struct RestartSource;

impl RestartSource {
    /// Re-subscribe to the source returned by the factory after it completes
    /// (and every element it produced has been emitted). Equivalent to
    /// `RestartSource.WithBackoff` when combined with the built-in backoff.
    pub fn with_backoff<T, F>(settings: RestartSettings, factory: F) -> Source<T>
    where
        T: Send + 'static,
        F: FnMut() -> Source<T> + Send + 'static,
    {
        let state = RestartState { factory, settings, attempts: 0 };
        let s = futures::stream::unfold(
            (state, None::<futures::stream::BoxStream<'static, T>>),
            |(mut state, current)| async move {
                // Lazily open a subscription.
                let mut stream = match current {
                    Some(s) => s,
                    None => state.next_stream().await?,
                };
                if let Some(v) = stream.next().await {
                    Some((v, (state, Some(stream))))
                } else {
                    // Completed; check restart policy.
                    let maybe_next = state.next_stream().await;
                    match maybe_next {
                        Some(mut s) => s.next().await.map(|v| (v, (state, Some(s)))),
                        None => None,
                    }
                }
            },
        )
        .boxed();
        Source { inner: s }
    }
}

struct RestartState<T, F>
where
    F: FnMut() -> Source<T> + Send + 'static,
{
    factory: F,
    settings: RestartSettings,
    attempts: usize,
}

impl<T, F> RestartState<T, F>
where
    T: Send + 'static,
    F: FnMut() -> Source<T> + Send + 'static,
{
    async fn next_stream(&mut self) -> Option<futures::stream::BoxStream<'static, T>> {
        if let Some(limit) = self.settings.max_restarts {
            if self.attempts >= limit {
                return None;
            }
        }
        if self.attempts > 0 {
            let base = self.settings.min_backoff.as_millis() as u64;
            let cap = self.settings.max_backoff.as_millis() as u64;
            let back = (base.saturating_mul(1 << self.attempts.min(20))).min(cap.max(base));
            tokio::time::sleep(Duration::from_millis(back)).await;
        }
        self.attempts += 1;
        let src = (self.factory)();
        Some(src.into_boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::Sink;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn restart_source_resubscribes_until_max() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_c = calls.clone();
        let settings = RestartSettings {
            min_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(5),
            random_factor: 0.0,
            max_restarts: Some(3),
        };
        let source = RestartSource::with_backoff(settings, move || {
            calls_c.fetch_add(1, Ordering::SeqCst);
            crate::source::Source::from_iter(vec![1])
        });
        let out = Sink::collect(source).await;
        assert_eq!(out, vec![1, 1, 1]);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }
}
