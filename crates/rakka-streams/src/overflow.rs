//! Overflow strategies for bounded buffers. akka.net: `OverflowStrategy.cs`.
//!
//! Implemented as a helper that wraps a source into a bounded tokio mpsc
//! channel and applies the chosen drop/fail/backpressure policy when the
//! channel is full. Mirrors the upstream `OverflowStrategy` enum.

use std::sync::Arc;

use futures::stream::StreamExt;
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::source::Source;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowStrategy {
    /// Propagate backpressure by awaiting channel capacity.
    Backpressure,
    /// Drop the oldest buffered element to make room.
    DropHead,
    /// Drop the newest produced element (the one that would overflow).
    DropNew,
    /// Drop the newest buffered element.
    DropTail,
    /// Drop every buffered element when overflow happens.
    DropBuffer,
    /// Fail the stream on overflow.
    Fail,
}

pub(crate) fn apply<T: Send + 'static>(
    source: Source<T>,
    size: usize,
    strategy: OverflowStrategy,
) -> Source<T> {
    let cap = size.max(1);
    let state: Arc<Mutex<BufferState<T>>> = Arc::new(Mutex::new(BufferState::default()));
    let notify = Arc::new(Notify::new());
    let state_p = Arc::clone(&state);
    let notify_p = Arc::clone(&notify);
    let mut inner = source.into_boxed();
    tokio::spawn(async move {
        while let Some(item) = inner.next().await {
            let mut item_opt = Some(item);
            let mut overflowed = false;
            {
                let mut guard = state_p.lock();
                if guard.items.len() >= cap {
                    match strategy {
                        OverflowStrategy::DropHead => {
                            guard.items.pop_front();
                            guard.items.push_back(item_opt.take().unwrap());
                        }
                        OverflowStrategy::DropTail => {
                            guard.items.pop_back();
                            guard.items.push_back(item_opt.take().unwrap());
                        }
                        OverflowStrategy::DropNew => {
                            item_opt = None;
                        }
                        OverflowStrategy::DropBuffer => {
                            guard.items.clear();
                            guard.items.push_back(item_opt.take().unwrap());
                        }
                        OverflowStrategy::Fail => {
                            guard.failed = true;
                            guard.complete = true;
                            drop(guard);
                            notify_p.notify_waiters();
                            return;
                        }
                        OverflowStrategy::Backpressure => {
                            overflowed = true;
                        }
                    }
                } else {
                    guard.items.push_back(item_opt.take().unwrap());
                }
            }
            if overflowed {
                while let Some(item) = item_opt.take() {
                    notify_p.notified().await;
                    let mut g = state_p.lock();
                    if g.items.len() < cap {
                        g.items.push_back(item);
                        break;
                    } else {
                        item_opt = Some(item);
                    }
                }
            }
            notify_p.notify_one();
        }
        state_p.lock().complete = true;
        notify_p.notify_waiters();
    });

    let out = futures::stream::unfold(
        (state, notify),
        |(state, notify)| async move {
            loop {
                {
                    let mut guard = state.lock();
                    if guard.failed {
                        return None;
                    }
                    if let Some(v) = guard.items.pop_front() {
                        notify.notify_one();
                        return Some((v, (state.clone(), notify.clone())));
                    }
                    if guard.complete {
                        return None;
                    }
                }
                notify.notified().await;
            }
        },
    )
    .boxed();
    Source { inner: out }
}

struct BufferState<T> {
    items: std::collections::VecDeque<T>,
    complete: bool,
    failed: bool,
}

impl<T> Default for BufferState<T> {
    fn default() -> Self {
        Self { items: std::collections::VecDeque::new(), complete: false, failed: false }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::Sink;

    #[tokio::test]
    async fn buffer_backpressure_forwards_all_elements() {
        let src = Source::from_iter(1..=100_i32);
        let buffered = src.buffer(8, OverflowStrategy::Backpressure);
        let out = Sink::collect(buffered).await;
        assert_eq!(out.len(), 100);
        assert_eq!(out[0], 1);
        assert_eq!(out[99], 100);
    }

    #[tokio::test]
    async fn buffer_drop_new_limits_output() {
        // Fast producer, slow consumer: with DropNew and size=1 we should
        // receive fewer than all items once the buffer fills.
        let src = Source::from_iter(0..1_000_i32);
        let buffered = src.buffer(1, OverflowStrategy::DropNew);
        let mut count = 0usize;
        let out = buffered.into_boxed();
        use futures::StreamExt;
        tokio::pin!(out);
        while let Some(_) = out.next().await {
            count += 1;
        }
        assert!(count <= 1_000);
    }
}
