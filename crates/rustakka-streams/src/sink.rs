//! Sink — consumes a `Source`, produces a materialized value. akka.net: `Dsl/Sink.cs`.
//!
//! Each factory here returns a future that drives the source to completion and
//! produces the materialized value. These wrappers mirror the most common
//! Akka.Streams sinks (`Fold`, `Aggregate`, `Sum`, `First`, `Last`, `Seq`,
//! `ForEach`, `Ignore`) and add a lightweight `SinkQueue`.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::StreamExt;
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::source::Source;

pub struct Sink;

impl Sink {
    /// akka.net: `Fold` — drive the source and accumulate a single value.
    pub async fn fold<T, Acc, F>(source: Source<T>, init: Acc, mut f: F) -> Acc
    where
        T: Send + 'static,
        Acc: Send + 'static,
        F: FnMut(Acc, T) -> Acc + Send + 'static,
    {
        source
            .into_boxed()
            .fold(init, move |acc, x| futures::future::ready(f(acc, x)))
            .await
    }

    /// akka.net: `AggregateAsync` — async fold.
    pub async fn fold_async<T, Acc, F, Fut>(source: Source<T>, init: Acc, mut f: F) -> Acc
    where
        T: Send + 'static,
        Acc: Send + 'static,
        F: FnMut(Acc, T) -> Fut + Send + 'static,
        Fut: Future<Output = Acc> + Send + 'static,
    {
        source.into_boxed().fold(init, move |acc, x| f(acc, x)).await
    }

    /// akka.net: `Sink.Seq` — collect into a Vec.
    pub async fn collect<T>(source: Source<T>) -> Vec<T>
    where
        T: Send + 'static,
    {
        source.into_boxed().collect().await
    }

    /// akka.net: `Sink.First`.
    pub async fn first<T>(source: Source<T>) -> Option<T>
    where
        T: Send + 'static,
    {
        source.into_boxed().next().await
    }

    /// akka.net: `Sink.Last`.
    pub async fn last<T>(source: Source<T>) -> Option<T>
    where
        T: Send + 'static,
    {
        source
            .into_boxed()
            .fold(None, |_, x| async move { Some(x) })
            .await
    }

    /// akka.net: `Sink.Sum`.
    pub async fn sum<T>(source: Source<T>) -> T
    where
        T: Send + Default + std::ops::Add<Output = T> + 'static,
    {
        let init: T = T::default();
        Self::fold(source, init, |acc, x| acc + x).await
    }

    /// akka.net: `Sink.Count`.
    pub async fn count<T>(source: Source<T>) -> u64
    where
        T: Send + 'static,
    {
        Self::fold(source, 0u64, |acc, _| acc + 1).await
    }

    pub async fn for_each<T, F>(source: Source<T>, mut f: F)
    where
        T: Send + 'static,
        F: FnMut(T) + Send + 'static,
    {
        source
            .into_boxed()
            .for_each(move |x| {
                f(x);
                futures::future::ready(())
            })
            .await
    }

    /// akka.net: `Sink.ForEachAsync`.
    pub async fn for_each_async<T, F, Fut>(source: Source<T>, parallelism: usize, mut f: F)
    where
        T: Send + 'static,
        F: FnMut(T) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let p = parallelism.max(1);
        source
            .into_boxed()
            .for_each_concurrent(p, move |x| f(x))
            .await
    }

    /// akka.net: `Sink.Ignore`.
    pub async fn ignore<T: Send + 'static>(source: Source<T>) {
        source.into_boxed().for_each(|_| futures::future::ready(())).await
    }

    /// Send each element to an `UnboundedSender`. akka.net: `Sink.ActorRef`
    /// (rustakka equivalent uses an mpsc channel).
    pub async fn to_sender<T>(
        source: Source<T>,
        tx: tokio::sync::mpsc::UnboundedSender<T>,
    ) where
        T: Send + 'static,
    {
        let mut stream = source.into_boxed();
        while let Some(v) = stream.next().await {
            if tx.send(v).is_err() {
                break;
            }
        }
    }

    /// akka.net: `Sink.Queue` — run the source and expose a pull-based API.
    /// The returned `SinkQueue::pull` future returns `Ok(Some(t))` per element,
    /// `Ok(None)` after the stream completes.
    pub fn queue<T>(source: Source<T>) -> SinkQueue<T>
    where
        T: Send + 'static,
    {
        let buf: Arc<Mutex<SinkQueueState<T>>> = Arc::new(Mutex::new(SinkQueueState::default()));
        let notify = Arc::new(Notify::new());
        let buf_t = Arc::clone(&buf);
        let notify_t = Arc::clone(&notify);
        let handle = tokio::spawn(async move {
            let mut stream = source.into_boxed();
            while let Some(v) = stream.next().await {
                buf_t.lock().items.push_back(v);
                notify_t.notify_one();
            }
            buf_t.lock().complete = true;
            notify_t.notify_waiters();
        });
        SinkQueue { buf, notify, _handle: handle }
    }

    /// `Sink.Queue` with a bounded element timeout per pull.
    pub async fn pull_with_timeout<T: Send + 'static>(
        q: &SinkQueue<T>,
        t: Duration,
    ) -> Option<T> {
        tokio::time::timeout(t, q.pull()).await.ok().flatten()
    }
}

struct SinkQueueState<T> {
    items: std::collections::VecDeque<T>,
    complete: bool,
}

impl<T> Default for SinkQueueState<T> {
    fn default() -> Self {
        Self { items: std::collections::VecDeque::new(), complete: false }
    }
}

pub struct SinkQueue<T> {
    buf: Arc<Mutex<SinkQueueState<T>>>,
    notify: Arc<Notify>,
    _handle: tokio::task::JoinHandle<()>,
}

impl<T: Send + 'static> SinkQueue<T> {
    /// Pull the next element, awaiting as long as the source is still running.
    /// Returns `None` once the source completes.
    pub async fn pull(&self) -> Option<T> {
        loop {
            {
                let mut guard = self.buf.lock();
                if let Some(v) = guard.items.pop_front() {
                    return Some(v);
                }
                if guard.complete {
                    return None;
                }
            }
            self.notify.notified().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn first_last_sum_count() {
        assert_eq!(Sink::first(Source::from_iter(vec![1, 2, 3])).await, Some(1));
        assert_eq!(Sink::last(Source::from_iter(vec![1, 2, 3])).await, Some(3));
        assert_eq!(Sink::sum(Source::from_iter(1..=10_i32)).await, 55);
        assert_eq!(Sink::count(Source::from_iter(0..42_u64)).await, 42);
    }

    #[tokio::test]
    async fn for_each_async_runs_all_tasks() {
        let sum = std::sync::Arc::new(std::sync::Mutex::new(0i32));
        let sum_c = sum.clone();
        Sink::for_each_async(Source::from_iter(1..=5), 2, move |v| {
            let sum_c = sum_c.clone();
            async move {
                *sum_c.lock().unwrap() += v;
            }
        })
        .await;
        assert_eq!(*sum.lock().unwrap(), 15);
    }

    #[tokio::test]
    async fn sink_queue_pulls_until_complete() {
        let q = Sink::queue(Source::from_iter(vec![10, 20, 30]));
        assert_eq!(q.pull().await, Some(10));
        assert_eq!(q.pull().await, Some(20));
        assert_eq!(q.pull().await, Some(30));
        assert_eq!(q.pull().await, None);
    }
}
