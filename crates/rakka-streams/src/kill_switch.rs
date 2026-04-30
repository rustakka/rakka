//! KillSwitch — external shutdown for a running stream. akka.net: `KillSwitch.cs`.
//!
//! `KillSwitch::shutdown()` completes every attached source; `abort(err)`
//! makes attached sources fail (modelled as early completion plus the
//! caller inspecting the latched error).

use std::sync::Arc;

use futures::stream::StreamExt;
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::source::Source;

#[derive(Clone)]
pub struct KillSwitch {
    inner: Arc<KillSwitchInner>,
}

struct KillSwitchInner {
    notify: Notify,
    state: Mutex<KillState>,
}

#[derive(Default, Clone)]
struct KillState {
    killed: bool,
    error: Option<String>,
}

impl Default for KillSwitch {
    fn default() -> Self {
        Self::new()
    }
}

impl KillSwitch {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(KillSwitchInner {
                notify: Notify::new(),
                state: Mutex::new(KillState::default()),
            }),
        }
    }

    /// Gracefully complete any sources attached via [`Self::flow`].
    pub fn shutdown(&self) {
        let mut s = self.inner.state.lock();
        s.killed = true;
        drop(s);
        self.inner.notify.notify_waiters();
    }

    /// Abort attached sources with the given error message.
    pub fn abort(&self, err: impl Into<String>) {
        let mut s = self.inner.state.lock();
        s.killed = true;
        s.error = Some(err.into());
        drop(s);
        self.inner.notify.notify_waiters();
    }

    pub fn is_shut_down(&self) -> bool {
        self.inner.state.lock().killed
    }

    pub fn error(&self) -> Option<String> {
        self.inner.state.lock().error.clone()
    }

    /// Wrap a source so it completes when this switch fires.
    pub fn flow<T: Send + 'static>(&self, source: Source<T>) -> Source<T> {
        let inner = Arc::clone(&self.inner);
        let s = futures::stream::unfold(
            (source.into_boxed(), inner),
            |(mut s, inner)| async move {
                if inner.state.lock().killed {
                    return None;
                }
                let next = {
                    let notified = inner.notify.notified();
                    tokio::pin!(notified);
                    tokio::select! {
                        biased;
                        _ = &mut notified => None,
                        item = s.next() => item,
                    }
                };
                next.map(|v| (v, (s, inner)))
            },
        )
        .boxed();
        Source { inner: s }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::Sink;
    use std::time::Duration;

    #[tokio::test]
    async fn kill_switch_completes_long_running_source() {
        let ks = KillSwitch::new();
        let src = Source::tick(Duration::from_millis(1), Duration::from_millis(1), 1_u32);
        let gated = ks.flow(src);
        let handle = tokio::spawn(async move { Sink::collect(gated).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        ks.shutdown();
        let out = handle.await.unwrap();
        assert!(out.len() < 10_000, "stream should complete after shutdown");
    }

    #[tokio::test]
    async fn abort_latches_error_message() {
        let ks = KillSwitch::new();
        ks.abort("boom");
        assert_eq!(ks.error().as_deref(), Some("boom"));
        assert!(ks.is_shut_down());
    }
}
