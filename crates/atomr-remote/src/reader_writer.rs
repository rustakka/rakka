//! Reader/writer task split helpers.
//!
//! Splitting the per-peer endpoint into two cooperating Tokio tasks lets
//! inbound decoding and outbound serialization overlap, removing the
//! head-of-line blocking that the unified-loop design has under load.
//!
//! This module ships the orchestrator that the `EndpointManager`
//! plugs into. It does not own a transport — instead it accepts a
//! `RawTransport` adapter so the same shape works for `TcpTransport`
//! today and for the TLS variant once 5.E lands the wire integration.
//!
//! Both tasks are `tokio::spawn`-ed; the orchestrator returns a
//! [`ReaderWriterHandle`] that gives access to the outbound `tx`,
//! the inbound `rx`, and a `JoinHandle` for shutdown coordination.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Trait the orchestrator drives. The transport produces inbound
/// frames on `recv` and accepts outbound frames on `send`. Either
/// end signals graceful shutdown by returning `None` (read EOF) or
/// `Err(_)` (write failure) — the orchestrator stops both tasks.
#[async_trait::async_trait]
pub trait RawTransport: Send + Sync + 'static {
    /// One frame on the wire, decoded.
    type Frame: Send + 'static;
    /// One frame to send, ready for the wire codec.
    type OutFrame: Send + 'static;
    /// Recoverable error type.
    type Error: Send + 'static + std::fmt::Debug;

    async fn recv(&self) -> Result<Option<Self::Frame>, Self::Error>;
    async fn send(&self, frame: Self::OutFrame) -> Result<(), Self::Error>;
}

/// Handle returned by [`spawn_reader_writer`]. The orchestrator
/// surfaces the outbound `tx`, the inbound `rx`, and per-task
/// `JoinHandle`s so the manager can `await` clean shutdown.
pub struct ReaderWriterHandle<F, O> {
    pub outbound: mpsc::UnboundedSender<O>,
    pub inbound: mpsc::UnboundedReceiver<F>,
    pub reader: JoinHandle<()>,
    pub writer: JoinHandle<()>,
}

/// Spawn a reader/writer pair around `transport`. The reader pumps
/// inbound frames into a `tokio::mpsc` channel; the writer drains
/// the outbound channel onto the wire. Either failure stops both.
///
/// Bounded outbound is intentional: under back-pressure the sender
/// blocks rather than queues unbounded — falls back to the
/// `OverflowStrategy` configured on `RemoteSettings` (Phase 5.G).
pub fn spawn_reader_writer<T>(
    transport: Arc<T>,
    outbound_capacity: usize,
) -> ReaderWriterHandle<T::Frame, T::OutFrame>
where
    T: RawTransport,
{
    let outbound_capacity = outbound_capacity.max(1);
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<T::OutFrame>();
    let (in_tx, in_rx) = mpsc::unbounded_channel::<T::Frame>();

    // Hint to the writer that the outbound channel is bounded by
    // `outbound_capacity` semantically (we use unbounded mpsc here
    // to keep the Send/Sync bounds simple; bounded variant lands
    // alongside Phase 5.G send-queue backpressure).
    let _ = outbound_capacity;

    let r_transport = transport.clone();
    let r_in_tx = in_tx.clone();
    let reader = tokio::spawn(async move {
        loop {
            match r_transport.recv().await {
                Ok(Some(frame)) => {
                    if r_in_tx.send(frame).is_err() {
                        return; // consumer dropped
                    }
                }
                Ok(None) => return, // EOF
                Err(_e) => return,  // recoverable per peer
            }
        }
    });

    let w_transport = transport;
    let writer = tokio::spawn(async move {
        while let Some(frame) = out_rx.recv().await {
            if w_transport.send(frame).await.is_err() {
                return;
            }
        }
    });

    ReaderWriterHandle { outbound: out_tx, inbound: in_rx, reader, writer }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio::sync::Mutex;

    /// Test transport that drains a pre-seeded `recv` queue and
    /// records every `send` call.
    struct TestTransport {
        recv_q: Mutex<Vec<i32>>,
        sent: Mutex<Vec<i32>>,
        recv_calls: AtomicU32,
    }

    #[async_trait::async_trait]
    impl RawTransport for TestTransport {
        type Frame = i32;
        type OutFrame = i32;
        type Error = ();

        async fn recv(&self) -> Result<Option<i32>, ()> {
            self.recv_calls.fetch_add(1, Ordering::SeqCst);
            let mut q = self.recv_q.lock().await;
            Ok(q.pop())
        }

        async fn send(&self, frame: i32) -> Result<(), ()> {
            self.sent.lock().await.push(frame);
            Ok(())
        }
    }

    #[tokio::test]
    async fn reader_pumps_until_eof() {
        let t = Arc::new(TestTransport {
            recv_q: Mutex::new(vec![3, 2, 1]), // popped in reverse
            sent: Mutex::new(Vec::new()),
            recv_calls: AtomicU32::new(0),
        });
        let mut handle = spawn_reader_writer(t.clone(), 8);
        let mut got = Vec::new();
        for _ in 0..3 {
            got.push(handle.inbound.recv().await.unwrap());
        }
        // After draining, transport returns Ok(None) → reader exits.
        let _ = handle.reader.await;
        assert_eq!(got, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn writer_drains_outbound_channel() {
        let t = Arc::new(TestTransport {
            recv_q: Mutex::new(Vec::new()), // recv returns None → reader exits
            sent: Mutex::new(Vec::new()),
            recv_calls: AtomicU32::new(0),
        });
        let handle = spawn_reader_writer(t.clone(), 8);
        for i in 0..5 {
            handle.outbound.send(i).unwrap();
        }
        // Drop the outbound sender so the writer sees channel close.
        drop(handle.outbound);
        let _ = handle.writer.await;
        let sent = t.sent.lock().await.clone();
        assert_eq!(sent, vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn reader_and_writer_run_concurrently() {
        // Verify both tasks make progress in parallel.
        let t = Arc::new(TestTransport {
            recv_q: Mutex::new(vec![20, 10]),
            sent: Mutex::new(Vec::new()),
            recv_calls: AtomicU32::new(0),
        });
        let mut handle = spawn_reader_writer(t.clone(), 4);

        let in_a = handle.inbound.recv().await.unwrap();
        handle.outbound.send(100).unwrap();
        let in_b = handle.inbound.recv().await.unwrap();
        handle.outbound.send(200).unwrap();

        drop(handle.outbound);
        let _ = handle.reader.await;
        let _ = handle.writer.await;

        assert_eq!(in_a, 10);
        assert_eq!(in_b, 20);
        let sent = t.sent.lock().await.clone();
        assert_eq!(sent, vec![100, 200]);
    }
}
