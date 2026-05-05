//! Out-of-process `MultiNodeSpec`. akka.net analog:
//! `Akka.Remote.TestKit.MultiNodeSpec` with a controller process and
//! N child processes coordinated over the loopback transport.
//!
//! The line protocol is intentionally trivial (one ASCII command per
//! line) so a child written in any language could join the rendezvous.
//! Each barrier label is a separate sync point: every node calls
//! `barrier(label)`; the controller blocks until N nodes have arrived
//! on that label, then echoes `OK <label>` to each.
//!
//! Wire protocol (all `\n`-terminated UTF-8):
//!   child → controller   `BARRIER <label>`
//!   controller → child   `OK <label>` once N have arrived
//!   controller → child   `TIMEOUT <label>` if the controller's
//!                        per-barrier timer fires first
//!
//! The harness purposely does not impose any actor-system or runtime
//! contract on the child side — a child is just "code that connects
//! to a TCP port and exchanges barrier labels". Tests pair this with
//! whatever node bootstrapping their assertion needs.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MultiNodeOopError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("controller already shut down")]
    ControllerDown,
    #[error("barrier `{label}` timed out at controller (got {got}/{expected})")]
    BarrierTimeout { label: String, got: usize, expected: usize },
    #[error("malformed line from peer: {0}")]
    Malformed(String),
    #[error("unexpected reply: {0}")]
    UnexpectedReply(String),
}

/// Per-label rendezvous state on the controller.
struct LabelState {
    expected: usize,
    notify: Arc<Notify>,
    /// Senders waiting to be notified; we send a single byte ('O' or
    /// 'T') so the child handler can emit the appropriate response.
    waiters: Vec<oneshot::Sender<bool>>,
    arrived: usize,
    completed: bool,
}

/// Out-of-process barrier controller. Bind it on the test driver,
/// then point the children at `local_addr()` (e.g. via env var).
pub struct MultiNodeOopController {
    addr: SocketAddr,
    inner: Arc<ControllerInner>,
    handle: JoinHandle<()>,
}

struct ControllerInner {
    expected: usize,
    labels: Mutex<HashMap<String, LabelState>>,
}

impl MultiNodeOopController {
    /// Start the controller with the expected node count. Listens on
    /// `127.0.0.1:0` (kernel-assigned port). The accepted address can
    /// be read via `local_addr()`.
    pub async fn start(expected_nodes: usize) -> Result<Self, MultiNodeOopError> {
        assert!(expected_nodes >= 1, "expected_nodes must be ≥ 1");
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let inner = Arc::new(ControllerInner { expected: expected_nodes, labels: Mutex::new(HashMap::new()) });
        let inner_a = inner.clone();
        let handle = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((s, _)) => {
                        let i = inner_a.clone();
                        tokio::spawn(async move {
                            handle_child(s, i).await;
                        });
                    }
                    Err(_) => return,
                }
            }
        });
        Ok(Self { addr, inner, handle })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Time-bound a label: if the requested label has not been
    /// reached by `timeout`, every connected child waiter receives
    /// `TIMEOUT label`. Returns the count of arrivals when the timer
    /// fired.
    pub async fn timeout_barrier(&self, label: &str, timeout: Duration) -> Result<usize, MultiNodeOopError> {
        tokio::time::sleep(timeout).await;
        let mut g = self.inner.labels.lock();
        let state = g.entry(label.to_string()).or_insert_with(|| LabelState {
            expected: self.inner.expected,
            notify: Arc::new(Notify::new()),
            waiters: Vec::new(),
            arrived: 0,
            completed: false,
        });
        if state.completed {
            return Ok(state.arrived);
        }
        // Trigger every waiter with `false` (timeout).
        let arrived = state.arrived;
        for w in state.waiters.drain(..) {
            let _ = w.send(false);
        }
        state.completed = true;
        if arrived < state.expected {
            return Err(MultiNodeOopError::BarrierTimeout {
                label: label.into(),
                got: arrived,
                expected: state.expected,
            });
        }
        Ok(arrived)
    }

    /// Stop accepting new connections and drop the listener task.
    /// Pending child connections continue but new BARRIERs against a
    /// shut-down controller will fail.
    pub fn shutdown(self) {
        self.handle.abort();
    }
}

async fn handle_child(stream: TcpStream, inner: Arc<ControllerInner>) {
    let (r, mut w) = stream.into_split();
    let mut lines = BufReader::new(r).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let trimmed = line.trim().to_string();
        if let Some(label) = trimmed.strip_prefix("BARRIER ") {
            let rx = enroll(&inner, label);
            // Wait for the rendezvous resolution.
            let outcome = match rx.await {
                Ok(true) => format!("OK {label}\n"),
                Ok(false) => format!("TIMEOUT {label}\n"),
                Err(_) => format!("TIMEOUT {label}\n"),
            };
            if w.write_all(outcome.as_bytes()).await.is_err() {
                return;
            }
        }
    }
}

fn enroll(inner: &Arc<ControllerInner>, label: &str) -> oneshot::Receiver<bool> {
    let (tx, rx) = oneshot::channel();
    let mut g = inner.labels.lock();
    let state = g.entry(label.to_string()).or_insert_with(|| LabelState {
        expected: inner.expected,
        notify: Arc::new(Notify::new()),
        waiters: Vec::new(),
        arrived: 0,
        completed: false,
    });
    if state.completed {
        // Already-completed label: responder will see we've moved on.
        let _ = tx.send(true);
        return rx;
    }
    state.arrived += 1;
    state.waiters.push(tx);
    if state.arrived >= state.expected {
        // Trigger every waiter with `true` (success).
        for w in state.waiters.drain(..) {
            let _ = w.send(true);
        }
        state.completed = true;
    }
    rx
}

/// Child-side handle. Construct one per node by passing the
/// controller's `local_addr()`.
pub struct MultiNodeOopNode {
    stream: tokio::sync::Mutex<TcpStream>,
}

impl MultiNodeOopNode {
    pub async fn connect(controller: SocketAddr) -> Result<Self, MultiNodeOopError> {
        let s = TcpStream::connect(controller).await?;
        s.set_nodelay(true)?;
        Ok(Self { stream: tokio::sync::Mutex::new(s) })
    }

    /// Block until every node has arrived on `label`, or until the
    /// controller's timer fires first. Returns Ok on success and
    /// `BarrierTimeout` on failure.
    pub async fn barrier(&self, label: &str) -> Result<(), MultiNodeOopError> {
        let mut g = self.stream.lock().await;
        g.write_all(format!("BARRIER {label}\n").as_bytes()).await?;
        let mut buf = String::new();
        let mut reader = BufReader::new(&mut *g);
        reader.read_line(&mut buf).await?;
        let trimmed = buf.trim();
        if let Some(rest) = trimmed.strip_prefix("OK ") {
            if rest == label {
                return Ok(());
            }
        }
        if let Some(rest) = trimmed.strip_prefix("TIMEOUT ") {
            return Err(MultiNodeOopError::BarrierTimeout {
                label: rest.to_string(),
                got: 0,
                expected: 0,
            });
        }
        Err(MultiNodeOopError::UnexpectedReply(trimmed.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_nodes_meet_at_barrier() {
        let ctrl = MultiNodeOopController::start(3).await.unwrap();
        let addr = ctrl.local_addr();

        let mut handles = Vec::new();
        for _ in 0..3 {
            handles.push(tokio::spawn(async move {
                let n = MultiNodeOopNode::connect(addr).await.unwrap();
                n.barrier("converged").await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn barrier_times_out_when_only_some_arrive() {
        let ctrl = MultiNodeOopController::start(3).await.unwrap();
        let addr = ctrl.local_addr();
        let label = "incomplete";

        // Only two of three nodes arrive.
        let h1 = tokio::spawn(async move {
            let n = MultiNodeOopNode::connect(addr).await.unwrap();
            let _ = n.barrier(label).await;
        });
        let h2 = tokio::spawn(async move {
            let n = MultiNodeOopNode::connect(addr).await.unwrap();
            let _ = n.barrier(label).await;
        });

        // Drive the controller's timer.
        let to = ctrl.timeout_barrier(label, Duration::from_millis(50)).await;
        // We expect a timeout error because only 2/3 arrived.
        assert!(matches!(to, Err(MultiNodeOopError::BarrierTimeout { .. })));

        let _ = h1.await;
        let _ = h2.await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multiple_independent_labels() {
        let ctrl = MultiNodeOopController::start(2).await.unwrap();
        let addr = ctrl.local_addr();

        let h1 = tokio::spawn(async move {
            let n = MultiNodeOopNode::connect(addr).await.unwrap();
            n.barrier("phase-a").await.unwrap();
            n.barrier("phase-b").await.unwrap();
        });
        let h2 = tokio::spawn(async move {
            let n = MultiNodeOopNode::connect(addr).await.unwrap();
            n.barrier("phase-a").await.unwrap();
            n.barrier("phase-b").await.unwrap();
        });
        h1.await.unwrap();
        h2.await.unwrap();
    }

    #[tokio::test]
    async fn controller_addr_is_loopback() {
        let ctrl = MultiNodeOopController::start(1).await.unwrap();
        let addr = ctrl.local_addr();
        assert!(addr.ip().is_loopback());
        assert_ne!(addr.port(), 0);
    }
}
