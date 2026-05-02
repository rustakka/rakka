//! `MultiNodeSpec` — shared-barrier harness for multi-node tests.
//!
//! Phase 4 of `docs/full-port-plan.md`. Akka.NET's
//! `Akka.Remote.TestKit.MultiNodeSpec` spawns N OS processes and
//! synchronizes them via a controller; we instead spawn N
//! `ActorSystem`s in the same Tokio runtime (each on a distinct
//! local address/port) and synchronize them via in-process barriers.
//! That covers the cluster / sharding / persistence integration
//! suites without needing a separate test runner.
//!
//! For genuine OS-process isolation (TCP loopback, real sockets),
//! `MultiNodeSpec` exposes `node_address(i)` so callers can ship
//! that into a `RemoteSystem` builder. The Phase 5 remote-depth pass
//! adds a real cross-process variant on top.
//!
//! Typical pattern:
//!
//! ```no_run
//! # use std::time::Duration;
//! # use rakka_testkit::MultiNodeSpec;
//! # async fn run() {
//! let spec = MultiNodeSpec::new("ClusterTest", 3);
//! let nodes = spec.boot().await.unwrap();
//! // ...do work on each node...
//! spec.barrier("converged", Duration::from_secs(2)).await.unwrap();
//! spec.shutdown(nodes).await;
//! # }
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rakka_config::Config;
use rakka_core::actor::{ActorSystem, ActorSystemError};
use thiserror::Error;
use tokio::sync::Barrier;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MultiNodeError {
    #[error("failed to boot node `{name}`: {source}")]
    Boot {
        name: String,
        #[source]
        source: ActorSystemError,
    },
    #[error("barrier `{name}` timed out (got {got}/{expected})")]
    BarrierTimeout { name: String, got: usize, expected: usize },
}

/// Multi-node test specification.
pub struct MultiNodeSpec {
    name: String,
    node_count: usize,
    barriers: Arc<Mutex<HashMap<String, Arc<Barrier>>>>,
    arrivals: Arc<Mutex<HashMap<String, usize>>>,
}

impl MultiNodeSpec {
    pub fn new(name: impl Into<String>, node_count: usize) -> Self {
        assert!(node_count >= 1, "node_count must be ≥ 1");
        Self {
            name: name.into(),
            node_count,
            barriers: Arc::new(Mutex::new(HashMap::new())),
            arrivals: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn node_count(&self) -> usize {
        self.node_count
    }

    /// Synthesize a per-node identity. Real cross-process tests can
    /// derive a TCP address from this string.
    pub fn node_address(&self, i: usize) -> String {
        format!("{}@node-{}", self.name, i)
    }

    /// Boot `node_count` distinct in-process `ActorSystem`s. Each
    /// gets a name `"<spec>-N"`. The reference config is used
    /// because per-node config knobs come into play in Phase 6.
    pub async fn boot(&self) -> Result<Vec<ActorSystem>, MultiNodeError> {
        let mut out = Vec::with_capacity(self.node_count);
        for i in 0..self.node_count {
            let name = format!("{}-{}", self.name, i);
            let sys = ActorSystem::create(&name, Config::reference())
                .await
                .map_err(|e| MultiNodeError::Boot { name, source: e })?;
            out.push(sys);
        }
        Ok(out)
    }

    /// Each node calls `barrier(label, timeout)` with the same label;
    /// the future resolves once all `node_count` callers have arrived
    /// or `timeout` elapses (whichever is first).
    ///
    /// Backed by [`tokio::sync::Barrier`] per label; this avoids the
    /// `Notify::notify_waiters` race where late waiters miss an
    /// already-fired notification.
    pub async fn barrier(&self, label: &str, timeout: Duration) -> Result<(), MultiNodeError> {
        let bar = {
            let mut g = self.barriers.lock().unwrap();
            g.entry(label.to_string()).or_insert_with(|| Arc::new(Barrier::new(self.node_count))).clone()
        };
        {
            let mut a = self.arrivals.lock().unwrap();
            *a.entry(label.to_string()).or_insert(0) += 1;
        }
        match tokio::time::timeout(timeout, bar.wait()).await {
            Ok(_) => Ok(()),
            Err(_) => {
                let arrivals = *self.arrivals.lock().unwrap().get(label).unwrap_or(&0);
                Err(MultiNodeError::BarrierTimeout {
                    name: label.to_string(),
                    got: arrivals,
                    expected: self.node_count,
                })
            }
        }
    }

    /// Convenience: terminate every node booted by [`Self::boot`].
    pub async fn shutdown(&self, nodes: Vec<ActorSystem>) {
        for sys in nodes {
            sys.terminate().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn boot_three_nodes_and_barrier() {
        let spec = Arc::new(MultiNodeSpec::new("BarrierTest", 3));
        let nodes = spec.boot().await.unwrap();
        assert_eq!(nodes.len(), 3);

        let mut handles = Vec::new();
        for _ in 0..3 {
            let s = spec.clone();
            handles.push(tokio::spawn(async move {
                s.barrier("step1", Duration::from_secs(2)).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        spec.shutdown(nodes).await;
    }

    #[tokio::test]
    async fn barrier_times_out_when_only_some_arrive() {
        let spec = Arc::new(MultiNodeSpec::new("BarrierTimeoutTest", 3));
        let _ = spec.boot().await.unwrap();
        // Only 2 of 3 arrive — barrier must time out.
        let s2 = spec.clone();
        let h = tokio::spawn(async move { s2.barrier("only-two", Duration::from_millis(50)).await });
        spec.barrier("only-two", Duration::from_millis(50)).await.err();
        let r = h.await.unwrap();
        assert!(matches!(r, Err(MultiNodeError::BarrierTimeout { .. })));
    }

    #[test]
    fn node_addresses_are_distinct() {
        let s = MultiNodeSpec::new("X", 4);
        let addrs: Vec<String> = (0..4).map(|i| s.node_address(i)).collect();
        let unique: std::collections::HashSet<_> = addrs.iter().cloned().collect();
        assert_eq!(unique.len(), 4);
    }
}
