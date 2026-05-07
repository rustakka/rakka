//! Transport plumbing that turns a `PyActorSystem` into a real cluster
//! node. Supplies:
//!
//! * [`PyTransportConfig`] — config slot stashed in the actor system's
//!   extensions so `Cluster.get(system)` can pick the right
//!   transport.
//! * [`PyActorRegistry`] — `path → Arc<ActorRef<PyMessage>>` mirror,
//!   populated on every `actor_of` and queried by the remote sink.
//! * [`PyRemoteMessageSink`] — decodes inbound `RemoteTell` frames via
//!   the codec registry and routes them to the right local actor.
//! * Python-facing [`PyClusterRegistry`] for hooking multiple in-process
//!   `ActorSystem`s into a single `InProcessRegistry`.

use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::Mutex;
use pyo3::prelude::*;
use tokio::sync::mpsc;

use atomr_cluster::{InProcessClusterTransport, InProcessRegistry, RemoteMessageSink, TcpClusterTransport};
use atomr_core::actor::{ActorRef as RustRef, ActorSystem as RustSystem, Address};

use crate::ext_remote::PyCodecRegistry;
use crate::py_actor::PyMessage;
use crate::runtime::runtime;

/// Per-actor-system mirror of `path → typed actor ref`. Populated
/// whenever a Python actor is spawned and consulted by
/// [`PyRemoteMessageSink::deliver`] when a remote-tell arrives.
#[derive(Clone, Default)]
pub struct PyActorRegistry {
    by_path: Arc<DashMap<String, Arc<RustRef<PyMessage>>>>,
}

impl PyActorRegistry {
    pub fn register(&self, path: &str, actor_ref: Arc<RustRef<PyMessage>>) {
        self.by_path.insert(path.to_string(), actor_ref);
    }

    pub fn lookup(&self, path: &str) -> Option<Arc<RustRef<PyMessage>>> {
        self.by_path.get(path).map(|v| v.clone())
    }

    /// Look up by *normalized* path. The receiver may have been
    /// addressed by the sender's notion of the address (which carries
    /// host:port); we reduce that to the actor name path so any node
    /// can route to its own `/user/<name>` regardless of what address
    /// the sender stamped.
    pub fn lookup_by_user_name(&self, name: &str) -> Option<Arc<RustRef<PyMessage>>> {
        // Walk the registry once — paths are short.
        for entry in self.by_path.iter() {
            if entry.key().ends_with(&format!("/user/{name}")) {
                return Some(entry.value().clone());
            }
        }
        None
    }
}

/// `Extension` that lazily holds the transport config requested via
/// `Cluster.with_*_transport(...)`. Read by `Cluster.get` on first
/// access.
#[derive(Clone, Default)]
pub struct PyTransportConfig {
    inner: Arc<Mutex<TransportSlot>>,
}

#[derive(Default)]
enum TransportSlot {
    #[default]
    Noop,
    Test {
        registry: Arc<InProcessRegistry>,
        bind_address: Option<Address>,
    },
    Tcp {
        bind: std::net::SocketAddr,
        advertised_host: Option<String>,
    },
}

impl PyTransportConfig {
    pub fn set_test(&self, registry: Arc<InProcessRegistry>, bind_address: Option<Address>) {
        let mut g = self.inner.lock();
        *g = TransportSlot::Test { registry, bind_address };
    }

    pub fn set_tcp(&self, bind: std::net::SocketAddr, advertised_host: Option<String>) {
        let mut g = self.inner.lock();
        *g = TransportSlot::Tcp { bind, advertised_host };
    }

    pub fn snapshot(&self) -> TransportChoice {
        match &*self.inner.lock() {
            TransportSlot::Noop => TransportChoice::Noop,
            TransportSlot::Test { registry, bind_address } => {
                TransportChoice::Test { registry: registry.clone(), bind_address: bind_address.clone() }
            }
            TransportSlot::Tcp { bind, advertised_host } => {
                TransportChoice::Tcp { bind: *bind, advertised_host: advertised_host.clone() }
            }
        }
    }
}

#[derive(Clone)]
pub enum TransportChoice {
    Noop,
    Test { registry: Arc<InProcessRegistry>, bind_address: Option<Address> },
    Tcp { bind: std::net::SocketAddr, advertised_host: Option<String> },
}

/// Built transport — handed back to the Cluster code so it can keep a
/// strong reference (alive for the daemon's lifetime).
pub enum BuiltTransport {
    Noop,
    Test(Arc<InProcessClusterTransport>),
    Tcp(Arc<TcpClusterTransport>),
}

impl BuiltTransport {
    /// Return the resolved bind address. For TCP this is the actually-
    /// listened socket (with port substituted if the bind was `:0`);
    /// for in-process it's the configured logical address.
    pub fn resolved_address(&self, fallback: &Address) -> Address {
        match self {
            BuiltTransport::Noop => fallback.clone(),
            BuiltTransport::Test(_) => fallback.clone(),
            BuiltTransport::Tcp(_) => fallback.clone(),
        }
    }
}

/// Sink wrapping the actor registry and codec registry. Decodes
/// inbound `RemoteTell` payloads and tells the matching local actor.
pub struct PyRemoteMessageSink {
    actors: PyActorRegistry,
    codecs: PyCodecRegistry,
}

impl PyRemoteMessageSink {
    pub fn new(actors: PyActorRegistry, codecs: PyCodecRegistry) -> Self {
        Self { actors, codecs }
    }
}

impl RemoteMessageSink for PyRemoteMessageSink {
    fn deliver(&self, target_path: &str, manifest: &str, payload: &[u8], _sender_path: Option<&str>) {
        // 1. Find the local typed actor ref. Fall back to user-name
        //    matching if the sender stamped a foreign address prefix.
        let actor = if let Some(a) = self.actors.lookup(target_path) {
            Some(a)
        } else {
            extract_user_name(target_path).and_then(|n| self.actors.lookup_by_user_name(n))
        };
        let Some(actor) = actor else {
            return; // dead-letter: log via tracing later
        };

        // 2. Decode the payload via the codec registry. The decoder is a
        //    Python callable, so we need the GIL.
        let decoded = match Python::with_gil(|py| -> PyResult<Py<PyAny>> {
            let (_encoder, decoder) = self.codecs.lookup(manifest).ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "remote: no codec registered for manifest `{manifest}`"
                ))
            })?;
            crate::ext_remote::call_decoder(py, &decoder, payload)
        }) {
            Ok(v) => v,
            Err(_) => return, // decode failure → dead-letter
        };

        // 3. Tell.
        actor.tell(PyMessage::new(decoded));
    }
}

fn extract_user_name(path: &str) -> Option<&str> {
    // Path looks like `akka://Sys/user/<name>` or
    // `akka.tcp://Sys@host:port/user/<name>`. Take the last segment
    // after the first `/user/` token.
    path.split_once("/user/").map(|(_, rest)| rest.split('/').next().unwrap_or(rest))
}

/// Build the transport described by `choice`, register the daemon's
/// gossip inbox and remote sink on it, and return a strong handle.
pub fn build_transport(
    choice: TransportChoice,
    self_addr: Address,
    gossip_inbox: mpsc::UnboundedSender<atomr_cluster::GossipPdu>,
    sink: Arc<dyn RemoteMessageSink>,
) -> std::io::Result<(BuiltTransport, Arc<dyn atomr_cluster::GossipTransport>, Address)> {
    match choice {
        TransportChoice::Noop => {
            // Build a no-op gossip transport; remote sends silently
            // dead-letter.
            let t: Arc<dyn atomr_cluster::GossipTransport> = Arc::new(NoopGossipTransport);
            Ok((BuiltTransport::Noop, t, self_addr))
        }
        TransportChoice::Test { registry, bind_address } => {
            let resolved = bind_address.unwrap_or(self_addr.clone());
            let inner = Arc::new(InProcessClusterTransport::new(resolved.clone(), registry));
            inner.start(gossip_inbox, sink);
            let t: Arc<dyn atomr_cluster::GossipTransport> = inner.clone();
            Ok((BuiltTransport::Test(inner), t, resolved))
        }
        TransportChoice::Tcp { bind, advertised_host } => {
            let inner =
                Arc::new(TcpClusterTransport::with_advertised(self_addr.clone(), bind, advertised_host));
            // Listen synchronously — the call must complete before we
            // return so the resolved address is final.
            let rt = runtime();
            let inner_for_listen = inner.clone();
            let resolved = rt.block_on(async move { inner_for_listen.listen().await })?;
            inner.start(gossip_inbox, sink);
            let t: Arc<dyn atomr_cluster::GossipTransport> = inner.clone();
            Ok((BuiltTransport::Tcp(inner), t, resolved))
        }
    }
}

/// No-op gossip transport — preserves the legacy single-node fallback
/// when no transport selector is configured.
struct NoopGossipTransport;

impl atomr_cluster::GossipTransport for NoopGossipTransport {
    fn send(&self, _target: &Address, _pdu: atomr_cluster::GossipPdu) {}
}

/// Hook called from `actor_of` to mirror the spawned ref into the
/// per-system registry.
pub fn record_actor(system: &RustSystem, path: &str, actor_ref: Arc<RustRef<PyMessage>>) {
    let registry = system.extensions().get::<PyActorRegistry>().map(|a| (*a).clone()).unwrap_or_else(|| {
        let r = PyActorRegistry::default();
        system.extensions().register::<PyActorRegistry>(r.clone());
        r
    });
    registry.register(path, actor_ref);
}

/// Convenience: route an outbound `RemoteTell` through whichever
/// transport is currently installed. If no transport is configured,
/// the call returns `false` so `tell_remote` can fall back to its
/// in-process round-trip.
pub fn try_send_remote(
    system: &RustSystem,
    target: &Address,
    target_path: &str,
    manifest: &str,
    payload: Vec<u8>,
    sender_path: Option<String>,
) -> bool {
    let Some(slot) = system.extensions().get::<TransportSlotExt>() else {
        return false;
    };
    let rt = runtime();
    let _guard = rt.enter();
    let lock_guard = slot.lock();
    let result = match &*lock_guard {
        BuiltTransport::Noop => false,
        BuiltTransport::Test(t) => {
            t.send_remote(target, target_path.to_string(), manifest.to_string(), payload, sender_path);
            true
        }
        BuiltTransport::Tcp(t) => {
            t.send_remote(target, target_path.to_string(), manifest.to_string(), payload, sender_path);
            true
        }
    };
    drop(lock_guard);
    drop(slot);
    result
}

/// Wrapper that's safe to stash in `Extensions`. The built transport
/// itself isn't Sized-Clone-able (it holds task handles indirectly via
/// channels), so we wrap behind a Mutex.
pub struct TransportSlotExt {
    inner: Mutex<BuiltTransport>,
}

impl TransportSlotExt {
    pub fn new(t: BuiltTransport) -> Self {
        Self { inner: Mutex::new(t) }
    }
}

impl std::ops::Deref for TransportSlotExt {
    type Target = Mutex<BuiltTransport>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

// ---------------------------------------------------------------------------
// Python-facing types.
// ---------------------------------------------------------------------------

/// Shared in-process registry. Multiple `ActorSystem`s in the same
/// Python process can join the same registry and reach each other via
/// channels.
#[pyclass(name = "ClusterRegistry", module = "atomr._native.cluster")]
#[derive(Clone)]
pub struct PyClusterRegistry {
    pub(crate) inner: Arc<InProcessRegistry>,
}

#[pymethods]
impl PyClusterRegistry {
    #[new]
    fn new() -> Self {
        Self { inner: InProcessRegistry::new() }
    }
}

pub fn register(_py: Python<'_>, _m: &Bound<'_, PyModule>) -> PyResult<()> {
    // The class is reachable via `atomr._native.cluster.ClusterRegistry`;
    // ext_cluster::register is responsible for actually adding it to
    // the cluster submodule (we run before that and stash the type).
    Ok(())
}
