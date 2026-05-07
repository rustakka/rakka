//! Cluster-sharding (Phase 6).
//!
//! Wires `atomr_cluster_sharding` into Python:
//!
//! * `MessageExtractor` — Rust struct holding three Python callables
//!   (`entity_id`, `shard_id`, `unwrap`) that route an inbound message
//!   to the correct shard + entity.
//! * `ShardingSettings` — allocation strategy, rebalance threshold,
//!   passivation TTL, remember-entities flag.
//! * `ShardRegion.start(system, type_name, entity_props, message_extractor,
//!   settings)` — spawns the region and binds it into the Phase 5
//!   cluster daemon (single-node today; multi-node deferred until the
//!   real gossip transport lands).
//! * `region.tell(msg)` / `region.entity_count()` /
//!   `region.request_passivation(entity_id)` /
//!   `region.shard_ids()` / `region.shutdown()`.
//!
//! Entity actors are real `PyActor`s spawned through the Phase 1 spawn
//! path under `/user/<type_name>-<sanitized-entity_id>`. They inherit
//! the region creator's interpreter pool by default; props can override
//! per `props(...)` like any other actor.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use atomr_cluster_sharding::{
    InMemoryRememberStore, MessageExtractor as MessageExtractorTrait, PassivationTracker,
    RememberEntitiesStore, ShardCoordinator, ShardRegion as RustShardRegion,
};
use atomr_core::actor::{ActorRef as RustRef, ActorSystem as RustSystem, Props as RustProps};
use atomr_core::supervision::SupervisorStrategy;

use crate::actor_ref::PyActorRef;
use crate::actor_system::{registry, PyActorSystem};
use crate::dispatcher;
use crate::interpreter::InterpreterQuota;
use crate::props::PyProps;
use crate::py_actor::{PyActor, PyMessage};
use crate::runtime::runtime;

// ---------------------------------------------------------------------------
// SendPyAny — local Send/Sync newtype with GIL-safe Drop & Clone.
// ---------------------------------------------------------------------------

/// `Py<PyAny>` newtype safe to ship through `Send + 'static` channels.
/// Drops and clones acquire the GIL.
pub struct SendPyAny(pub Py<PyAny>);

unsafe impl Send for SendPyAny {}
unsafe impl Sync for SendPyAny {}

impl SendPyAny {
    pub fn new(obj: Py<PyAny>) -> Self {
        Self(obj)
    }

    pub fn into_inner(self) -> Py<PyAny> {
        let inner = unsafe { std::ptr::read(&self.0) };
        std::mem::forget(self);
        inner
    }
}

impl Drop for SendPyAny {
    fn drop(&mut self) {
        Python::with_gil(|_py| {
            // Compiler-generated drop runs while we hold the GIL.
        });
    }
}

impl Clone for SendPyAny {
    fn clone(&self) -> Self {
        Python::with_gil(|py| SendPyAny(self.0.clone_ref(py)))
    }
}

// ---------------------------------------------------------------------------
// Python message extractor.
// ---------------------------------------------------------------------------

/// Holds the three Python callables and (optionally) an integer
/// `number_of_shards` so int-returning shard_id callables can be
/// moduloed.
struct PyExtractor {
    entity_id_fn: SendPyAny,
    shard_id_fn: SendPyAny,
    unwrap_fn: Option<SendPyAny>,
    number_of_shards: Option<u64>,
}

impl PyExtractor {
    fn new(
        entity_id_fn: Py<PyAny>,
        shard_id_fn: Py<PyAny>,
        unwrap_fn: Option<Py<PyAny>>,
        number_of_shards: Option<u64>,
    ) -> Self {
        Self {
            entity_id_fn: SendPyAny::new(entity_id_fn),
            shard_id_fn: SendPyAny::new(shard_id_fn),
            unwrap_fn: unwrap_fn.map(SendPyAny::new),
            number_of_shards,
        }
    }

    fn extract_entity_id(&self, py: Python<'_>, msg: &Py<PyAny>) -> PyResult<String> {
        let f = self.entity_id_fn.0.bind(py);
        let res = f.call1((msg.clone_ref(py),))?;
        if let Ok(s) = res.extract::<String>() {
            return Ok(s);
        }
        Ok(res.str().map(|s| s.to_string()).unwrap_or_default())
    }

    fn extract_shard_id(&self, py: Python<'_>, msg: &Py<PyAny>) -> PyResult<String> {
        let f = self.shard_id_fn.0.bind(py);
        let res = f.call1((msg.clone_ref(py),))?;
        if let Ok(s) = res.extract::<String>() {
            return Ok(s);
        }
        if let Ok(i) = res.extract::<i64>() {
            let n = self.number_of_shards.unwrap_or(16).max(1);
            let r = i.unsigned_abs() % n;
            return Ok(r.to_string());
        }
        Ok(res.str().map(|s| s.to_string()).unwrap_or_default())
    }

    fn extract_payload(&self, py: Python<'_>, msg: Py<PyAny>) -> PyResult<Py<PyAny>> {
        if let Some(unwrap) = &self.unwrap_fn {
            let f = unwrap.0.bind(py);
            Ok(f.call1((msg,))?.unbind())
        } else {
            Ok(msg)
        }
    }
}

/// Carries the message + cached shard_id + cached entity_id from the
/// region's enqueue site through to the entity dispatcher. We compute
/// (entity_id, shard_id) once under the GIL, then ferry the values
/// through the `cluster_sharding` `ShardRegion::deliver` path.
pub struct ShardEnvelope {
    payload: SendPyAny,
    entity_id: String,
    shard_id: String,
}

/// Bare-bones extractor on the Rust side: ids are pre-computed in
/// the envelope, so `entity_id` / `shard_id` are O(1) clones.
struct CachedExtractor;

impl MessageExtractorTrait for CachedExtractor {
    type Message = ShardEnvelope;

    fn entity_id(&self, m: &Self::Message) -> String {
        m.entity_id.clone()
    }

    fn shard_id(&self, m: &Self::Message) -> String {
        m.shard_id.clone()
    }
}

// ---------------------------------------------------------------------------
// ShardingSettings.
// ---------------------------------------------------------------------------

#[pyclass(name = "ShardingSettings", module = "atomr._native.cluster_sharding")]
#[derive(Clone)]
pub struct PyShardingSettings {
    pub allocation_strategy: String,
    pub rebalance_threshold: usize,
    pub max_simultaneous_rebalance: usize,
    pub passivation_idle_timeout: Option<f64>,
    pub remember_entities: bool,
    pub number_of_shards: Option<u64>,
}

#[pymethods]
impl PyShardingSettings {
    #[new]
    #[pyo3(signature = (
        allocation_strategy = "least-shards".to_string(),
        rebalance_threshold = 1,
        max_simultaneous_rebalance = 3,
        passivation_idle_timeout = None,
        remember_entities = false,
        number_of_shards = None,
    ))]
    fn new(
        allocation_strategy: String,
        rebalance_threshold: usize,
        max_simultaneous_rebalance: usize,
        passivation_idle_timeout: Option<f64>,
        remember_entities: bool,
        number_of_shards: Option<u64>,
    ) -> PyResult<Self> {
        match allocation_strategy.as_str() {
            "least-shards" | "pinned" => {}
            other => {
                return Err(PyValueError::new_err(format!(
                    "unknown allocation_strategy {other:?}: expected `least-shards` or `pinned`"
                )))
            }
        }
        Ok(Self {
            allocation_strategy,
            rebalance_threshold,
            max_simultaneous_rebalance,
            passivation_idle_timeout,
            remember_entities,
            number_of_shards,
        })
    }

    #[getter]
    fn allocation_strategy(&self) -> &str {
        &self.allocation_strategy
    }
    #[getter]
    fn rebalance_threshold(&self) -> usize {
        self.rebalance_threshold
    }
    #[getter]
    fn max_simultaneous_rebalance(&self) -> usize {
        self.max_simultaneous_rebalance
    }
    #[getter]
    fn passivation_idle_timeout(&self) -> Option<f64> {
        self.passivation_idle_timeout
    }
    #[getter]
    fn remember_entities(&self) -> bool {
        self.remember_entities
    }
    #[getter]
    fn number_of_shards(&self) -> Option<u64> {
        self.number_of_shards
    }
}

// ---------------------------------------------------------------------------
// ShardRegion.
// ---------------------------------------------------------------------------

/// State shared by the region front-end and the periodic passivation
/// sweep / shutdown coordination.
struct RegionInner {
    type_name: String,
    system: RustSystem,
    extractor: Arc<PyExtractor>,
    /// Per-entity-id active actor refs.
    entities: RwLock<HashMap<String, EntityHandle>>,
    /// Tracks last-activity per entity for passivation.
    passivation: Arc<PassivationTracker>,
    /// Optional remember-entities store + per-shard cache.
    remember: Option<RememberCtx>,
    /// Idle timeout, if passivation is enabled.
    idle_timeout: Option<Duration>,
    /// Coordinator + region id for cluster-sharding bookkeeping.
    coordinator: Arc<ShardCoordinator>,
    region_id: String,
    #[allow(dead_code)]
    strategy: AllocationStrategyKind,
    /// Props blueprint for entity actors.
    entity_factory: SendPyAny,
    entity_dispatcher: String,
    entity_role: String,
    /// True once `shutdown()` has been called; suppresses sweep loop.
    closed: AtomicBool,
    /// Per-entity-id incarnation counter. Bumped each time we
    /// respawn a previously-stopped entity so the underlying
    /// `system.actor_of` name doesn't collide with the lingering
    /// guardian entry. Names look like `<type_name>-<sanitized-id>`
    /// for the first incarnation in the first region instance, and
    /// `<type_name>-<sanitized-id>__r<region_inst>__inc<n>` for
    /// later ones.
    incarnations: RwLock<HashMap<String, u32>>,
    /// Per-system-and-type counter that lets multiple `ShardRegion`
    /// instances share an `ActorSystem` without clashing on
    /// `system.actor_of` names. The lingering user-guardian entry from
    /// a previous region's stopped actors keeps the bare name in use,
    /// so the second region must use a distinct suffix.
    region_instance: u32,
}

#[derive(Clone)]
#[allow(dead_code)]
enum AllocationStrategyKind {
    LeastShards { rebalance_threshold: usize, max_simultaneous_rebalance: usize },
    Pinned { region: String },
}

#[derive(Clone)]
struct EntityHandle {
    actor_ref: Arc<RustRef<PyMessage>>,
    name: String,
}

struct RememberCtx {
    store: Arc<dyn RememberEntitiesStore>,
    /// Per-shard set of remembered entity ids (book-keeping).
    seen_shards: RwLock<HashSet<String>>,
}

#[pyclass(name = "ShardRegion", module = "atomr._native.cluster_sharding")]
pub struct PyShardRegion {
    inner: Arc<RegionInner>,
    /// The cluster_sharding ShardRegion. Single-node: messages only
    /// route locally because the coordinator records us as the owner
    /// of every shard.
    rust_region: Arc<RustShardRegion<CachedExtractor>>,
}

impl Drop for PyShardRegion {
    fn drop(&mut self) {
        self.inner.closed.store(true, Ordering::SeqCst);
    }
}

#[pymethods]
impl PyShardRegion {
    /// Start a shard region. Spawns one entity actor per `entity_id`
    /// the first time a message is routed to it (or, when
    /// `remember_entities=True` and a snapshot exists, eagerly during
    /// startup).
    #[staticmethod]
    #[pyo3(signature = (
        system,
        type_name,
        entity_props,
        message_extractor,
        settings = None,
        shard_id_extractor = None,
        unwrap_extractor = None,
    ))]
    pub fn start(
        py: Python<'_>,
        system: Py<PyActorSystem>,
        type_name: String,
        entity_props: Py<PyProps>,
        message_extractor: Py<PyAny>,
        settings: Option<Py<PyShardingSettings>>,
        shard_id_extractor: Option<Py<PyAny>>,
        unwrap_extractor: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyShardRegion>> {
        let settings = match settings {
            Some(s) => s.borrow(py).clone(),
            None => PyShardingSettings {
                allocation_strategy: "least-shards".into(),
                rebalance_threshold: 1,
                max_simultaneous_rebalance: 3,
                passivation_idle_timeout: None,
                remember_entities: false,
                number_of_shards: None,
            },
        };

        let extractor = build_extractor(
            py,
            message_extractor,
            shard_id_extractor,
            unwrap_extractor,
            settings.number_of_shards,
        )?;

        let sys = system.borrow(py).inner.clone();
        let region_id = format!("{}@{}", type_name, sys.address());

        let strategy = match settings.allocation_strategy.as_str() {
            "least-shards" => AllocationStrategyKind::LeastShards {
                rebalance_threshold: settings.rebalance_threshold,
                max_simultaneous_rebalance: settings.max_simultaneous_rebalance,
            },
            "pinned" => AllocationStrategyKind::Pinned { region: region_id.clone() },
            _ => unreachable!(),
        };

        let coordinator = Arc::new(ShardCoordinator::new());
        // Pre-allocate a sentinel so the coordinator records this
        // region as a known owner; subsequent shards default-allocate
        // here on first mention.
        coordinator.allocate("__bootstrap__", &region_id);

        let (entity_factory, entity_dispatcher, entity_role) = {
            let p = entity_props.borrow(py);
            (
                SendPyAny::new(p.factory.clone_ref(py)),
                p.dispatcher.clone(),
                p.interpreter_role.clone(),
            )
        };

        let remember = if settings.remember_entities {
            let store = remember_store_for(&sys, &type_name);
            Some(RememberCtx {
                store,
                seen_shards: RwLock::new(HashSet::new()),
            })
        } else {
            None
        };

        let idle_timeout = settings.passivation_idle_timeout.map(Duration::from_secs_f64);

        let region_instance = next_region_instance(&sys, &type_name);

        let inner = Arc::new(RegionInner {
            type_name: type_name.clone(),
            system: sys,
            extractor: Arc::new(extractor),
            entities: RwLock::new(HashMap::new()),
            passivation: Arc::new(PassivationTracker::new()),
            remember,
            idle_timeout,
            coordinator: coordinator.clone(),
            region_id: region_id.clone(),
            strategy,
            entity_factory,
            entity_dispatcher,
            entity_role,
            closed: AtomicBool::new(false),
            incarnations: RwLock::new(HashMap::new()),
            region_instance,
        });

        let inner_for_handler = inner.clone();
        let rust_region = RustShardRegion::new(
            region_id.clone(),
            Arc::new(CachedExtractor),
            coordinator,
            Arc::new(move || {
                let inner = inner_for_handler.clone();
                Box::new(move |entity_id: &str, env: ShardEnvelope| {
                    deliver_to_entity(&inner, entity_id, env);
                })
            }),
        );

        // Cluster integration: lazy-touch the Phase 5 daemon so the
        // bus exists for a future transport-equipped subscriber. Today
        // the no-op transport means there are no events to react to.
        attach_cluster_subscription(py, &system);

        if let Some(timeout) = idle_timeout {
            spawn_passivation_sweeper(inner.clone(), timeout);
        }

        if let Some(rem) = inner.remember.as_ref() {
            warm_remembered(inner.clone(), rem.store.clone());
        }

        Py::new(
            py,
            PyShardRegion {
                inner,
                rust_region,
            },
        )
    }

    #[getter]
    fn region_id(&self) -> String {
        self.inner.region_id.clone()
    }

    #[getter]
    fn type_name(&self) -> String {
        self.inner.type_name.clone()
    }

    fn entity_count(&self) -> usize {
        self.inner.entities.read().len()
    }

    fn shard_count(&self) -> usize {
        self.rust_region.shard_count()
    }

    fn shard_ids(&self) -> Vec<String> {
        self.rust_region.shard_ids()
    }

    fn entity_ids(&self) -> Vec<String> {
        self.inner.entities.read().keys().cloned().collect()
    }

    /// Route a message to its entity. Fire-and-forget; the entity
    /// actor handles the payload like any normal `tell`.
    fn tell(&self, py: Python<'_>, msg: Py<PyAny>) -> PyResult<()> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(PyRuntimeError::new_err("ShardRegion is shut down"));
        }
        let env = build_envelope(py, &self.inner.extractor, msg)?;
        // Track shard for remember-entities so shutdown/passivation
        // can remove the right entries.
        if let Some(rem) = &self.inner.remember {
            rem.seen_shards.write().insert(env.shard_id.clone());
        }
        let region = self.rust_region.clone();
        py.allow_threads(|| {
            region.deliver(env);
        });
        Ok(())
    }

    /// Look up the active actor ref for `entity_id`, spawning the
    /// entity if it doesn't yet exist.
    fn entity_ref(&self, py: Python<'_>, entity_id: String) -> PyResult<Py<PyActorRef>> {
        let handle = ensure_entity(&self.inner, &entity_id)
            .ok_or_else(|| PyRuntimeError::new_err("entity spawn failed"))?;
        let path = format!("akka://{}/user/{}", self.inner.system.name(), handle.name);
        Py::new(
            py,
            PyActorRef::from_arc(handle.actor_ref.clone(), path),
        )
    }

    /// Stop an entity actor and remove it from the active map.
    /// Distinct from passivation only in that it runs immediately;
    /// when `remember_entities=True`, this also clears the remembered
    /// entry.
    fn request_passivation(&self, entity_id: String) -> PyResult<()> {
        passivate_entity(&self.inner, &entity_id, /*forget=*/ true);
        Ok(())
    }

    /// Stop the region. Stops every entity actor, removes them from
    /// the active map, halts the passivation sweep loop. Idempotent.
    fn shutdown(&self) {
        self.inner.closed.store(true, Ordering::SeqCst);
        let entity_ids: Vec<String> = self.inner.entities.read().keys().cloned().collect();
        for id in entity_ids {
            // forget=false so remember-entities snapshot is preserved
            // for a later region restart.
            passivate_entity(&self.inner, &id, false);
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "<ShardRegion type={} id={} entities={} shards={}>",
            self.inner.type_name,
            self.inner.region_id,
            self.entity_count(),
            self.shard_count()
        )
    }
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

fn build_extractor(
    py: Python<'_>,
    message_extractor: Py<PyAny>,
    shard_id_extractor: Option<Py<PyAny>>,
    unwrap_extractor: Option<Py<PyAny>>,
    number_of_shards: Option<u64>,
) -> PyResult<PyExtractor> {
    if let Some(shard_fn) = shard_id_extractor {
        Ok(PyExtractor::new(
            message_extractor,
            shard_fn,
            unwrap_extractor,
            number_of_shards,
        ))
    } else {
        // Single-callable mode: the extractor returns
        // (entity_id, shard_id, payload). Synthesise three selector
        // lambdas via a tiny helper module.
        let module = PyModule::from_code_bound(
            py,
            EXTRACTOR_HELPERS,
            "atomr_pycore_extractor_helpers.py",
            "atomr_pycore_extractor_helpers",
        )?;
        let make_selectors = module.getattr("make_selectors")?;
        let tup = make_selectors.call1((message_extractor,))?;
        let (entity_id_fn, shard_id_fn, unwrap_fn): (Py<PyAny>, Py<PyAny>, Py<PyAny>) =
            tup.extract()?;
        Ok(PyExtractor::new(
            entity_id_fn,
            shard_id_fn,
            Some(unwrap_fn),
            number_of_shards,
        ))
    }
}

const EXTRACTOR_HELPERS: &str = r#"
def make_selectors(extractor):
    """Wrap `extractor(msg) -> (entity_id, shard_id, payload)` into three
    selector callables. We tolerate 2-tuples (entity_id, payload) and
    bare ids for back-compat with the legacy ShardRegion shape."""
    def _expand(msg):
        out = extractor(msg)
        if isinstance(out, tuple):
            if len(out) == 3:
                return out
            if len(out) == 2:
                eid, payload = out
                return (str(eid), str(hash(str(eid)) % 16), payload)
        return (str(out), str(hash(str(out)) % 16), msg)

    def entity_id(msg):
        return _expand(msg)[0]
    def shard_id(msg):
        return _expand(msg)[1]
    def unwrap(msg):
        return _expand(msg)[2]
    return (entity_id, shard_id, unwrap)
"#;

fn build_envelope(
    py: Python<'_>,
    extractor: &PyExtractor,
    msg: Py<PyAny>,
) -> PyResult<ShardEnvelope> {
    let entity_id = extractor.extract_entity_id(py, &msg)?;
    let shard_id = extractor.extract_shard_id(py, &msg)?;
    let payload = extractor.extract_payload(py, msg)?;
    Ok(ShardEnvelope {
        payload: SendPyAny::new(payload),
        entity_id,
        shard_id,
    })
}

/// Synchronous dispatch from the cluster_sharding handler back into
/// our entity map. Spawns the entity actor lazily and forwards the
/// payload as a `PyMessage`.
fn deliver_to_entity(inner: &Arc<RegionInner>, entity_id: &str, env: ShardEnvelope) {
    if inner.closed.load(Ordering::SeqCst) {
        return;
    }
    let Some(handle) = ensure_entity(inner, entity_id) else {
        tracing::error!(entity = entity_id, "entity spawn failed; dropping message");
        return;
    };
    inner.passivation.record_activity(entity_id);

    if let Some(rem) = &inner.remember {
        let store = rem.store.clone();
        let shard_id = env.shard_id.clone();
        let eid = entity_id.to_string();
        rem.seen_shards.write().insert(shard_id.clone());
        let rt = runtime();
        rt.spawn(async move {
            let _ = store.add(&shard_id, &eid).await;
        });
    }

    handle.actor_ref.tell(PyMessage::new(env.payload.into_inner()));
}

/// Look up or create the entity actor.
fn ensure_entity(inner: &Arc<RegionInner>, entity_id: &str) -> Option<EntityHandle> {
    {
        let g = inner.entities.read();
        if let Some(h) = g.get(entity_id) {
            return Some(h.clone());
        }
    }
    let mut g = inner.entities.write();
    if let Some(h) = g.get(entity_id) {
        return Some(h.clone());
    }
    let incarnation = {
        let mut inc = inner.incarnations.write();
        let n = inc.entry(entity_id.to_string()).or_insert(0);
        *n += 1;
        *n
    };
    let base = format!("{}-{}", inner.type_name, sanitize(entity_id));
    // For the first region instance and the first incarnation we use
    // the bare name; later instances append `__rN__incM` to dodge the
    // `system.user_guardian` name collision left over from previous
    // stopped actors.
    let name = if inner.region_instance == 1 && incarnation == 1 {
        base
    } else {
        format!("{}__r{}__inc{}", base, inner.region_instance, incarnation)
    };

    let kind = dispatcher::parse(&inner.entity_dispatcher, 1);
    let role = if inner.entity_role == "default" {
        format!("sharding-{}", inner.type_name)
    } else {
        inner.entity_role.clone()
    };
    let pool = registry().get_or_create(&role, kind, InterpreterQuota::default());
    if pool.register_actor().is_err() {
        tracing::error!(
            entity = entity_id,
            "interpreter pool refused entity registration"
        );
        return None;
    }

    let factory = inner.entity_factory.clone();
    let strategy = SupervisorStrategy::default();
    let pool_cl = pool.clone();
    let hash_seed = stable_hash(&format!("{}/{}", inner.system.name(), name));

    let rust_props = RustProps::<PyActor>::create(move || {
        let factory_inner = factory.clone();
        let factory_py = Python::with_gil(|py| factory_inner.0.clone_ref(py));
        PyActor::new(factory_py, pool_cl.clone(), hash_seed, strategy.clone())
    });

    let _guard = runtime().enter();
    let actor_ref = match inner.system.actor_of(rust_props, &name) {
        Ok(r) => Arc::new(r),
        Err(e) => {
            tracing::error!(error = %e, entity = entity_id, "actor_of failed");
            return None;
        }
    };

    g.insert(
        entity_id.to_string(),
        EntityHandle { actor_ref: actor_ref.clone(), name: name.clone() },
    );
    Some(EntityHandle { actor_ref, name })
}

fn passivate_entity(inner: &Arc<RegionInner>, entity_id: &str, forget: bool) {
    let removed = inner.entities.write().remove(entity_id);
    if let Some(handle) = removed {
        inner.system.stop(&handle.name);
        inner.passivation.drop_entity(entity_id);
        if forget {
            if let Some(rem) = &inner.remember {
                let store = rem.store.clone();
                let eid = entity_id.to_string();
                let shards: Vec<String> = rem.seen_shards.read().iter().cloned().collect();
                let rt = runtime();
                rt.spawn(async move {
                    for s in shards {
                        let _ = store.remove(&s, &eid).await;
                    }
                });
            }
        }
    }
}

fn spawn_passivation_sweeper(inner: Arc<RegionInner>, timeout: Duration) {
    let rt = runtime();
    rt.spawn(async move {
        // Sweep at half the idle timeout, capped to [50ms, 5s].
        let half = timeout / 2;
        let interval = half.clamp(Duration::from_millis(50), Duration::from_secs(5));
        let mut tick = tokio::time::interval(interval);
        tick.tick().await;
        loop {
            if inner.closed.load(Ordering::SeqCst) {
                return;
            }
            let idle = inner.passivation.idle_since(timeout);
            for id in idle {
                passivate_entity(&inner, &id, /*forget=*/ false);
            }
            tick.tick().await;
        }
    });
}

fn warm_remembered(inner: Arc<RegionInner>, store: Arc<dyn RememberEntitiesStore>) {
    let rt = runtime();
    rt.spawn(async move {
        // We don't know which shards were used previously; iterate
        // over the canonical 0..16 shard space (matching the helper
        // module's default). Production stores would expose
        // `list_shards`; in-memory handles this implicitly by
        // returning empty sets for unknown shards.
        for shard_id in (0..16u32).map(|i| i.to_string()) {
            if let Ok(ids) = store.load(&shard_id).await {
                if !ids.is_empty() {
                    if let Some(rem) = inner.remember.as_ref() {
                        rem.seen_shards.write().insert(shard_id);
                    }
                    for entity_id in ids {
                        let _ = ensure_entity(&inner, &entity_id);
                    }
                }
            }
        }
    });
}

fn attach_cluster_subscription(_py: Python<'_>, _system: &Py<PyActorSystem>) {
    // Reserved for Phase 9: when the real gossip transport is wired
    // up, subscribe to membership events through the Phase 5 cluster
    // bus and trigger rebalance via `coordinator.rebalance_with_strategy`.
    // Today the no-op transport produces no events to react to.
}

/// Per-system, per-type registry of remember-entities stores plus a
/// per-type instance counter so successive regions can pick a unique
/// child name. Reusing the same store across region restarts is what
/// makes remember-entities recover.
struct StoreRegistry {
    stores: parking_lot::Mutex<HashMap<String, Arc<dyn RememberEntitiesStore>>>,
    instance_counters: parking_lot::Mutex<HashMap<String, u32>>,
}

fn store_registry(sys: &RustSystem) -> Arc<StoreRegistry> {
    let ext = sys.extensions();
    if let Some(r) = ext.get::<StoreRegistry>() {
        return r;
    }
    ext.register::<StoreRegistry>(StoreRegistry {
        stores: parking_lot::Mutex::new(HashMap::new()),
        instance_counters: parking_lot::Mutex::new(HashMap::new()),
    });
    ext.get::<StoreRegistry>().expect("just registered")
}

fn remember_store_for(
    sys: &RustSystem,
    type_name: &str,
) -> Arc<dyn RememberEntitiesStore> {
    let reg = store_registry(sys);
    let mut g = reg.stores.lock();
    if let Some(existing) = g.get(type_name) {
        return existing.clone();
    }
    let store: Arc<dyn RememberEntitiesStore> = Arc::new(InMemoryRememberStore::new());
    g.insert(type_name.to_string(), store.clone());
    store
}

/// Bump and return the next region-instance counter for `(system,
/// type_name)`. The first region instance for a given type is `1`.
fn next_region_instance(sys: &RustSystem, type_name: &str) -> u32 {
    let reg = store_registry(sys);
    let mut g = reg.instance_counters.lock();
    let n = g.entry(type_name.to_string()).or_insert(0);
    *n += 1;
    *n
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
}

fn stable_hash(s: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}

// ---------------------------------------------------------------------------
// Module registration.
// ---------------------------------------------------------------------------

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "cluster_sharding")?;
    sub.add_class::<PyShardingSettings>()?;
    sub.add_class::<PyShardRegion>()?;
    m.add_submodule(&sub)?;
    Ok(())
}
