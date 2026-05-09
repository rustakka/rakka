//! [`CqrsPattern`], [`CqrsBuilder`], [`CqrsTopology`], [`CqrsHandles`].

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use atomr_core::actor::{ActorRef, ActorSystem, Props};
use atomr_core::pattern::RetrySchedule;
use atomr_persistence::{Journal, RecoveryPermitter, SnapshotPolicy, SnapshotStore};
use atomr_persistence_query::ReadJournal;
use futures::future::BoxFuture;
use tokio::sync::{Mutex, RwLock};

use crate::bus::BusHandles;
use crate::cqrs::command_gateway::{CommandEnvelope, CommandGateway, SnapshotConfig};
use crate::cqrs::event_codec::EventCodecRegistry;
use crate::cqrs::projection::ProjectionHandle;
use crate::cqrs::reader::{Reader, ReaderFilter};
use crate::ddd::Repository;
use crate::extensions::{CommandInterceptor, EventListener, ExtensionSlots};
use crate::topology::Topology;
use crate::{AggregateRoot, Command, DomainEvent, PatternError};

/// Public, zero-sized handle to the CQRS pattern. Use
/// [`CqrsPattern::builder`] to start configuring an instance.
pub struct CqrsPattern<A>(PhantomData<A>);

impl<A> CqrsPattern<A>
where
    A: AggregateRoot,
    A::Command: Command<AggregateId = <A as AggregateRoot>::Id>,
    A::Event: DomainEvent,
{
    /// Start a fluent builder for a CQRS instance backed by `journal`.
    ///
    /// The journal type `J` flows into the rest of the builder, so the
    /// rest of the configuration is type-checked. Call
    /// [`CqrsBuilder::factory`], [`CqrsBuilder::read_journal`] (if you
    /// want readers), and [`CqrsBuilder::build`] to obtain a
    /// [`CqrsTopology`].
    pub fn builder<J: Journal>(journal: Arc<J>) -> CqrsBuilder<A, J> {
        CqrsBuilder::new(journal)
    }
}

/// Fluent builder for a CQRS instance.
pub struct CqrsBuilder<A, J>
where
    A: AggregateRoot,
    A::Command: Command<AggregateId = <A as AggregateRoot>::Id>,
    A::Event: DomainEvent,
    J: Journal,
{
    name: Option<String>,
    factory: Option<Arc<dyn Fn(<A as AggregateRoot>::Id) -> A + Send + Sync>>,
    journal: Arc<J>,
    read_journal: Option<Arc<dyn ReadJournal>>,
    recovery_permits: usize,
    writer_uuid: String,
    poll_interval: Duration,
    repo_timeout: Duration,
    extensions: ExtensionSlots<A::Command, A::Event, A::Error>,
    readers: Vec<Box<dyn ErasedReader<A::Event>>>,
    rebuild_contexts: Vec<RebuildContext<A::Event>>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_policy: SnapshotPolicy,
    snapshot_keep_last: usize,
    shards: usize,
    event_codecs: Option<Arc<EventCodecRegistry<A::Event>>>,
    reader_retry: Option<(u32, RetrySchedule)>,
    event_bus: Option<BusHandles<A::Event>>,
    dedupe_window: usize,
}

impl<A, J> CqrsBuilder<A, J>
where
    A: AggregateRoot,
    A::Command: Command<AggregateId = <A as AggregateRoot>::Id>,
    A::Event: DomainEvent,
    J: Journal,
{
    fn new(journal: Arc<J>) -> Self {
        Self {
            name: None,
            factory: None,
            journal,
            read_journal: None,
            recovery_permits: 8,
            writer_uuid: format!("cqrs-{}", rand_writer_id()),
            poll_interval: Duration::from_millis(50),
            repo_timeout: Duration::from_secs(5),
            extensions: ExtensionSlots::default(),
            readers: Vec::new(),
            rebuild_contexts: Vec::new(),
            snapshot_store: None,
            snapshot_policy: SnapshotPolicy::Manual,
            snapshot_keep_last: 1,
            shards: 1,
            event_codecs: None,
            reader_retry: None,
            event_bus: None,
            dedupe_window: 0,
        }
    }

    /// Cap on the per-aggregate command-id dedupe ring. `0` (default)
    /// disables dedupe — every command runs through the handler.
    /// Non-zero enables idempotent retries: commands carrying a
    /// previously-seen [`crate::Command::command_id`] return the
    /// cached events without re-running the handler. v2 caches
    /// successes only; failed commands always re-execute.
    pub fn dedupe_window(mut self, n: usize) -> Self {
        self.dedupe_window = n;
        self
    }

    /// Provide an [`EventCodecRegistry`] that decodes events based on
    /// their journal manifest. Lets you evolve event schemas without
    /// rewriting old events.
    pub fn with_event_codecs(mut self, registry: EventCodecRegistry<A::Event>) -> Self {
        self.event_codecs = Some(Arc::new(registry));
        self
    }

    /// Reader runners retry transient `apply` failures up to
    /// `max_attempts` times with the given backoff schedule.
    /// Default: no retry — failures are logged and the offset
    /// advances.
    pub fn with_reader_retry(mut self, max_attempts: u32, schedule: RetrySchedule) -> Self {
        self.reader_retry = Some((max_attempts.max(1), schedule));
        self
    }

    /// Wire a [`crate::bus::DomainEventBus`] into the gateway.
    /// Persisted events are published to the bus on success, and
    /// readers subscribe to the bus for live-tail delivery instead of
    /// polling. Lower latency than polling at the cost of in-process
    /// coupling to the bus's lifetime.
    pub fn with_event_bus(mut self, bus: BusHandles<A::Event>) -> Self {
        self.event_bus = Some(bus);
        self
    }

    /// Spawn `n` parallel command-gateway actors and route commands
    /// across them by hashing [`crate::Command::aggregate_id`]. Per-id
    /// FIFO ordering is preserved — every command for the same id
    /// reaches the same gateway. v2 supports intra-process sharding
    /// only; cross-node distribution via `atomr-cluster-sharding` is
    /// a v3 follow-on.
    pub fn shards(mut self, n: usize) -> Self {
        self.shards = n.max(1);
        self
    }

    /// Provide a [`SnapshotStore`]. When set together with
    /// [`AggregateRoot::encode_state`] / [`AggregateRoot::decode_state`],
    /// the gateway prefers snapshots on recovery and saves new ones
    /// according to [`Self::snapshot_policy`].
    pub fn snapshot_store<S: SnapshotStore + ?Sized>(mut self, store: Arc<S>) -> Self
    where
        Arc<S>: Into<Arc<dyn SnapshotStore>>,
    {
        self.snapshot_store = Some(store.into());
        self
    }

    /// Override the snapshot cadence policy. Default: `Manual` (no
    /// auto-snapshots; users must call `save_snapshot` themselves).
    pub fn snapshot_policy(mut self, policy: SnapshotPolicy) -> Self {
        self.snapshot_policy = policy;
        self
    }

    /// Cap on retained snapshots per persistence id. Default: 1.
    pub fn snapshot_keep_last(mut self, n: usize) -> Self {
        self.snapshot_keep_last = n.max(1);
        self
    }

    /// Set the user-guardian name for this pattern's root actor.
    /// Default: `"cqrs"`.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Provide a factory that constructs a fresh aggregate for a given id.
    /// The framework calls this lazily — once per id — and reuses the
    /// instance for every subsequent command targeting that id.
    pub fn factory<F>(mut self, factory: F) -> Self
    where
        F: Fn(<A as AggregateRoot>::Id) -> A + Send + Sync + 'static,
    {
        self.factory = Some(Arc::new(factory));
        self
    }

    /// Provide the read-side journal that readers subscribe to. Required
    /// only if you register any readers via [`Self::with_reader`].
    pub fn read_journal<R: ReadJournal>(mut self, rj: Arc<R>) -> Self {
        self.read_journal = Some(rj);
        self
    }

    /// Cap on concurrently-recovering aggregates. Default: 8.
    pub fn recovery_permits(mut self, n: usize) -> Self {
        self.recovery_permits = n;
        self
    }

    /// How often the reader runners poll the read journal. Default: 50ms.
    pub fn poll_interval(mut self, d: Duration) -> Self {
        self.poll_interval = d;
        self
    }

    /// Timeout the [`Repository`] applies to each ask. Default: 5s.
    pub fn repository_timeout(mut self, d: Duration) -> Self {
        self.repo_timeout = d;
        self
    }

    /// Override the writer UUID stamped onto every persisted event.
    pub fn writer_uuid(mut self, w: impl Into<String>) -> Self {
        self.writer_uuid = w.into();
        self
    }

    /// Register a synchronous pre-handler interceptor (named slot
    /// `on_command`). Returning `Err` short-circuits the persist with
    /// [`PatternError::Intercepted`] (or any other variant the closure
    /// constructs).
    pub fn on_command<F>(mut self, hook: F) -> Self
    where
        F: Fn(&A::Command) -> Result<(), PatternError<A::Error>> + Send + Sync + 'static,
    {
        let hook: CommandInterceptor<A::Command, A::Error> = Arc::new(hook);
        self.extensions.command_interceptors.push(hook);
        self
    }

    /// Register a synchronous post-persist event listener (named slot
    /// `on_event`). Listeners run in the gateway's actor task; keep
    /// them fast — push to a tap if you need async work.
    pub fn on_event<F>(mut self, hook: F) -> Self
    where
        F: Fn(&A::Event) + Send + Sync + 'static,
    {
        let hook: EventListener<A::Event> = Arc::new(hook);
        self.extensions.event_listeners.push(hook);
        self
    }

    /// Register an async event tap. The runner pushes a clone of every
    /// successfully-persisted event into the channel. Closed receivers
    /// are pruned silently.
    pub fn tap_events(mut self, tx: tokio::sync::mpsc::UnboundedSender<A::Event>) -> Self {
        self.extensions.event_taps.push(tx);
        self
    }

    /// Register a [`Reader`] and receive a [`ProjectionHandle`] you
    /// can use later to read the projection state. The reader's
    /// `Event` type must equal the aggregate's `Event` type.
    pub fn with_reader<R>(mut self, reader: R) -> (Self, ProjectionHandle<R::Projection>)
    where
        R: Reader<Event = A::Event>,
    {
        let name = reader.name().to_string();
        let filter = reader.filter();
        let state: Arc<RwLock<R::Projection>> = Arc::new(RwLock::new(R::Projection::default()));
        let offset = Arc::new(AtomicU64::new(0));
        let handle = ProjectionHandle { state: state.clone(), offset: offset.clone() };
        let spec = ReaderSpec::<R> {
            reader: Arc::new(Mutex::new(reader)),
            state,
            offset: offset.clone(),
            name: name.clone(),
            filter,
        };
        let ctx = spec.rebuild_context();
        self.rebuild_contexts.push(ctx);
        self.readers.push(Box::new(spec));
        (self, handle)
    }

    /// Finalize the builder. Returns a [`CqrsTopology`] that you call
    /// [`Topology::materialize`] on to spawn the actors and start the
    /// readers.
    pub fn build(self) -> Result<CqrsTopology<A, J>, PatternError<A::Error>> {
        let factory = self.factory.ok_or(PatternError::NotConfigured("factory"))?;
        if !self.readers.is_empty() && self.read_journal.is_none() && self.event_bus.is_none() {
            return Err(PatternError::NotConfigured("read_journal"));
        }
        let snapshot = self.snapshot_store.map(|store| SnapshotConfig {
            store,
            policy: self.snapshot_policy,
            keep_last: self.snapshot_keep_last,
        });

        // Plumb the bus into the gateway as a tap so persisted events
        // flow to the bus automatically.
        let mut extensions = self.extensions;
        if let Some(bus) = self.event_bus.as_ref() {
            let bus_for_listener = bus.clone();
            let listener: crate::extensions::EventListener<A::Event> =
                Arc::new(move |e: &A::Event| bus_for_listener.publish(e.clone()));
            extensions.event_listeners.push(listener);
        }

        Ok(CqrsTopology {
            name: self.name.unwrap_or_else(|| "cqrs".into()),
            factory,
            journal: self.journal,
            read_journal: self.read_journal,
            recovery_permits: self.recovery_permits,
            writer_uuid: self.writer_uuid,
            poll_interval: self.poll_interval,
            repo_timeout: self.repo_timeout,
            extensions,
            readers: self.readers,
            rebuild_contexts: self.rebuild_contexts,
            snapshot,
            shards: self.shards,
            event_codecs: self.event_codecs,
            reader_retry: self.reader_retry,
            event_bus: self.event_bus,
            dedupe_window: self.dedupe_window,
        })
    }
}

/// Inspectable description of a CQRS topology — actors not yet spawned,
/// readers not yet running. Call [`Topology::materialize`] to bring it
/// to life.
pub struct CqrsTopology<A, J>
where
    A: AggregateRoot,
    A::Command: Command<AggregateId = <A as AggregateRoot>::Id>,
    A::Event: DomainEvent,
    J: Journal,
{
    name: String,
    factory: Arc<dyn Fn(<A as AggregateRoot>::Id) -> A + Send + Sync>,
    journal: Arc<J>,
    read_journal: Option<Arc<dyn ReadJournal>>,
    recovery_permits: usize,
    writer_uuid: String,
    poll_interval: Duration,
    repo_timeout: Duration,
    extensions: ExtensionSlots<A::Command, A::Event, A::Error>,
    readers: Vec<Box<dyn ErasedReader<A::Event>>>,
    rebuild_contexts: Vec<RebuildContext<A::Event>>,
    snapshot: Option<SnapshotConfig>,
    shards: usize,
    event_codecs: Option<Arc<EventCodecRegistry<A::Event>>>,
    reader_retry: Option<(u32, RetrySchedule)>,
    event_bus: Option<BusHandles<A::Event>>,
    dedupe_window: usize,
}

#[async_trait]
impl<A, J> Topology for CqrsTopology<A, J>
where
    A: AggregateRoot,
    A::Command: Command<AggregateId = <A as AggregateRoot>::Id>,
    A::Event: DomainEvent,
    J: Journal,
{
    type Handles = CqrsHandles<A>;

    async fn materialize(self, system: &ActorSystem) -> Result<Self::Handles, PatternError<()>> {
        // Capture-by-move into the Props factory. The actor restart
        // path will produce a fresh gateway with empty entity cache —
        // recovery refills state from the journal on demand.
        let factory = self.factory.clone();
        let journal = self.journal.clone();
        let permits = Arc::new(RecoveryPermitter::new(self.recovery_permits));
        let writer_uuid = self.writer_uuid.clone();
        let extensions = self.extensions.clone();
        let snapshot = self.snapshot.clone();
        let shards = self.shards.max(1);
        let dedupe_window = self.dedupe_window;

        let mut gateways: Vec<ActorRef<CommandEnvelope<A>>> = Vec::with_capacity(shards);
        for shard_idx in 0..shards {
            let factory = factory.clone();
            let journal = journal.clone();
            let permits = permits.clone();
            let writer_uuid = writer_uuid.clone();
            let extensions = extensions.clone();
            let snapshot = snapshot.clone();
            let actor_name = if shards == 1 {
                self.name.clone()
            } else {
                format!("{}-shard-{shard_idx}", self.name)
            };
            let actor_ref = system
                .actor_of(
                    Props::create(move || CommandGateway::<A, J> {
                        factory: factory.clone(),
                        journal: journal.clone(),
                        permits: permits.clone(),
                        writer_uuid: writer_uuid.clone(),
                        entities: HashMap::new(),
                        extensions: extensions.clone(),
                        snapshot: snapshot.clone(),
                        dedupe_window,
                    }),
                    &actor_name,
                )
                .map_err(|e| PatternError::Invariant(format!("spawn gateway: {e}")))?;
            gateways.push(actor_ref);
        }

        // Capture the read_journal early so we can both spawn readers
        // and build rebuild closures.
        let read_journal = self.read_journal.clone();
        // Spawn reader runners — one tokio task per reader. When an
        // event bus is wired, readers run in live-tail mode; otherwise
        // they poll the read journal.
        let bus = self.event_bus.clone();
        let codecs = self.event_codecs.clone();
        let retry_cfg = self.reader_retry;
        if !self.readers.is_empty() {
            let need_journal = bus.is_none();
            let rj = if need_journal {
                Some(read_journal.clone().expect("checked in build()"))
            } else {
                read_journal.clone()
            };
            for spec in self.readers {
                let codecs = codecs.clone();
                let retry = retry_cfg;
                if let Some(bus_handles) = &bus {
                    let rx = bus_handles.subscribe();
                    tokio::spawn(run_reader_live(spec, rx, retry));
                } else {
                    let rj_clone = rj.clone().expect("checked above");
                    let interval = self.poll_interval;
                    tokio::spawn(run_reader_poll(spec, rj_clone, interval, codecs, retry));
                }
            }
        }

        let repo: Arc<dyn Repository<Aggregate = A>> = Arc::new(ShardedRepository::<A> {
            gateways,
            timeout: self.repo_timeout,
        });

        // Build rebuild closures for each registered reader. Rebuild
        // requires a read_journal; live-tail-only readers (no journal
        // configured) get a closure that returns an explanatory error.
        let mut rebuilds: HashMap<String, RebuildFn> = HashMap::new();
        let rebuild_journal = read_journal.clone();
        let rebuild_codecs = self.event_codecs.clone();
        for ctx in self.rebuild_contexts {
            let journal = rebuild_journal.clone();
            let codecs = rebuild_codecs.clone();
            let name = ctx.name.clone();
            let f: RebuildFn = Arc::new(move || {
                let ctx = RebuildContext {
                    name: ctx.name.clone(),
                    state_reset: ctx.state_reset.clone(),
                    apply: ctx.apply.clone(),
                    filter: ctx.filter.clone(),
                    offset: ctx.offset.clone(),
                };
                let journal = journal.clone();
                let codecs = codecs.clone();
                Box::pin(async move {
                    let Some(rj) = journal else {
                        return Err(
                            "rebuild_projection requires a read_journal".into(),
                        );
                    };
                    rebuild_one_projection(ctx, rj, codecs).await
                })
            });
            rebuilds.insert(name, f);
        }

        Ok(CqrsHandles { repository: repo, rebuilds })
    }
}

async fn rebuild_one_projection<E: Send + Clone + 'static>(
    ctx: RebuildContext<E>,
    rj: Arc<dyn ReadJournal>,
    codecs: Option<Arc<EventCodecRegistry<E>>>,
) -> Result<(), String> {
    (ctx.state_reset)().await;
    let pids = match &ctx.filter {
        ReaderFilter::All | ReaderFilter::Tag(_) => rj
            .all_persistence_ids()
            .await
            .map_err(|e| format!("list pids: {e:?}"))?,
        ReaderFilter::PersistenceId(id) => vec![id.clone()],
        ReaderFilter::PersistenceIds(ids) => ids.clone(),
    };
    let mut max_seq: u64 = 0;
    for pid in pids {
        let events = rj
            .events_by_persistence_id(&pid, 1, u64::MAX)
            .await
            .map_err(|e| format!("read pid {pid}: {e:?}"))?;
        for env in events {
            if let ReaderFilter::Tag(t) = &ctx.filter {
                if !env.tags.iter().any(|x| x == t) {
                    continue;
                }
            }
            let decoded = codecs
                .as_ref()
                .and_then(|r| r.decode(&env.manifest, &env.payload))
                .ok_or_else(|| {
                    format!(
                        "no decoder for manifest `{}` (configure EventCodecRegistry)",
                        env.manifest
                    )
                })?;
            let event = decoded?;
            (ctx.apply)(event)
                .await
                .map_err(|e| format!("apply during rebuild: {e}"))?;
            if env.sequence_nr > max_seq {
                max_seq = env.sequence_nr;
            }
        }
    }
    ctx.offset.store(max_seq, Ordering::Release);
    Ok(())
}

/// Strongly-typed handles into a materialized CQRS instance.
pub struct CqrsHandles<A>
where
    A: AggregateRoot,
    A::Command: Command<AggregateId = <A as AggregateRoot>::Id>,
    A::Event: DomainEvent,
{
    repository: Arc<dyn Repository<Aggregate = A>>,
    rebuilds: HashMap<String, RebuildFn>,
}

type RebuildFn =
    Arc<dyn Fn() -> BoxFuture<'static, Result<(), String>> + Send + Sync + 'static>;

impl<A> CqrsHandles<A>
where
    A: AggregateRoot,
    A::Command: Command<AggregateId = <A as AggregateRoot>::Id>,
    A::Event: DomainEvent,
{
    /// Type-erased repository handle.
    pub fn repository(&self) -> Arc<dyn Repository<Aggregate = A>> {
        self.repository.clone()
    }

    /// Reset and replay the projection associated with the named
    /// reader. Returns `Err` if no reader by that name was registered
    /// at build time, or if no [`atomr_persistence_query::ReadJournal`]
    /// is configured (live-tail-only readers can't be rebuilt — they
    /// have no journal to scan).
    pub async fn rebuild_projection(&self, name: &str) -> Result<(), String> {
        let f = self
            .rebuilds
            .get(name)
            .ok_or_else(|| format!("no reader named `{name}`"))?
            .clone();
        f().await
    }
}

// ─── Implementation details below ──────────────────────────────────────

struct ShardedRepository<A>
where
    A: AggregateRoot,
    A::Command: Command<AggregateId = <A as AggregateRoot>::Id>,
    A::Event: DomainEvent,
{
    gateways: Vec<ActorRef<CommandEnvelope<A>>>,
    timeout: Duration,
}

#[async_trait]
impl<A> Repository for ShardedRepository<A>
where
    A: AggregateRoot,
    A::Command: Command<AggregateId = <A as AggregateRoot>::Id>,
    A::Event: DomainEvent,
{
    type Aggregate = A;

    async fn send(
        &self,
        cmd: A::Command,
    ) -> Result<Vec<A::Event>, PatternError<A::Error>> {
        let id = cmd.aggregate_id();
        let idx = shard_index(&id, self.gateways.len());
        match self.gateways[idx]
            .ask_with(|reply| CommandEnvelope { cmd, reply }, self.timeout)
            .await
        {
            Ok(inner) => inner,
            Err(ask) => Err(PatternError::Ask(ask)),
        }
    }
}

fn shard_index<I: std::hash::Hash>(id: &I, n: usize) -> usize {
    use std::hash::Hasher;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    id.hash(&mut h);
    (h.finish() as usize) % n.max(1)
}

trait ErasedReader<E>: Send + Sync + 'static {
    fn name(&self) -> String;
    fn filter(&self) -> ReaderFilter;
    fn offset(&self) -> Arc<AtomicU64>;
    fn decode_payload(&self, bytes: &[u8]) -> Result<E, String>;
    fn apply<'a>(&'a self, event: E) -> BoxFuture<'a, Result<(), String>>;
}

struct ReaderSpec<R: Reader> {
    reader: Arc<Mutex<R>>,
    state: Arc<RwLock<R::Projection>>,
    offset: Arc<AtomicU64>,
    name: String,
    filter: ReaderFilter,
}

impl<R: Reader> ErasedReader<R::Event> for ReaderSpec<R> {
    fn name(&self) -> String {
        self.name.clone()
    }
    fn filter(&self) -> ReaderFilter {
        self.filter.clone()
    }
    fn offset(&self) -> Arc<AtomicU64> {
        self.offset.clone()
    }
    fn decode_payload(&self, bytes: &[u8]) -> Result<R::Event, String> {
        R::decode(bytes)
    }
    fn apply<'a>(&'a self, event: R::Event) -> BoxFuture<'a, Result<(), String>> {
        let state = self.state.clone();
        let reader = self.reader.clone();
        Box::pin(async move {
            let mut state = state.write().await;
            let mut reader = reader.lock().await;
            reader.apply(&mut *state, event).await.map_err(|e| e.to_string())
        })
    }
}

type ResetFn = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;
type ApplyFn<E> = Arc<dyn Fn(E) -> BoxFuture<'static, Result<(), String>> + Send + Sync>;

/// Standalone reference to a reader's apply path used by rebuild
/// closures (which need access without going through the
/// `Box<dyn ErasedReader>` that lives inside the runner).
struct RebuildContext<E: Send + Clone + 'static> {
    name: String,
    state_reset: ResetFn,
    apply: ApplyFn<E>,
    filter: ReaderFilter,
    offset: Arc<AtomicU64>,
}

impl<R: Reader> ReaderSpec<R> {
    fn rebuild_context(&self) -> RebuildContext<R::Event> {
        let state = self.state.clone();
        let offset = self.offset.clone();
        let reader = self.reader.clone();
        let state_clone = state.clone();
        let offset_clone = offset.clone();
        let reader_clone = reader.clone();
        let state_reset: ResetFn = Arc::new(move || {
            let state = state_clone.clone();
            let offset = offset_clone.clone();
            Box::pin(async move {
                *state.write().await = R::Projection::default();
                offset.store(0, Ordering::Release);
            })
        });
        let apply: ApplyFn<R::Event> = Arc::new(move |event: R::Event| {
            let state = state.clone();
            let reader = reader_clone.clone();
            Box::pin(async move {
                let mut state = state.write().await;
                let mut reader = reader.lock().await;
                reader.apply(&mut *state, event).await.map_err(|e| e.to_string())
            })
        });
        RebuildContext {
            name: self.name.clone(),
            state_reset,
            apply,
            filter: self.filter.clone(),
            offset,
        }
    }
}

async fn run_reader_poll<E: Send + Clone + 'static>(
    reader: Box<dyn ErasedReader<E>>,
    read_journal: Arc<dyn ReadJournal>,
    poll_interval: Duration,
    codecs: Option<Arc<EventCodecRegistry<E>>>,
    retry: Option<(u32, RetrySchedule)>,
) {
    let mut pid_offsets: HashMap<String, u64> = HashMap::new();
    let offset_handle = reader.offset();
    let filter = reader.filter();
    let name = reader.name();

    loop {
        let pids = match resolve_pids(&filter, &*read_journal).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(reader = %name, error = ?e, "list pids failed");
                tokio::time::sleep(poll_interval).await;
                continue;
            }
        };

        let mut max_seq_seen = offset_handle.load(Ordering::Acquire);

        for pid in pids {
            let from = pid_offsets.get(&pid).copied().unwrap_or(0).saturating_add(1);
            let events = match read_journal
                .events_by_persistence_id(&pid, from, u64::MAX)
                .await
            {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(reader = %name, pid = %pid, error = ?e, "read failed");
                    continue;
                }
            };

            for env in events {
                if let ReaderFilter::Tag(t) = &filter {
                    if !env.tags.iter().any(|x| x == t) {
                        pid_offsets.insert(pid.clone(), env.sequence_nr);
                        continue;
                    }
                }

                let decoded = codecs
                    .as_ref()
                    .and_then(|r| r.decode(&env.manifest, &env.payload))
                    .unwrap_or_else(|| reader.decode_payload(&env.payload));

                match decoded {
                    Ok(event) => {
                        apply_with_retry(&*reader, event, retry, &name).await;
                        pid_offsets.insert(pid.clone(), env.sequence_nr);
                        if env.sequence_nr > max_seq_seen {
                            max_seq_seen = env.sequence_nr;
                        }
                    }
                    Err(s) => {
                        tracing::warn!(reader = %name, error = %s, "decode failed");
                        pid_offsets.insert(pid.clone(), env.sequence_nr);
                    }
                }
            }
        }

        offset_handle.store(max_seq_seen, Ordering::Release);
        tokio::time::sleep(poll_interval).await;
    }
}

async fn run_reader_live<E: Send + Clone + 'static>(
    reader: Box<dyn ErasedReader<E>>,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<E>,
    retry: Option<(u32, RetrySchedule)>,
) {
    let name = reader.name();
    while let Some(event) = rx.recv().await {
        apply_with_retry(&*reader, event, retry, &name).await;
    }
}

async fn apply_with_retry<E: Send + Clone + 'static>(
    reader: &dyn ErasedReader<E>,
    event: E,
    retry: Option<(u32, RetrySchedule)>,
    name: &str,
) {
    let result = if let Some((max_attempts, sched)) = retry {
        let mut last: Option<String> = None;
        for attempt in 0..max_attempts {
            match reader.apply(event.clone()).await {
                Ok(()) => return,
                Err(e) => {
                    last = Some(e);
                    if attempt + 1 < max_attempts {
                        tokio::time::sleep(sched.delay_for(attempt)).await;
                    }
                }
            }
        }
        Err(last.unwrap_or_else(|| "unknown".into()))
    } else {
        reader.apply(event).await
    };
    if let Err(err) = result {
        tracing::warn!(reader = %name, error = %err, "apply failed (retries exhausted)");
    }
}

async fn resolve_pids(
    filter: &ReaderFilter,
    rj: &dyn ReadJournal,
) -> Result<Vec<String>, atomr_persistence::JournalError> {
    match filter {
        ReaderFilter::All | ReaderFilter::Tag(_) => rj.all_persistence_ids().await,
        ReaderFilter::PersistenceId(id) => Ok(vec![id.clone()]),
        ReaderFilter::PersistenceIds(ids) => Ok(ids.clone()),
    }
}

fn rand_writer_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{nanos:x}")
}
