//! [`CqrsPattern`], [`CqrsBuilder`], [`CqrsTopology`], [`CqrsHandles`].

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use atomr_core::actor::{ActorRef, ActorSystem, Props};
use atomr_persistence::{Journal, RecoveryPermitter};
use atomr_persistence_query::ReadJournal;
use futures::future::BoxFuture;
use tokio::sync::{Mutex, RwLock};

use crate::cqrs::command_gateway::{CommandEnvelope, CommandGateway};
use crate::cqrs::projection::ProjectionHandle;
use crate::cqrs::reader::Reader;
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
        }
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
        let tag = reader.tag();
        let state: Arc<RwLock<R::Projection>> = Arc::new(RwLock::new(R::Projection::default()));
        let offset = Arc::new(AtomicU64::new(0));
        let handle = ProjectionHandle { state: state.clone(), offset: offset.clone() };
        let spec = ReaderSpec::<R> {
            reader: Arc::new(Mutex::new(reader)),
            state,
            offset: offset.clone(),
            name,
            tag,
        };
        self.readers.push(Box::new(spec));
        (self, handle)
    }

    /// Finalize the builder. Returns a [`CqrsTopology`] that you call
    /// [`Topology::materialize`] on to spawn the actors and start the
    /// readers.
    pub fn build(self) -> Result<CqrsTopology<A, J>, PatternError<A::Error>> {
        let factory = self.factory.ok_or(PatternError::NotConfigured("factory"))?;
        if !self.readers.is_empty() && self.read_journal.is_none() {
            return Err(PatternError::NotConfigured("read_journal"));
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
            extensions: self.extensions,
            readers: self.readers,
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

        let actor_ref: ActorRef<CommandEnvelope<A>> = system
            .actor_of(
                Props::create(move || CommandGateway::<A, J> {
                    factory: factory.clone(),
                    journal: journal.clone(),
                    permits: permits.clone(),
                    writer_uuid: writer_uuid.clone(),
                    entities: HashMap::new(),
                    extensions: extensions.clone(),
                }),
                &self.name,
            )
            .map_err(|e| PatternError::Invariant(format!("spawn gateway: {e}")))?;

        // Spawn reader runners — one tokio task per reader.
        if !self.readers.is_empty() {
            let rj = self.read_journal.expect("checked in build()");
            for spec in self.readers {
                let rj_clone = rj.clone();
                let interval = self.poll_interval;
                tokio::spawn(run_reader(spec, rj_clone, interval));
            }
        }

        let repo: Arc<dyn Repository<Aggregate = A>> = Arc::new(ShardedRepository::<A> {
            gateway: actor_ref,
            timeout: self.repo_timeout,
        });

        Ok(CqrsHandles { repository: repo })
    }
}

/// Strongly-typed handles into a materialized CQRS instance.
pub struct CqrsHandles<A>
where
    A: AggregateRoot,
    A::Command: Command<AggregateId = <A as AggregateRoot>::Id>,
    A::Event: DomainEvent,
{
    repository: Arc<dyn Repository<Aggregate = A>>,
}

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
}

// ─── Implementation details below ──────────────────────────────────────

struct ShardedRepository<A>
where
    A: AggregateRoot,
    A::Command: Command<AggregateId = <A as AggregateRoot>::Id>,
    A::Event: DomainEvent,
{
    gateway: ActorRef<CommandEnvelope<A>>,
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
        match self
            .gateway
            .ask_with(|reply| CommandEnvelope { cmd, reply }, self.timeout)
            .await
        {
            Ok(inner) => inner,
            Err(ask) => Err(PatternError::Ask(ask)),
        }
    }
}

trait ErasedReader<E>: Send + Sync + 'static {
    fn name(&self) -> String;
    fn tag(&self) -> Option<String>;
    fn offset(&self) -> Arc<AtomicU64>;
    fn decode_payload(&self, bytes: &[u8]) -> Result<E, String>;
    fn apply<'a>(&'a self, event: E) -> BoxFuture<'a, Result<(), String>>;
}

struct ReaderSpec<R: Reader> {
    reader: Arc<Mutex<R>>,
    state: Arc<RwLock<R::Projection>>,
    offset: Arc<AtomicU64>,
    name: String,
    tag: Option<String>,
}

impl<R: Reader> ErasedReader<R::Event> for ReaderSpec<R> {
    fn name(&self) -> String {
        self.name.clone()
    }
    fn tag(&self) -> Option<String> {
        self.tag.clone()
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

async fn run_reader<E: Send + Clone + 'static>(
    reader: Box<dyn ErasedReader<E>>,
    read_journal: Arc<dyn ReadJournal>,
    poll_interval: Duration,
) {
    let mut pid_offsets: HashMap<String, u64> = HashMap::new();
    let offset_handle = reader.offset();
    let tag = reader.tag();
    let name = reader.name();

    loop {
        let pids = match read_journal.all_persistence_ids().await {
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
                if let Some(t) = &tag {
                    if !env.tags.iter().any(|x| x == t) {
                        pid_offsets.insert(pid.clone(), env.sequence_nr);
                        continue;
                    }
                }

                match reader.decode_payload(&env.payload) {
                    Ok(event) => {
                        if let Err(err) = reader.apply(event).await {
                            tracing::warn!(reader = %name, error = %err, "apply failed");
                        }
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

fn rand_writer_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{nanos:x}")
}
