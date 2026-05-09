//! [`CommandGateway`] — actor that routes commands to per-id aggregate
//! state, persists events, and notifies post-persist hooks.
//!
//! Internal to the CQRS pattern; not exposed in the public API. Users
//! interact through the [`crate::Repository`] handle returned from
//! [`super::CqrsTopology::materialize`].

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use atomr_core::actor::{Actor, Context};
use atomr_persistence::{
    AsyncSnapshotter, Journal, PersistentRepr, RecoveryPermitter, SnapshotPolicy, SnapshotStore,
};
use tokio::sync::oneshot;

use crate::extensions::ExtensionSlots;
use crate::{AggregateRoot, Command, DomainEvent, PatternError};

fn push_dedupe<E: Clone>(
    ring: &mut std::collections::VecDeque<(String, Vec<E>)>,
    key: String,
    events: Vec<E>,
    cap: usize,
) {
    if cap == 0 {
        return;
    }
    if ring.iter().any(|(k, _)| k == &key) {
        return;
    }
    if ring.len() >= cap {
        ring.pop_front();
    }
    ring.push_back((key, events));
}

/// Snapshot configuration the gateway consults during recovery and
/// after each successful persist. Created from
/// [`crate::cqrs::CqrsBuilder::snapshot_store`] / `snapshot_policy` /
/// `snapshot_keep_last`.
pub(crate) struct SnapshotConfig {
    pub store: Arc<dyn SnapshotStore>,
    pub policy: SnapshotPolicy,
    pub keep_last: usize,
}

impl Clone for SnapshotConfig {
    fn clone(&self) -> Self {
        Self { store: self.store.clone(), policy: self.policy, keep_last: self.keep_last }
    }
}

impl SnapshotConfig {
    fn should_snapshot(&self, seq: u64) -> bool {
        AsyncSnapshotter::new(self.store.clone(), self.policy).should_snapshot(seq)
    }

    async fn save(&self, pid: String, seq: u64, payload: Vec<u8>) {
        AsyncSnapshotter::new(self.store.clone(), self.policy)
            .with_keep_last(self.keep_last)
            .save(pid, seq, payload)
            .await
    }
}

pub(crate) type CommandReply<A> = oneshot::Sender<
    Result<
        Vec<<A as atomr_persistence::Eventsourced>::Event>,
        PatternError<<A as atomr_persistence::Eventsourced>::Error>,
    >,
>;

/// Envelope received by the gateway actor.
pub(crate) struct CommandEnvelope<A: AggregateRoot>
where
    A::Command: Command<AggregateId = <A as AggregateRoot>::Id>,
    A::Event: DomainEvent,
{
    pub cmd: A::Command,
    pub reply: CommandReply<A>,
}

pub(crate) struct EntityState<A: AggregateRoot> {
    pub aggregate: A,
    pub state: A::State,
    pub seq: u64,
    pub recovered: bool,
    /// LRU ring of `(command_id, persisted_events)` for idempotent
    /// retries. Only successes are cached; failed commands re-run on
    /// re-receipt. Cap defined by gateway's `dedupe_window`.
    pub dedupe: std::collections::VecDeque<(String, Vec<A::Event>)>,
}

impl<A: AggregateRoot> EntityState<A> {
    pub(crate) fn new(aggregate: A) -> Self {
        Self {
            aggregate,
            state: <A::State as Default>::default(),
            seq: 0,
            recovered: false,
            dedupe: std::collections::VecDeque::new(),
        }
    }
}

pub(crate) struct CommandGateway<A, J>
where
    A: AggregateRoot,
    A::Command: Command<AggregateId = <A as AggregateRoot>::Id>,
    A::Event: DomainEvent,
    J: Journal,
{
    pub factory: Arc<dyn Fn(<A as AggregateRoot>::Id) -> A + Send + Sync>,
    pub journal: Arc<J>,
    pub permits: Arc<RecoveryPermitter>,
    pub writer_uuid: String,
    pub entities: HashMap<<A as AggregateRoot>::Id, EntityState<A>>,
    pub extensions: ExtensionSlots<A::Command, A::Event, A::Error>,
    pub snapshot: Option<SnapshotConfig>,
    /// Per-aggregate command-id dedupe ring. `0` disables dedupe.
    pub dedupe_window: usize,
}

#[async_trait]
impl<A, J> Actor for CommandGateway<A, J>
where
    A: AggregateRoot,
    A::Command: Command<AggregateId = <A as AggregateRoot>::Id>,
    A::Event: DomainEvent,
    J: Journal,
{
    type Msg = CommandEnvelope<A>;

    async fn handle(&mut self, _ctx: &mut Context<Self>, env: Self::Msg) {
        let result = self.process(env.cmd).await;
        let _ = env.reply.send(result);
    }
}

impl<A, J> CommandGateway<A, J>
where
    A: AggregateRoot,
    A::Command: Command<AggregateId = <A as AggregateRoot>::Id>,
    A::Event: DomainEvent,
    J: Journal,
{
    async fn process(&mut self, cmd: A::Command) -> Result<Vec<A::Event>, PatternError<A::Error>> {
        // 1. Pre-handler interceptors. Any rejection short-circuits.
        self.extensions.run_interceptors(&cmd)?;

        let id = cmd.aggregate_id();

        // 2. Take entity out of the map so we can borrow it mutably
        //    across the async work without colliding with `&mut self`.
        let mut entity =
            self.entities.remove(&id).unwrap_or_else(|| EntityState::new((self.factory)(id.clone())));

        // 3. Idempotency check: is this command_id already cached?
        if self.dedupe_window > 0 {
            if let Some(key) = cmd.command_id().map(|s| s.to_string()) {
                if let Some(prev) = entity.dedupe.iter().find(|(k, _)| k == &key) {
                    let cached = Ok(prev.1.clone());
                    self.entities.insert(id, entity);
                    return cached;
                }
            }
        }

        let result = self.process_entity(&mut entity, cmd).await;

        // 4. Cache successful (and rejected) results keyed by command_id.
        // We do this AFTER process so we have the actual outcome.
        // We don't have access to the original command_id here — the
        // process_entity consumed `cmd`. We rely on the caller having
        // populated `entity.dedupe` already if needed; the cache push
        // happens inside process_entity below where we still have the
        // id.

        // 5. Always restore the entity, even on failure.
        self.entities.insert(id, entity);

        result
    }

    async fn process_entity(
        &mut self,
        entity: &mut EntityState<A>,
        cmd: A::Command,
    ) -> Result<Vec<A::Event>, PatternError<A::Error>> {
        // Capture the command_id before consuming `cmd` in
        // `command_to_events`. We populate the dedupe cache once we
        // have the result.
        let dedupe_key = if self.dedupe_window > 0 { cmd.command_id().map(|s| s.to_string()) } else { None };
        // Optimistic concurrency: caller-supplied expected_version.
        let expected = cmd.expected_version();

        // Lazy recovery on first command for this id.
        if !entity.recovered {
            self.recover_entity(entity).await?;
        }

        // Concurrency check happens after recovery so seq is current.
        if let Some(expected) = expected {
            if entity.seq != expected {
                let actual = entity.seq;
                let err = PatternError::ConcurrencyConflict { expected, actual };
                return Err(err);
            }
        }

        // Pure projection of command -> events. Domain validation lives here.
        let events = entity.aggregate.command_to_events(&entity.state, cmd).map_err(PatternError::Domain)?;

        if events.is_empty() {
            return Ok(events);
        }

        // Build PersistentRepr with tags taken from DomainEvent.
        let manifest = entity.aggregate.event_manifest().to_string();
        let pid = entity.aggregate.persistence_id();
        let pre_seq = entity.seq;
        let mut reprs = Vec::with_capacity(events.len());
        for e in &events {
            entity.seq += 1;
            let payload = A::encode_event(e).map_err(PatternError::Codec)?;
            reprs.push(PersistentRepr {
                persistence_id: pid.clone(),
                sequence_nr: entity.seq,
                payload,
                manifest: manifest.clone(),
                writer_uuid: self.writer_uuid.clone(),
                deleted: false,
                tags: e.tags(),
            });
        }

        if let Err(e) = self.journal.write_messages(reprs).await {
            entity.seq = pre_seq;
            return Err(PatternError::Journal(e));
        }

        for e in &events {
            A::apply_event(&mut entity.state, e);
        }

        A::check_invariants(&entity.state).map_err(PatternError::Domain)?;

        // Snapshot if policy fires for this sequence number AND the
        // aggregate opted into state encoding.
        if let Some(sc) = &self.snapshot {
            if sc.should_snapshot(entity.seq) {
                if let Some(encode_result) = A::encode_state(&entity.state) {
                    match encode_result {
                        Ok(payload) => {
                            sc.save(entity.aggregate.persistence_id(), entity.seq, payload).await;
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "snapshot encode failed; skipping");
                        }
                    }
                }
            }
        }

        // Post-persist: run sync listeners, then drop closed taps.
        for e in &events {
            self.extensions.notify_listeners(e);
        }
        for e in &events {
            self.extensions.push_event_taps(e);
        }

        // Cache the success result for command_id dedupe.
        if let Some(key) = dedupe_key {
            push_dedupe(&mut entity.dedupe, key, events.clone(), self.dedupe_window);
        }

        Ok(events)
    }

    /// Snapshot-first recovery: load latest snapshot (if configured
    /// and decodable), then replay only events written *after* the
    /// snapshot's `sequence_nr`. Falls back to full replay on cache
    /// miss or decode failure.
    async fn recover_entity(&mut self, entity: &mut EntityState<A>) -> Result<(), PatternError<A::Error>> {
        let _permit = self
            .permits
            .acquire()
            .await
            .ok_or_else(|| PatternError::Invariant("recovery permit denied".into()))?;

        let pid = entity.aggregate.persistence_id();

        // Snapshot first.
        let snapshot_seq: Option<u64> = if let Some(sc) = &self.snapshot {
            match sc.store.load(&pid).await {
                Some((meta, payload)) => match A::decode_state(&payload) {
                    Ok(state) => {
                        entity.state = state;
                        Some(meta.sequence_nr)
                    }
                    Err(e) => {
                        tracing::warn!(
                            pid = %pid,
                            error = %e,
                            "snapshot decode failed; falling back to full journal replay"
                        );
                        None
                    }
                },
                None => None,
            }
        } else {
            None
        };

        // Replay events from (snapshot_seq + 1) .. highest.
        let highest = self.journal.highest_sequence_nr(&pid, 0).await.map_err(PatternError::Journal)?;
        let from = snapshot_seq.map(|s| s + 1).unwrap_or(1);
        if highest >= from {
            let events = self
                .journal
                .replay_messages(&pid, from, highest, u64::MAX)
                .await
                .map_err(PatternError::Journal)?;
            for e in &events {
                let evt = A::decode_event(&e.payload).map_err(PatternError::Codec)?;
                A::apply_event(&mut entity.state, &evt);
            }
        }
        entity.seq = highest;
        entity.recovered = true;
        drop(_permit);
        entity.aggregate.recovery_completed(&entity.state, highest).await;
        Ok(())
    }
}
