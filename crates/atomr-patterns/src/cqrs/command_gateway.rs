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
    EventsourcedError, Journal, PersistentRepr, RecoveryPermitter,
};
use tokio::sync::oneshot;

use crate::extensions::ExtensionSlots;
use crate::{AggregateRoot, Command, DomainEvent, PatternError};

pub(crate) type CommandReply<A> = oneshot::Sender<
    Result<Vec<<A as atomr_persistence::Eventsourced>::Event>,
           PatternError<<A as atomr_persistence::Eventsourced>::Error>>,
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
    async fn process(
        &mut self,
        cmd: A::Command,
    ) -> Result<Vec<A::Event>, PatternError<A::Error>> {
        // 1. Pre-handler interceptors. Any rejection short-circuits.
        self.extensions.run_interceptors(&cmd)?;

        let id = cmd.aggregate_id();

        // 2. Take entity out of the map so we can borrow it mutably
        //    across the async work without colliding with `&mut self`.
        let mut entity = self.entities.remove(&id).unwrap_or_else(|| EntityState {
            aggregate: (self.factory)(id.clone()),
            state: <A::State as Default>::default(),
            seq: 0,
            recovered: false,
        });

        let result = self.process_entity(&mut entity, cmd).await;

        // 3. Always restore the entity, even on failure.
        self.entities.insert(id, entity);

        result
    }

    async fn process_entity(
        &mut self,
        entity: &mut EntityState<A>,
        cmd: A::Command,
    ) -> Result<Vec<A::Event>, PatternError<A::Error>> {
        // Lazy recovery on first command for this id.
        if !entity.recovered {
            let highest = entity
                .aggregate
                .recover(self.journal.clone(), &mut entity.state, &self.permits)
                .await
                .map_err(map_eventsourced_err)?;
            entity.seq = highest;
            entity.recovered = true;
        }

        // Pure projection of command -> events. Domain validation lives here.
        let events = entity
            .aggregate
            .command_to_events(&entity.state, cmd)
            .map_err(PatternError::Domain)?;

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

        // Post-persist: run sync listeners, then drop closed taps.
        for e in &events {
            self.extensions.notify_listeners(e);
        }
        for e in &events {
            self.extensions.push_event_taps(e);
        }

        Ok(events)
    }
}

fn map_eventsourced_err<E>(e: EventsourcedError<E>) -> PatternError<E> {
    match e {
        EventsourcedError::Journal(j) => PatternError::Journal(j),
        EventsourcedError::Codec(s) => PatternError::Codec(s),
        EventsourcedError::PermitDenied => {
            PatternError::Invariant("recovery permit denied".into())
        }
        EventsourcedError::Domain(d) => PatternError::Domain(d),
        _ => PatternError::Invariant("unknown eventsourced error".into()),
    }
}
