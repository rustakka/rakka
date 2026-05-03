---
name: rakka-persistence
description: Use when adding event sourcing to an actor in a project that depends on rakka. Covers the Eventsourced trait, journals, snapshots, recovery, RecoveryPermitter, AsyncSnapshotter, and choosing a storage adapter (sql / redis / mongodb / cassandra / aws / azure). Triggers when implementing PersistentActor or Eventsourced, configuring a Journal/SnapshotStore, or wiring a backend.
---

# Event sourcing with rakka-persistence

`rakka-persistence` provides the plugin traits (`Journal`,
`SnapshotStore`, `ReadJournal`) and a high-level `Eventsourced` trait
that hides most of the recovery plumbing. Each backend ships as its
own crate so applications only pull in the drivers they actually use.

## Pick a storage adapter

| Crate | Backend | Default feature |
|---|---|---|
| `rakka-persistence-sql` | SQLite, Postgres, MySQL, MSSQL (via `sqlx`) | `sqlite` |
| `rakka-persistence-redis` | Redis / KeyDB (via `fred`) | — |
| `rakka-persistence-mongodb` | MongoDB | — |
| `rakka-persistence-cassandra` | Cassandra / ScyllaDB | — |
| `rakka-persistence-aws` | DynamoDB single-table | — |
| `rakka-persistence-azure` | Azure Table Storage | — |

```toml
[dependencies]
rakka = { package = "rakka-rs", version = "0.2", features = ["persistence"] }
rakka-persistence-sql = { version = "0.1", default-features = false, features = ["postgres"] }
```

Every adapter is validated against `rakka-persistence-tck`'s shared
conformance suite — if the suite passes, the adapter is interchangeable.

See `docs/persistence-providers.md` for the full provider matrix and
the integration-test environment variables (`RAKKA_IT_REDIS_URL`, etc.).

## The `Eventsourced` trait

Express a persistent entity as: state + commands → events + how events
mutate state. The trait derives the recovery loop for you.

```rust
use async_trait::async_trait;
use rakka_persistence::{Eventsourced, EventsourcedError};

struct Counter { id: String }

#[async_trait]
impl Eventsourced for Counter {
    type Command = CounterCmd;
    type Event   = CounterEvent;
    type State   = CounterState;
    type Error   = CounterErr;

    fn persistence_id(&self) -> String { self.id.clone() }

    fn command_to_events(
        &self,
        state: &Self::State,
        cmd: Self::Command,
    ) -> Result<Vec<Self::Event>, Self::Error> { /* validate, emit events */ }

    fn apply_event(state: &mut Self::State, e: &Self::Event) { /* mutate state */ }

    fn encode_event(e: &Self::Event) -> Result<Vec<u8>, String> { /* ... */ }
    fn decode_event(bytes: &[u8])     -> Result<Self::Event, String> { /* ... */ }
}
```

Rules:

- **`apply_event` must be pure and total.** It runs both during command
  handling AND during replay. Side effects belong in the actor's
  command handler, not here.
- **`command_to_events` is allowed to fail.** Validation lives here —
  reject invalid commands before any event is persisted.
- **Events are forever.** Your encoded events are your schema. Plan
  for evolution: include version tags, never delete variants, prefer
  upcasting decoders.
- **`persistence_id` is the entity's address in the journal.** It must
  be stable across restarts and must not collide with another entity's id.

See `examples/event-sourced-counter/` for the full pattern with
snapshots and bounded recovery.

## Snapshots

Snapshots compress recovery. With a snapshot every N events, recovery
loads `latest_snapshot + events_since_snapshot` instead of replaying
the entire journal.

```rust
use rakka_persistence::{AsyncSnapshotter, SnapshotPolicy, InMemorySnapshotStore};

let snapshots   = InMemorySnapshotStore::new();
let snapshotter = AsyncSnapshotter::new(snapshots, SnapshotPolicy::Periodic { every: 100 });
```

`AsyncSnapshotter` writes off the actor's hot path — the actor
doesn't block on snapshot persistence.

Tune `every` to your event size and recovery SLO. Smaller = faster
recovery + more snapshot writes. Larger = the opposite.

## Bounded recovery: `RecoveryPermitter`

When many entities recover concurrently (e.g. cold cluster start),
unbounded parallel replay can saturate the journal. `RecoveryPermitter`
caps the number of simultaneous recoveries:

```rust
let permits = RecoveryPermitter::new(64);
```

Wire it through your `Eventsourced` setup; it acts like a semaphore
around recovery.

## Tagged event streams: `persistence-query`

For projections, CQRS read models, or audit, enable `persistence-query`
and use `ReadJournal` to consume tagged events. Today this is supported
by the SQL adapter; others may follow. See
`crates/rakka-persistence-query/`.

## Local development

`InMemoryJournal` and `InMemorySnapshotStore` are real implementations
of the journal/snapshot traits intended for tests and examples. They
satisfy the same conformance suite as the production adapters but lose
data on restart.

```rust
use rakka_persistence::{InMemoryJournal, InMemorySnapshotStore};

let journal   = Arc::new(InMemoryJournal::default());
let snapshots = InMemorySnapshotStore::new();
```

## Implementing a custom backend

Your backend must satisfy `rakka-persistence-tck`'s suites:

- `journal_suite` — append, read range, delete range
- `journal_tag_suite` — tagged stream replay (if implementing query)
- `snapshot_suite` — save, load latest, delete

A passing TCK run is the bar for "compatible with rakka-persistence".

## Canonical references

- `examples/event-sourced-counter/` — `Eventsourced` + snapshots
- `crates/rakka-persistence/src/eventsourced.rs` — trait definition
- `crates/rakka-persistence/src/recovery_permitter.rs` — bounded recovery
- `crates/rakka-persistence/src/async_snapshot.rs` — `AsyncSnapshotter`
- `crates/rakka-persistence-tck/` — conformance suite
- `docs/persistence-providers.md` — provider matrix and env vars

## Common mistakes

- **Side effects in `apply_event`.** They will run during replay too,
  causing double-sends, duplicate writes, etc.
- **Mutable state derived outside `apply_event`.** It won't be present
  after recovery.
- **Encoding events with a serializer that breaks on field reorders.**
  Pick a format with explicit field tags (protobuf, MessagePack with
  named fields, JSON) — not naïve `bincode` of structs you'll edit.
- **Snapshotting too often.** Snapshot writes are not free; tune
  `Periodic { every: N }` based on event size.
- **Reusing a `persistence_id` for a different entity type.** The
  journal will happily serve those events back to the wrong decoder.
