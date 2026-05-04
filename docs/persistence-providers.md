# Persistence Providers

`atomr-persistence` defines the plugin traits (`Journal`,
`SnapshotStore`, `ReadJournal`). Each backend lives in its own crate so
applications only pull in the drivers they actually use. Every provider
is validated against the shared conformance suite exposed by
`atomr-persistence-tck` (`journal_suite`, `journal_tag_suite`,
`snapshot_suite`).

## Provider matrix

| Crate | Backend | Default feature | Integration test env var |
| ---- | ---- | ---- | ---- |
| `atomr-persistence-sql` | SQLite, Postgres, MySQL, MSSQL (via `sqlx`) | `sqlite` | none (`sqlite::memory:`) |
| `atomr-persistence-redis` | Redis / KeyDB (via `fred`) | — | `ATOMR_IT_REDIS_URL` |
| `atomr-persistence-mongodb` | MongoDB (official driver) | — | `ATOMR_IT_MONGO_URL` |
| `atomr-persistence-cassandra` | Cassandra / ScyllaDB (`scylla`) | — | `ATOMR_IT_CASSANDRA_NODES` |
| `atomr-persistence-aws` | DynamoDB single-table | — | `ATOMR_IT_DYNAMO_ENDPOINT` |
| `atomr-persistence-azure` | Azure Table Storage (SharedKeyLite) | — | `ATOMR_IT_AZURE_CONNECTION_STRING` |

Integration tests short-circuit cleanly when the env var is absent, so
`cargo test --workspace` remains hermetic.

## Choosing a provider

```toml
[dependencies]
atomr-persistence = "0.1"
# Pick ONE (or more) backend:
atomr-persistence-sql = { version = "0.1", default-features = false, features = ["postgres"] }
```

At runtime the provider crate exposes:

- `*Config::from_env()` — loads connection settings from
  `ATOMR_PERSISTENCE_*` or provider-specific env vars.
- `*Journal::connect(cfg)` / `*SnapshotStore::connect(cfg)` — returns
  `Arc<impl Journal>` / `Arc<impl SnapshotStore>` that plugs directly
  into the core `PersistentActor` machinery.
- `*ReadJournal::new(journal)` (SQL today) — tag-based replay.

### SQL (unified via `sqlx`)

```rust
use atomr_persistence_sql::{SqlConfig, SqlJournal, SqlSnapshotStore};

let cfg = SqlConfig::new("postgres://app:app@db/app");
let journal = SqlJournal::connect(cfg.clone()).await?;
let snapshots = SqlSnapshotStore::connect(cfg).await?;
```

Each feature flag (`sqlite`, `postgres`, `mysql`, `mssql`) enables the
matching `sqlx` driver plus the DDL in `migrations/<dialect>/`. Schema
creation is idempotent — safe to call on every boot.

### Redis

Uses Redis sorted sets (`<prefix>:events:<persistence_id>`) for
append-only journals and a hash per persistence-id for snapshots.
`MULTI`/`EXEC` ensures atomic batch writes. Payloads are JSON with
Base64-encoded bytes.

### MongoDB

Uses a dedicated `events` collection with a unique compound index on
`(persistence_id, sequence_nr)`. Tags are stored as an array for
`$elemMatch` queries. Snapshots live in a single collection keyed on
`persistence_id`.

### Cassandra / ScyllaDB

Partitions events by `(persistence_id, partition)` to keep partitions
bounded. `current_max` walks partitions descending via prepared
statements, and replay uses paged token scans.

### AWS DynamoDB

Single-table design: `pid` is the partition key, `sk` uses
`E#<zero-padded sequence>` for events and `S#<sequence>` for snapshots.
Conditional writes guarantee sequence-nr uniqueness. Soft deletes flip
the `deleted` attribute via `UpdateItem`.

### Azure Table Storage

Implements the REST API directly with a custom SharedKeyLite signer.
Events use `RowKey` = zero-padded sequence nr, payloads are Base64 and
tags are stored as CSV. The crate targets Azurite locally and ships a
`cosmos` feature placeholder for a future Cosmos Table API backend.

## Writing your own conformance tests

Every provider crate includes a `tests/tck.rs` that wraps the shared
suite:

```rust
use atomr_persistence_tck::{journal_suite, journal_tag_suite, snapshot_suite};

#[tokio::test]
async fn my_journal_is_conformant() {
    let j = MyJournal::connect(cfg).await.unwrap();
    journal_suite(j.clone(), "my-journal").await;
    journal_tag_suite(j, "my-journal-tags").await;
}
```

Reuse the same suites when writing your own provider — they exercise
idempotent batch writes, ordered replay, soft deletes, highest-seq
tracking, and tag-scoped queries.

## CI

`.github/workflows/persistence-integration.yml` spins up service
containers for Postgres, MySQL, Redis, Mongo, Cassandra,
`dynamodb-local`, and Azurite and runs the provider TCKs against real
backends on every change to a persistence crate. Compile-only jobs
cover the non-default SQL feature flags.

## Release

`.github/workflows/release.yml` publishes crates in dependency order
(`atomr-persistence` → `-tck` → `-query` → each provider) when a
GitHub Release is published. Use the `workflow_dispatch` with
`dry_run=true` to rehearse publishing without touching crates.io.
