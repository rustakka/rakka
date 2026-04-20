//! rustakka-persistence-sql. Unified SQL Journal + SnapshotStore provider.
//!
//! akka.net: `Akka.Persistence.Sql`. Uses `sqlx` under the hood so a single
//! code path targets SQLite (default), Postgres, MySQL, and (later) MSSQL.

mod config;
mod dialect;
mod journal;
mod query;
mod schema;
mod snapshot;

pub use config::{SqlConfig, SqlDialect};
pub use dialect::detect_dialect;
pub use journal::SqlJournal;
pub use query::SqlReadJournal;
pub use schema::ensure_schema;
pub use snapshot::SqlSnapshotStore;
