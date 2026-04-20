//! Keyspace + table bootstrap (idempotent).

use rustakka_persistence::JournalError;
use scylla::client::session::Session;

use crate::config::CassandraConfig;

pub async fn ensure_schema(session: &Session, cfg: &CassandraConfig) -> Result<(), JournalError> {
    let create_ks = format!(
        "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {};",
        cfg.keyspace, cfg.replication
    );
    session.query_unpaged(create_ks, &[]).await.map_err(JournalError::backend)?;

    let create_journal = format!(
        "CREATE TABLE IF NOT EXISTS {ks}.{table} (\n\
             persistence_id text,\n\
             partition_nr bigint,\n\
             sequence_nr bigint,\n\
             payload blob,\n\
             manifest text,\n\
             writer_uuid text,\n\
             deleted boolean,\n\
             tags set<text>,\n\
             created_at bigint,\n\
             PRIMARY KEY ((persistence_id, partition_nr), sequence_nr))\n\
         WITH CLUSTERING ORDER BY (sequence_nr ASC);",
        ks = cfg.keyspace,
        table = cfg.journal_table
    );
    session.query_unpaged(create_journal, &[]).await.map_err(JournalError::backend)?;

    let create_snapshot = format!(
        "CREATE TABLE IF NOT EXISTS {ks}.{table} (\n\
             persistence_id text,\n\
             sequence_nr bigint,\n\
             payload blob,\n\
             timestamp bigint,\n\
             PRIMARY KEY (persistence_id, sequence_nr))\n\
         WITH CLUSTERING ORDER BY (sequence_nr DESC);",
        ks = cfg.keyspace,
        table = cfg.snapshot_table
    );
    session.query_unpaged(create_snapshot, &[]).await.map_err(JournalError::backend)?;

    Ok(())
}
