//! Idempotent schema bootstrap.
//!
//! `ensure_schema` is safe to call on startup in dev / test; in prod it is
//! guarded by the `auto_migrate` config flag (no-op when disabled).

use rustakka_persistence::JournalError;
use sqlx::AnyPool;

use crate::config::{SqlConfig, SqlDialect};

/// Install sqlx runtime drivers for every enabled feature. Idempotent.
pub(crate) fn init_drivers() {
    sqlx::any::install_default_drivers();
}

/// Apply the migration DDL for the configured dialect. Statements are split
/// on `;` so a single embedded SQL file can bootstrap every required table.
pub async fn ensure_schema(pool: &AnyPool, cfg: &SqlConfig) -> Result<(), JournalError> {
    if !cfg.auto_migrate {
        return Ok(());
    }
    let ddl = crate::dialect::migration_for(cfg.dialect);
    for stmt in split_statements(ddl) {
        sqlx::query(&stmt).execute(pool).await.map_err(JournalError::backend)?;
    }
    Ok(())
}

fn split_statements(ddl: &str) -> Vec<String> {
    let stripped: String = ddl
        .lines()
        .map(|l| l.split("--").next().unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n");
    stripped
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

/// Drop every journal/snapshot row for tests that want a clean slate.
/// Requires the schema to already exist.
#[allow(dead_code)]
pub async fn truncate(pool: &AnyPool, _dialect: SqlDialect) -> Result<(), JournalError> {
    for table in ["event_tags", "event_journal", "snapshot_store"] {
        sqlx::query(&format!("DELETE FROM {table}"))
            .execute(pool)
            .await
            .map_err(JournalError::backend)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitter_skips_blank_and_comments() {
        let sql = "-- hello\nCREATE TABLE a (id INT);\n\nCREATE TABLE b (id INT);";
        let out = split_statements(sql);
        assert_eq!(out.len(), 2);
        assert!(out[0].starts_with("CREATE TABLE a"));
    }
}
