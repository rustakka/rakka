//! Connection configuration for the unified SQL provider.
//!
//! Resolves connection URLs from env vars with a dev / test / prod aware
//! fallback ladder.

use std::env;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    Sqlite,
    Postgres,
    MySql,
    MsSql,
}

impl SqlDialect {
    pub fn scheme(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
            Self::MySql => "mysql",
            Self::MsSql => "mssql",
        }
    }
}

#[derive(Debug, Clone)]
pub struct SqlConfig {
    pub url: String,
    pub dialect: SqlDialect,
    pub max_connections: u32,
    pub auto_migrate: bool,
}

impl SqlConfig {
    pub fn new(url: impl Into<String>) -> Self {
        let url = url.into();
        let dialect = crate::dialect::detect_dialect(&url).unwrap_or(SqlDialect::Sqlite);
        // SQLite `:memory:` tied to a single connection is the only way every
        // caller sees the same database; we also keep connections low by default
        // because most event-sourced workloads are write-heavy through a small
        // pool.
        let is_memory_sqlite =
            dialect == SqlDialect::Sqlite && (url.contains(":memory:") || url.ends_with(":memory:"));
        let max_connections = if is_memory_sqlite { 1 } else { 5 };
        Self { url, dialect, max_connections, auto_migrate: true }
    }

    pub fn with_max_connections(mut self, n: u32) -> Self {
        self.max_connections = n;
        self
    }

    pub fn with_auto_migrate(mut self, auto: bool) -> Self {
        self.auto_migrate = auto;
        self
    }

    /// Resolve a config from environment variables.
    ///
    /// Lookup order:
    /// 1. `ATOMR_PERSISTENCE_SQL_URL` (any environment, explicit override).
    /// 2. `ATOMR_IT_SQL_URL` (test integration).
    /// 3. `DATABASE_URL` (prod conventions).
    /// 4. Dev fallback: `sqlite::memory:`.
    pub fn from_env() -> Self {
        let url = env::var("ATOMR_PERSISTENCE_SQL_URL")
            .or_else(|_| env::var("ATOMR_IT_SQL_URL"))
            .or_else(|_| env::var("DATABASE_URL"))
            .unwrap_or_else(|_| "sqlite::memory:".to_string());
        let auto_migrate = env::var("ATOMR_PERSISTENCE_SQL_AUTO_MIGRATE")
            .map(|v| !matches!(v.as_str(), "0" | "false" | "no"))
            .unwrap_or(true);
        Self::new(url).with_auto_migrate(auto_migrate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_sqlite_scheme() {
        let cfg = SqlConfig::new("sqlite::memory:");
        assert_eq!(cfg.dialect, SqlDialect::Sqlite);
    }

    #[test]
    fn detects_postgres_scheme() {
        let cfg = SqlConfig::new("postgres://u:p@h/db");
        assert_eq!(cfg.dialect, SqlDialect::Postgres);
    }

    #[test]
    fn builder_overrides() {
        let cfg = SqlConfig::new("sqlite::memory:").with_max_connections(12).with_auto_migrate(false);
        assert_eq!(cfg.max_connections, 12);
        assert!(!cfg.auto_migrate);
    }
}
