//! URL scheme detection for the supported SQL dialects.

use crate::config::SqlDialect;

pub fn detect_dialect(url: &str) -> Option<SqlDialect> {
    let lower = url.to_ascii_lowercase();
    if lower.starts_with("sqlite:") {
        Some(SqlDialect::Sqlite)
    } else if lower.starts_with("postgres:") || lower.starts_with("postgresql:") {
        Some(SqlDialect::Postgres)
    } else if lower.starts_with("mysql:") || lower.starts_with("mariadb:") {
        Some(SqlDialect::MySql)
    } else if lower.starts_with("mssql:") || lower.starts_with("sqlserver:") {
        Some(SqlDialect::MsSql)
    } else {
        None
    }
}

pub(crate) fn sqlite_migration() -> &'static str {
    include_str!("../migrations/sqlite/001_init.sql")
}

pub(crate) fn postgres_migration() -> &'static str {
    include_str!("../migrations/postgres/001_init.sql")
}

pub(crate) fn mysql_migration() -> &'static str {
    include_str!("../migrations/mysql/001_init.sql")
}

pub(crate) fn mssql_migration() -> &'static str {
    include_str!("../migrations/mssql/001_init.sql")
}

pub(crate) fn migration_for(dialect: SqlDialect) -> &'static str {
    match dialect {
        SqlDialect::Sqlite => sqlite_migration(),
        SqlDialect::Postgres => postgres_migration(),
        SqlDialect::MySql => mysql_migration(),
        SqlDialect::MsSql => mssql_migration(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_all_schemes() {
        assert_eq!(detect_dialect("sqlite::memory:"), Some(SqlDialect::Sqlite));
        assert_eq!(detect_dialect("postgres://a"), Some(SqlDialect::Postgres));
        assert_eq!(detect_dialect("postgresql://a"), Some(SqlDialect::Postgres));
        assert_eq!(detect_dialect("mysql://a"), Some(SqlDialect::MySql));
        assert_eq!(detect_dialect("mssql://a"), Some(SqlDialect::MsSql));
        assert_eq!(detect_dialect("https://x"), None);
    }

    #[test]
    fn migrations_embedded() {
        assert!(migration_for(SqlDialect::Sqlite).contains("event_journal"));
        assert!(migration_for(SqlDialect::Postgres).contains("event_journal"));
        assert!(migration_for(SqlDialect::MySql).contains("event_journal"));
    }
}
