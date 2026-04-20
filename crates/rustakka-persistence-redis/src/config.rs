//! Connection configuration for the Redis provider.

use std::env;

#[derive(Debug, Clone)]
pub struct RedisConfig {
    pub url: String,
    pub key_prefix: String,
    pub pool_size: usize,
}

impl RedisConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into(), key_prefix: "rustakka".into(), pool_size: 4 }
    }

    pub fn with_key_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.key_prefix = prefix.into();
        self
    }

    pub fn with_pool_size(mut self, size: usize) -> Self {
        self.pool_size = size.max(1);
        self
    }

    /// Env lookup order: `RUSTAKKA_PERSISTENCE_REDIS_URL`, `RUSTAKKA_IT_REDIS_URL`,
    /// `REDIS_URL`, then dev fallback `redis://127.0.0.1:6379`.
    pub fn from_env() -> Self {
        let url = env::var("RUSTAKKA_PERSISTENCE_REDIS_URL")
            .or_else(|_| env::var("RUSTAKKA_IT_REDIS_URL"))
            .or_else(|_| env::var("REDIS_URL"))
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
        let prefix = env::var("RUSTAKKA_PERSISTENCE_REDIS_PREFIX")
            .unwrap_or_else(|_| "rustakka".into());
        Self::new(url).with_key_prefix(prefix)
    }

    pub(crate) fn journal_key(&self, pid: &str) -> String {
        format!("{}:journal:{}", self.key_prefix, pid)
    }

    pub(crate) fn snapshot_key(&self, pid: &str) -> String {
        format!("{}:snapshot:{}", self.key_prefix, pid)
    }

    pub(crate) fn tag_key(&self, tag: &str) -> String {
        format!("{}:tag:{}", self.key_prefix, tag)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_respect_prefix() {
        let cfg = RedisConfig::new("redis://h").with_key_prefix("demo");
        assert_eq!(cfg.journal_key("p1"), "demo:journal:p1");
        assert_eq!(cfg.snapshot_key("p1"), "demo:snapshot:p1");
        assert_eq!(cfg.tag_key("red"), "demo:tag:red");
    }

    #[test]
    fn from_env_default() {
        std::env::remove_var("RUSTAKKA_PERSISTENCE_REDIS_URL");
        std::env::remove_var("RUSTAKKA_IT_REDIS_URL");
        std::env::remove_var("REDIS_URL");
        let cfg = RedisConfig::from_env();
        assert!(cfg.url.starts_with("redis://"));
    }
}
