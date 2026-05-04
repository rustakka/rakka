//! Journal implementation on Redis sorted sets.

use std::sync::Arc;

use async_trait::async_trait;
use atomr_persistence::{Journal, JournalError, PersistentRepr};
use fred::prelude::*;

use crate::codec::StoredRepr;
use crate::config::RedisConfig;

pub struct RedisJournal {
    client: Pool,
    cfg: RedisConfig,
}

impl RedisJournal {
    /// Connect to Redis using `cfg.url` and return a ready journal.
    pub async fn connect(cfg: RedisConfig) -> Result<Arc<Self>, JournalError> {
        let mut builder = Builder::from_config(Config::from_url(&cfg.url).map_err(JournalError::backend)?);
        let pool = builder
            .set_policy(ReconnectPolicy::new_constant(0, 500))
            .build_pool(cfg.pool_size)
            .map_err(JournalError::backend)?;
        pool.init().await.map_err(JournalError::backend)?;
        Ok(Arc::new(Self { client: pool, cfg }))
    }

    pub fn from_pool(pool: Pool, cfg: RedisConfig) -> Arc<Self> {
        Arc::new(Self { client: pool, cfg })
    }

    pub fn config(&self) -> &RedisConfig {
        &self.cfg
    }

    pub fn client(&self) -> &Pool {
        &self.client
    }
}

fn encode(repr: &PersistentRepr) -> Result<String, JournalError> {
    serde_json::to_string(&StoredRepr::from(repr)).map_err(JournalError::backend)
}

fn decode(raw: &str) -> Result<PersistentRepr, JournalError> {
    let stored: StoredRepr = serde_json::from_str(raw).map_err(JournalError::backend)?;
    Ok(stored.into_repr())
}

#[async_trait]
impl Journal for RedisJournal {
    async fn write_messages(&self, messages: Vec<PersistentRepr>) -> Result<(), JournalError> {
        if messages.is_empty() {
            return Ok(());
        }
        let mut by_pid: std::collections::BTreeMap<String, Vec<PersistentRepr>> =
            std::collections::BTreeMap::new();
        for m in messages {
            by_pid.entry(m.persistence_id.clone()).or_default().push(m);
        }

        for (pid, batch) in by_pid {
            let key = self.cfg.journal_key(&pid);
            let current: i64 = self.client.zcard(&key).await.map_err(JournalError::backend)?;
            for (expected, msg) in (current as u64 + 1..).zip(batch.iter()) {
                if msg.sequence_nr != expected {
                    return Err(JournalError::SequenceOutOfOrder { expected, got: msg.sequence_nr });
                }
            }
            let tx = self.client.next().multi();
            for msg in &batch {
                let payload = encode(msg)?;
                let _: () = tx
                    .zadd(
                        &key,
                        Some(SetOptions::NX),
                        None,
                        false,
                        false,
                        (msg.sequence_nr as f64, payload.clone()),
                    )
                    .await
                    .map_err(JournalError::backend)?;
                for tag in &msg.tags {
                    let tag_key = self.cfg.tag_key(tag);
                    let member = format!("{}:{}", msg.persistence_id, msg.sequence_nr);
                    let _: () = tx
                        .zadd(
                            &tag_key,
                            Some(SetOptions::NX),
                            None,
                            false,
                            false,
                            (msg.sequence_nr as f64, member),
                        )
                        .await
                        .map_err(JournalError::backend)?;
                }
            }
            let _: () = tx.exec(true).await.map_err(JournalError::backend)?;
        }
        Ok(())
    }

    async fn delete_messages_to(
        &self,
        persistence_id: &str,
        to_sequence_nr: u64,
    ) -> Result<(), JournalError> {
        let key = self.cfg.journal_key(persistence_id);
        let members: Vec<String> = self
            .client
            .zrangebyscore(&key, 0.0, to_sequence_nr as f64, false, None)
            .await
            .map_err(JournalError::backend)?;
        for raw in members {
            let mut repr = decode(&raw)?;
            repr.deleted = true;
            let new_payload = encode(&repr)?;
            let _: () = self
                .client
                .zadd(&key, Some(SetOptions::XX), None, false, false, (repr.sequence_nr as f64, new_payload))
                .await
                .map_err(JournalError::backend)?;
            let _: () = self.client.zrem(&key, raw).await.map_err(JournalError::backend)?;
        }
        Ok(())
    }

    async fn replay_messages(
        &self,
        persistence_id: &str,
        from: u64,
        to: u64,
        max: u64,
    ) -> Result<Vec<PersistentRepr>, JournalError> {
        let key = self.cfg.journal_key(persistence_id);
        let limit = if max > i64::MAX as u64 { None } else { Some((0i64, max as i64)) };
        let members: Vec<String> = self
            .client
            .zrangebyscore(&key, from as f64, to as f64, false, limit)
            .await
            .map_err(JournalError::backend)?;
        let mut out = Vec::with_capacity(members.len());
        for raw in members {
            let repr = decode(&raw)?;
            if !repr.deleted {
                out.push(repr);
            }
        }
        Ok(out)
    }

    async fn highest_sequence_nr(&self, persistence_id: &str, _from: u64) -> Result<u64, JournalError> {
        let key = self.cfg.journal_key(persistence_id);
        let members: Vec<(String, f64)> =
            self.client.zrange(&key, -1, -1, None, false, None, true).await.map_err(JournalError::backend)?;
        Ok(members.into_iter().next().map(|(_, s)| s as u64).unwrap_or(0))
    }

    async fn events_by_tag(
        &self,
        tag: &str,
        from_offset: u64,
        max: u64,
    ) -> Result<Vec<PersistentRepr>, JournalError> {
        let key = self.cfg.tag_key(tag);
        let limit = if max > i64::MAX as u64 { None } else { Some((0i64, max as i64)) };
        let entries: Vec<String> = self
            .client
            .zrangebyscore(&key, from_offset as f64, f64::INFINITY, false, limit)
            .await
            .map_err(JournalError::backend)?;
        let mut out = Vec::new();
        for entry in entries {
            let (pid, _, seq) = match entry.rsplit_once(':') {
                Some((p, s)) => (p.to_string(), entry.as_str(), s.parse::<u64>().unwrap_or(0)),
                None => continue,
            };
            let journal_key = self.cfg.journal_key(&pid);
            let members: Vec<String> = self
                .client
                .zrangebyscore(&journal_key, seq as f64, seq as f64, false, None)
                .await
                .map_err(JournalError::backend)?;
            for raw in members {
                let repr = decode(&raw)?;
                if !repr.deleted {
                    out.push(repr);
                }
            }
        }
        Ok(out)
    }
}
