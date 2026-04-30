//! `ReadJournal` adapter that reuses the SQL journal for event-by-id /
//! event-by-tag queries.

use std::sync::Arc;

use async_trait::async_trait;
use rakka_persistence::{Journal, JournalError};
use rakka_persistence_query::{EventEnvelope, ReadJournal};

use crate::journal::SqlJournal;

pub struct SqlReadJournal {
    journal: Arc<SqlJournal>,
}

impl SqlReadJournal {
    pub fn new(journal: Arc<SqlJournal>) -> Self {
        Self { journal }
    }

    pub async fn events_by_tag(
        &self,
        tag: &str,
        from_offset: u64,
        max: u64,
    ) -> Result<Vec<EventEnvelope>, JournalError> {
        let reprs = self.journal.events_by_tag(tag, from_offset, max).await?;
        Ok(reprs.into_iter().map(Into::into).collect())
    }
}

#[async_trait]
impl ReadJournal for SqlReadJournal {
    async fn events_by_persistence_id(
        &self,
        persistence_id: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<EventEnvelope>, JournalError> {
        let reprs =
            self.journal.replay_messages(persistence_id, from, to, u64::MAX).await?;
        Ok(reprs.into_iter().map(Into::into).collect())
    }
}
