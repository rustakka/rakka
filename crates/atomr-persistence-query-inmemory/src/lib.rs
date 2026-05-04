//! In-memory persistence-query read journal.

use std::sync::Arc;

use atomr_persistence::InMemoryJournal;
use atomr_persistence_query::SimpleReadJournal;

pub type InMemoryReadJournal = SimpleReadJournal<InMemoryJournal>;

pub fn read_journal(journal: Arc<InMemoryJournal>) -> InMemoryReadJournal {
    SimpleReadJournal::new(journal)
}
