//! In-memory persistence-query read journal.

use std::sync::Arc;

use rakka_persistence::InMemoryJournal;
use rakka_persistence_query::SimpleReadJournal;

pub type InMemoryReadJournal = SimpleReadJournal<InMemoryJournal>;

pub fn read_journal(journal: Arc<InMemoryJournal>) -> InMemoryReadJournal {
    SimpleReadJournal::new(journal)
}
