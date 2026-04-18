//! In-memory persistence-query read journal.

use std::sync::Arc;

use rustakka_persistence::InMemoryJournal;
use rustakka_persistence_query::SimpleReadJournal;

pub type InMemoryReadJournal = SimpleReadJournal<InMemoryJournal>;

pub fn read_journal(journal: Arc<InMemoryJournal>) -> InMemoryReadJournal {
    SimpleReadJournal::new(journal)
}
