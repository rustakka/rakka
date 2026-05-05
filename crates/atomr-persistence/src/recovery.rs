//! Recovery parameters.

#[derive(Debug, Clone, Copy)]
pub struct Recovery {
    pub from_snapshot: bool,
    pub to_sequence_nr: u64,
    pub replay_max: u64,
}

impl Default for Recovery {
    fn default() -> Self {
        Self { from_snapshot: true, to_sequence_nr: u64::MAX, replay_max: u64::MAX }
    }
}

impl Recovery {
    pub fn none() -> Self {
        Self { from_snapshot: false, to_sequence_nr: 0, replay_max: 0 }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum RecoveryState {
    NotStarted,
    ReadingSnapshot,
    ReplayingEvents,
    Completed,
    Failed,
}
