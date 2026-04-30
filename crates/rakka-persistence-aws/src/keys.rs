//! Helpers for the single-table sort key layout.

pub const EVENT_PREFIX: &str = "E#";
pub const SNAPSHOT_PREFIX: &str = "S#";

const SK_WIDTH: usize = 20;

pub fn event_sk(sequence_nr: u64) -> String {
    format!("{EVENT_PREFIX}{seq:0width$}", seq = sequence_nr, width = SK_WIDTH)
}

pub fn snapshot_sk(sequence_nr: u64) -> String {
    format!("{SNAPSHOT_PREFIX}{seq:0width$}", seq = sequence_nr, width = SK_WIDTH)
}

pub fn parse_sequence(sk: &str) -> Option<u64> {
    let stripped = sk
        .strip_prefix(EVENT_PREFIX)
        .or_else(|| sk.strip_prefix(SNAPSHOT_PREFIX))?;
    stripped.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lex_order_matches_numeric() {
        let low = event_sk(1);
        let high = event_sk(1_000_000);
        assert!(low < high, "{low} vs {high}");
    }

    #[test]
    fn parse_round_trip() {
        assert_eq!(parse_sequence(&event_sk(42)), Some(42));
        assert_eq!(parse_sequence(&snapshot_sk(7)), Some(7));
        assert_eq!(parse_sequence("bogus"), None);
    }
}
