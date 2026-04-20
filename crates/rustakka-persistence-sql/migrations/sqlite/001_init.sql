CREATE TABLE IF NOT EXISTS event_journal (
    persistence_id TEXT NOT NULL,
    sequence_nr    INTEGER NOT NULL,
    payload        BLOB NOT NULL,
    manifest       TEXT NOT NULL DEFAULT '',
    writer_uuid    TEXT NOT NULL DEFAULT '',
    deleted        INTEGER NOT NULL DEFAULT 0,
    created_at     INTEGER NOT NULL,
    PRIMARY KEY (persistence_id, sequence_nr)
);

CREATE TABLE IF NOT EXISTS event_tags (
    persistence_id TEXT NOT NULL,
    sequence_nr    INTEGER NOT NULL,
    tag            TEXT NOT NULL,
    PRIMARY KEY (persistence_id, sequence_nr, tag),
    FOREIGN KEY (persistence_id, sequence_nr)
        REFERENCES event_journal(persistence_id, sequence_nr)
);

CREATE INDEX IF NOT EXISTS idx_event_tags_tag ON event_tags(tag, persistence_id, sequence_nr);

CREATE TABLE IF NOT EXISTS snapshot_store (
    persistence_id TEXT NOT NULL,
    sequence_nr    INTEGER NOT NULL,
    payload        BLOB NOT NULL,
    timestamp      INTEGER NOT NULL,
    created_at     INTEGER NOT NULL,
    PRIMARY KEY (persistence_id, sequence_nr)
);
