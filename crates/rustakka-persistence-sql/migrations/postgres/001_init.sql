CREATE TABLE IF NOT EXISTS event_journal (
    persistence_id TEXT NOT NULL,
    sequence_nr    BIGINT NOT NULL,
    payload        BYTEA NOT NULL,
    manifest       TEXT NOT NULL DEFAULT '',
    writer_uuid    TEXT NOT NULL DEFAULT '',
    deleted        BOOLEAN NOT NULL DEFAULT FALSE,
    created_at     BIGINT NOT NULL,
    PRIMARY KEY (persistence_id, sequence_nr)
);

CREATE TABLE IF NOT EXISTS event_tags (
    persistence_id TEXT NOT NULL,
    sequence_nr    BIGINT NOT NULL,
    tag            TEXT NOT NULL,
    PRIMARY KEY (persistence_id, sequence_nr, tag)
);

CREATE INDEX IF NOT EXISTS idx_event_tags_tag ON event_tags(tag, persistence_id, sequence_nr);

CREATE TABLE IF NOT EXISTS snapshot_store (
    persistence_id TEXT NOT NULL,
    sequence_nr    BIGINT NOT NULL,
    payload        BYTEA NOT NULL,
    timestamp      BIGINT NOT NULL,
    created_at     BIGINT NOT NULL,
    PRIMARY KEY (persistence_id, sequence_nr)
);
