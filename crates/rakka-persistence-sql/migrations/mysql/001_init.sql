CREATE TABLE IF NOT EXISTS event_journal (
    persistence_id VARCHAR(255) NOT NULL,
    sequence_nr    BIGINT UNSIGNED NOT NULL,
    payload        BLOB NOT NULL,
    manifest       VARCHAR(255) NOT NULL DEFAULT '',
    writer_uuid    VARCHAR(64) NOT NULL DEFAULT '',
    deleted        TINYINT(1) NOT NULL DEFAULT 0,
    created_at     BIGINT NOT NULL,
    PRIMARY KEY (persistence_id, sequence_nr)
);

CREATE TABLE IF NOT EXISTS event_tags (
    persistence_id VARCHAR(255) NOT NULL,
    sequence_nr    BIGINT UNSIGNED NOT NULL,
    tag            VARCHAR(255) NOT NULL,
    PRIMARY KEY (persistence_id, sequence_nr, tag),
    INDEX idx_event_tags_tag (tag, persistence_id, sequence_nr)
);

CREATE TABLE IF NOT EXISTS snapshot_store (
    persistence_id VARCHAR(255) NOT NULL,
    sequence_nr    BIGINT UNSIGNED NOT NULL,
    payload        BLOB NOT NULL,
    timestamp      BIGINT NOT NULL,
    created_at     BIGINT NOT NULL,
    PRIMARY KEY (persistence_id, sequence_nr)
);
