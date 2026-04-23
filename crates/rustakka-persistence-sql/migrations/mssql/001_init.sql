IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = N'event_journal')
BEGIN
    CREATE TABLE event_journal (
        persistence_id NVARCHAR(255) NOT NULL,
        sequence_nr    BIGINT        NOT NULL,
        payload        VARBINARY(MAX) NOT NULL,
        manifest       NVARCHAR(255) NOT NULL DEFAULT '',
        writer_uuid    NVARCHAR(64)  NOT NULL DEFAULT '',
        deleted        BIT           NOT NULL DEFAULT 0,
        created_at     BIGINT        NOT NULL,
        CONSTRAINT pk_event_journal PRIMARY KEY (persistence_id, sequence_nr)
    );
END;

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = N'event_tags')
BEGIN
    CREATE TABLE event_tags (
        persistence_id NVARCHAR(255) NOT NULL,
        sequence_nr    BIGINT        NOT NULL,
        tag            NVARCHAR(255) NOT NULL,
        CONSTRAINT pk_event_tags PRIMARY KEY (persistence_id, sequence_nr, tag)
    );
    CREATE INDEX idx_event_tags_tag ON event_tags(tag, persistence_id, sequence_nr);
END;

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = N'snapshot_store')
BEGIN
    CREATE TABLE snapshot_store (
        persistence_id NVARCHAR(255) NOT NULL,
        sequence_nr    BIGINT        NOT NULL,
        payload        VARBINARY(MAX) NOT NULL,
        timestamp      BIGINT        NOT NULL,
        created_at     BIGINT        NOT NULL,
        CONSTRAINT pk_snapshot_store PRIMARY KEY (persistence_id, sequence_nr)
    );
END;
