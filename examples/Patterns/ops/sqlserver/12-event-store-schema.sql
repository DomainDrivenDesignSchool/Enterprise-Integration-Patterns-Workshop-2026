/* SQL Server Event Store. Keep domain events append-only; grant application role INSERT/SELECT only. */
IF OBJECT_ID('dbo.aggregate_stream_heads', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.aggregate_stream_heads
    (
        aggregate_id uniqueidentifier NOT NULL CONSTRAINT PK_aggregate_stream_heads PRIMARY KEY,
        current_version bigint NOT NULL,
        row_version rowversion NOT NULL,
        CONSTRAINT CK_aggregate_stream_heads_version CHECK (current_version >= 0)
    );
END;
GO

IF OBJECT_ID('dbo.domain_events', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.domain_events
    (
        event_id uniqueidentifier NOT NULL CONSTRAINT PK_domain_events PRIMARY KEY,
        aggregate_id uniqueidentifier NOT NULL,
        stream_version bigint NOT NULL,
        event_type nvarchar(200) NOT NULL,
        schema_version int NOT NULL,
        payload_json nvarchar(max) NOT NULL,
        occurred_at_utc datetimeoffset(7) NOT NULL,
        correlation_id uniqueidentifier NULL,
        causation_id uniqueidentifier NULL,
        CONSTRAINT CK_domain_events_stream_version CHECK (stream_version > 0),
        CONSTRAINT CK_domain_events_schema_version CHECK (schema_version > 0),
        CONSTRAINT CK_domain_events_payload_json CHECK (ISJSON(payload_json) = 1),
        CONSTRAINT UQ_domain_events_aggregate_version UNIQUE (aggregate_id, stream_version),
        CONSTRAINT FK_domain_events_stream_head FOREIGN KEY (aggregate_id)
            REFERENCES dbo.aggregate_stream_heads(aggregate_id)
    );
    CREATE INDEX IX_domain_events_aggregate_time
        ON dbo.domain_events(aggregate_id, occurred_at_utc)
        INCLUDE(stream_version, event_type, schema_version);
END;
GO

IF OBJECT_ID('dbo.aggregate_snapshots', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.aggregate_snapshots
    (
        aggregate_id uniqueidentifier NOT NULL,
        stream_version bigint NOT NULL,
        state_json nvarchar(max) NOT NULL,
        created_at_utc datetimeoffset(7) NOT NULL,
        CONSTRAINT PK_aggregate_snapshots PRIMARY KEY (aggregate_id, stream_version),
        CONSTRAINT CK_aggregate_snapshots_version CHECK (stream_version > 0),
        CONSTRAINT CK_aggregate_snapshots_state_json CHECK (ISJSON(state_json) = 1),
        CONSTRAINT FK_aggregate_snapshots_stream_head FOREIGN KEY (aggregate_id)
            REFERENCES dbo.aggregate_stream_heads(aggregate_id)
    );
END;
GO

/* Separate append-only event permissions from mutable stream-head/snapshot permissions. */
IF DATABASE_PRINCIPAL_ID(N'workshop_event_store_writer') IS NULL
    CREATE ROLE [workshop_event_store_writer];
GO
GRANT SELECT, INSERT ON dbo.domain_events TO [workshop_event_store_writer];
DENY UPDATE, DELETE ON dbo.domain_events TO [workshop_event_store_writer];
GRANT SELECT, INSERT, UPDATE ON dbo.aggregate_stream_heads TO [workshop_event_store_writer];
GRANT SELECT, INSERT ON dbo.aggregate_snapshots TO [workshop_event_store_writer];
GO
-- Map the application database user to this role through your deployment/secret-management process.
