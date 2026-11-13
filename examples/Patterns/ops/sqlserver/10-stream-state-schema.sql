USE WorkshopStreamState;
GO
IF OBJECT_ID(N'dbo.stream_inbox',N'U') IS NULL
CREATE TABLE dbo.stream_inbox(
 event_id uniqueidentifier NOT NULL,
 topic nvarchar(249) NOT NULL,
 partition_id int NOT NULL,
 offset_value bigint NOT NULL,
 payload_hash char(64) NOT NULL,
 processed_at_utc datetimeoffset(7) NOT NULL,
 CONSTRAINT PK_stream_inbox PRIMARY KEY(event_id),
 CONSTRAINT UQ_stream_inbox_topic_offset UNIQUE(topic,partition_id,offset_value)
);
IF OBJECT_ID(N'dbo.stream_key_state',N'U') IS NULL
CREATE TABLE dbo.stream_key_state(
 topic nvarchar(249) NOT NULL,
 customer_key nvarchar(200) NOT NULL,
 currency char(3) NOT NULL,
 max_event_time_utc datetimeoffset(7) NOT NULL,
 CONSTRAINT PK_stream_key_state PRIMARY KEY NONCLUSTERED(topic,customer_key,currency)
);
IF OBJECT_ID(N'dbo.stream_late_events',N'U') IS NULL
CREATE TABLE dbo.stream_late_events(
 event_id uniqueidentifier NOT NULL CONSTRAINT PK_stream_late_events PRIMARY KEY,
 customer_key nvarchar(200) NOT NULL,
 currency char(3) NOT NULL,
 event_time_utc datetimeoffset(7) NOT NULL,
 amount decimal(18,2) NOT NULL,
 reason varchar(100) NOT NULL,
 recorded_at_utc datetimeoffset(7) NOT NULL
);
IF OBJECT_ID(N'dbo.stream_window_state',N'U') IS NULL
CREATE TABLE dbo.stream_window_state(
 customer_key nvarchar(200) NOT NULL,
 currency char(3) NOT NULL,
 window_start_utc datetimeoffset(7) NOT NULL,
 window_end_utc datetimeoffset(7) NOT NULL,
 event_count bigint NOT NULL,
 total_amount decimal(18,2) NOT NULL,
 last_event_time_utc datetimeoffset(7) NOT NULL,
 emitted bit NOT NULL,
 CONSTRAINT PK_stream_window_state PRIMARY KEY(customer_key,currency,window_start_utc)
);
IF OBJECT_ID(N'dbo.stream_outbox',N'U') IS NULL
CREATE TABLE dbo.stream_outbox(
 id uniqueidentifier NOT NULL CONSTRAINT PK_stream_outbox PRIMARY KEY,
 customer_key nvarchar(200) NOT NULL,
 currency char(3) NOT NULL,
 payload nvarchar(max) NOT NULL CONSTRAINT CK_stream_outbox_json CHECK(ISJSON(payload)=1),
 created_at_utc datetimeoffset(7) NOT NULL,
 published_at_utc datetimeoffset(7) NULL,
 lock_owner nvarchar(200) NULL,
 locked_until_utc datetimeoffset(7) NULL
);
IF NOT EXISTS(SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'dbo.stream_outbox') AND name=N'IX_stream_outbox_pending')
 CREATE INDEX IX_stream_outbox_pending ON dbo.stream_outbox(customer_key,currency,created_at_utc,id) INCLUDE(locked_until_utc,lock_owner,published_at_utc) WHERE published_at_utc IS NULL;
GO
