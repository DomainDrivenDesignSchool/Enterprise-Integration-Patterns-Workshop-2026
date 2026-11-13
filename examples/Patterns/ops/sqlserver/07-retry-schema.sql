USE WorkshopRetry;
GO
IF OBJECT_ID(N'dbo.scheduled_retries',N'U') IS NULL
CREATE TABLE dbo.scheduled_retries(
 id uniqueidentifier NOT NULL CONSTRAINT PK_scheduled_retries PRIMARY KEY,
 original_topic nvarchar(249) NOT NULL,
 original_partition int NOT NULL,
 source_offset bigint NOT NULL,
 message_key nvarchar(1000) NULL,
 payload varbinary(max) NOT NULL,
 headers_json nvarchar(max) NOT NULL CONSTRAINT CK_scheduled_retries_headers_json CHECK(ISJSON(headers_json)=1),
 attempt int NOT NULL,
 due_at_utc datetimeoffset(7) NOT NULL,
 created_at_utc datetimeoffset(7) NOT NULL,
 published_at_utc datetimeoffset(7) NULL,
 lock_owner nvarchar(200) NULL,
 locked_until_utc datetimeoffset(7) NULL
);
IF NOT EXISTS(SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'dbo.scheduled_retries') AND name=N'IX_scheduled_retries_due')
 CREATE INDEX IX_scheduled_retries_due ON dbo.scheduled_retries(published_at_utc,due_at_utc,original_topic,original_partition,source_offset) INCLUDE(message_key,attempt,lock_owner,locked_until_utc);
GO
