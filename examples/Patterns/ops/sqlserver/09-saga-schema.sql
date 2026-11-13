USE WorkshopSaga;
GO
IF OBJECT_ID(N'dbo.saga_instances',N'U') IS NULL
CREATE TABLE dbo.saga_instances(
 order_id uniqueidentifier NOT NULL CONSTRAINT PK_saga_instances PRIMARY KEY,
 status varchar(30) NOT NULL,
 payment_reference nvarchar(200) NULL,
 reservation_id uniqueidentifier NULL,
 version int NOT NULL CONSTRAINT DF_saga_version DEFAULT 0,
 order_payload nvarchar(max) NOT NULL CONSTRAINT CK_saga_order_payload_json CHECK(ISJSON(order_payload)=1),
 created_at_utc datetimeoffset(7) NOT NULL,
 updated_at_utc datetimeoffset(7) NOT NULL
);
IF OBJECT_ID(N'dbo.saga_inbox',N'U') IS NULL
CREATE TABLE dbo.saga_inbox(
 event_id uniqueidentifier NOT NULL CONSTRAINT PK_saga_inbox PRIMARY KEY,
 payload_hash char(64) NOT NULL,
 received_at_utc datetimeoffset(7) NOT NULL
);
IF OBJECT_ID(N'dbo.saga_outbox',N'U') IS NULL
CREATE TABLE dbo.saga_outbox(
 id uniqueidentifier NOT NULL CONSTRAINT PK_saga_outbox PRIMARY KEY,
 aggregate_id uniqueidentifier NOT NULL,
 aggregate_version bigint NOT NULL,
 event_type varchar(100) NOT NULL,
 payload nvarchar(max) NOT NULL CONSTRAINT CK_saga_outbox_json CHECK(ISJSON(payload)=1),
 created_at_utc datetimeoffset(7) NOT NULL,
 published_at_utc datetimeoffset(7) NULL,
 lock_owner nvarchar(200) NULL,
 locked_until_utc datetimeoffset(7) NULL,
 CONSTRAINT UQ_saga_outbox_aggregate_version UNIQUE(aggregate_id,aggregate_version)
);
IF NOT EXISTS(SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'dbo.saga_outbox') AND name=N'IX_saga_outbox_pending')
 CREATE INDEX IX_saga_outbox_pending ON dbo.saga_outbox(aggregate_id,aggregate_version) INCLUDE(created_at_utc,event_type,published_at_utc) WHERE published_at_utc IS NULL;
GO

/* Durable command timeout/recovery metadata. Safe for existing WorkshopSaga databases. */
IF COL_LENGTH(N'dbo.saga_instances',N'pending_command_type') IS NULL
 ALTER TABLE dbo.saga_instances ADD pending_command_type varchar(100) NULL;
IF COL_LENGTH(N'dbo.saga_instances',N'pending_since_utc') IS NULL
 ALTER TABLE dbo.saga_instances ADD pending_since_utc datetimeoffset(7) NULL;
IF COL_LENGTH(N'dbo.saga_instances',N'recovery_attempts') IS NULL
 ALTER TABLE dbo.saga_instances ADD recovery_attempts int NOT NULL CONSTRAINT DF_saga_recovery_attempts DEFAULT 0;
IF COL_LENGTH(N'dbo.saga_instances',N'last_recovery_at_utc') IS NULL
 ALTER TABLE dbo.saga_instances ADD last_recovery_at_utc datetimeoffset(7) NULL;
IF COL_LENGTH(N'dbo.saga_instances',N'recovery_lock_owner') IS NULL
 ALTER TABLE dbo.saga_instances ADD recovery_lock_owner nvarchar(200) NULL;
IF COL_LENGTH(N'dbo.saga_instances',N'recovery_locked_until_utc') IS NULL
 ALTER TABLE dbo.saga_instances ADD recovery_locked_until_utc datetimeoffset(7) NULL;
IF COL_LENGTH(N'dbo.saga_instances',N'dead_at_utc') IS NULL
 ALTER TABLE dbo.saga_instances ADD dead_at_utc datetimeoffset(7) NULL;
IF COL_LENGTH(N'dbo.saga_instances',N'dead_reason') IS NULL
 ALTER TABLE dbo.saga_instances ADD dead_reason nvarchar(1000) NULL;
IF OBJECT_ID(N'dbo.saga_operator_audit',N'U') IS NULL
BEGIN
 CREATE TABLE dbo.saga_operator_audit(
  id uniqueidentifier NOT NULL CONSTRAINT PK_saga_operator_audit PRIMARY KEY,
  order_id uniqueidentifier NOT NULL,
  actor nvarchar(200) NOT NULL,
  action varchar(50) NOT NULL,
  detail nvarchar(1000) NOT NULL,
  created_at_utc datetimeoffset(7) NOT NULL
 );
 CREATE INDEX IX_saga_operator_audit_order ON dbo.saga_operator_audit(order_id,created_at_utc DESC);
END;
IF NOT EXISTS(SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'dbo.saga_instances') AND name=N'IX_saga_recovery_due')
 CREATE INDEX IX_saga_recovery_due ON dbo.saga_instances(status,pending_since_utc,recovery_locked_until_utc)
  INCLUDE(pending_command_type,recovery_attempts) WHERE pending_command_type IS NOT NULL;
GO
