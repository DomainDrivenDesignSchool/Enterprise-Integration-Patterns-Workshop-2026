/* Run in WorkshopPayment. Idempotent SQL Server 2019+ schema. */
IF OBJECT_ID(N'dbo.payment_attempts',N'U') IS NULL
BEGIN
 CREATE TABLE dbo.payment_attempts(
  id uniqueidentifier NOT NULL CONSTRAINT PK_payment_attempts PRIMARY KEY,
  order_id uniqueidentifier NOT NULL,
  idempotency_key nvarchar(200) NOT NULL,
  amount decimal(18,2) NOT NULL CONSTRAINT CK_payment_amount_positive CHECK(amount>0),
  currency char(3) NOT NULL,
  status varchar(20) NOT NULL CONSTRAINT CK_payment_status CHECK(status IN ('Initiated','Unknown','Authorized','Rejected','ManualReview','Refunded')),
  provider_reference nvarchar(500) NULL,
  decline_reason nvarchar(1000) NULL,
  created_at_utc datetimeoffset(7) NOT NULL,
  updated_at_utc datetimeoffset(7) NULL,
  next_reconcile_at_utc datetimeoffset(7) NULL,
  reconciliation_attempts int NOT NULL CONSTRAINT DF_payment_attempts_count DEFAULT 0 CONSTRAINT CK_payment_attempts_count CHECK(reconciliation_attempts>=0),
  reconciliation_owner nvarchar(200) NULL,
  reconciliation_locked_until_utc datetimeoffset(7) NULL,
  last_error nvarchar(2000) NULL,
  CONSTRAINT UQ_payment_attempts_idempotency UNIQUE(idempotency_key)
 );
END;
IF NOT EXISTS(SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'dbo.payment_attempts') AND name=N'UX_payment_attempts_provider_reference')
 CREATE UNIQUE INDEX UX_payment_attempts_provider_reference ON dbo.payment_attempts(provider_reference) WHERE provider_reference IS NOT NULL;
IF NOT EXISTS(SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'dbo.payment_attempts') AND name=N'IX_payment_reconciliation_due')
 CREATE INDEX IX_payment_reconciliation_due ON dbo.payment_attempts(status,next_reconcile_at_utc,created_at_utc) INCLUDE(order_id,idempotency_key,amount,currency,reconciliation_attempts,reconciliation_owner,reconciliation_locked_until_utc);

IF OBJECT_ID(N'dbo.payment_ledger',N'U') IS NULL
BEGIN
 CREATE TABLE dbo.payment_ledger(
  id uniqueidentifier NOT NULL CONSTRAINT PK_payment_ledger PRIMARY KEY,
  order_id uniqueidentifier NOT NULL,
  entry_type varchar(10) NOT NULL CONSTRAINT CK_payment_ledger_type CHECK(entry_type IN ('Charge','Refund')),
  amount decimal(18,2) NOT NULL,
  currency char(3) NOT NULL,
  reference nvarchar(500) NOT NULL,
  created_at_utc datetimeoffset(7) NOT NULL
 );
END;
IF NOT EXISTS(SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'dbo.payment_ledger') AND name=N'IX_payment_ledger_order')
 CREATE INDEX IX_payment_ledger_order ON dbo.payment_ledger(order_id,created_at_utc);
IF NOT EXISTS(SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'dbo.payment_ledger') AND name=N'UX_payment_ledger_reference_type')
 CREATE UNIQUE INDEX UX_payment_ledger_reference_type ON dbo.payment_ledger(reference,entry_type);
GO
/* Append-only guard. Corrections must be compensating ledger entries, never UPDATE/DELETE. */
CREATE OR ALTER TRIGGER dbo.TR_payment_ledger_append_only ON dbo.payment_ledger
AFTER UPDATE, DELETE
AS
BEGIN
 SET NOCOUNT ON;
 IF EXISTS(SELECT 1 FROM inserted) OR EXISTS(SELECT 1 FROM deleted)
 BEGIN
  ;THROW 51001, 'payment_ledger is append-only; UPDATE/DELETE is forbidden.', 1;
 END;
END;
GO
IF OBJECT_ID(N'dbo.payment_outbox',N'U') IS NULL
BEGIN
 CREATE TABLE dbo.payment_outbox(
  id uniqueidentifier NOT NULL CONSTRAINT PK_payment_outbox PRIMARY KEY,
  aggregate_id uniqueidentifier NOT NULL,
  event_type varchar(100) NOT NULL,
  payload nvarchar(max) NOT NULL CONSTRAINT CK_payment_outbox_json CHECK(ISJSON(payload)=1),
  created_at_utc datetimeoffset(7) NOT NULL,
  published_at_utc datetimeoffset(7) NULL,
  lock_owner nvarchar(200) NULL,
  locked_until_utc datetimeoffset(7) NULL
 );
END;
IF NOT EXISTS(SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'dbo.payment_outbox') AND name=N'IX_payment_outbox_pending')
 CREATE INDEX IX_payment_outbox_pending ON dbo.payment_outbox(created_at_utc,id) INCLUDE(aggregate_id,event_type,published_at_utc) WHERE published_at_utc IS NULL;

IF OBJECT_ID(N'dbo.payment_refund_attempts',N'U') IS NULL
BEGIN
 CREATE TABLE dbo.payment_refund_attempts(
  id uniqueidentifier NOT NULL CONSTRAINT PK_payment_refund_attempts PRIMARY KEY,
  order_id uniqueidentifier NOT NULL,
  payment_reference nvarchar(500) NOT NULL,
  idempotency_key nvarchar(600) NOT NULL,
  amount decimal(18,2) NOT NULL CONSTRAINT CK_payment_refund_amount CHECK(amount>0),
  currency char(3) NOT NULL,
  status varchar(20) NOT NULL CONSTRAINT CK_payment_refund_status CHECK(status IN ('Initiated','Unknown','Refunded','Rejected','ManualReview')),
  reconciliation_attempts int NOT NULL CONSTRAINT DF_payment_refund_attempts_count DEFAULT 0 CONSTRAINT CK_payment_refund_attempts_count CHECK(reconciliation_attempts>=0),
  next_reconcile_at_utc datetimeoffset(7) NULL,
  reconciliation_owner nvarchar(200) NULL,
  reconciliation_locked_until_utc datetimeoffset(7) NULL,
  last_error nvarchar(2000) NULL,
  created_at_utc datetimeoffset(7) NOT NULL,
  updated_at_utc datetimeoffset(7) NULL,
  CONSTRAINT UQ_payment_refund_idempotency UNIQUE(idempotency_key)
 );
END;
IF NOT EXISTS(SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'dbo.payment_refund_attempts') AND name=N'IX_payment_refund_reconciliation_due')
 CREATE INDEX IX_payment_refund_reconciliation_due ON dbo.payment_refund_attempts(status,next_reconcile_at_utc,created_at_utc) INCLUDE(order_id,payment_reference,idempotency_key,amount,currency,reconciliation_attempts,reconciliation_owner,reconciliation_locked_until_utc);
GO

/* Durable PSP webhook Inbox. The unique provider/event key makes retries idempotent. */
IF OBJECT_ID(N'dbo.psp_webhook_inbox',N'U') IS NULL
BEGIN
 CREATE TABLE dbo.psp_webhook_inbox(
  id bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_psp_webhook_inbox PRIMARY KEY,
  provider nvarchar(100) NOT NULL,
  provider_event_id nvarchar(200) NOT NULL,
  payload_hash char(64) NOT NULL,
  payload nvarchar(max) NOT NULL CONSTRAINT CK_psp_webhook_payload_json CHECK(ISJSON(payload)=1),
  event_type varchar(40) NOT NULL,
  idempotency_key nvarchar(600) NOT NULL,
  provider_reference nvarchar(500) NULL,
  occurred_at_utc datetimeoffset(7) NOT NULL,
  received_at_utc datetimeoffset(7) NOT NULL,
  processed_at_utc datetimeoffset(7) NULL,
  status varchar(20) NOT NULL CONSTRAINT CK_psp_webhook_status CHECK(status IN ('Pending','Processing','Retry','Processed','Conflict','Dead')),
  attempts int NOT NULL CONSTRAINT DF_psp_webhook_attempts DEFAULT 0,
  next_attempt_at_utc datetimeoffset(7) NOT NULL,
  lock_owner nvarchar(200) NULL,
  locked_until_utc datetimeoffset(7) NULL,
  last_error nvarchar(1000) NULL,
  CONSTRAINT UQ_psp_webhook_provider_event UNIQUE(provider,provider_event_id)
 );
 CREATE INDEX IX_psp_webhook_pending ON dbo.psp_webhook_inbox(status,next_attempt_at_utc,received_at_utc)
  INCLUDE(provider,idempotency_key,event_type,attempts,lock_owner,locked_until_utc) WHERE status <> 'Processed' AND status <> 'Conflict' AND status <> 'Dead';
END;
GO
