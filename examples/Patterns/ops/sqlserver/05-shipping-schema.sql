USE WorkshopShipping;
GO
IF OBJECT_ID(N'dbo.shipments',N'U') IS NULL
CREATE TABLE dbo.shipments(
 id uniqueidentifier NOT NULL CONSTRAINT PK_shipments PRIMARY KEY,
 order_id uniqueidentifier NOT NULL,
 reservation_id uniqueidentifier NOT NULL,
 status varchar(30) NOT NULL,
 created_at_utc datetimeoffset(7) NOT NULL,
 CONSTRAINT UQ_shipments_order UNIQUE(order_id)
);
IF OBJECT_ID(N'dbo.shipping_outbox',N'U') IS NULL
CREATE TABLE dbo.shipping_outbox(
 id uniqueidentifier NOT NULL CONSTRAINT PK_shipping_outbox PRIMARY KEY,
 order_id uniqueidentifier NOT NULL,
 event_type varchar(100) NOT NULL,
 payload nvarchar(max) NOT NULL CONSTRAINT CK_shipping_outbox_json CHECK(ISJSON(payload)=1),
 created_at_utc datetimeoffset(7) NOT NULL,
 published_at_utc datetimeoffset(7) NULL,
 lock_owner nvarchar(200) NULL,
 locked_until_utc datetimeoffset(7) NULL
);
IF NOT EXISTS(SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'dbo.shipping_outbox') AND name=N'IX_shipping_outbox_pending')
 CREATE INDEX IX_shipping_outbox_pending ON dbo.shipping_outbox(created_at_utc,id) INCLUDE(order_id,event_type) WHERE published_at_utc IS NULL;
GO
