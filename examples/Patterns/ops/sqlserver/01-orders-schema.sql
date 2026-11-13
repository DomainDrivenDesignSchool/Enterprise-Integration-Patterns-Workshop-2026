/* Run in WorkshopOrders. SQL Server 2019+. */
IF OBJECT_ID(N'dbo.orders',N'U') IS NULL
CREATE TABLE dbo.orders(
 id uniqueidentifier NOT NULL CONSTRAINT PK_orders PRIMARY KEY,
 customer_id uniqueidentifier NOT NULL,
 total_amount decimal(18,2) NOT NULL CONSTRAINT CK_orders_total CHECK(total_amount>=0),
 currency char(3) NOT NULL,
 status varchar(30) NOT NULL,
 created_at_utc datetimeoffset(7) NOT NULL
);
IF OBJECT_ID(N'dbo.order_items',N'U') IS NULL
CREATE TABLE dbo.order_items(
 id uniqueidentifier NOT NULL CONSTRAINT PK_order_items PRIMARY KEY,
 order_id uniqueidentifier NOT NULL,
 sku nvarchar(100) NOT NULL,
 quantity int NOT NULL CONSTRAINT CK_order_items_quantity CHECK(quantity>0),
 unit_price decimal(18,2) NOT NULL CONSTRAINT CK_order_items_price CHECK(unit_price>0),
 CONSTRAINT FK_order_items_orders FOREIGN KEY(order_id) REFERENCES dbo.orders(id)
);
IF OBJECT_ID(N'dbo.outbox_messages',N'U') IS NULL
CREATE TABLE dbo.outbox_messages(
 id uniqueidentifier NOT NULL CONSTRAINT PK_outbox_messages PRIMARY KEY,
 aggregate_id uniqueidentifier NOT NULL,
 event_type varchar(100) NOT NULL,
 payload nvarchar(max) NOT NULL CONSTRAINT CK_outbox_messages_json CHECK(ISJSON(payload)=1),
 occurred_at_utc datetimeoffset(7) NOT NULL,
 published_at_utc datetimeoffset(7) NULL,
 attempts int NOT NULL CONSTRAINT DF_outbox_attempts DEFAULT 0,
 next_attempt_at_utc datetimeoffset(7) NOT NULL,
 locked_until_utc datetimeoffset(7) NULL,
 lock_owner nvarchar(200) NULL
);
IF NOT EXISTS(SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'dbo.outbox_messages') AND name=N'IX_outbox_messages_pending')
 CREATE INDEX IX_outbox_messages_pending ON dbo.outbox_messages(next_attempt_at_utc,occurred_at_utc,id) INCLUDE(aggregate_id,event_type,published_at_utc) WHERE published_at_utc IS NULL;
IF OBJECT_ID(N'dbo.order_command_inbox',N'U') IS NULL
CREATE TABLE dbo.order_command_inbox(
 event_id uniqueidentifier NOT NULL CONSTRAINT PK_order_command_inbox PRIMARY KEY,
 payload_hash char(64) NOT NULL,
 processed_at_utc datetimeoffset(7) NOT NULL
);
GO
