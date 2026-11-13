USE WorkshopInventory;
GO
IF OBJECT_ID(N'dbo.stock_items',N'U') IS NULL
CREATE TABLE dbo.stock_items(
 sku nvarchar(100) NOT NULL CONSTRAINT PK_stock_items PRIMARY KEY,
 available int NOT NULL CONSTRAINT CK_stock_available CHECK(available>=0),
 reserved int NOT NULL CONSTRAINT DF_stock_reserved DEFAULT 0 CONSTRAINT CK_stock_reserved CHECK(reserved>=0),
 version bigint NOT NULL CONSTRAINT DF_stock_version DEFAULT 1
);
IF OBJECT_ID(N'dbo.stock_reservations',N'U') IS NULL
CREATE TABLE dbo.stock_reservations(
 id uniqueidentifier NOT NULL CONSTRAINT PK_stock_reservations PRIMARY KEY,
 order_id uniqueidentifier NOT NULL,
 status varchar(20) NOT NULL CONSTRAINT CK_stock_reservation_status CHECK(status IN ('Reserving','Reserved','Released','Rejected')),
 request_hash char(64) NULL CONSTRAINT CK_stock_reservation_hash CHECK(request_hash IS NULL OR LEN(request_hash)=64),
 created_at_utc datetimeoffset(7) NOT NULL,
 released_at_utc datetimeoffset(7) NULL,
 CONSTRAINT UQ_stock_reservations_order UNIQUE(order_id)
);
IF OBJECT_ID(N'dbo.stock_reservation_lines',N'U') IS NULL
CREATE TABLE dbo.stock_reservation_lines(
 reservation_id uniqueidentifier NOT NULL,
 sku nvarchar(100) NOT NULL,
 quantity int NOT NULL CONSTRAINT CK_stock_reservation_quantity CHECK(quantity>0),
 CONSTRAINT PK_stock_reservation_lines PRIMARY KEY(reservation_id,sku),
 CONSTRAINT FK_stock_reservation_lines_reservation FOREIGN KEY(reservation_id) REFERENCES dbo.stock_reservations(id),
 CONSTRAINT FK_stock_reservation_lines_stock FOREIGN KEY(sku) REFERENCES dbo.stock_items(sku)
);
IF OBJECT_ID(N'dbo.inventory_outbox',N'U') IS NULL
CREATE TABLE dbo.inventory_outbox(
 id uniqueidentifier NOT NULL CONSTRAINT PK_inventory_outbox PRIMARY KEY,
 aggregate_id uniqueidentifier NOT NULL,
 event_type varchar(100) NOT NULL,
 payload nvarchar(max) NOT NULL CONSTRAINT CK_inventory_outbox_json CHECK(ISJSON(payload)=1),
 created_at_utc datetimeoffset(7) NOT NULL,
 published_at_utc datetimeoffset(7) NULL,
 lock_owner nvarchar(200) NULL,
 locked_until_utc datetimeoffset(7) NULL
);
IF NOT EXISTS(SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'dbo.inventory_outbox') AND name=N'IX_inventory_outbox_pending')
 CREATE INDEX IX_inventory_outbox_pending ON dbo.inventory_outbox(created_at_utc,id) INCLUDE(aggregate_id,event_type) WHERE published_at_utc IS NULL;
IF NOT EXISTS(SELECT 1 FROM dbo.stock_items WHERE sku=N'SKU-KEYBOARD') INSERT dbo.stock_items(sku,available,reserved) VALUES(N'SKU-KEYBOARD',100,0);
IF NOT EXISTS(SELECT 1 FROM dbo.stock_items WHERE sku=N'SKU-MOUSE') INSERT dbo.stock_items(sku,available,reserved) VALUES(N'SKU-MOUSE',250,0);
IF NOT EXISTS(SELECT 1 FROM dbo.stock_items WHERE sku=N'SKU-MONITOR') INSERT dbo.stock_items(sku,available,reserved) VALUES(N'SKU-MONITOR',40,0);
GO
