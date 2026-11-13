USE WorkshopAnalytics;
GO
IF OBJECT_ID(N'dbo.analytics_inbox',N'U') IS NULL
CREATE TABLE dbo.analytics_inbox(
 event_id uniqueidentifier NOT NULL CONSTRAINT PK_analytics_inbox PRIMARY KEY,
 payload_hash char(64) NOT NULL,
 processed_at_utc datetimeoffset(7) NOT NULL
);
IF OBJECT_ID(N'dbo.customer_order_windows',N'U') IS NULL
CREATE TABLE dbo.customer_order_windows(
 customer_key nvarchar(200) NOT NULL,
 currency char(3) NOT NULL,
 window_start_utc datetimeoffset(7) NOT NULL,
 window_end_utc datetimeoffset(7) NOT NULL,
 change_count bigint NOT NULL,
 net_paid_amount decimal(18,2) NOT NULL,
 last_event_id uniqueidentifier NOT NULL,
 updated_at_utc datetimeoffset(7) NOT NULL,
 CONSTRAINT PK_customer_order_windows PRIMARY KEY(customer_key,currency,window_start_utc)
);
GO
