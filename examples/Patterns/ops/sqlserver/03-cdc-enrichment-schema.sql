USE WorkshopCdcEnrichment;
GO
IF OBJECT_ID(N'dbo.customer_enrichment',N'U') IS NULL
CREATE TABLE dbo.customer_enrichment(
 customer_id uniqueidentifier NOT NULL CONSTRAINT PK_customer_enrichment PRIMARY KEY,
 tier nvarchar(40) NULL,
 region nvarchar(100) NULL,
 shipping_zone nvarchar(100) NULL,
 is_active bit NOT NULL CONSTRAINT DF_customer_enrichment_active DEFAULT 1,
 updated_at_utc datetimeoffset(7) NOT NULL CONSTRAINT DF_customer_enrichment_updated DEFAULT SYSUTCDATETIME()
);
GO
