/* Run in WorkshopStreamState as a schema owner. Educational monthly RANGE partition example. */
USE WorkshopStreamState;
GO
IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name=N'pf_event_month')
    EXEC(N'CREATE PARTITION FUNCTION pf_event_month(datetime2(0)) AS RANGE RIGHT FOR VALUES
      (''2026-02-01T00:00:00'',''2026-03-01T00:00:00'',''2026-04-01T00:00:00'',''2026-05-01T00:00:00'')');
GO
IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name=N'ps_event_month')
    EXEC(N'CREATE PARTITION SCHEME ps_event_month AS PARTITION pf_event_month ALL TO ([PRIMARY])');
GO
IF OBJECT_ID(N'dbo.partitioned_event_archive',N'U') IS NULL
BEGIN
    CREATE TABLE dbo.partitioned_event_archive(
      event_time_utc datetime2(0) NOT NULL,
      event_id uniqueidentifier NOT NULL,
      aggregate_id uniqueidentifier NOT NULL,
      payload nvarchar(max) NOT NULL,
      CONSTRAINT PK_partitioned_event_archive PRIMARY KEY CLUSTERED(event_time_utc,event_id)
    ) ON ps_event_month(event_time_utc);
END;
GO
/* Inspect partition distribution and query plan pruning. */
SELECT $PARTITION.pf_event_month(event_time_utc) AS partition_number, COUNT_BIG(*) AS rows_in_partition
FROM dbo.partitioned_event_archive
GROUP BY $PARTITION.pf_event_month(event_time_utc)
ORDER BY partition_number;
GO
