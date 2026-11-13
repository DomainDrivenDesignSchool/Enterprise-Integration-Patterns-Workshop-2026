USE WorkshopOrders;
GO
/* Run in WorkshopOrders as a sysadmin/db_owner. SQL Server Agent must be running. */
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name=DB_NAME() AND is_cdc_enabled=1)
    EXEC sys.sp_cdc_enable_db;
GO
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id=OBJECT_ID(N'dbo.orders'))
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'orders',
        @role_name = N'cdc_reader',
        @supports_net_changes = 0;
END;
GO
