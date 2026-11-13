/* Run as a SQL Server administrator. Separate databases preserve service ownership. */
IF DB_ID(N'WorkshopOrders') IS NULL EXEC(N'CREATE DATABASE [WorkshopOrders]');
GO
IF DB_ID(N'WorkshopPayment') IS NULL EXEC(N'CREATE DATABASE [WorkshopPayment]');
GO
IF DB_ID(N'WorkshopInventory') IS NULL EXEC(N'CREATE DATABASE [WorkshopInventory]');
GO
IF DB_ID(N'WorkshopSaga') IS NULL EXEC(N'CREATE DATABASE [WorkshopSaga]');
GO
IF DB_ID(N'WorkshopShipping') IS NULL EXEC(N'CREATE DATABASE [WorkshopShipping]');
GO
IF DB_ID(N'WorkshopAnalytics') IS NULL EXEC(N'CREATE DATABASE [WorkshopAnalytics]');
GO
IF DB_ID(N'WorkshopStreamState') IS NULL EXEC(N'CREATE DATABASE [WorkshopStreamState]');
GO
IF DB_ID(N'WorkshopRetry') IS NULL EXEC(N'CREATE DATABASE [WorkshopRetry]');
GO
IF DB_ID(N'WorkshopCdcEnrichment') IS NULL EXEC(N'CREATE DATABASE [WorkshopCdcEnrichment]');
GO
IF DB_ID(N'WorkshopEventStore') IS NULL EXEC(N'CREATE DATABASE [WorkshopEventStore]');
GO
