# SQL Server migration status

## Provider decision

The workshop is standardizing on Microsoft SQL Server (SQL Server 2019+), not SQLite. The target is a service-owned database per bounded context. SQL Server is selected because the labs rely on multiple worker replicas, row-level claim/lease coordination, transactional outboxes, inbox deduplication, durable sagas and native CDC. SQLite's single-writer model and lack of a built-in CDC feature would materially change those demonstrations.

## Completed in this increment

- `PaymentWorker` uses EF Core for ordinary PSP webhook inbox persistence; direct `Microsoft.Data.SqlClient` remains in reconciliation/processor paths that require explicit SQL Server locking, atomic claim, or multi-statement transaction semantics.
- `OrderService` / `Persistence` now use EF Core SQL Server; the order outbox claim uses SQL Server locking hints (`UPDLOCK`, `READPAST`, `ROWLOCK`) and `TOP` instead of PostgreSQL locking syntax.
- `CdcEnricher` enrichment lookup now uses `Microsoft.Data.SqlClient`; an SQL Server CDC enablement script and Debezium SQL Server connector example are included.
- `ShippingWorker` now uses SQL Server row/key-range locking and a SQL Server Outbox claim query.
- `AnalyticsWorker` now uses SQL Server Inbox deduplication and a `MERGE ... WITH (HOLDLOCK)` projection upsert.
- `RetryScheduler` now stores retry envelopes in SQL Server and claims due retries with `UPDLOCK`, `READPAST` and `OUTPUT`.
- `InventoryWorker` now uses SQL Server transactions, key-range/row locks, parameterized SKU sets and a SQL Server transactional Outbox.
- `SagaOrchestrator` now persists Saga Inbox/state/Outbox in SQL Server; aggregate-version ordering is preserved by a unique constraint and lock-aware claim query.
- `StreamProcessor` now persists Inbox, watermark/key state, window aggregates, late events and output Outbox in SQL Server.
- `WorkshopPayment` database schema is defined in `ops/sqlserver/04-payment-schema.sql`.
- `WorkshopOrders` schema is defined in `ops/sqlserver/01-orders-schema.sql`.
- `WorkshopCdcEnrichment` schema is defined in `ops/sqlserver/03-cdc-enrichment-schema.sql`.
- `WorkshopShipping` schema is defined in `ops/sqlserver/05-shipping-schema.sql`.
- `WorkshopAnalytics` schema is defined in `ops/sqlserver/06-analytics-schema.sql`.
- `WorkshopRetry` schema is defined in `ops/sqlserver/07-retry-schema.sql`.
- `WorkshopInventory` schema and seed stock are defined in `ops/sqlserver/08-inventory-schema.sql`.
- `WorkshopSaga` schema is defined in `ops/sqlserver/09-saga-schema.sql`.
- `WorkshopStreamState` schema is defined in `ops/sqlserver/10-stream-state-schema.sql`.
- `WorkshopEventStore` schema is defined in `ops/sqlserver/12-event-store-schema.sql`; it is a standalone Event Sourcing lab at this stage.
- Reconciliation claims use `UPDLOCK + READPAST + ROWLOCK` and `UPDATE ... OUTPUT` rather than PostgreSQL `FOR UPDATE SKIP LOCKED` / `RETURNING`.
- Retry scheduling uses `DATEADD` and UTC SQL Server time.
- Idempotent inserts use serializable key-range locking (`UPDLOCK,HOLDLOCK`) plus unique constraints.
- Ledger is append-only using a SQL Server trigger; corrections must be compensating entries.
- Payment Outbox dispatch retains per-aggregate ordering and leases.

## Remaining PostgreSQL artifacts

All application workers and the shared EF Core persistence project in this solution now use SQL Server. The original PostgreSQL SQL files and PostgreSQL Debezium connector are retained as historical/reference material; they are not referenced by the migrated service projects.

## Apply the migrated databases

Create all service databases and apply every SQL Server schema in one step:

```powershell
.\ops\sqlserver\apply-all-schemas.ps1 -Server localhost,1433 -User sa -Password $env:SA_PASSWORD
```

The script creates the service databases and applies each schema. CDC enablement is separate because SQL Server Agent and the appropriate server permissions are required.

## Configure service connections

Set these environment variables in the shell that launches the services. Use a secret store/User Secrets in real deployments; do not commit passwords.

```powershell
$env:ConnectionStrings__Orders = 'Server=localhost,1433;Database=WorkshopOrders;User Id=sa;Password=<secret>;Encrypt=True;TrustServerCertificate=True'
$env:ConnectionStrings__Payment = 'Server=localhost,1433;Database=WorkshopPayment;User Id=sa;Password=<secret>;Encrypt=True;TrustServerCertificate=True'
$env:ConnectionStrings__Inventory = 'Server=localhost,1433;Database=WorkshopInventory;User Id=sa;Password=<secret>;Encrypt=True;TrustServerCertificate=True'
$env:ConnectionStrings__Saga = 'Server=localhost,1433;Database=WorkshopSaga;User Id=sa;Password=<secret>;Encrypt=True;TrustServerCertificate=True'
$env:ConnectionStrings__Shipping = 'Server=localhost,1433;Database=WorkshopShipping;User Id=sa;Password=<secret>;Encrypt=True;TrustServerCertificate=True'
$env:ConnectionStrings__Analytics = 'Server=localhost,1433;Database=WorkshopAnalytics;User Id=sa;Password=<secret>;Encrypt=True;TrustServerCertificate=True'
$env:ConnectionStrings__RetrySchedule = 'Server=localhost,1433;Database=WorkshopRetry;User Id=sa;Password=<secret>;Encrypt=True;TrustServerCertificate=True'
$env:ConnectionStrings__StreamState = 'Server=localhost,1433;Database=WorkshopStreamState;User Id=sa;Password=<secret>;Encrypt=True;TrustServerCertificate=True'
$env:ConnectionStrings__Enrichment = 'Server=localhost,1433;Database=WorkshopCdcEnrichment;User Id=sa;Password=<secret>;Encrypt=True;TrustServerCertificate=True'
```

## Concurrency semantics

`READPAST` skips rows currently locked by another worker; `UPDLOCK` reserves selected rows for update; `ROWLOCK` requests row-level locks but SQL Server may still escalate under pressure. Correctness does not depend on row-lock escalation being impossible: owner-conditional updates, lease expiry, and unique constraints remain mandatory. Keep claim transactions short and never call the PSP while a database transaction is open.

## Verification status

The SQL scripts and source transformations were statically inspected. This environment does not contain the .NET SDK or a SQL Server instance, so `dotnet build`, schema application, concurrency tests and fault-injection integration tests have not been run. Do not treat this increment as production-certified until those tests pass against SQL Server.

## SQL Server CDC for orders

After applying the `WorkshopOrders` schema, enable CDC with `ops/sqlserver/02-enable-orders-cdc.sql` as a database owner/sysadmin. SQL Server Agent must be running. The Debezium connector template is `ops/debezium/sqlserver-orders-connector.example.json`; provide credentials through the Connect secret provider, not by committing them to the JSON. The SQL Server connector emits the `workshop-sqlserver.dbo.orders` topic consumed by CdcEnricher. The enrichment database is separate (`WorkshopCdcEnrichment`).
