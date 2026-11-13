# Practical CDC: SQL Server CDC → Debezium → Kafka → Enrichment → Stream Processing

This Lab uses SQL Server Change Data Capture and the SQL Server transaction log; PostgreSQL logical replication and pgoutput play no role in this path.

```text
OrderService
  └─ SQL Server: WorkshopOrders.dbo.orders
       └─ SQL Server CDC capture tables + LSN
            └─ Debezium SQL Server Connector
                 └─ Kafka: workshop-sqlserver.dbo.orders
                      └─ CdcEnricher (WorkshopCdcEnrichment lookup + Kafka transaction)
                           └─ commerce.order-change-enriched.v1
                                └─ StreamProcessor (WorkshopStreamState)
                                     └─ analytics.customer-order-windows.v1
                                          └─ AnalyticsWorker (WorkshopAnalytics)
```

## 1. Prerequisites

- SQL Server 2019+ with SQL Server Agent running
- Kafka and Kafka Connect
- The Debezium SQL Server Connector plugin installed on all Kafka Connect workers
- Databases and tables created per `docs/SQLSERVER-MIGRATION-STATUS.md`

## 2. Enabling CDC

After running the schemas, run the following with DBA access:

```powershell
.\ops\sqlserver\enable-orders-cdc.ps1 -Server localhost,1433 -User sa -Password $env:SA_PASSWORD
```

This script enables CDC in `WorkshopOrders` and registers capture for `dbo.orders`. SQL Server Agent must be Running; monitor the capture and cleanup jobs in SQL Server Agent.

To check the status:

```sql
USE WorkshopOrders;
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = DB_NAME();
SELECT capture_instance, source_object_id, start_lsn FROM cdc.change_tables;
SELECT TOP (20) * FROM cdc.lsn_time_mapping ORDER BY tran_end_time DESC;
```

CDC captures asynchronously from the transaction log; therefore a database change and the event arriving in Kafka are not simultaneous. Monitor `capture lag`, retention and transaction-log space. If a consumer falls behind retention, a re-snapshot/recovery may be needed.

## 3. Registering the Debezium Connector

Sample file:

`ops/debezium/sqlserver-orders-connector.example.json`

Before registering:

- Enable Kafka Connect's secret provider for the username/password.
- Adapt `database.hostname`, credentials, the topic prefix and bootstrap servers to your environment.
- Register the connector with `ops/debezium/register-sqlserver-connector.sh`; avoid committing credentials in the JSON.

CdcEnricher's input topic defaults to `workshop-sqlserver.dbo.orders`. Check the connector and task status through the Connect REST API.

## 4. Running the Enricher

`WorkshopCdcEnrichment` is an independent database for enrichment. Fill the `dbo.customer_enrichment` table with test data and set the Connection String:

```powershell
$env:ConnectionStrings__Enrichment = 'Server=localhost,1433;Database=WorkshopCdcEnrichment;User Id=sa;Password=<secret>;Encrypt=True;TrustServerCertificate=True'
dotnet run --project src/CdcEnricher
```

CdcEnricher reads the Debezium operations `c/u/d/r`, examines before/after to compute the delta, fetches customer information from the SQL Server enrichment database, and with a Kafka transaction commits the enriched message together with the input offset.

On a SQL Server source, the stable event identifier is built from `commit_lsn`/`change_lsn` and `event_serial_no`; these values matter for distinguishing multiple changes of one order.

## 5. Real experiment

1. Create an order from `OrderService`.
2. Change that order's status with a controlled UPDATE:

```sql
USE WorkshopOrders;
UPDATE dbo.orders SET status='Paid' WHERE id='00000000-0000-0000-0000-000000000000';
```

3. Check the Debezium message in `workshop-sqlserver.dbo.orders` and the output in `commerce.order-change-enriched.v1`.
4. Changing the status to Paid must produce a positive `paidAmountDelta`; repeating the update without a status change must not add the amount to the total again.
5. Run a DELETE in the test database and examine the delete/tombstone event semantics with the converter and Kafka Connect settings.
6. Test the initial snapshot separately. A snapshot is a rebuild of the existing state, not necessarily a live financial delta; the destination projection must keep the rebuild path separate from the live-consumption path.

## 6. Failure scenarios

- Stop Kafka Connect and monitor capture lag and retention; then bring it back and check LSN continuity.
- Stop SQL Server Agent; capture stops, but application transactions must not depend on Kafka because of a missing consumer.
- Restart the connector and check that the offset and the CDC LSN checkpoint are recovered correctly.
- Replay an event again; StreamProcessor must neutralize it with the Inbox.
- Test schema evolution by adding a nullable column to the orders table; the Debezium schema and CdcEnricher must handle version compatibility.

## Operational note

SQL Server CDC has its own Change Tables and capture/cleanup jobs. CDC is not a replacement for the Outbox for business-intent events; CDC shows real row changes. For domain events with a defined business meaning, keep the Outbox, and use CDC for suitable integration, audit and read-model use cases.
