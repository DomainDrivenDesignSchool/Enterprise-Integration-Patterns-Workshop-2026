# Enterprise Integration Workshop

This Solution is a hands-on integrated lab for the **Order Fulfillment** scenario. Its goal is to demonstrate architectural decisions, behavior under failure, observability and capacity testing — not merely to show a few Producers and Consumers.

## Prerequisites

- .NET 10 SDK
- Microsoft SQL Server 2019+ Developer Edition for the workshop (or Standard/Enterprise with CDC enabled; a direct install or an enterprise service)
- Apache Kafka 3.8+ and the Kafka CLI
- RabbitMQ 4.x and the Management Plugin
- For CDC experiments: SQL Server Agent, SQL Server CDC, Kafka Connect and the Debezium SQL Server Connector
- For the dashboard: Prometheus and Grafana
- For Load Testing: k6
- Optional: Jaeger or Grafana Tempo as an OTLP trace backend

**There is no Dockerfile, Compose file or Docker dependency in this repository.** All infrastructure must be installed and available beforehand. The files in `ops/` are sample configuration and direct start-up commands.

## Version and validation status

- Target Framework: `net10.0`
- C# and ASP.NET Core
- Package versions are centralized in `Directory.Packages.props`.
- The .NET SDK and the brokers were not installed in the environment where these files were produced; therefore `dotnet restore/build/test` and real End-to-End tests have not been run. Before operational use, run the validation steps below. No real Benchmark claim is made in this repository.

## Scenario architecture

```text
Order API → SQL Server WorkshopOrders + Outbox → Kafka commerce.orders
                                      └→ Saga Orchestrator + Inbox/Outbox
                                           ├→ commerce.commands → Payment Worker → payment DB/ledger
                                           ├→ commerce.commands → Inventory Worker → inventory DB
                                           └→ commerce.commands → Shipping Worker → shipping DB
                                                └→ commerce.saga-events → Saga

SQL Server CDC → Debezium Connect → workshop-sqlserver.dbo.orders
  → CdcEnricher (customer read-model enrichment; Kafka transaction)
  → commerce.order-change-enriched.v1
  → DurableWindowWorker (Kafka consumer group + SQL Server state/inbox/outbox)
  → analytics.customer-order-windows.v1 → AnalyticsWorker → analytics DB

Kafka failure path → retry topics → RetryScheduler (durable due-time) → original topic
                   └→ commerce.dlq
```

Each service owns its own database. In this workshop, to reduce running cost, you may have several separate databases on a single SQL Server instance, but cross-service access to each other's OLTP tables is forbidden.

## Structure

- `src/OrderService`: API, order registration, Outbox, and status updates from Saga events with an Inbox
- `src/PaymentWorker`: payment Consumer, reconciliation, HTTP PSP webhook ingress, durable Inbox and idempotent financial ledger
- `src/InventoryWorker`: inventory reservation and release operations
- `src/ShippingWorker`: shipment initiation
- `src/NotificationWorker`: independent consumption of events
- `src/AnalyticsWorker`: financial window projection and deduplication
- `src/SagaOrchestrator`: durable Saga with Inbox/Outbox, timeout recovery, DeadSaga and operator API
- `src/StreamProcessor`: distributed event-time processing with a Kafka consumer group and durable state/inbox/outbox in SQL Server
- `src/EventStore`: an EF Core and SQL Server based Event Store with expected-version append, aggregate replay, snapshot and event upcasting
- `src/RabbitMqWorker`: Routing and DLX/Retry labs
- `src/CdcEnricher`: consumption of raw Debezium CDC, before/after, enrichment and Kafka transactional forwarding
- `src/RetryScheduler`: durable retry scheduling and returning the message to the original topic/partition
- `src/Shared`: contracts, resilience primitives and `VirtualBucketShardMap` for routing tenant-scoped queries/commands
- `tests`: unit tests for contracts, window semantics, financial delta, shard routing, Event Store/upcasting; plus an independent SQL Server probe for Event Store writer races
- `ops`: Broker, Debezium and Prometheus configuration and CLI commands
- `load`: k6 scenarios
- `docs/advanced`: CDC, Kafka Retry/DLQ, financial Saga, Stream Processing and Partitioning/Sharding scenarios
- `docs`: architecture, pattern matrix, anti-patterns, runbooks, workshop labs

## Infrastructure setup without Docker

1. Install and start SQL Server 2019+. Then run `ops/sqlserver/apply-all-schemas.ps1` to create each service's databases and schemas.
2. Install and start Kafka. Create the topics with `ops/kafka/create-topics.sh` (or read the equivalent PowerShell commands from the file).
3. Install RabbitMQ and enable the Management Plugin. Import `ops/rabbitmq/definitions.json`.
4. Set the Connection String and Broker values in appsettings or Environment Variables.
5. For CDC, keep SQL Server Agent enabled and run `ops/sqlserver/enable-orders-cdc.ps1`; then install Kafka Connect and the Debezium SQL Server Plugin, configure the sample connector `ops/debezium/sqlserver-orders-connector.example.json` with a secret provider and register it with `ops/debezium/register-sqlserver-connector.sh`.
6. Run Prometheus with `ops/prometheus/prometheus.yml` and connect Grafana to it.
7. Run the services with `dotnet run --project ...`; the exact recommended order follows below.

Sample Environment Variables:
```powershell
$env:ConnectionStrings__Orders = 'Server=localhost,1433;Database=WorkshopOrders;User Id=sa;Password=<secret>;Encrypt=True;TrustServerCertificate=True'
$env:ConnectionStrings__Payment = 'Server=localhost,1433;Database=WorkshopPayment;User Id=sa;Password=<secret>;Encrypt=True;TrustServerCertificate=True'
$env:Kafka__BootstrapServers = 'localhost:9092'
$env:RabbitMq__HostName = 'localhost'
dotnet run --project src/OrderService
```

## Lab execution pattern

Each Lab contains a problem, code/configuration, failure injection, output observation and an analysis question. The recommended order for the three-hour workshop is in `docs/WORKSHOP-RUNBOOK.md`.

## Event Sourcing lab

The Event Store lives in the independent `WorkshopEventStore` database. The script `ops/sqlserver/apply-all-schemas.ps1` now applies its schema as well. Details of append concurrency, snapshot, replay and upcasting are in `docs/advanced/EVENT-SOURCING-EVENT-STORE.md`. This implementation was added to the source, and the two-writer race probe is in `tests/EventStore.Integration`; because SQL Server/.NET SDK were not run in this environment, its result is not yet runtime-verified.

## Pattern coverage

The full matrix is in `docs/PATTERN-MATRIX.md`. The statuses are explicit:
- **Implemented**: executable code or configuration exists in the repository.
- **Runnable lab**: runnable with installed infrastructure, but must be adapted to your environment.
- **Concept/Config**: a configuration file/guide is provided; setup depends on an external service.
- **Not production implementation**: the limitation is deliberately documented.

## Local validation

```bash
dotnet restore EnterpriseIntegration.Workshop.slnx
dotnet build EnterpriseIntegration.Workshop.slnx -c Release
dotnet test EnterpriseIntegration.Workshop.slnx -c Release
```

For the real Event Store probe on SQL Server (after applying the schema):
```powershell
$env:EVENTSTORE_SQLSERVER_CONNECTION = 'Server=localhost,1433;Database=WorkshopEventStore;User Id=sa;Password=<secret>;Encrypt=True;TrustServerCertificate=True'
dotnet run --project tests/EventStore.Integration -c Release
```

To run the load test:
```bash
k6 run load/order-load.js
k6 run load/burst-overload.js
```

To compare settings, change only one variable at a time and record p50/p95/p99, throughput, error rate, consumer lag and resources. The sample numbers in the slides (10 thousand produced and 5 thousand consumed per second) are only a conceptual ratio and are not a Benchmark.

## Important boundaries and cautions

1. Kafka exactly-once alone is not a global transaction guarantee between Kafka and a database or an external payment service; the business effect must be controlled with idempotency and a local transaction.
2. In this repository, StreamProcessor persists its state in SQL Server and divides work between replicas with a Kafka consumer group; this is a teachable .NET pattern, not a complete replacement for Flink for complex joins, event-time timers and large state backends.
3. The Debezium connector and CdcEnricher are runnable, but need Kafka Connect/the SQL Server Plugin, SQL Server CDC, SQL Server Agent and your environment's secret management. CDC on the orders table creates a capture/cleanup job, and its latency and retention must be monitored.
4. The RabbitMQ retry topology must be tested against TTL/DLX and your Broker version's policies; for production use quorum queues, publisher confirms, durable topology and monitoring.
5. The Outbox must be implemented in the DB with an atomic claim and a lease; the sample code uses a short transaction and `UPDLOCK`/`READPAST` with a lease. With multiple instances, be sure to test contention and crashes.
6. For financial messages, a DLQ does not mean discarding the message; until reconciliation and a human decision, the order status must be observable and compensatable.
7. Do not put any real password or credential in the repository.
8. `SandboxPaymentGateway` moves no real funds; for a real PSP, an adapter, callback validation, inquiry and reconciliation must be implemented. Shipping is also an experimental adapter, not a connection to a real carrier.
9. StreamProcessor is a durable pattern with a SQL Server state store; for very large event-time processing, joins and complex timers, Flink or a specialized platform should be evaluated.

## End-to-end execution of the extended scenario

1. Create the databases and tables:
   ```powershell
   .\ops\sqlserver\apply-all-schemas.ps1 -Server localhost,1433 -User sa -Password $env:SA_PASSWORD
   ```
2. Enable SQL Server Agent and turn on CDC for `dbo.orders`:
   ```powershell
   .\ops\sqlserver\enable-orders-cdc.ps1 -Server localhost,1433 -User sa -Password $env:SA_PASSWORD
   ```
3. Create the Kafka topics:
   ```bash
   ./ops/kafka/create-topics.sh
   ```
4. First run `RetryScheduler`, `SagaOrchestrator`, `PaymentWorker`, `InventoryWorker`, `ShippingWorker`.
5. Run `OrderService` and create an order with a valid line item:
   ```bash
   curl -X POST http://localhost:5000/orders -H 'Content-Type: application/json' \
     -d '{"customerId":"11111111-1111-1111-1111-111111111111","currency":"IRR","lines":[{"sku":"SKU-KEYBOARD","quantity":1,"unitPrice":100000}]}'
   ```
6. For CDC, run Kafka Connect and register the sample SQL Server connector; then run `CdcEnricher`, `StreamProcessor` and `AnalyticsWorker`.
7. After payment is confirmed, `OrderLifecycleWorker` changes the order status to `Paid` in the order-owning DB; this change enters Debezium automatically through SQL Server CDC. For an independent CDC experiment you can also change the status with SQL. The Enricher uses before/after and the SQL Server LSN/serial to compute `PaidAmountDelta` and to build a stable event identifier.
8. To see a closed window, later events with a further-advanced event time are needed so the watermark passes the end of the window; or wait until later data arrives.

The databases' Connection Strings are empty in appsettings, and each Worker stops with a clear error when no connection is configured. Inject the values through environment variables/a secret store; do not commit passwords. Details are in `docs/SQLSERVER-MIGRATION-STATUS.md`.

Details of CDC, retry, Saga, stream state and sharding are in `docs/advanced/`.

## Anti-pattern labs

Anti-pattern samples are described in `docs/ANTI-PATTERNS.md` together with symptoms, cost, an experiment and the path to correction. The broken version is deliberately for teaching and must not be moved to production.


## Hardening stage

Corrections for Outbox races, financial idempotency, inventory-reservation fingerprint and Retry scheduling ordering are documented in `docs/advanced/FAILURE-INJECTION-AND-HARDENING.md`. All application services and the shared persistence use SQL Server; the PostgreSQL scripts in `ops/sql/` and the old connector are kept only as historical reference. Details of the migration and the validation limitations are in `docs/SQLSERVER-MIGRATION-STATUS.md`.

- A controlled DLQ replay tool with a Kafka Transaction was added in `src/DlqReplay`.
- Payment Reconciliation for ambiguous Charge and Refund, backoff, ManualReview, an append-only Ledger and fault injection was added in `docs/advanced/PAYMENT-RECONCILIATION.md`.

## Hardening tests

- Failure Injection guide: `docs/advanced/FAILURE-INJECTION-AND-HARDENING.md`
- Integration tests without Docker: `tests/integration/README.md`
- Last-stock reservation race test: `tests/integration/sqlserver-stock-race.ps1`


## Database migration

All application persistence has been migrated to SQL Server. See [SQL Server migration and setup](docs/SQLSERVER-MIGRATION-STATUS.md) for schema deployment, connection strings and CDC setup.


## PSP Webhook and Saga Recovery

The complete guide to the webhook path, Inbox, signature, replay protection, conflict handling, timeout recovery and operator operations is in `docs/advanced/PSP-WEBHOOK-AND-SAGA-RECOVERY.md`.

To run `PaymentWorker`, in addition to the Connection String and Kafka, inject the provider's separate secret:

```powershell
$env:Psp__WebhookSecrets__sandbox = '<long-random-secret>'
```

To enable manual Saga operations:

```powershell
$env:Operations__ApiKey = '<long-random-operator-api-key>'
```

Do not expose the operational endpoints to the internet without TLS, network restriction and proper authentication. The shared API key is only a workshop guardrail.
