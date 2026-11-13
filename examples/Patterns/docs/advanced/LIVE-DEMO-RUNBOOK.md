# Live Execution Scenario: from Order to CDC, Saga and Stream

## Prerequisites

The infrastructure must be installed without Docker: .NET 10 SDK, SQL Server 2019+, SQL Server Agent, Kafka 3.8+, Kafka Connect with the Debezium SQL Server plugin. For the RabbitMQ experiments, RabbitMQ 4.x is also needed.

## Execution order

1. Create the databases and schemas:
   ```powershell
   .\ops\sqlserver\apply-all-schemas.ps1 -Server localhost,1433 -User sa -Password $env:SA_PASSWORD
   ```
2. Enable CDC for the order table:
   ```powershell
   .\ops\sqlserver\enable-orders-cdc.ps1 -Server localhost,1433 -User sa -Password $env:SA_PASSWORD
   ```
3. Set all SQL Server Connection Strings in Environment Variables per `docs/SQLSERVER-MIGRATION-STATUS.md`. Each service connects only to its own database.
4. Create the topics:
   ```bash
   ./ops/kafka/create-topics.sh
   ```
5. Run the services in separate terminals:
   ```bash
   dotnet run --project src/RetryScheduler
   dotnet run --project src/SagaOrchestrator
   dotnet run --project src/PaymentWorker
   dotnet run --project src/InventoryWorker
   dotnet run --project src/ShippingWorker
   dotnet run --project src/OrderService
   ```
6. Create an order:
   ```bash
   curl -X POST http://localhost:5000/orders \
     -H 'Content-Type: application/json' \
     -d '{"customerId":"11111111-1111-1111-1111-111111111111","currency":"IRR","lines":[{"sku":"SKU-KEYBOARD","quantity":1,"unitPrice":100000}]}'
   ```
7. Configure the sample connector `ops/debezium/sqlserver-orders-connector.example.json` with a secret provider and your environment's addresses, and register it with `ops/debezium/register-sqlserver-connector.sh` (set the `CONNECT_URL` variable if needed).
8. Run the CDC and analytics path:
   ```bash
   dotnet run --project src/CdcEnricher
   dotnet run --project src/StreamProcessor
   dotnet run --project src/AnalyticsWorker
   ```

## Observation points

- `commerce.orders`: the `OrderCreated` event from the order Outbox.
- `commerce.commands`: the commands `RequestPayment`, `ReserveInventory`, `RequestShipment`.
- `commerce.saga-events`: the payment, inventory and shipping results.
- `workshop-sqlserver.dbo.orders`: the raw Debezium SQL Server event with before/after.
- `commerce.order-change-enriched.v1`: the Enrichment contract, independent of Debezium.
- `analytics.customer-order-windows.v1`: the closed-window output.
- The tables `payment_ledger`, `stock_reservations`, `saga_instances`, `scheduled_retries`, `stream_window_state` and the Outboxes: durable SQL Server evidence.

## Failure injection

- Insufficient stock: register an order with `SKU-MONITOR` and a quantity greater than the stock; you should see `InventoryRejected` and then a Refund.
- Transient failure: for a controlled experiment, temporarily cut a Worker's access to its own DB; the message should pass through the Retry/DLQ path. Restore the connection after the experiment.
- Duplicate: re-publish an identical event; the Inbox/unique key must not repeat the business effect.
- CDC: change `dbo.orders.status` from `Created` to `Paid` and check before/after and `PaidAmountDelta`.
- Late event: publish an event with an `occurredAtUtc` older than the watermark; it must be recorded in `stream_late_events`.
- Hot key: use `ops/kafka/hot-partition-demo.sh` and compare the message/lag distribution per partition.

## Success criteria

- No offset advances before the DB/Inbox effect is durably recorded.
- On the Kafka-to-Kafka failure path, the destination publish and the input offset are in one Kafka transaction.
- On a crash after the Outbox publish, a duplicate is possible but the downstream effect must be idempotent.
- Inventory never goes negative.
- The Ledger is append-only and a refund is recorded as a new entry.
- CDC is fed from the SQL Server Change Tables without polling and publishes table changes with before/after.
- Late events beyond the allowed limit are not silently dropped.

## Controlled DLQ replay

After fixing the cause of the failure, run it with an operator-specific group and transactional ID:

```bash
export DLQ_REPLAY_GROUP=dlq-replay-operator-01
export DLQ_REPLAY_TRANSACTIONAL_ID=dlq-replay-operator-01
export DLQ_REPLAY_MAX_MESSAGES=20
export DLQ_REPLAY_RATE_PER_SECOND=2
dotnet run --project src/DlqReplay
```

This tool takes the destination only from `x-original-topic`, does not replay a message without that header, and performs the destination publish together with the DLQ offset commit in one Kafka Transaction.
