# Integration and Failure-Injection Tests (no Docker)

These tests run against an installed SQL Server and Kafka cluster; no Docker/Compose is required. Create the service databases and schemas with `ops/sqlserver/apply-all-schemas.ps1` first.

## 1. Build and unit tests

```powershell
dotnet restore EnterpriseIntegration.Workshop.slnx
dotnet build EnterpriseIntegration.Workshop.slnx -c Release --no-restore
dotnet test EnterpriseIntegration.Workshop.slnx -c Release --no-build
```

## 2. SQL Server inventory concurrency

Run `sqlserver-stock-race.ps1` with SQL Server credentials. It starts 24 concurrent sessions trying to reserve the last unit of a SKU. Exactly one conditional update must win; final state must be `available=0,reserved=1`.

## 3. Outbox ordering with multiple replicas

- Run two replicas of `SagaOrchestrator`; both use the same Saga database and Kafka cluster with the same consumer group.
- Produce several events for one Saga and interrupt the first publish using broker latency or a controlled network interruption.
- Expected: `aggregate_version` prevents a later event of the same Saga from publishing before an earlier one. Other Sagas can progress concurrently.
- `payment_outbox`, `inventory_outbox`, `shipping_outbox`, `stream_outbox` and `outbox_messages` similarly claim the oldest unpublished message per aggregate.

## 4. Kafka transaction and crash

- Produce a test message to the input topic and run a test consumer with `IsolationLevel=ReadCommitted`.
- Kill the process before `CommitTransaction`: the destination must not expose a committed message and the offset must not advance.
- Kill after `CommitTransaction`: the destination must contain the message and the group offset must have advanced.
- Run two processes with the same `TransactionalId`: the second process should fence the first. In production, every replica must use a unique transactional ID.

## 5. Outbox duplicate window

- Kill a publisher after broker ACK but before marking the Outbox row in SQL Server.
- After lease expiry, the message is published again; this is expected at-least-once behavior.
- The consumer must persist EventId in its Inbox with a unique constraint and apply the business effect only once.

## 6. CDC operational

- Enable SQL Server CDC for `dbo.orders` and confirm SQL Server Agent capture/cleanup jobs are healthy.
- Register the Debezium SQL Server connector and inspect connector/task status.
- Stop Kafka Connect and monitor CDC capture lag/retention. Restore Connect and verify it resumes from the persisted LSN without gaps.
- Execute `UPDATE dbo.orders SET status='Paid' WHERE id=...` and compare the before/after image and `paidAmountDelta` in the output topic.
- Test a DELETE and verify the connector emits the delete/tombstone semantics expected by the configured converter.
- Test the initial snapshot separately; snapshot rows can rebuild historical paid amount, so downstream projection rebuild must be distinguished from live delta consumption.

## 7. Stream processor

- Send one event twice; `stream_inbox` must apply only one aggregate effect.
- Send out-of-order events inside and outside Allowed Lateness; the latter must be stored in `stream_late_events`.
- Kill the process after SQL Server commit and before Kafka offset commit; replay must be neutralized by Inbox.
- Note: the watermark is per customer/currency; an idle key may close windows late. For strict time SLAs, define idle-partition watermark/timer policy explicitly.

## Verification boundary

The current generation environment did not contain the .NET SDK or a SQL Server instance. The scripts are provided, but the build, SQL schema execution, concurrency race, CDC integration and fault-injection tests must be run in the workshop environment before treating the migration as verified.


## 8. PSP webhook Inbox and Saga recovery

- Apply `ops/sqlserver/04-payment-schema.sql` and `ops/sqlserver/09-saga-schema.sql` (or run `apply-all-schemas.ps1`).
- Configure `Psp__WebhookSecrets__sandbox` with a random secret and run `PaymentWorker` on its default port 5081.
- Run `tests/integration/payment-webhook-smoke.ps1` with SQL Server credentials and the same secret. It seeds an Initiated payment, signs the exact raw body, sends the callback twice, and asserts the attempt is moved to `Unknown` for provider inquiry without a direct Ledger write. The Sandbox provider has no matching charge for this manually seeded attempt, so the reconciliation worker keeps it unresolved.
- Send a request with a changed body/signature, an expired timestamp, or a header event ID that differs from the signed JSON `providerEventId`; each must be rejected before Inbox persistence.
- Send the same provider event ID with a different signed body; expect HTTP 409. Send a valid callback before its payment attempt exists; expect durable Retry and eventual `Dead` after the configured bound if the attempt never appears.
- Stop `PspWebhookProcessor` after Inbox claim and restart it; the expired lease must be reclaimed. Run two PaymentWorker replicas and verify a single ledger entry/outbox event.
- Configure `Operations__ApiKey`, run `SagaOrchestrator` on port 5082, then hold a downstream command without producing its response. After two minutes, the same outbox event ID should be republished. After eight recoveries, expect `DeadSaga` and an audit record. Use `GET /ops/sagas/dead` and the retry endpoint to verify an operator retry reuses the original command and records the supplied reason.
- Test the race between a delayed downstream response and a recovery publish. The downstream service must deduplicate the repeated command using its durable Inbox/idempotency key.
