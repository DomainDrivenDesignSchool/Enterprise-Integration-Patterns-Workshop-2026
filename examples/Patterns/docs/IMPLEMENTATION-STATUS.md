# Implementation and Validation Status

This file defines the exact delivery boundary; the presence of code in the repository by itself does not mean it has been tested in a real environment.

## Existing implementations in the Solution

| Area | Existing components | Guarantee boundary |
|---|---|---|
| Kafka Retry/DLQ | `KafkaTransactionalFailureRouter`, retry topics, `RetryScheduler`, durable scheduling table | Publication to retry/DLQ and the input offset happen in a single Kafka transaction; scheduled re-publication from SQL Server can be duplicated on a crash between publish and mark, so the consumer must be idempotent. |
| Outbox/Inbox | Outbox for order, Saga, payment, inventory, shipping and stream; Inbox on stateful paths | DB effect and Inbox in a local transaction; publish to Kafka with broker acknowledgement and lease. A crash after publish and before mark can create a duplicate. |
| Saga | Durable orchestrator, transitions, command outbox, compensation, timeout scanner, republish of the same command with a fixed event ID, DeadSaga and operator audit/API | The default timeout is two minutes with at most 8 republications; the API uses a shared workshop key and must be placed behind an identity gateway/TLS. |
| Financial | PaymentAttempt/RefundAttempt, Unknown, reconciliation, HMAC webhook ingress, durable Inbox, deduplication, conflict quarantine, append-only Ledger and Outbox | The PSP is Sandbox only; the sample HMAC is a normalized workshop contract and must be replaced with each real PSP's rules. Partial Refund, chargeback and settlement are not yet complete. |
| Inventory | Multi-SKU reservation in a transaction, conditional decrement, unique order reservation, release | Sample data is inserted for the SKUs; connection to WMS/ERP and expired-reservation policy are outside this adapter. |
| CDC | SQL Server CDC, Debezium SQL Server connector, before/after, enrichment read model and transactional forwarding | Requires an installed SQL Server Agent/CDC and Kafka Connect/Debezium SQL Server and configuration of real secrets. The CDC capture/cleanup job and retention must be monitored. |
| Stream Processing | Consumer group, inbox, state/window/watermark/late-event/outbox in SQL Server | A durable pattern for moderate load and teaching; it is not a full replacement for Flink's distributed state backend, joins and complex event-time timers. |
| Sharding | Virtual bucket map and tenant query/command router | The real connection to each shard's DbContext/session must be configured in deployment with a secret store and a versioned shard map. |
| Hot partition | Script and key-skew experiment, salt explanation and two-stage aggregation | Throughput results must be measured on the target hardware/cluster; there is no ready benchmark or numeric claim. |

## Items whose completion is deliberately not claimed

- Connection to a real PSP, bank or shipping carrier.
- Real restore/build/test execution; the .NET SDK was not available in the environment where this file was produced.
- Multi-broker failure-injection tests, concurrent rebalance, fencing and crashes at Outbox boundaries.
- A valid numeric benchmark for Kafka, SQL Server or sharding.
- A global exactly-once guarantee across Kafka, SQL Server and external services; no such shared transaction exists in this architecture.

## Mandatory checklist before operational use

1. Run `dotnet restore`, `dotnet build -c Release`, `dotnet test -c Release` with the .NET 10 SDK.
2. Run integration tests with a multi-broker Kafka and a real SQL Server.
3. In all consumers, inject a crash between the DB commit and the Kafka offset commit; a duplicate must not create a repeated business effect.
4. Inject a crash between the broker publish and the Outbox mark; downstream must deduplicate the duplicate.
5. Test the Kafka transaction with `read_committed`, transaction timeout and producer fencing.
6. For CDC, test stopping Connect, growth of CDC capture lag and retention, the initial snapshot and schema evolution.
7. For financial flows, run an ambiguous PSP timeout with fault injection; test duplicate/late webhooks and mismatches with the normalized workshop contract; for a real PSP, complete the provider-specific signature and inquiry adapter.
8. For inventory, run hundreds of concurrent reservations on one SKU and on multiple SKUs and assert that stock never goes negative.
9. For streaming, test duplicates, out-of-order, late events, idle keys and restart.
10. For shard migration, design the map version, bucket transfer, dual-read/verification and cutover separately.

## Additional Payment Reconciliation in the current version

- An ambiguous Charge/Refund result goes to `Unknown`, and the command is committed after it is durably recorded; an ordinary retry does not mean retrying the debit.
- An independent Worker with a lease and `READPAST` queries Charge and Refund; it has a bounded exponential backoff and moves to `ManualReview` after 20 inconclusive inquiries.
- The final result + Ledger + Outbox are recorded in a single SQL Server transaction. The Ledger has an append-only trigger and a unique index to prevent re-recording a completed Charge/Refund.
- The Sandbox has fault injection for "the PSP succeeded but the response was lost". The simulated PSP state is in memory and is cleared on restart. Details and verification SQL are in `docs/advanced/PAYMENT-RECONCILIATION.md`.

## Additional hardening in the current version

- The Outbox claim in services selects the oldest unpublished message per aggregate; Saga uses `aggregate_version` and OrderService has `aggregate_id`.
- Inventory reservation uses a key-range lock with `UPDLOCK,HOLDLOCK` at order level, an ordered row lock at SKU level, a request fingerprint and a durable `Rejected` state. An inventory rejection is also recorded idempotently as a result in the same database.
- The payment idempotency key is bound to order/amount/currency, and reuse with a different payload is rejected.
- Retry Scheduler records the source offset and preserves the ordering of retries for a message key.
- The `src/DlqReplay` tool provides controlled, rate-limited replay with a Kafka Transaction for destination + source offset.
- Shipping binds the reservation to the order and re-validates it on replay.
- CDC accepts only Debezium `c/u/d/r` operations.

These items have not yet been built or integration-tested with a real Kafka/SQL Server in this environment; the scripts and their execution matrix are in `tests/integration/README.md`.

## PSP Webhook and Saga Recovery in the current version

- `PaymentWorker` now has an HTTP endpoint for `POST /webhooks/psp/{provider}`. HMAC over timestamp + raw body bytes, a five-minute time window, a body size limit, matching of the signed event ID and deduplication by `(provider, provider_event_id)` are enforced.
- The initial storage and duplicate check in `PspWebhookIngress` are done with EF Core and `PaymentWebhookDbContext`. `PspWebhookProcessor`, for multi-replica claiming with `UPDLOCK/READPAST` and a lease, still uses direct SQL Server T-SQL; this part deliberately does not use raw EF because of the required locking semantics.
- `dbo.psp_webhook_inbox` durably stores the event before the 202 response. The multi-replica processor converts the event into an Inquiry signal using a lease and `READPAST`; Reconciliation queries the provider and atomically records the final result + Ledger + Outbox. A contradictory/late callback is quarantined in the `Conflict` state and does not overwrite a definitive financial state.
- The current HMAC format is an adapter contract for the lab; it is in no way equivalent to the signature of every PSP. Before connecting a real one, the signature algorithm, canonicalization, key rotation, replay policy and fields of each provider must be implemented exactly.
- When Saga creates each command, it records the pending command and the wait start time. `SagaRecoveryWorker` republishes the same Outbox row with the previous event ID/idempotency key after two minutes; after 8 republications without a response, the Saga goes to `DeadSaga`.
- `GET /ops/sagas/dead` and `POST /ops/sagas/{orderId}/retry` with `X-Operator-Key` exist for viewing and manual retry. Every operator action and automatic recovery is recorded in `dbo.saga_operator_audit`.
- HMAC signature unit tests were added to the project. Build and SQL Server integration tests were not run in this environment.
