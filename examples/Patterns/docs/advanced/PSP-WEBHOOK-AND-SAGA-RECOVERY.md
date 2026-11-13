# PSP Webhook Inbox and Saga Recovery (SQL Server)

This milestone adds a durable PSP callback ingress and timeout-driven Saga recovery. It is deliberately **at-least-once** end to end; it does not claim global exactly-once delivery across SQL Server, Kafka, and a payment provider.

## 1. PSP webhook ingress

`PaymentWorker` now hosts `POST /webhooks/psp/{provider}` on port 5081 by default. Configure a different high-entropy secret for every provider:

```powershell
$env:Psp__WebhookSecrets__sandbox = '<at-least-32-random-bytes>'
$env:ConnectionStrings__Payment = '<SQL Server connection string>'
dotnet run --project src/PaymentWorker
```

The sample adapter contract uses:

- `X-Webhook-Id`: provider event ID.
- `X-Webhook-Timestamp`: Unix seconds.
- `X-Webhook-Signature`: `sha256=<hex HMAC-SHA256>`.
- HMAC input: UTF-8 timestamp, a dot, then the exact raw request-body bytes.
- Timestamp tolerance: five minutes; maximum body size: 256 KiB by default.
- Signed JSON must include `providerEventId` matching `X-Webhook-Id`, `eventType`, `idempotencyKey`, optional `providerReference`, and optional `occurredAtUtc`.

Supported normalized event types:

- `payment.authorized`
- `payment.declined`
- `refund.succeeded`
- `refund.failed`

**Important:** this HMAC format is a workshop adapter contract, not a claim that every PSP signs webhooks this way. A real provider adapter must implement that provider's exact signature/base-string rules, key rotation, and timestamp/replay semantics. Never normalize or deserialize the body before verifying the signature. Terminate TLS at a trusted edge and restrict ingress at the network layer. Do not store PAN, CVV, or other card authentication data in webhook payloads.

The HTTP handler only authenticates, validates the normalized envelope, and inserts the raw payload in `dbo.psp_webhook_inbox` through EF Core (`PaymentWebhookDbContext`). The unique `(provider, provider_event_id)` index is the concurrency boundary. It returns `202` after durable insert. Duplicate `(provider, provider_event_id)` with the same payload hash also returns `202`; the same ID with a different hash returns `409`. Invalid signatures and stale timestamps return `401` and are not persisted.

`PspWebhookProcessor` claims Inbox rows using `UPDLOCK, READPAST` plus a lease. A callback is deliberately treated as a **wake-up signal**, not as final financial truth: it moves a non-terminal attempt to `Unknown` and schedules immediate provider inquiry. The existing reconciliation worker queries the provider; only that authoritative inquiry changes the final attempt state and writes Ledger + Outbox in one SQL transaction. This prevents out-of-order contradictory callbacks from letting the first callback win. Missing attempts are retried because a callback can race with local persistence. A callback contradicting an already terminal attempt is marked `Conflict`; it does not overwrite terminal financial state or create a second charge/refund.

The processor is safe to run on multiple replicas. Expired `Processing` leases are reclaimable. Retry is bounded; after 20 attempts an Inbox row becomes `Dead` and requires investigation.

## 2. Sandbox request example

Example normalized body (the event ID in the body and header must match):

```json
{
  "providerEventId": "evt_01JEXAMPLE0001",
  "eventType": "payment.authorized",
  "idempotencyKey": "pay:00000000000000000000000000000001",
  "providerReference": "psp-ref-123",
  "occurredAtUtc": "2026-11-11T12:00:00Z"
}
```

The signature is calculated over `timestamp + "." + exact_raw_body`. See `PspWebhookSignatureTests` for executable signing examples. Never use the placeholder/example secret in a deployed environment.

## 3. Saga timeout recovery

Each non-terminal Saga records `pending_command_type` and `pending_since_utc` in the same transaction that changes Saga state and inserts its command in `saga_outbox`.

`SagaRecoveryWorker` scans every ten seconds. After two minutes without a response it republishes the **same outbox row**, retaining the original event ID and command idempotency key. Downstream Inbox/idempotency is therefore essential. Recovery is bounded to eight re-publications. If no response arrives, or the expected command row is missing, the Saga moves to `DeadSaga` and an audit entry is written.

The recovery worker does not invent a new command or run compensation blindly. `RefundPayment` and `ReleaseInventory` are retried only as the same durable command, relying on the receiving service's idempotency contract.

## 4. Operator API

Configure a strong key before exposing the operations endpoints:

```powershell
$env:Operations__ApiKey = '<long-random-operator-api-key>'
dotnet run --project src/SagaOrchestrator
```

All operations endpoints require `X-Operator-Key`:

- `GET /ops/sagas/dead` on port 5082 lists up to 200 Dead Sagas.
- `POST /ops/sagas/{orderId}/retry` resumes the recorded pending command. JSON body: `{ "reason": "provider status verified; safe to retry" }`.

Manual retry re-publishes the existing outbox row, resets the bounded recovery counter, and records actor/action/reason in `dbo.saga_operator_audit`. The API does not mutate payment ledger entries. Put these endpoints behind TLS, network allow-lists, and a proper identity/authorization gateway in production; the shared API key is only a workshop guardrail.

## 5. SQL diagnostics

```sql
-- Inbox health
SELECT status, COUNT(*) AS total, MIN(received_at_utc) AS oldest
FROM dbo.psp_webhook_inbox GROUP BY status;

-- Webhook conflicts / poison events
SELECT TOP (100) provider, provider_event_id, event_type, idempotency_key,
       attempts, last_error, received_at_utc
FROM dbo.psp_webhook_inbox
WHERE status IN ('Conflict','Dead')
ORDER BY received_at_utc DESC;

-- Stuck/dead sagas
SELECT order_id,status,pending_command_type,pending_since_utc,recovery_attempts,dead_reason
FROM dbo.saga_instances
WHERE status='DeadSaga' OR pending_command_type IS NOT NULL
ORDER BY pending_since_utc;

-- Operator and automatic recovery history
SELECT order_id,actor,action,detail,created_at_utc
FROM dbo.saga_operator_audit
ORDER BY created_at_utc DESC;
```

## 6. Known limitations / next hardening

- The webhook HMAC is a normalized sandbox adapter; no real PSP-specific adapter or key-rotation store is included.
- The operator API uses one shared API key, not OIDC/RBAC or dual approval.
- Webhook raw payload retention/PII redaction and archival policy need a provider-specific review.
- A conflict is quarantined in Inbox; automatic provider inquiry is scheduled for non-terminal attempts, but terminal-state conflict resolution and settlement comparison still require an operator/provider-specific workflow.
- No chargebacks, partial refunds, multiple refund installments, or settlement reconciliation.
- No global exactly-once guarantee. Kafka and SQL Server remain connected through durable Outbox/Inbox and idempotent consumers.
- Build and SQL integration tests must run against the target .NET 10 SDK and SQL Server before production use.
