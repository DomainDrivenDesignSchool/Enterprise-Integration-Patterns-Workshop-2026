# Payment Reconciliation and Recovering an Ambiguous State

## The problem we are solving

With an external payment, a timeout does not mean the payment failed. The PSP may have recorded the operation while the response was lost on the network. If the service retries the same message like an ordinary error, a duplicate debit can result.

The implementation rule:

> Once the PSP call has started, if we have no definitive result we record the state as `Unknown`, not `Rejected`. Until a valid inquiry returns, we issue no new Charge or Refund.

## States

### Charge

`Initiated -> Authorized | Rejected | Unknown -> Authorized | Rejected | ManualReview`

- `Initiated`: the intent is recorded in the DB; the request may or may not have been sent to the PSP yet.
- `Unknown`: no definitive response was received; only Reconciliation may resolve it.
- `Authorized`: the PSP confirmed, and the Ledger and Outbox were recorded in one local transaction.
- `Rejected`: the PSP announced a definitive rejection.
- `ManualReview`: after 20 inconclusive inquiries, an operator must review it.

### Refund

The Refund status is kept in `payment_refund_attempts`, independently of the Charge. This separation matters: a Charge being Authorized does not mean the Refund succeeded.

## Practical path

1. `RequestPayment` is stored in `payment_attempts` with a unique Idempotency Key.
2. The PSP call is made outside the DB transaction; we do not keep a DB transaction open during a network call.
3. A definitive response records status + Ledger + Outbox in a single SQL Server transaction.
4. A timeout/exception other than shutdown becomes `Unknown` and the inquiry time is recorded; the Kafka message is committed only after this responsibility is durably recorded.
5. `PaymentReconciliationWorker` divides due jobs between replicas using `UPDLOCK` + `READPAST` and a lease.
6. The PSP inquiry is made with the same Idempotency Key.
7. `Authorized` or `Declined` moves to the final state only after a definitive inquiry. `Pending`, `Unknown` and `NotFound` are re-checked with bounded backoff.
8. After 20 attempts the status becomes `ManualReview`; no automatic Charge/Refund is ever issued on the basis of `NotFound`.

## Why does NotFound not mean it is allowed to Charge again?

Some PSPs make their query side eventually consistent. A Charge may have been recorded while the inquiry endpoint does not show it yet. In a real adapter, the consistency SLA and the lookup contract with the PSP must be clear. Until then, `NotFound` is only a temporary result and ends up in manual review.

## Fault Injection test in the Sandbox

In `src/PaymentWorker/appsettings.json`, temporarily set one of the options to true:

```json
"Sandbox": {
  "SimulateChargeResponseLossOnce": true,
  "SimulateRefundResponseLossOnce": false
}
```

or, for Refund:

```json
"Sandbox": {
  "SimulateChargeResponseLossOnce": false,
  "SimulateRefundResponseLossOnce": true
}
```

The Sandbox first records the successful state in the provider simulator and then deliberately raises a `TimeoutException`. Expected behavior:

- The attempt moves to `Unknown`.
- The Kafka command is committed after Unknown is recorded.
- Reconciliation finds the successful result by querying the same idempotency key.
- Only one Charge/Refund is recorded in the Ledger.
- The corresponding event is created in the Outbox and then published.

Important limitation: the PSP simulator's state is in memory and is cleared on restart. This behavior is only for fault injection in a single local run; a real PSP is a durable, independent source of truth.

## Ledger

- The Ledger is append-only; a database trigger rejects UPDATE and DELETE.
- A Charge amount is positive and a Refund is negative.
- A unique index on `(reference, entry_type)` prevents recording multiple Charges or multiple full Refunds for one reference.
- For now, this model supports a full Refund, not a Partial Refund or multiple installments of Refund.

## SQL and running the Lab

First apply the schema to the `payment` database:

```bash
`ops/sqlserver/apply-payment-schema.ps1` with SQL Server access.
```

Then run PaymentWorker. No Docker is required in this repository; Kafka and SQL Server must already be available.

Inspecting ambiguous cases:

```sql
SELECT id, order_id, idempotency_key, status, reconciliation_attempts,
       next_reconcile_at_utc, last_error
FROM payment_attempts
WHERE status IN ('Unknown', 'ManualReview')
ORDER BY created_at_utc;

SELECT id, order_id, payment_reference, idempotency_key, status,
       reconciliation_attempts, next_reconcile_at_utc, last_error
FROM payment_refund_attempts
WHERE status IN ('Unknown', 'ManualReview')
ORDER BY created_at_utc;

SELECT order_id, currency, SUM(amount) AS net_amount
FROM payment_ledger
GROUP BY order_id, currency;
```

## Things that must still be added/completed before Production

- A real PSP adapter with an exact lookup by idempotency key, request and response signing, timeout and rate limit.
- A real PSP adapter and matching of the provider-specific signature; the workshop's normalized endpoint/Inbox with HMAC, replay window and conflict quarantine is implemented in `PSP-WEBHOOK-AND-SAGA-RECOVERY.md`.
- A dedicated Operator API for reviewing and resolving Payment `ManualReview`, with an audit trail and four-eyes control of sensitive operations; the current API is only for `DeadSaga`.
- Partial Refund, multiple refunds, chargeback and settlement reconciliation against the PSP statement.
- Integration tests with a real PSP sandbox and failure injection between the DB commits and the PSP.
- Build/test with the .NET 10 SDK; the environment that produced this artifact lacked an SDK at the time.

This implementation makes no claim of global exactly-once. The guarantees are built at the boundary of each system, using idempotency, durable state, unique constraints, Outbox and reconciliation.

## Verified PSP callback path

The SQL Server version also has a durable PSP webhook ingress and processor. See `PSP-WEBHOOK-AND-SAGA-RECOVERY.md` for the normalized HMAC adapter contract, timestamp/replay checks, Inbox uniqueness, conflict quarantine, and the `tests/integration/payment-webhook-smoke.ps1` smoke test. The webhook does not blindly override a terminal attempt; conflicts require provider inquiry/review.
