# Second-stage Hardening: Races, Duplicates and Crashes

## Fixes in this version

1. The Saga, Payment, Inventory, Shipping and Stream Outboxes claim, for each Aggregate, only the oldest unpublished message. This prevents another Replica from getting the second event of an order ahead of the first; parallelism between independent Aggregates remains. In addition to creation time, Saga has an ascending `aggregate_version`, so Saga ordering does not depend on a random timestamp. OrderService also stores `aggregate_id` in the Outbox.
2. Inventory sorts lines by SKU so that the order of taking row locks is the same in all transactions and the chance of a deadlock is reduced.
3. Inventory reservation keeps the request fingerprint. Repeating the same order with different lines is rejected as an Idempotency conflict. A persisted status of `Reserving` no longer causes another decrement and requires reconciliation.
4. The Payment idempotency key is bound to order/amount/currency. Reusing the same key with a different payload is rejected; Authorized/Rejected attempts are not sent to the PSP again.
5. Retry Scheduler keeps the source offset, preserves the retry ordering for a message key, and makes mark/unlock conditional on the lock owner.
6. Shipping takes a key-range lock with `UPDLOCK,HOLDLOCK` for each order and validates the existing reservationId; a replay with a different reservation is no longer silently ignored.
7. CDC accepts only the Debezium operations `c/u/d/r` and treats an unknown event as a poison message.

## Failure Injection experiments

### Outbox
- Run two replicas of one publisher. For one aggregate, create at least 3 outbox rows with consecutive timestamps.
- Delay the publish of the first message by 90 seconds and shorten the lease so that the second worker becomes active.
- Expected: the second message of the same aggregate is not claimed/published before the first. Another aggregate must progress concurrently.
- Kill the process after the broker ack and before the DB mark. Expected: the message may be duplicated; the consumer applies it once using the Inbox and EventId.

### Payment
- Send one `idempotencyKey` with amount 100 and then with 200. The second request must be rejected before calling the gateway.
- Kill the process after a successful PSP response and before the local commit. The retry must give the same key to the PSP and record the ledger/outbox exactly once in the DB.
- Test the ambiguous PSP timeout separately; in Production the adapter must have inquiry/reconciliation rather than assuming a timeout is equivalent to rejection.

### Inventory
- Send two concurrent orders for the last unit of a SKU; only one must be Reserved and available must never go negative.
- Send a multi-SKU order with the line order reversed from two workers; the internal lock order must be the same.
- Resend the same orderId with a different quantity; it must produce an Idempotency conflict.

### Retry Scheduler
- Create a message with a lower source offset and a later due time, and a next message with the same key and an earlier due time. The second must not be published first.
- Kill the process after a successful Produce and before the mark. A duplicate is allowed but the downstream consumer must be idempotent.

## Limits of the guarantees

- A Kafka Transaction makes only the Kafka destination and the source offset in the same cluster atomic.
- The SQL Server Outbox does not give global exactly-once up to Kafka; after a crash between publish and mark, a duplicate is possible.
- Retry ordering is preserved only for the same message key. Messages with different keys can deliberately progress independently. If whole-partition ordering is a business requirement, you must knowingly accept head-of-line blocking of the entire partition.
- The scheduled lease must be longer than the maximum publish latency, and a heartbeat/lease renewal must be added for long publishes. The current Outbox dispatchers have a two-minute lease; for slow sends, lease renewal must be added.
- These fixes could not be built or integration-tested in this environment without the .NET SDK and a real cluster.
