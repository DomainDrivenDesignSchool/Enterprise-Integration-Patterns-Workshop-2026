# Replay commands and safety

Stop all instances of the target consumer group before resetting offsets. Inspect group lag:

```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group analytics-window-projection-v1
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-saga-v1
```

For a rebuildable analytics projection, create a new group and use a clean analytics database/schema:

```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group analytics-window-projection-v1 --topic analytics.customer-order-windows.v1 --reset-offsets --to-earliest --execute
```

For a stream state rebuild, reset `customer-order-window-v1` only after truncating/recreating the stream state tables and outbox in a controlled maintenance window. Do not reset only the Kafka offsets while retaining old state; that double-counts data.

For DLQ replay:
1. Inspect `x-original-topic`, `x-original-partition`, `x-original-offset`, `x-retry-attempt`, error headers and payload.
2. Fix the root cause and validate schema/business invariants.
3. Replay at a controlled rate to the original topic/partition or a dedicated replay topic.
4. Preserve event ID and idempotency keys; add replay metadata.
5. Monitor lag and downstream effects. For financial commands, reconcile provider and ledger before replay.

Never replay a raw payment command blindly. `PaymentWorker` uses provider idempotency keys, but real PSP reconciliation and operator approval remain mandatory.
