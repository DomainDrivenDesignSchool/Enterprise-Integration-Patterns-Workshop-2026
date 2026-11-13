# Operations and Failure Injection Runbook

## Kafka
- Inspect: `kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group payment-v1`
- Reset only a stopped group: `kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group analytics-rebuild-2026 --topic orders.events --reset-offsets --to-earliest --execute`
- Replay into a new group for analytics; do not reset a live business consumer casually.
- Increase partitions only after evaluating key-to-partition remapping and ordering implications.

## RabbitMQ
- Enable management plugin; import definitions.
- Inspect queue depth, unacked, redeliveries, DLQ depth and consumer utilisation.
- Publisher confirms and durable queues/exchanges are required for reliable publishing.
- Avoid infinite immediate requeue loops.

## Failure injection
- Kill one consumer while messages are flowing; observe lag and redelivery.
- Stop broker; observe bounded producer failure and outbox backlog.
- Make downstream DB unavailable; verify retry budget and circuit breaker.
- Publish malformed payload; verify quarantine/DLQ and unrelated key progress.
- Pause consumer; observe lag and disk growth.
- Restore dependency; measure recovery time and backlog drain rate.

## Capacity
Track CPU, RSS, GC heap, thread pool queue, DB pool saturation, disk IOPS, broker disk, network, p95/p99, error rate and lag age. Increase one dimension at a time.
