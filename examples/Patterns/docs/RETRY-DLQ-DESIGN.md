# Kafka Retry and Dead Letter Queue — Runnable implementation

This lab is now implemented in `src/Messaging/Reliability/` and `src/RetryScheduler/`. The architecture and the boundaries of its guarantees are described in `docs/advanced/KAFKA-RELIABILITY.md`.

## Path
1. The consumer detects a technical/serialization failure.
2. `KafkaTransactionalFailureRouter` sends the message, with metadata and attempt count, to a retry tier or the DLQ.
3. In the same Kafka transaction, `SendOffsetsToTransaction` advances the input offset.
4. `RetryScheduler` durably stores the retry message in `scheduled_retries` and commits its own offset after the DB commit.
5. After `x-not-before-utc`, the dispatcher returns the message to the original topic and partition.

## Retry tiers
- `commerce.retry.1m`
- `commerce.retry.5m`
- `commerce.retry.15m`
- `commerce.retry.1h`
- After the fifth attempt: `commerce.dlq`

Kafka has no native per-message delay; scheduling is done in SQL Server. At the DB↔Kafka boundary a duplicate can occur; an Inbox/idempotency is mandatory.

## Operational limitations
- For transactional offset forwarding, the source and the destination must be in the same Kafka cluster.
- `transactional.id` must be unique per replica and stable across restarts of the same replica; in the local sample the default id is for a single instance. With multiple replicas, use a stable and unique instance id.
- Retry topics must have a partition count compatible with the source topic; the scheduler returns the message to the original partition.
- DLQ retention, ACLs, PII masking, replay approval and replay rate must be set according to organizational policy.
- For business rejections such as insufficient stock or a declined payment, no technical retry is performed; a business-rejection event is published instead.

## RabbitMQ
Use TTL + DLX, or a delayed exchange with a delivery limit. An unbounded `nack(requeue:true)` creates a retry storm.
