# Delivery Semantics and Failure Windows

| Strategy | Commit/Ack timing | Failure result | Use |
|---|---|---|---|
| At-most-once | Commit before business processing | Message can be lost | Telemetry where loss is acceptable |
| At-least-once | Commit after durable business effect | Duplicate possible | Business events + idempotency |
| Kafka EOS | Transactional producer + read-process-write within Kafka | Atomic Kafka input/output offsets and output records | Kafka-to-Kafka pipelines |
| End-to-end exactly-once | Requires all side effects participate in one atomic protocol or idempotent effect | Not provided merely by Kafka EOS | Usually model as effectively-once via idempotency |

Failure windows to inject:
1. crash before DB commit: no effect; message redelivered.
2. crash after DB commit before broker ACK: redelivery; unique processed-event constraint suppresses duplicate effect.
3. crash after external payment succeeds but before local result: provider idempotency key/reconciliation is mandatory.
4. publish succeeds but outbox mark fails: duplicate event; consumer deduplicates.
5. same eventId with different payload: quarantine as contract/security incident, do not silently ignore.
