# Kafka Retry / DLQ and Delivery Guarantees

## Designed path

```text
source topic
  ├─ success → process → commit offset
  └─ failure → Kafka transaction:
                 publish retry-topic OR DLQ
                 + SendOffsetsToTransaction(source offset + 1)
               retry scheduler persists due-time in SQL Server
               due dispatcher republishes to original topic/partition
```

`KafkaTransactionalFailureRouter` uses a producer with a `transactional.id`, `enable.idempotence=true` and `acks=all`. The failed message and the consumer's offset are published/committed in one Kafka transaction. As a result, either both happen or neither does. After calling this router, never issue a separate `consumer.Commit`.

## Broker prerequisite for Transactions

For a local single-broker Kafka, set the following in the broker config and restart the broker:

```properties
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
offsets.topic.replication.factor=1
default.replication.factor=1
```

In a multi-broker environment, use an appropriate RF (usually 3) and `transaction.state.log.min.isr=2`. The transactional-state and offsets topics must be reachable; otherwise `InitTransactions` or the transaction commit fails.

## Backoff and DLQ

- Attempt 1: `commerce.retry.1m`
- Attempt 2: `commerce.retry.5m`
- Attempt 3: `commerce.retry.15m`
- Attempt 4: `commerce.retry.1h`
- Attempt 5: `commerce.dlq`

Kafka has no native per-message delay. `RetryScheduler` durably stores the retry message, with its original headers and `x-not-before-utc`, in the `scheduled_retries` table; after the due time the dispatcher returns it to the original topic and partition. If the publish succeeded but the DB mark failed, a duplicate is possible; consumers must have an Inbox/Idempotency.

## Distinguishing errors

- Transient error: timeout, broker unavailable, connection reset; retry.
- Definitive business error: insufficient stock, declined payment; do not do a technical retry; publish a business-rejection event.
- Poison message: corrupt JSON, unknown schema, an unrecoverable invariant; DLQ after a limited number of attempts.
- Prolonged infrastructure error: circuit breaker and pause/throttle; sending every message to the DLQ during an outage destroys the meaning of retry.

## Exact Atomicity

A Kafka transaction covers only the Kafka messages and consumer offsets of the same cluster. An external database, a PSP, an HTTP API or a file side effect is not inside it. For external effects, use Inbox + a local transaction + Outbox and an idempotency key. Do not claim `exactly-once` for the whole world; state the guarantee boundary.

## Preserving ordering versus Retry

A retry topic can change ordering. Consumers that apply an aggregate's state transitions (such as OrderLifecycleWorker) fail-stop on an infrastructure error so that the offset does not advance and the next message of the same partition does not pass it. By contrast, idempotent handlers such as payment/inventory can have scheduled retry. For strict ordering in each consumer, the policy must be chosen based on the semantics of that flow.

## DLQ replay

Before replay, fix the cause, check the schema and business invariant, limit the volume and rate of replay, use the destination topic and the original key, and publish the message with metadata relating to DLQ/replay. Replaying directly to the main topic without throttling can recreate the outage.


### Controlled Replay tool

The `src/DlqReplay` project reads a message from the DLQ, extracts the original topic from `x-original-topic`, removes the previous failure metadata and the retry attempt, adds new replay metadata, and publishes to the original topic together with the input offset commit in a Kafka Transaction. The message key is preserved.

Example run (with environment variables, without Docker):

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export DLQ_TOPIC=commerce.dlq
export DLQ_REPLAY_GROUP=dlq-replay-operator-01
export DLQ_REPLAY_TRANSACTIONAL_ID=dlq-replay-operator-01
export DLQ_REPLAY_MAX_MESSAGES=50
export DLQ_REPLAY_RATE_PER_SECOND=5
dotnet run --project src/DlqReplay
```

Replay must be done only after fixing the cause of the failure. The tool limits the rate and stops after a few seconds without messages. For two simultaneous operators, choose an independent group and transactional ID; a transactional ID must not be shared between two processes. The tool deliberately does not replay a record that lacks `x-original-topic`.
