# Kafka vs RabbitMQ vs Other Tooling

| Concern | Kafka | RabbitMQ |
|---|---|---|
| Primary abstraction | Partitioned append-only log | Exchange, queue, binding |
| Replay | Native by offset while retention exists | Not native after ack; use application archive/re-publish |
| Routing | Topic/partition key | Direct, Topic, Fanout, Headers |
| Scaling | Partitions bound parallelism per group | Queue consumers; queue semantics and ordering depend on topology |
| Typical fit | Event streams, multiple independent consumers, analytics | Work queues, command routing, flexible routing |
| Acknowledgement | Offset commit | Per-message ack/nack |
| Poison handling | retry/DLQ topics and offset policy | DLX/TTL retry queues, quorum queue delivery limits |
| Ordering | Within partition | Queue-level ordering with caveats under multiple consumers/requeue |

Do not compare a single Kafka partition to a highly parallel RabbitMQ quorum queue as if equivalent. Measure with the same payload, durability, replication, producer confirms/acks, consumer work, batch sizes and hardware.

Pulsar: consider when multi-tenancy, geo-replication and tiered storage are central requirements.
NATS: consider lightweight low-latency pub/sub and request/reply, with JetStream when persistence is needed.
AWS SNS/SQS: consider managed fanout/queue integration in AWS-native environments.
No universal winner; select by replay, routing, operational model, ecosystem, latency and team capability.
