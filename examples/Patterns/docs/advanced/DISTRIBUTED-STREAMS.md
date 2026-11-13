# Durable and Distributed Stream Processing

`DurableWindowWorker` is a .NET implementation pattern with a Kafka consumer group and a durable SQL Server state store; Kafka partitions are distributed among replicas, and the state/window/inbox remain in a shared DB.

## Safe processing order

1. Read the message from Kafka.
2. In one DB transaction, insert into `stream_inbox` with the EventId and topic/partition/offset; if it is a duplicate, apply no other effect.
3. Update the max event-time and watermark of that customer key; because the Kafka input is partitioned by orderId and a customer may have several orders in several partitions.
4. Upsert the window state, or record a late event beyond the allowed lateness in `stream_late_events`.
5. Mark and record the closed windows with `stream_outbox` in the same transaction.
6. DB commit.
7. Kafka offset commit.

If a crash occurs between steps 6 and 7, the message comes again but the Inbox recognizes it as a duplicate. The Outbox publisher sends the output with confirmation; a crash between publish and mark may create a duplicate output, so the downstream must also dedupe the EventId.

## Windows and time

- Event time comes from the time of occurrence at the source; processing time is the time the worker receives it.
- The watermark equals `maxEventTime - allowedLateness`.
- This sample has a one-minute Tumbling window with two minutes of allowed lateness. Its input is `PaidAmountDelta`, not the total order amount; therefore each table UPDATE does not cause the full amount to be counted again. Windows are separate per `customerId + currency` so that IRR and EUR or USD are never summed into one financial total.
- An event later than the watermark goes to late-events; in a real system a correction/upsert or a side output may be chosen.
- In this sample the watermark is kept per customer key in SQL Server; this choice prevents a customer's windows from closing early based on the progress of an unrelated partition. If a partition becomes idle, an idle-partition policy and watermark coordination must be designed.

## Scalability and limitations

This sample is for the workshop and moderate load, not a complete replacement for Flink/Kafka Streams. The state is kept in SQL Server; the DB can become a bottleneck. Under high load, distribute the state store by key/partition and evaluate batch upsert, connection pool, table partitioning and compaction. If you need complex joins, event-time timers, a state backend and large checkpoints, Flink or a specialized stream-processing platform is more suitable.
