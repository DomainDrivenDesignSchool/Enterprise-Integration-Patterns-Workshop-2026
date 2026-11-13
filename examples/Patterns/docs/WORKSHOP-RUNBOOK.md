# Three-hour workshop runbook

| Time | Lab | Outcome |
|---|---|---|
| 00–15 | Architecture & contracts | service boundaries, sync/async, envelope |
| 15–35 | Kafka groups/offsets + Rabbit routing | compare log and queue |
| 35–55 | Outbox + idempotency | crash windows and duplicates |
| 55–75 | Retry/DLQ/poison | transient vs permanent failure |
| 75–100 | Saga | orchestration, choreography, compensation |
| 100–120 | Partitioning/parallelism | skew, hot key, rebalance |
| 120–140 | Backpressure/resilience | bounded queue, throttle, bulkhead |
| 140–155 | Windowing and replay | event time, watermark, rebuild |
| 155–170 | Observability + CDC | trace propagation, Debezium |
| 170–180 | Load test & anti-pattern review | p95, lag, SLO, design review |

For a 3-hour live session, not every external infrastructure path can be installed from scratch. Prepare SQL Server, Kafka, RabbitMQ, Prometheus and Connect before class. The repository includes the runnable code and runbooks; use the remaining labs as guided experiments.
