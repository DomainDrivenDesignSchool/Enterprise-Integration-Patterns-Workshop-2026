# Workshop Gap Audit Against the Original Mission Brief

> Audit date: 2026-11-11  
> Basis: the full Workshop Blueprint brief provided by the user + a static review of the Solution and existing files.  
> This document is not a certificate of successful execution; no build, integration test or benchmark has been run in this environment.

## Overall verdict

The current version is a **laboratory sample with a few relatively advanced paths**, not a complete or production-ready implementation of the defined workshop. The existence of a class, an SQL script or a runbook alone does not count as operational coverage. This audit uses four coverage levels:

- **Implemented (unverified):** visible code/scripts exist, but real execution and integration testing have not been confirmed.
- **Partial:** part of the behavior exists, but the main/edge scenarios or the required guarantees are incomplete.
- **Docs only:** an explanation or claim exists in the documentation, but no corresponding executable implementation was found.
- **Missing:** no dependable implementation was found in the Solution or the current infrastructure.

## Critical gaps discovered

| Area | Current status | Existing evidence | Real gap |
|---|---|---|---|
| Event Sourcing | **Partial / unverified (standalone foundation)** | `src/EventStore`, `ops/sqlserver/12-event-store-schema.sql` | A standalone Event Store, an append-only event table, expected-version, aggregate rehydration, snapshot and upcaster were added; it is not yet connected to the order command flow and the atomic Outbox. A SQL Server writer-race probe was added but has not been run; crash tests, projection rebuild and retention are still incomplete. |
| CDC for Event Sourcing | **Missing / next step** | SQL Server CDC on `dbo.orders`; Event Store in `src/EventStore` | A separate connector for `dbo.domain_events`, LSN recovery and a CDC-vs-Outbox comparison are needed. CDC is not a replacement for Event Sourcing. |
| Operational CDC | **Partial** | Sample Debezium SQL Server connector and `CdcEnricher` | Snapshot/restart/retention/schema evolution/cleanup lag and LSN recovery have no executable tests; the connector and SQL Agent have not been validated in a real environment. |
| CQRS | **Partial** | Analytics projection and separation of Workers | Full Command/Query separation at the Aggregate level and a rebuildable Read Model with a documented/tested consistency contract do not exist. |
| RabbitMQ | **Partial** | A small Worker with Exchange/Queue and TTL/DLX | Publisher confirms, mandatory returns, topology recovery, connection/channel recovery, quorum queues, retry parking-lot, durable poison handling, prefetch/bulkhead and crash/reconnect tests are absent or unproven. |
| Kafka delivery | **Partial** | Consumers, retry router and Kafka transaction code | Multi-broker integration tests, rebalance/crash/fencing, transactional.id uniqueness, read_committed, max.poll and failure windows have not been executed. |
| Exactly-once | **Partial** | Limited atomicity inside a Kafka transaction | There is no global guarantee across SQL Server, Kafka and the PSP; DB commit + offset commit is still an at-least-once + Inbox/idempotency pattern. |
| Outbox/Inbox | **Partial** | SQL Server tables and claim workers | Real crash tests at every boundary, lease expiry and fencing token, poison/outbox starvation, cleanup/retention and multi-aggregate ordering have not been executed. |
| Saga | **Partial** | Orchestrator, compensation, timeout and DeadSaga | The race policy between a late response and a timeout, duplicate compensation commands, non-compensatable compensation, operator approval and recovery after a full restart are not proven. |
| Payment | **Partial** | Sandbox charge/refund, reconciliation and ledger | No real PSP, partial/multiple refunds, chargeback, settlement-file reconciliation, dispute lifecycle, maker-checker or multi-currency financial controls. |
| Inventory | **Partial** | SQL conditional update and reservation | No expiry/reaper, oversell under real concurrent testing, backorder, multi-warehouse, reservation extension or WMS integration (or untested). |
| Stream Processing | **Partial** | Window engine and state in SQL Server | No state partitioning/distribution, ownership/lease, rebalance-safe checkpoint, scalable event-time timers, joins, state TTL or recovery/load testing at scale. This is not Flink. |
| Window semantics | **Partial** | Tumbling/sliding/count and watermark | The precise definition of allowed lateness, idleness, per-partition/per-key watermark, correction/retraction and a late-data side output must be completed and tested. |
| Observability | **Partial** | OTel in OrderService and Prometheus scrape config | Worker-wide instrumentation, Kafka/Rabbit/SQL spans, real lag/retry/DLQ/outbox metrics, exemplars, dashboards, alert rules, Tempo/Jaeger and log correlation are absent or unproven. |
| Load testing | **Partial** | Two k6 scripts | No workload model, data setup, thresholds, run profile, real results, repeatability, saturation test or recovery measurement. No performance number has been verified. |
| Failure injection | **Partial** | Runbook and a few smoke scripts | The 15 requested scenarios have not been turned into automated, repeatable tests with assertions and recovery evidence. |
| Security | **Partial** | Sandbox HMAC and operator key | Secret rotation, TLS/mTLS, OAuth/OIDC/RBAC, rate limiting, audit protection, PII retention and provider-specific signature verification are incomplete. |
| Event contracts | **Partial** | Envelope and several event versions | No schema registry, compatibility gate in CI, upcaster, producer/consumer contract tests or deprecation policy. |
| Infrastructure | **Partial** | SQL Server/Kafka/Rabbit scripts and Prometheus | No reproducible deployment or complete monitoring/connect provisioning. Given the no-Docker constraint, native/VM installation and a health-check script must be provided. |
| Workshop deliverables | **Partial** | Agenda and runbooks | The instructor guide, guided questions, expected output for each demo, rubric and a comprehensive production readiness checklist are not complete. |

## Documentation error that must be fixed

`docs/PATTERN-MATRIX.md` presents some items as Lab coverage whose corresponding code was not found in the Solution or exists only partially. These include Event Sourcing, Projection Rebuild, OpenTelemetry/Prometheus across the whole system, and Event Versioning/Schema Evolution. These items must not be shown as full coverage until code and executable tests exist.

## Correct separation of CDC and Event Sourcing

### Required Event Sourcing

A real Event Store must be created in SQL Server for the Order Aggregate:

- An append-only table with `stream_id`, `stream_version`, `event_id`, `event_type`, `schema_version`, `payload`, `metadata`, `occurred_at_utc`.
- A unique constraint on `(stream_id, stream_version)` and on `event_id`.
- Append with expected version and optimistic concurrency; two concurrent commands must not overwrite the same version.
- Rehydrating the Aggregate from events, with an optional Snapshot at a defined version.
- An Upcaster for old event versions.
- Separate Projections with a checkpoint and rebuild from the stream.
- Recording command idempotency and the command result.
- Tests for corruption, duplicate append, concurrent append, replay, snapshot mismatch and projection crash.

Kafka can fan out events, but the SQL Event Store is the source of truth. The same-transaction Outbox must guarantee publication of newly stored events. CDC on the Event Store table is an alternative/comparison path for publishing log changes, not the source of domain semantics.

### Required independent CDC

- SQL Server CDC on specific tables with a capture instance and retention policy.
- Debezium SQL Server Connector with a reviewed initial-snapshot and incremental/recovery behavior.
- Detection of insert/update/delete, before/after, LSN and commit order.
- Tests for stopping SQL Agent, Connect, Kafka and the consumer.
- Monitoring of log reuse wait, capture lag, cleanup, retention and transaction log growth.
- Tests for schema evolution and re-snapshot after a CDC range is lost.
- Comparison of CDC with Outbox: row changes versus business intent.

## Order of project remediation

### P0 — Stabilize the baseline and technical correctness

1. Lock the .NET version, NuGet packages and the SQL Server target.
2. Run restore/build/test on a real SDK and fix all errors.
3. Create an Integration Test Harness without Docker: SQL Server, Kafka and RabbitMQ installed on a VM/machine or as standalone services.
4. Turn `PATTERN-MATRIX` into an honest matrix of code + test + run command + evidence.
5. Remove Production-Ready claims and state the real guarantee of each transaction boundary.

### P1 — Domain core and Event Sourcing

1. Complete the writer-race and crash validation for the Order Event Store; then projection rebuild and CDC on the Event Store.
2. Aggregate rehydration, snapshot and upcasting.
3. CQRS Read Model and independent rebuild.
4. Atomic Outbox in the same transaction that records the event.
5. CDC on the Event Store and comparison with Outbox.
6. Concurrency, replay and crash scenarios.

### P2 — Messaging and Broker Reliability

1. Complete the Kafka producer/consumer lifecycle, rebalance, fencing and transaction semantics.
2. Retry/DLQ with an ordering policy, retry budget, delayed scheduling and safe DLQ replay.
3. RabbitMQ with confirms/returns, recovery, prefetch, quorum, DLX and redelivery tests.
4. A real Kafka/Rabbit comparison with an identical workload.

### P3 — Business and distributed flows

1. Complete the Saga with state-transition guards, timeout races and compensation recovery.
2. Payment: partial refund, chargeback and settlement reconciliation.
3. Inventory: expiry, backorder and multiple warehouses.
4. Shipping and Notification with provider adapters and idempotency.
5. Inbox/Outbox retention, cleanup, fencing and an operational API.

### P4 — Streaming and operations

1. Stream state partition ownership and checkpoint recovery.
2. Complete event-time semantics and late-data correction.
3. System-wide OpenTelemetry, metrics, dashboards and alert rules.
4. k6 workload profiles, thresholds and a real benchmark report.
5. Repeatable failure injection for all mission scenarios.
6. Runbook, instructor guide and production readiness checklist.

## Acceptance rule for every capability from now on

No item is declared "complete" unless it has these five pieces of evidence:

1. Real code at a specified path in the Solution.
2. An executable schema/migration and configuration.
3. Unit and integration tests covering the relevant failure window.
4. A repeatable run command and expected output.
5. Limitations, semantics and an operational runbook.

Until real execution is possible, the status will remain **Implemented — Not Runtime Verified**.
