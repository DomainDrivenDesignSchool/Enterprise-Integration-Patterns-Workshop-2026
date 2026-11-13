# Pattern Coverage Matrix — audited status

This matrix was updated after a static audit of the repository code. "Code exists" is not equal to "operationally validated".

Status:
- **Partial / unverified:** part of the implementation exists in the repository, but integration/runtime validation is not complete.
- **Docs only:** an explanation or runbook exists, but there is not enough executable code.
- **Missing:** no dependable implementation was found.

| Topic | Audited status | Evidence/path | Important gap |
|---|---|---|---|
| Kafka topics, partitions, offsets, consumer groups | Partial / unverified | `src/*Worker`, `ops/kafka` | rebalance/crash/fencing integration tests have not been run |
| At-most-once / at-least-once | Docs + partial | `docs/DELIVERY-SEMANTICS.md`, consumers | a runnable comparative demo and crash assertions are incomplete |
| Kafka transactions | Partial / unverified | `src/Messaging/Reliability` | multi-broker, fencing and `read_committed` not tested |
| Rabbit Direct/Topic/Fanout/Headers | Partial | `src/RabbitMqWorker`, `ops/rabbitmq` | confirms, returns, recovery, quorum, robust retry and integration tests are incomplete |
| Transactional Outbox | Partial / unverified | Order/Payment/Inventory/Saga SQL workers | crash-window tests and lease fencing not proven |
| Inbox / idempotency | Partial / unverified | service inbox tables and workers | concurrency/crash/payload-mismatch integration matrix is incomplete |
| Retry / DLQ | Partial / unverified | `src/Messaging/Reliability`, `src/RetryScheduler`, `src/DlqReplay` | ordering policy and operational failure injection not fully tested |
| Saga orchestration / compensation | Partial / unverified | `src/SagaOrchestrator` | timeout races, late responses and failed-compensation recovery not tested |
| Payment reconciliation / webhook | Partial / unverified | `src/PaymentWorker` | PSP sandbox; partial refund, chargeback and settlement absent |
| Inventory reservation | Partial / unverified | `src/InventoryWorker` | expiry/backorder/multi-warehouse and stress integration are incomplete |
| SQL Server CDC + Debezium | Partial / unverified | `ops/sqlserver`, `ops/debezium`, `src/CdcEnricher` | real connector/agent/retention/recovery not tested |
| Event Sourcing | **Partial / unverified (standalone foundation)** | `src/EventStore`, `ops/sqlserver/12-event-store-schema.sql`, `tests/EventStore.Tests` | not connected to OrderService/Outbox; the SQL Server writer-race probe was added but not run; crash tests and projection rebuild are still needed |
| CDC over Event Store | **Missing / next step** | The Event Store now exists in `src/EventStore`; Debezium is configured only for orders | CDC on `dbo.domain_events`, a separate connector, LSN recovery and a comparison with Outbox are needed |
| CQRS / projection rebuild | Partial | `src/AnalyticsWorker` | no full rebuild from the Event Store and no checkpoint recovery |
| Tumbling/sliding/count windows | Partial / unverified | `src/StreamProcessor` | distributed state ownership and completeness of event-time semantics not proven |
| Backpressure / bulkhead / throttle | Partial | `src/Shared/Resilience` | integration/load evidence and end-to-end propagation are incomplete |
| Distributed tracing | Partial | `src/OrderService/Program.cs` | worker/broker/DB-wide instrumentation and trace continuity are incomplete |
| Metrics / dashboards / alerts | Partial | `ops/prometheus/prometheus.yml` | exporters, dashboards and alert rules are incomplete |
| Load testing | Partial | `load/*.js` | run profiles, thresholds, measured report and reproducibility are incomplete |
| Failure injection | Partial | `tests/integration`, `docs/advanced` | not all mission scenarios are executable and assertable |
| Event schema evolution | Partial / docs | `src/Contracts`, docs | schema registry, compatibility CI, upcasters and contract tests are incomplete |
| Anti-patterns | Docs only / partial | `docs/ANTI-PATTERNS.md` | two runnable broken-vs-fixed, observable examples are needed |
| Workshop agenda | Partial | `docs/WORKSHOP-RUNBOOK.md` | instructor guide, expected outputs and assessment rubric are incomplete |
| Production readiness checklist | Missing / partial | `docs/IMPLEMENTATION-STATUS.md` | a comprehensive operational/security/DR/capacity checklist is needed |

## Acceptance rule

No capability is presented as Complete or Production-Ready unless it has code, schema/config, unit + integration tests, a repeatable run command, failure evidence and documented limitations.

For details and the order of gap remediation: `docs/WORKSHOP-GAP-AUDIT.md`.
