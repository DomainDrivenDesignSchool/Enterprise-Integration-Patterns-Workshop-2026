# Session 4 + Session 5 — Concept-to-Code Coverage Audit

This document maps every concept explicitly requested in the workshop brief to concrete implementation evidence. A concept is not marked complete merely because it appears in a README, class name, or slide.

Status definitions:
- `Partial`: a limited implementation exists; it is not a complete lab or production-grade demonstration.
- `Docs`: only explanatory material/runbook is present, or executable behavior is not demonstrated.
- `Missing`: no corresponding implementation was found in the current source tree.
- `Not runtime-verified`: source exists, but build/integration/runtime tests have not been run in this environment.

## Session 4 — Event Streaming, Tooling & Integration Architecture

| Required concept | Current evidence | Status | What the code still needs |
|---|---|---|---|
| Event Streaming and Real-Time Integration | Kafka producers/consumers across workers | Partial, not runtime-verified | End-to-end latency/lag lab, independent consumers, replay and outage recovery evidence |
| Traditional Messaging vs Event Streaming | RabbitMQ worker and Kafka workers | Partial | Same business workload implemented on both brokers and compared under failure/load |
| Log-based Architecture | Kafka topic/partition usage | Partial | Explicit append-log retention/compaction/replay lab and offset experiments |
| Kafka Topics, Partitions, Offsets, Consumer Groups | Topic scripts and consumers | Partial | Multi-partition group scaling, rebalance, assignment, lag and offset-reset exercises |
| RabbitMQ Exchanges, Queues, Bindings, Routing | `src/RabbitMqWorker/Program.cs` | Partial | Full topology, routing matrix, durable queue policy, alternate exchange and operational tests |
| Kafka vs RabbitMQ | Both appear in solution | Docs/partial | Controlled benchmark and semantic comparison using same event workload |
| Direct Exchange | Exchange declaration only | Partial | Binding/routing-key exercise with positive and negative assertions |
| Topic Exchange | Exchange + one binding | Partial | Wildcard routing matrix (`*`, `#`) and tests |
| Fanout Exchange | Exchange + one binding | Partial | Multiple subscriber queues and independent consumer behavior |
| Headers Exchange | Exchange declaration only | Partial | `x-match=all/any`, typed headers, routing tests |
| Event Time vs Processing Time | Both fields in `StreamEvent` | Partial | Explicit skew/clock drift lab, separate metrics and deterministic tests |
| Event Sourcing | `src/EventStore` EF Core SQL Server Event Store, expected-version append, aggregate replay, snapshots and upcaster registry | Partial standalone foundation; not build/runtime-verified | Not wired into OrderService command + Outbox flow; SQL Server concurrent-writer probe exists but is not executed here; crash-window tests, projection rebuild and retention/archival policy still required |
| CQRS | Analytics worker/read-side behavior | Partial | Explicit command model, query model, projection checkpoint, rebuild and eventual-consistency contract |
| Windowing | `src/StreamProcessor/WindowEngine.cs`, `DurableWindowWorker.cs` | Partial | Correct distributed window lifecycle, checkpoint/recovery, state ownership and late corrections |
| Tumbling Windows | Window calculation in durable worker | Partial | Dedicated configurable implementation and boundary tests |
| Sliding Windows | `WindowEngine` has a slide parameter | Partial | Boundary/fan-out tests and durable distributed state |
| Count Windows | `AddCountWindow` helper | Partial | Keyed count windows, persistent state and recovery semantics |
| Watermarks | Max event time minus allowed lateness | Partial | Per-partition/per-key watermark, idle partition policy, late side output and correction policy |
| Distributed Tracing | OTel registration in OrderService | Partial | Instrument all APIs/workers, SQL/Kafka/Rabbit spans, W3C context propagation and trace continuity tests |
| Metrics and Logging | Prometheus scrape config and structured logs | Partial | Actual exporters, metric instruments, dashboards, alert rules, cardinality policy and correlation |
| CDC | SQL Server CDC script and Debezium config | Partial | SQL Agent/CDC retention, LSN restart, lag monitoring, schema changes and failure/recovery tests |
| Debezium | Connector example and registration script | Partial | Reproducible Connect deployment, connector status checks, snapshot/restart/offset recovery and schema evolution |
| Integration Architecture Anti-Patterns | `docs/ANTI-PATTERNS.md` | Docs | Executable broken-vs-fixed labs with measurable symptoms |
| Central ESB Bottleneck | No dedicated implementation found | Missing | Centralized synchronous router bottleneck lab + decoupled alternative |
| Shared Database Anti-Pattern | Per-service DB scripts exist | Docs/partial | Deliberate anti-pattern demo showing schema coupling/lock contention, then extraction |
| Chatty Communication | No dedicated implementation found | Missing | N+1 synchronous call-chain lab, trace visualization and async/batch redesign |
| Death-Star Architecture | No dedicated implementation found | Missing | Highly connected dependency graph demo, failure propagation and bounded-context redesign |
| Message Bus as a God Object | No dedicated implementation found | Missing | Fat central consumer/router anti-pattern and domain-owned event contracts |
| Ignoring Idempotency | Some inbox/idempotency code exists | Partial | Duplicate delivery, key collision, payload mismatch, replay and concurrent duplicate tests |
| Integration Architecture Roadmap | `docs/DECISION-GUIDE.md`, architecture docs | Docs | Executable incremental migration labs and decision gates |

## Session 5 — Performance & Capacity

| Required concept | Current evidence | Status | What the code still needs |
|---|---|---|---|
| Latency | No complete latency lab found | Missing/partial | End-to-end and per-hop histograms, p50/p95/p99 and queueing decomposition |
| Throughput | k6 scripts exist | Partial | Measured sustained throughput, saturation point and reproducible result report |
| Parallelism | Kafka consumers and worker services | Partial | Controlled worker/partition concurrency experiments |
| Partitioning | Kafka topic scripts and shard router | Partial | Key distribution, hot key, skew, repartitioning and partition-count trade-offs |
| Throughput vs Latency Trade-offs | No controlled experiment | Missing | Batch-size/linger/concurrency matrix with latency percentiles |
| Batching | No explicit complete batch lab | Missing | Producer batching and consumer bulk processing with partial failure semantics |
| Vertical Scaling | No benchmark procedure | Docs/missing | CPU/memory constrained single-node experiment |
| Horizontal Scaling | Multiple worker instances are architecturally possible | Partial | Replica scale-out experiment and coordination/ownership validation |
| Fault Isolation | Bulkhead helper exists | Partial | Per-dependency pools, process isolation and fault-containment tests |
| Bulkhead | `src/Shared/Resilience/AsyncBulkhead.cs` | Partial | Cancellation, queue saturation, fairness and metrics tests |
| Circuit Breaker | No circuit breaker implementation found | Missing | State machine, half-open probes, jitter, metrics and deterministic tests |
| Timeout | Some operation timeouts/configuration | Partial | End-to-end deadline propagation and timeout-vs-cancellation distinction |
| Retry | `src/Shared/Resilience/RetryPolicy.cs`, Kafka retry router | Partial | Retry classification, budgets, jitter, deadline, ordering and storm prevention |
| Fallback | No general fallback policy found | Missing | Safe fallback vs stale/incorrect data, fallback metrics and tests |
| Backpressure | Bounded work queue exists | Partial | End-to-end demand propagation and broker pause/resume behavior |
| Throttling | Token bucket helper exists | Partial | Distributed quota semantics, fairness and observable 429/throttle behavior |
| Buffering | Bounded queue and broker buffers | Partial | Capacity limits, overflow policy and memory-pressure experiments |
| Dropping | No explicit policy lab | Missing | Drop-oldest/newest/sample policy, loss accounting and business acceptance |
| Consumer Scaling | Kafka consumer groups | Partial | Partition ceiling, rebalances, cooperative assignment and scale-up/down tests |
| Distributed Tracing | OrderService only clearly instrumented | Partial | Full path instrumentation and trace context propagation |
| Replay Strategies | Kafka replay examples and DLQ replay utility | Partial | Replay isolation, rate limit, dedupe, checkpoint and live-traffic coexistence |
| Offset Management | Manual commits in consumers | Partial | Commit ordering, rebalance races, pause/resume and replay/reset lab |
| Idempotency During Replay | Inbox tables and fingerprints | Partial | Replay-specific idempotency, key retention and old-schema event tests |
| Business KPIs | `docs/SLO-KPI.md` | Docs | KPI event model, aggregation and dashboard |
| SLAs | `docs/SLO-KPI.md` | Docs | SLI/SLO error-budget calculation and alerting |
| Benchmarking | No validated result | Missing | Repeatable benchmark harness and published measured baseline |
| Load Testing | `load/*.js` | Partial | Profiles, thresholds, warm-up, ramp, soak, stress and report artifacts |
| CPU Monitoring | Prometheus config only | Partial | Node/process exporter and dashboard |
| Memory Monitoring | Prometheus config only | Partial | Working-set/GC metrics, heap pressure and dashboard |
| Disk Monitoring | No complete implementation found | Missing | Disk throughput/latency/free-space and SQL transaction-log monitoring |

## Session 3 prerequisites — implementation coverage

| Required concept | Current evidence | Status | What the code still needs |
|---|---|---|---|
| Store & Forward | Broker/outbox flow | Partial | Explicit durable store-forward lab and broker outage recovery |
| Guaranteed Delivery | Outbox/inbox and durable broker configuration | Partial | Define guarantee boundaries; crash-window tests and retention assumptions |
| Retry and Exponential Backoff | Retry policy and scheduler | Partial | Full classification, jitter, retry budget, deadline and ordering |
| Idempotent Receiver | Inbox/fingerprint patterns | Partial | Concurrent duplicate and payload-collision tests |
| Dead Letter Channel | DLQ topic and replay utility | Partial | Quarantine metadata, operator workflow, safe replay and retention |
| Poison Messages | Failure routing code | Partial | Schema-invalid, oversized, deterministic poison and quarantine lab |
| Transactional Messaging | Kafka transactional router | Partial | Broker-specific semantics and crash/fencing integration tests |
| Transactional Outbox | SQL outbox workers | Partial | Same-DB atomicity proof and all crash windows |
| Saga | Saga orchestrator | Partial | Full state machine, timeout races, compensation retries and operator audit |
| Orchestration vs Choreography | Saga orchestration exists | Partial | Parallel choreography implementation and comparison |
| Event-Carried State Transfer | Events carry some payload fields | Partial | Explicit event contract, staleness/version and payload-size trade-off lab |
| Process Manager | Saga orchestrator | Partial | Long-running process state, timers, correlation and recovery |
| Batch vs Near Real-Time vs Streaming | Docs/stream worker | Partial | Three implementations over same business data and latency/cost comparison |
| Sync vs Async Integration | HTTP API and Kafka/Rabbit paths | Partial | Explicit decision matrix and failure/load comparison |

## Important semantics: do not overclaim

- Kafka transactions can atomically publish Kafka records and commit consumed offsets on the same Kafka cluster. They do not create a global transaction with SQL Server or an external payment provider.
- SQL Server CDC exposes database row changes. It does not create domain events and is not Event Sourcing.
- Event Sourcing requires an append-only domain event store and aggregate rehydration; an Outbox table or Kafka topic alone does not satisfy it.
- The current `WindowEngine` is a teaching/single-process implementation. `DurableWindowWorker` persists selected state in SQL Server, but this is not equivalent to a mature distributed stream-processing runtime.
- Existing resilience helpers are individual mechanisms, not a complete end-to-end resilience strategy.
- The RabbitMQ worker declares all four exchange types but does not demonstrate all routing semantics or publisher confirms/returns/recovery.
- No performance number is valid until a repeatable benchmark has actually been run and its environment/workload recorded.

## Required implementation order

1. Complete Event Store concurrency/failure integration tests; add projections/rebuild, retention and operational controls.
2. CQRS projections + rebuild + checkpoint + replay-safe idempotency.
3. CDC over Event Store and independent CDC lab over operational tables; Debezium snapshot/LSN/retention recovery.
4. Complete RabbitMQ topology and publisher/consumer reliability; same-workload Kafka comparison.
5. Event-time stream processing: tumbling/sliding/count windows, watermarks, idleness, late data, corrections and recovery.
6. Resilience lab: timeout, retry, circuit breaker, fallback, bulkhead, backpressure, throttle, buffering and dropping.
7. Performance lab: batch, parallelism, partitioning, scale, latency/throughput trade-offs, CPU/memory/disk and consumer scaling.
8. Observability: trace propagation, metrics, structured logs, dashboards, alerts and business KPIs/SLAs.
9. Executable anti-patterns and architecture roadmap.
10. Failure injection, load/soak/stress testing, runbooks and instructor material.

## Definition of Done

A concept is `Complete` only when all are present:
1. Concrete executable code and configuration.
2. Database/broker topology where applicable.
3. Unit tests and integration/failure tests.
4. Reproducible run instructions and expected output.
5. Operational metrics, failure behavior and recovery runbook.
6. Explicit semantics, limitations and capacity assumptions.
