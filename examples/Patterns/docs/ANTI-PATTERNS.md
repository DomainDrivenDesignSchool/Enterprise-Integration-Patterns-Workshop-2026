# Anti-patterns: Reproduce, Observe, Refactor

These are deliberate bad designs for review exercises, not implementation recommendations.

## 1. Central ESB Bottleneck
**Bad shape:** all commands/events pass through one central integration service that transforms, enriches, routes and owns orchestration.
**Symptoms:** rising queue depth at one node, correlated outage, deploy coordination, single scaling ceiling.
**Exercise:** add artificial 100 ms delay and cap concurrency to the central relay; compare end-to-end p95.
**Refactor:** domain-owned services, focused broker routing, decentralized ownership, explicit workflow coordinator only where needed.

## 2. Shared Database
**Bad shape:** Payment and Inventory query/update `orders` tables directly.
**Symptoms:** schema changes require synchronized deploys, hidden coupling, cross-service locks.
**Exercise:** change a column and observe compile/runtime coupling.
**Refactor:** service-owned DBs; publish facts or query APIs; use event-carried state when consumers need a snapshot.

## 3. Chatty Communication
**Bad shape:** Order API calls Payment, Inventory, Shipping and Notification sequentially, then polls each.
**Symptoms:** latency is sum of network hops, retry amplification, cascading failures.
**Exercise:** inject 100 ms latency per call; compare sequential vs async workflow.
**Refactor:** accept command quickly, emit event, process asynchronously; batch only when semantics allow.

## 4. Death-Star Architecture
**Bad shape:** every service calls every other service directly.
**Symptoms:** N*(N-1) possible edges, cyclic dependencies, unclear ownership.
**Exercise:** draw service dependency graph and count edges.
**Refactor:** bounded contexts, event contracts, few intentional synchronous query paths.

## 5. Message Bus as a God Object
**Bad shape:** broker wrapper contains business rules, validation, orchestration, retries and persistence.
**Symptoms:** opaque behavior, hard-to-test routing, vendor lock-in.
**Refactor:** broker adapter handles transport only; domain services own decisions; policies are explicit.

## 6. Ignoring Idempotency
**Bad shape:** increment balance or charge card every delivery.
**Exercise:** deliver same `eventId` twice and concurrently; observe double effect.
**Refactor:** unique processed-event key + atomic local transaction; provider idempotency key for external effects.

## Review checklist
- Does each service own its data?
- Can one consumer fail without blocking unrelated groups?
- Are retries bounded and jittered?
- Is DLQ monitored and replayable?
- Is every state transition observable?
- Does replay mutate money/inventory twice?
- Are buffers bounded?
