# Architecture Decisions

## Service boundaries
- Order owns order lifecycle and order database; `OrderLifecycleWorker` applies `PaymentAuthorized`, `OrderCompleted` and `OrderFailed` from the ordered saga-events topic and its status updates feed CDC.
- Payment owns payment attempts and ledger.
- Inventory owns stock and reservations.
- Shipping owns shipment lifecycle.
- Notification owns delivery preferences and notification attempts.
- Analytics owns projections and can rebuild them from retained events.

A single SQL Server instance may host multiple databases for workshop convenience; credentials and schema ownership remain isolated. No service reads another service's tables.

## Synchronous vs asynchronous
Synchronous HTTP is limited to command acceptance and query endpoints where the caller needs immediate validation/response. Business workflow transitions are asynchronous events/commands. Do not make the Order API synchronously call Payment, Inventory, Shipping and Notification in series: that creates chatty coupling and cascading latency.

## Event envelope
Every event carries:
- `eventId`: immutable unique identity
- `eventType`, `schemaVersion`
- `occurredAtUtc`: event time
- `correlationId`: business flow grouping
- `causationId`: event/command that directly caused this event
- `traceparent`/`tracestate`: W3C trace context
- `partitionKey`: stable ordering key (OrderId)
- `payload`: versioned contract

## Delivery
Broker delivery is at-least-once in the normal reliable configuration. Consumers must atomically persist their business effect and processed-event marker before ACK/offset commit. External side effects require provider idempotency keys or a local command/outbox state machine. Strict per-aggregate lifecycle consumers fail-stop on infrastructure errors rather than sending an old transition through a delayed retry that could arrive after a newer transition.

## Outbox claim
A publisher claims a bounded batch in a short DB transaction using row locks and a lease. It publishes outside the transaction, then marks each row delivered. A crash after publish but before marking creates duplicates by design; downstream idempotency handles them.
