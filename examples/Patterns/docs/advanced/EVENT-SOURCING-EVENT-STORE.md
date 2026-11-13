# Event Sourcing and the SQL Server Event Store

This section adds a real Event Store for the workshop scenario, rather than mistakenly calling the Outbox or a Kafka topic an Event Store.

## Components

- `src/EventStore/EventStoreDbContext.cs`: the EF Core model and the unique index on `(aggregate_id, stream_version)`.
- `src/EventStore/EfEventStore.cs`: append with `ExpectedVersion`, `rowversion`-based optimistic concurrency, and snapshots.
- `src/EventStore/EventSourcedAggregate.cs`: replay with a version-continuity check.
- `src/EventStore/EventUpcasters.cs`: a chain of payload-version conversions that fails fast if a step is missing.
- `src/EventStore/SalesOrderAggregate.cs`: a teaching aggregate that demonstrates immutable events and replay.
- `ops/sqlserver/12-event-store-schema.sql`: a standalone SQL Server schema.

## Setup

1. Create the separate `WorkshopEventStore` database with `ops/sqlserver/00-create-databases.sql`.
2. Run `ops/sqlserver/apply-event-store-schema.ps1` or `ops/sqlserver/apply-all-schemas.ps1`.
3. Register the service with `services.AddSqlServerEventStore(connectionString)`.
4. Make the service's database user a member of the `workshop_event_store_writer` role. This role has only `SELECT/INSERT` on `dbo.domain_events` (`UPDATE/DELETE` are denied), update permission on the stream head, and insert permission on snapshots.

## Append and concurrency

Each stream has its own independent version. The first event gets version 1. An append succeeds only if the stream's latest version exactly equals `expectedVersion`. The `aggregate_stream_heads` table holds one `rowversion` per stream. Each append updates the head version in the same EF transaction; with optimistic concurrency, SQL Server accepts only one of the concurrent writers. The unique index `(aggregate_id, stream_version)` is the final integrity boundary. For a new stream, the head's primary key resolves the race over creating the stream. With a stale expected version, `WrongExpectedVersionException` is returned. With a stale rowversion or a race over stream creation, the error is converted to `WrongExpectedVersionException` and the current version is re-read. If the response to an append is lost after the commit, retrying the same batch with the same EventIds and identical content returns the previously stored records; reusing an EventId with different content, or repeating only part of a batch, leads to `EventAppendConflictException`.

## Real SQL Server test

After applying the schema, give a connection string for the dedicated `WorkshopEventStore` database and run the probe:

```powershell
$env:EVENTSTORE_SQLSERVER_CONNECTION = 'Server=localhost,1433;Database=WorkshopEventStore;User Id=sa;Password=<secret>;Encrypt=True;TrustServerCertificate=True'
dotnet run --project tests/EventStore.Integration -c Release
```

For a random aggregate, this probe runs two concurrent appends with `expectedVersion=1` and must see exactly one success and one `WrongExpectedVersionException`; it then checks full replay and snapshot-tail, and deletes the probe data. This test has not yet been run in the development environment.

## Snapshot and replay

A snapshot is only a faster starting point for rehydration; the source of truth remains the append-only events. `LoadAsync(useSnapshot: false)` is used for full replay and audit. `SaveSnapshotAsync` accepts a snapshot only if the stream still has the same expected version.

## Versioning and upcasting

`EventUpcasterRegistry` advances each conversion by exactly one version. If a v1 event exists and only the v2 upcaster is registered, replay stops with an error. This behavior is deliberate: an old payload must never be converted to the new model by guesswork. Upcasters must be deterministic, pure, and covered by fixture tests built from real old payloads.

## Guarantee boundaries

- At this stage this Event Store is a standalone lab and has not yet replaced the order write path in `OrderService`.
- The Event Store alone does not make Kafka publication atomic. To publish an integration event, the Event Store append and the Outbox must be stored in the same database and the same transaction; that wiring has not been implemented yet.
- Event Sourcing is not a replacement for operational CDC: CDC sees table changes, while the Event Store records domain changes.
- This version provides the schema, append, expected-version, idempotent retry, snapshot and basic replay; the multi-writer probe exists in the source but has not been run. Crash-window testing, retention/archival, encryption/key rotation, snapshot schema upcasting and projection checkpoint/rebuild are not yet complete.
- The code has not been built with the .NET SDK in the current environment; treat the status as "implemented in source, not validated".
