using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;

namespace Workshop.EventStore;

/// <summary>
/// SQL Server Event Store. A stream-head rowversion provides optimistic concurrency for each aggregate.
/// A unique (aggregate_id, stream_version) index remains the final integrity boundary.
/// </summary>
public sealed class EfEventStore(IDbContextFactory<EventStoreDbContext> contextFactory) : IEventStore
{
    public async Task<IReadOnlyList<StoredDomainEvent>> AppendAsync(Guid aggregateId, long expectedVersion,
        IReadOnlyList<NewDomainEvent> events, CancellationToken cancellationToken = default)
    {
        if (aggregateId == Guid.Empty) throw new ArgumentException("Aggregate id is required.", nameof(aggregateId));
        ArgumentOutOfRangeException.ThrowIfNegative(expectedVersion);
        ArgumentNullException.ThrowIfNull(events);
        if (events.Count == 0) return [];
        if (events.Any(x => string.IsNullOrWhiteSpace(x.EventType) || x.SchemaVersion <= 0 || x.EventId == Guid.Empty || x.Payload.ValueKind is JsonValueKind.Undefined or JsonValueKind.Null))
            throw new ArgumentException("Every event must have an event id, event type, non-null payload and positive schema version.", nameof(events));
        if (events.Select(x => x.EventId).Distinct().Count() != events.Count)
            throw new ArgumentException("Event ids must be unique within an append batch.", nameof(events));

        await using var db = await contextFactory.CreateDbContextAsync(cancellationToken);
        await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
        try
        {
            // A client may lose the response after SQL commit. Reusing the same event ids makes that retry safe.
            var eventIds = events.Select(x => x.EventId).ToArray();
            var alreadyAppended = await db.Events.AsNoTracking()
                .Where(x => x.AggregateId == aggregateId && eventIds.Contains(x.EventId))
                .OrderBy(x => x.Version)
                .ToListAsync(cancellationToken);
            if (alreadyAppended.Count > 0)
            {
                if (alreadyAppended.Count != events.Count)
                    throw new EventAppendConflictException(aggregateId, "only part of the batch already exists.");
                var replayed = new List<StoredDomainEvent>(events.Count);
                for (var i = 0; i < events.Count; i++)
                {
                    var requested = events[i];
                    var persisted = alreadyAppended[i];
                    if (persisted.EventId != requested.EventId || persisted.Version != expectedVersion + i + 1 ||
                        persisted.EventType != requested.EventType || persisted.SchemaVersion != requested.SchemaVersion ||
                        persisted.Payload != requested.Payload.GetRawText() || persisted.CorrelationId != requested.CorrelationId ||
                        persisted.CausationId != requested.CausationId || persisted.OccurredAtUtc != requested.OccurredAtUtc)
                        throw new EventAppendConflictException(aggregateId, "an existing event id has different content or stream position.");
                    replayed.Add(new StoredDomainEvent(persisted.EventId, aggregateId, persisted.Version,
                        persisted.EventType, persisted.SchemaVersion, ParseJsonClone(persisted.Payload),
                        persisted.OccurredAtUtc, persisted.CorrelationId, persisted.CausationId));
                }
                await transaction.CommitAsync(cancellationToken);
                return replayed;
            }

            var head = await db.StreamHeads.SingleOrDefaultAsync(x => x.AggregateId == aggregateId, cancellationToken);
            var actualVersion = head?.CurrentVersion ?? 0L;
            if (actualVersion != expectedVersion)
                throw new WrongExpectedVersionException(aggregateId, expectedVersion, actualVersion);

            var nextVersion = expectedVersion;
            var stored = new List<StoredDomainEvent>(events.Count);
            foreach (var item in events)
            {
                nextVersion++;
                db.Events.Add(new DomainEventRow
                {
                    EventId = item.EventId,
                    AggregateId = aggregateId,
                    Version = nextVersion,
                    EventType = item.EventType,
                    SchemaVersion = item.SchemaVersion,
                    Payload = item.Payload.GetRawText(),
                    OccurredAtUtc = item.OccurredAtUtc,
                    CorrelationId = item.CorrelationId,
                    CausationId = item.CausationId
                });
                stored.Add(new StoredDomainEvent(item.EventId, aggregateId, nextVersion, item.EventType,
                    item.SchemaVersion, item.Payload.Clone(), item.OccurredAtUtc, item.CorrelationId, item.CausationId));
            }

            if (head is null)
            {
                db.StreamHeads.Add(new AggregateStreamHead { AggregateId = aggregateId, CurrentVersion = nextVersion });
            }
            else
            {
                // SQL Server rowversion turns a concurrent update into DbUpdateConcurrencyException.
                head.CurrentVersion = nextVersion;
            }

            await db.SaveChangesAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return stored;
        }
        catch (DbUpdateConcurrencyException)
        {
            await transaction.RollbackAsync(CancellationToken.None);
            var committedByConcurrentRetry = await TryReadExistingBatchAsync(aggregateId, expectedVersion, events);
            if (committedByConcurrentRetry is not null) return committedByConcurrentRetry;
            var actual = await ReadCurrentVersionAsync(aggregateId, CancellationToken.None);
            throw new WrongExpectedVersionException(aggregateId, expectedVersion, actual);
        }
        catch (DbUpdateException exception) when (IsUniqueOrDeadlock(exception))
        {
            await transaction.RollbackAsync(CancellationToken.None);
            var committedByConcurrentRetry = await TryReadExistingBatchAsync(aggregateId, expectedVersion, events);
            if (committedByConcurrentRetry is not null) return committedByConcurrentRetry;
            var actual = await ReadCurrentVersionAsync(aggregateId, CancellationToken.None);
            if (actual != expectedVersion)
                throw new WrongExpectedVersionException(aggregateId, expectedVersion, actual);
            throw;
        }
    }

    public async Task<EventStream> LoadAsync(Guid aggregateId, bool useSnapshot = true,
        CancellationToken cancellationToken = default)
    {
        if (aggregateId == Guid.Empty) throw new ArgumentException("Aggregate id is required.", nameof(aggregateId));
        await using var db = await contextFactory.CreateDbContextAsync(cancellationToken);
        AggregateSnapshotRow? snapshot = null;
        if (useSnapshot)
        {
            snapshot = await db.Snapshots.AsNoTracking()
                .Where(x => x.AggregateId == aggregateId)
                .OrderByDescending(x => x.Version)
                .FirstOrDefaultAsync(cancellationToken);
        }

        var headVersion = await db.StreamHeads.AsNoTracking()
            .Where(x => x.AggregateId == aggregateId)
            .Select(x => (long?)x.CurrentVersion)
            .SingleOrDefaultAsync(cancellationToken) ?? 0L;
        var latestEventVersion = await db.Events.AsNoTracking()
            .Where(x => x.AggregateId == aggregateId)
            .Select(x => (long?)x.Version)
            .MaxAsync(cancellationToken) ?? 0L;
        if (headVersion != latestEventVersion)
            throw new InvalidDataException($"Stream head version {headVersion} does not match last event version {latestEventVersion} for aggregate {aggregateId}.");

        var snapshotVersion = snapshot?.Version ?? 0L;
        if (snapshotVersion > headVersion)
            throw new InvalidDataException($"Snapshot version {snapshotVersion} is ahead of stream version {headVersion} for aggregate {aggregateId}.");

        var rows = await db.Events.AsNoTracking()
            .Where(x => x.AggregateId == aggregateId && x.Version > snapshotVersion)
            .OrderBy(x => x.Version)
            .ToListAsync(cancellationToken);

        var events = rows.Select(x => new StoredDomainEvent(x.EventId, x.AggregateId, x.Version,
            x.EventType, x.SchemaVersion, ParseJsonClone(x.Payload),
            x.OccurredAtUtc, x.CorrelationId, x.CausationId)).ToArray();

        return new EventStream(aggregateId, headVersion, snapshotVersion,
            snapshot is null ? null : ParseJsonClone(snapshot.State), events);
    }

    public async Task SaveSnapshotAsync(Guid aggregateId, long expectedStreamVersion, string stateJson,
        CancellationToken cancellationToken = default)
    {
        if (aggregateId == Guid.Empty) throw new ArgumentException("Aggregate id is required.", nameof(aggregateId));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(expectedStreamVersion);
        using var parsed = JsonDocument.Parse(stateJson);
        await using var db = await contextFactory.CreateDbContextAsync(cancellationToken);
        await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);

        var actualVersion = await db.StreamHeads.AsNoTracking()
            .Where(x => x.AggregateId == aggregateId)
            .Select(x => (long?)x.CurrentVersion)
            .SingleOrDefaultAsync(cancellationToken) ?? 0L;
        if (actualVersion != expectedStreamVersion)
            throw new WrongExpectedVersionException(aggregateId, expectedStreamVersion, actualVersion);

        var existing = await db.Snapshots
            .Where(x => x.AggregateId == aggregateId)
            .OrderByDescending(x => x.Version)
            .FirstOrDefaultAsync(cancellationToken);
        if (existing is not null && existing.Version >= expectedStreamVersion)
        {
            await transaction.CommitAsync(cancellationToken);
            return;
        }

        db.Snapshots.Add(new AggregateSnapshotRow
        {
            AggregateId = aggregateId,
            Version = expectedStreamVersion,
            State = parsed.RootElement.GetRawText(),
            CreatedAtUtc = DateTimeOffset.UtcNow
        });
        await db.SaveChangesAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }


    private async Task<IReadOnlyList<StoredDomainEvent>?> TryReadExistingBatchAsync(Guid aggregateId,
        long expectedVersion, IReadOnlyList<NewDomainEvent> events)
    {
        await using var db = await contextFactory.CreateDbContextAsync(CancellationToken.None);
        var eventIds = events.Select(x => x.EventId).ToArray();
        var persistedEvents = await db.Events.AsNoTracking()
            .Where(x => x.AggregateId == aggregateId && eventIds.Contains(x.EventId))
            .OrderBy(x => x.Version)
            .ToListAsync(CancellationToken.None);
        if (persistedEvents.Count == 0) return null;
        if (persistedEvents.Count != events.Count)
            throw new EventAppendConflictException(aggregateId, "only part of the concurrent batch exists.");

        var result = new List<StoredDomainEvent>(events.Count);
        for (var i = 0; i < events.Count; i++)
        {
            var requested = events[i];
            var persisted = persistedEvents[i];
            if (persisted.EventId != requested.EventId || persisted.Version != expectedVersion + i + 1 ||
                persisted.EventType != requested.EventType || persisted.SchemaVersion != requested.SchemaVersion ||
                persisted.Payload != requested.Payload.GetRawText() || persisted.CorrelationId != requested.CorrelationId ||
                persisted.CausationId != requested.CausationId || persisted.OccurredAtUtc != requested.OccurredAtUtc)
                throw new EventAppendConflictException(aggregateId, "a concurrent event id has different content or stream position.");
            result.Add(new StoredDomainEvent(persisted.EventId, aggregateId, persisted.Version,
                persisted.EventType, persisted.SchemaVersion, ParseJsonClone(persisted.Payload),
                persisted.OccurredAtUtc, persisted.CorrelationId, persisted.CausationId));
        }
        return result;
    }

    private async Task<long> ReadCurrentVersionAsync(Guid aggregateId, CancellationToken cancellationToken)
    {
        await using var db = await contextFactory.CreateDbContextAsync(cancellationToken);
        return await db.StreamHeads.AsNoTracking()
            .Where(x => x.AggregateId == aggregateId)
            .Select(x => (long?)x.CurrentVersion)
            .SingleOrDefaultAsync(cancellationToken) ?? 0L;
    }

    private static JsonElement ParseJsonClone(string json)
    {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.Clone();
    }

    private static bool IsUniqueOrDeadlock(DbUpdateException exception) =>
        exception.GetBaseException() is SqlException sqlException &&
        sqlException.Errors.Cast<SqlError>().Any(x => x.Number is 1205 or 2601 or 2627);
}
