using System.Text.Json;

namespace Workshop.EventStore;

public interface IEventSourcedAggregate
{
    Guid Id { get; }
    long Version { get; }
    void RestoreSnapshot(JsonElement state, long version);
    void Apply(StoredDomainEvent domainEvent);
}

public static class EventStoreRehydration
{
    public static async Task<TAggregate> RehydrateAsync<TAggregate>(this IEventStore store,
        Guid aggregateId,
        Func<TAggregate> factory,
        EventUpcasterRegistry upcasters,
        bool useSnapshot = true,
        CancellationToken cancellationToken = default)
        where TAggregate : IEventSourcedAggregate
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(upcasters);

        var stream = await store.LoadAsync(aggregateId, useSnapshot, cancellationToken);
        if (stream.AggregateId != aggregateId)
            throw new InvalidDataException("Event store returned a different aggregate stream.");

        if (stream.Snapshot is null && stream.SnapshotVersion != 0)
            throw new InvalidDataException("Snapshot version is set but snapshot state is missing.");

        var aggregate = factory();

        if (aggregate.Id != Guid.Empty && aggregate.Id != aggregateId)
            throw new InvalidOperationException("Aggregate factory returned an instance with a different id.");

        if (stream.Snapshot is { } snapshot)
        {
            if (stream.SnapshotVersion <= 0)
                throw new InvalidDataException("Snapshot state has an invalid stream version.");

            aggregate.RestoreSnapshot(snapshot, stream.SnapshotVersion);

            if (aggregate.Version != stream.SnapshotVersion)
                throw new InvalidDataException("Aggregate restored a snapshot with an unexpected version.");
        }

        foreach (var stored in stream.Events)
        {
            if (stored.Version != aggregate.Version + 1)
                throw new InvalidDataException($"Event stream gap for {aggregateId}: expected version {aggregate.Version + 1}, got {stored.Version}.");

            var (version, payload) = upcasters.Upcast(stored.EventType, stored.SchemaVersion, stored.Payload);

            aggregate.Apply(stored with { SchemaVersion = version, Payload = payload });
        }

        if (aggregate.Id != aggregateId)
            throw new InvalidDataException($"Rehydrated aggregate id {aggregate.Id} does not match requested stream {aggregateId}.");

        if (aggregate.Version != stream.CurrentVersion)
            throw new InvalidDataException($"Rehydrated version {aggregate.Version} does not match stream version {stream.CurrentVersion}.");

        return aggregate;
    }
}
