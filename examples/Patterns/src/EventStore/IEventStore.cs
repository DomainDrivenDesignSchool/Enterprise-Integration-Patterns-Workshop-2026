namespace Workshop.EventStore;

public interface IEventStore
{
    Task<IReadOnlyList<StoredDomainEvent>> AppendAsync(Guid aggregateId, long expectedVersion,
        IReadOnlyList<NewDomainEvent> events, CancellationToken cancellationToken = default);

    Task<EventStream> LoadAsync(Guid aggregateId, bool useSnapshot = true,
        CancellationToken cancellationToken = default);

    Task SaveSnapshotAsync(Guid aggregateId, long expectedStreamVersion, string stateJson,
        CancellationToken cancellationToken = default);
}

public sealed class WrongExpectedVersionException(Guid aggregateId, long expected, long actual)
    : Exception($"Wrong expected version for aggregate {aggregateId}. Expected {expected}, actual {actual}.")
{
    public Guid AggregateId { get; } = aggregateId;
    public long ExpectedVersion { get; } = expected;
    public long ActualVersion { get; } = actual;
}

public sealed class EventAppendConflictException(Guid aggregateId, string message)
    : Exception($"Event append conflict for aggregate {aggregateId}: {message}")
{
    public Guid AggregateId { get; } = aggregateId;
}
