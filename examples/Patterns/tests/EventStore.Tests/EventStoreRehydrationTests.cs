using System.Text.Json;
using Workshop.EventStore;
using Xunit;

public sealed class EventStoreRehydrationTests
{
    [Fact]
    public async Task RehydratesAggregateFromEventHistory()
    {
        var aggregateId = Guid.NewGuid();
        var events = new StoredDomainEvent[]
        {
            Stored(aggregateId, 1, "SalesOrderCreated", new SalesOrderCreated(Guid.NewGuid(), "IRR")),
            Stored(aggregateId, 2, "SalesOrderLineAdded", new SalesOrderLineAdded("SKU-1", 4, 12.5m))
        };
        var store = new StubEventStore(new EventStream(aggregateId, 2, 0, null, events));

        var aggregate = await store.RehydrateAsync(aggregateId, () => new SalesOrderAggregate(), new EventUpcasterRegistry());

        Assert.Equal(2, aggregate.Version);
        Assert.Equal(50m, aggregate.Total);
    }

    [Fact]
    public async Task RehydratesFromSnapshotAndAppliesOnlyTailEvents()
    {
        var aggregateId = Guid.NewGuid();
        var snapshotAggregate = new SalesOrderAggregate();
        snapshotAggregate.Apply(Stored(aggregateId, 1, "SalesOrderCreated", new SalesOrderCreated(Guid.NewGuid(), "IRR")));
        snapshotAggregate.Apply(Stored(aggregateId, 2, "SalesOrderLineAdded", new SalesOrderLineAdded("SKU-1", 2, 10m)));
        using var snapshot = JsonDocument.Parse(snapshotAggregate.CaptureSnapshot());
        var tail = Stored(aggregateId, 3, "SalesOrderLineAdded", new SalesOrderLineAdded("SKU-2", 1, 30m));
        var store = new StubEventStore(new EventStream(aggregateId, 3, 2, snapshot.RootElement.Clone(), [tail]));

        var aggregate = await store.RehydrateAsync(aggregateId, () => new SalesOrderAggregate(), new EventUpcasterRegistry());

        Assert.Equal(3, aggregate.Version);
        Assert.Equal(50m, aggregate.Total);
        Assert.Equal(2, aggregate.Lines.Count);
    }

    [Fact]
    public async Task RejectsGapInStoredEventVersions()
    {
        var aggregateId = Guid.NewGuid();
        var events = new[] { Stored(aggregateId, 2, "SalesOrderCreated", new SalesOrderCreated(Guid.NewGuid(), "IRR")) };
        var store = new StubEventStore(new EventStream(aggregateId, 2, 0, null, events));

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            store.RehydrateAsync(aggregateId, () => new SalesOrderAggregate(), new EventUpcasterRegistry()));
    }

    private static StoredDomainEvent Stored<T>(Guid aggregateId, long version, string type, T payload) =>
        new(Guid.NewGuid(), aggregateId, version, type, 1, JsonSerializer.SerializeToElement(payload),
            DateTimeOffset.UtcNow, null, null);

    private sealed class StubEventStore(EventStream stream) : IEventStore
    {
        public Task<EventStream> LoadAsync(Guid aggregateId, bool useSnapshot = true, CancellationToken cancellationToken = default) => Task.FromResult(stream);
        public Task<IReadOnlyList<StoredDomainEvent>> AppendAsync(Guid aggregateId, long expectedVersion, IReadOnlyList<NewDomainEvent> events, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task SaveSnapshotAsync(Guid aggregateId, long expectedStreamVersion, string stateJson, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }
}
