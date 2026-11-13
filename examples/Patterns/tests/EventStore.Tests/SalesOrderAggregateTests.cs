using System.Text.Json;
using Workshop.EventStore;
using Xunit;

public sealed class SalesOrderAggregateTests
{
    [Fact]
    public void RehydratesInStrictStreamVersionOrder()
    {
        var aggregateId = Guid.NewGuid();
        var aggregate = new SalesOrderAggregate();
        aggregate.Apply(Stored(aggregateId, 1, "SalesOrderCreated", new SalesOrderCreated(Guid.NewGuid(), "IRR")));
        aggregate.Apply(Stored(aggregateId, 2, "SalesOrderLineAdded", new SalesOrderLineAdded("SKU-1", 2, 100m)));

        Assert.Equal(2, aggregate.Version);
        Assert.Equal(200m, aggregate.Total);
        Assert.Equal("IRR", aggregate.Currency);
    }

    [Fact]
    public void PendingEventsBecomeAggregateStateOnlyAfterSuccessfulAppend()
    {
        var aggregateId = Guid.NewGuid();
        var aggregate = new SalesOrderAggregate();
        var created = SalesOrderAggregate.Create(Guid.NewGuid(), "IRR");
        var createdStored = ToStored(aggregateId, 1, created[0]);
        aggregate.Apply(createdStored);

        aggregate.AddLine("SKU-1", 2, 25m);
        Assert.Empty(aggregate.Lines); // command has not been committed yet
        var pending = aggregate.PendingEvents.ToArray();
        var committed = pending.Select((x, i) => ToStored(aggregateId, aggregate.Version + i + 1, x)).ToArray();
        aggregate.ApplyCommitted(committed);

        Assert.Empty(aggregate.PendingEvents);
        Assert.Equal(2, aggregate.Version);
        Assert.Equal(50m, aggregate.Total);
    }

    [Fact]
    public void DoesNotAllowCommandsAfterAConfirmationEventIsPending()
    {
        var aggregateId = Guid.NewGuid();
        var aggregate = new SalesOrderAggregate();
        aggregate.Apply(Stored(aggregateId, 1, "SalesOrderCreated", new SalesOrderCreated(Guid.NewGuid(), "IRR")));
        aggregate.Apply(Stored(aggregateId, 2, "SalesOrderLineAdded", new SalesOrderLineAdded("SKU-1", 1, 10m)));
        aggregate.Confirm();

        Assert.Throws<InvalidOperationException>(() => aggregate.AddLine("SKU-2", 1, 5m));
        Assert.Throws<InvalidOperationException>(() => aggregate.Confirm());
    }

    [Fact]
    public void SnapshotRoundTripRestoresAggregateStateAndVersion()
    {
        var aggregateId = Guid.NewGuid();
        var aggregate = new SalesOrderAggregate();
        aggregate.Apply(Stored(aggregateId, 1, "SalesOrderCreated", new SalesOrderCreated(Guid.NewGuid(), "IRR")));
        aggregate.Apply(Stored(aggregateId, 2, "SalesOrderLineAdded", new SalesOrderLineAdded("SKU-1", 3, 10m)));

        using var snapshot = JsonDocument.Parse(aggregate.CaptureSnapshot());
        var restored = new SalesOrderAggregate();
        restored.RestoreSnapshot(snapshot.RootElement, aggregate.Version);

        Assert.Equal(aggregate.Id, restored.Id);
        Assert.Equal(aggregate.Version, restored.Version);
        Assert.Equal(30m, restored.Total);
    }

    [Fact]
    public void RejectsVersionGap()
    {
        var aggregateId = Guid.NewGuid();
        var aggregate = new SalesOrderAggregate();
        Assert.Throws<InvalidDataException>(() => aggregate.Apply(
            Stored(aggregateId, 2, "SalesOrderCreated", new SalesOrderCreated(Guid.NewGuid(), "IRR"))));
    }

    [Fact]
    public void RejectsLineAfterConfirmation()
    {
        var aggregateId = Guid.NewGuid();
        var aggregate = new SalesOrderAggregate();
        aggregate.Apply(Stored(aggregateId, 1, "SalesOrderCreated", new SalesOrderCreated(Guid.NewGuid(), "IRR")));
        aggregate.Apply(Stored(aggregateId, 2, "SalesOrderLineAdded", new SalesOrderLineAdded("SKU-1", 1, 10m)));
        aggregate.Apply(Stored(aggregateId, 3, "SalesOrderConfirmed", new SalesOrderConfirmed(DateTimeOffset.UtcNow)));

        Assert.Throws<InvalidOperationException>(() => aggregate.AddLine("SKU-2", 1, 20m));
    }

    private static StoredDomainEvent Stored<T>(Guid aggregateId, long version, string type, T payload) =>
        new(Guid.NewGuid(), aggregateId, version, type, 1, JsonSerializer.SerializeToElement(payload),
            DateTimeOffset.UtcNow, null, null);

    private static StoredDomainEvent ToStored(Guid aggregateId, long version, NewDomainEvent item) =>
        new(item.EventId, aggregateId, version, item.EventType, item.SchemaVersion, item.Payload,
            item.OccurredAtUtc, item.CorrelationId, item.CausationId);
}
