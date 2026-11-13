using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Workshop.EventStore;

var connectionString = Environment.GetEnvironmentVariable("EVENTSTORE_SQLSERVER_CONNECTION");
if (string.IsNullOrWhiteSpace(connectionString))
{
    Console.Error.WriteLine("Set EVENTSTORE_SQLSERVER_CONNECTION to a dedicated WorkshopEventStore SQL Server database.");
    return 2;
}

var options = new DbContextOptionsBuilder<EventStoreDbContext>().UseSqlServer(connectionString).Options;
var store = new EfEventStore(new TestContextFactory(options));
var aggregateId = Guid.NewGuid();

try
{
    var first = NewDomainEvent.Create("IntegrationProbeStarted", 1, new { ProbeId = aggregateId });
    var firstAppend = await store.AppendAsync(aggregateId, 0, [first]);
    if (firstAppend.Single().Version != 1) throw new Exception("First stream version was not 1.");
    var duplicateAppend = await store.AppendAsync(aggregateId, 0, [first]);
    if (duplicateAppend.Single().Version != 1) throw new Exception("Idempotent retry did not return the original event.");

    var conflictingRetry = first with { Payload = JsonSerializer.SerializeToElement(new { ProbeId = Guid.NewGuid() }) };
    try
    {
        await store.AppendAsync(aggregateId, 0, [conflictingRetry]);
        throw new Exception("Expected event-id/content conflict was not raised.");
    }
    catch (EventAppendConflictException) { }

    var contenderA = NewDomainEvent.Create("ConcurrentAppendA", 1, new { Value = "A" });
    var contenderB = NewDomainEvent.Create("ConcurrentAppendB", 1, new { Value = "B" });
    var attempts = await Task.WhenAll(
        AttemptAsync(() => store.AppendAsync(aggregateId, 1, [contenderA])),
        AttemptAsync(() => store.AppendAsync(aggregateId, 1, [contenderB])));

    var successes = attempts.Count(x => x.Error is null);
    var conflicts = attempts.Count(x => x.Error is WrongExpectedVersionException);
    if (successes != 1 || conflicts != 1)
        throw new Exception($"Expected exactly one concurrent append success and one version conflict; got success={successes}, conflict={conflicts}, errors={string.Join(" | ", attempts.Where(x => x.Error is not null).Select(x => x.Error!.GetType().Name + ": " + x.Error.Message))}");

    var fullStream = await store.LoadAsync(aggregateId, useSnapshot: false);
    if (fullStream.CurrentVersion != 2 || fullStream.Events.Count != 2)
        throw new Exception($"Unexpected stream after race. Version={fullStream.CurrentVersion}, events={fullStream.Events.Count}.");

    var snapshotJson = JsonSerializer.Serialize(new { AggregateId = aggregateId, AppliedThrough = fullStream.CurrentVersion });
    await store.SaveSnapshotAsync(aggregateId, fullStream.CurrentVersion, snapshotJson);
    var snapshotStream = await store.LoadAsync(aggregateId);
    if (snapshotStream.SnapshotVersion != 2 || snapshotStream.Events.Count != 0)
        throw new Exception("Snapshot load did not return the expected checkpoint and empty tail.");

    Console.WriteLine("PASS: append, idempotent retry, event-id/content conflict, optimistic concurrency, full replay, snapshot persistence and snapshot-tail load.");
    Console.WriteLine($"AggregateId={aggregateId}; StreamVersion={fullStream.CurrentVersion}; SnapshotVersion={snapshotStream.SnapshotVersion}");
    return 0;
}
finally
{
    await using var db = new EventStoreDbContext(options);
    db.Events.RemoveRange(db.Events.Where(x => x.AggregateId == aggregateId));
    db.Snapshots.RemoveRange(db.Snapshots.Where(x => x.AggregateId == aggregateId));
    db.StreamHeads.RemoveRange(db.StreamHeads.Where(x => x.AggregateId == aggregateId));
    await db.SaveChangesAsync();
}

static async Task<(IReadOnlyList<StoredDomainEvent>? Result, Exception? Error)> AttemptAsync(
    Func<Task<IReadOnlyList<StoredDomainEvent>>> operation)
{
    try { return (await operation(), null); }
    catch (Exception exception) { return (null, exception); }
}

sealed class TestContextFactory(DbContextOptions<EventStoreDbContext> options) : IDbContextFactory<EventStoreDbContext>
{
    public EventStoreDbContext CreateDbContext() => new(options);
    public Task<EventStoreDbContext> CreateDbContextAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(new EventStoreDbContext(options));
}
