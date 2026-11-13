using System.Text.Json;

namespace Workshop.EventStore;

public sealed record EventStream(
    Guid AggregateId,
    long CurrentVersion,
    long SnapshotVersion,
    JsonElement? Snapshot,
    IReadOnlyList<StoredDomainEvent> Events);
