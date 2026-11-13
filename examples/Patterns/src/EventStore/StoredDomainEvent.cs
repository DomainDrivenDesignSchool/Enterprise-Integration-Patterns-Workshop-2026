using System.Text.Json;

namespace Workshop.EventStore;

/// <summary>Persisted event. Version is monotonically increasing within one aggregate stream.</summary>
public sealed record StoredDomainEvent(
    Guid EventId,
    Guid AggregateId,
    long Version,
    string EventType,
    int SchemaVersion,
    JsonElement Payload,
    DateTimeOffset OccurredAtUtc,
    Guid? CorrelationId,
    Guid? CausationId);
