using System.Diagnostics;

namespace Workshop.Shared.Events;

public sealed record EventEnvelope<T>(
    Guid EventId,
    string EventType,
    int SchemaVersion,
    DateTimeOffset OccurredAtUtc,
    string CorrelationId,
    string? CausationId,
    string? TraceParent,
    string? TraceState,
    string PartitionKey,
    T Payload)
{
    public static EventEnvelope<T> Create(string eventType,
        int version,
        string partitionKey,
        T payload,
        string correlationId,
        string? causationId = null)
    {
        var activity = Activity.Current;

        return new(Guid.NewGuid(), eventType, version, DateTimeOffset.UtcNow,
            correlationId, causationId, activity?.Id, activity?.TraceStateString,
            partitionKey, payload);
    }
}
