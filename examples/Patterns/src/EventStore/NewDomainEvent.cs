using System.Text.Json;

namespace Workshop.EventStore;

/// <summary>A new immutable domain event before it is assigned a stream version.</summary>
public sealed record NewDomainEvent(
    string EventType,
    int SchemaVersion,
    JsonElement Payload,
    DateTimeOffset OccurredAtUtc,
    Guid EventId,
    Guid? CorrelationId = null,
    Guid? CausationId = null)
{
    public static NewDomainEvent Create<T>(string eventType, int schemaVersion, T payload,
        Guid? correlationId = null, Guid? causationId = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(eventType);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(schemaVersion);
        return new(eventType, schemaVersion, JsonSerializer.SerializeToElement(payload),
            DateTimeOffset.UtcNow, Guid.NewGuid(), correlationId, causationId);
    }
}
