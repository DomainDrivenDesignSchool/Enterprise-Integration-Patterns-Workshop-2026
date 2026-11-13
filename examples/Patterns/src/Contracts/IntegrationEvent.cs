namespace Workshop.Contracts;

public sealed record IntegrationEvent<T>(Guid EventId, string EventType, int EventVersion,
    DateTimeOffset OccurredAt, string Producer, Guid CorrelationId, Guid? CausationId, T Payload);
