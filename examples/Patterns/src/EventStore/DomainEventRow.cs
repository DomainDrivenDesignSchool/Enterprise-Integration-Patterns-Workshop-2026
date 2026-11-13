namespace Workshop.EventStore;

public sealed class DomainEventRow
{
    public Guid EventId { get; set; }
    public Guid AggregateId { get; set; }
    public long Version { get; set; }
    public string EventType { get; set; } = "";
    public int SchemaVersion { get; set; }
    public string Payload { get; set; } = "";
    public DateTimeOffset OccurredAtUtc { get; set; }
    public Guid? CorrelationId { get; set; }
    public Guid? CausationId { get; set; }
}
