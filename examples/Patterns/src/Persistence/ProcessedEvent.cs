namespace Workshop.Persistence;

public sealed class ProcessedEvent
{
    public string ConsumerName { get; set; } = "";
    public Guid EventId { get; set; }
    public string PayloadHash { get; set; } = "";
    public DateTimeOffset ProcessedAt { get; set; }
}
