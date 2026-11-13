namespace Workshop.Persistence;

public sealed class OrderCommandInbox
{
    public Guid EventId { get; set; }
    public string PayloadHash { get; set; } = "";
    public DateTimeOffset ProcessedAt { get; set; }
}
