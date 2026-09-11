namespace EipWorkshop.Reliability;

public sealed class InboxMessage
{
    public Guid Id { get; set; }
    public string ConsumerId { get; set; } = default!;
    public DateTimeOffset ProcessedAt { get; set; } = DateTimeOffset.UtcNow;
}
