namespace Workshop.PaymentWorker;

public sealed class PspWebhookInboxRow
{
    public long Id { get; set; }
    public string Provider { get; set; } = "";
    public string ProviderEventId { get; set; } = "";
    public string PayloadHash { get; set; } = "";
    public string Payload { get; set; } = "";
    public string EventType { get; set; } = "";
    public string IdempotencyKey { get; set; } = "";
    public string ProviderReference { get; set; } = "";
    public DateTimeOffset OccurredAtUtc { get; set; }
    public DateTimeOffset ReceivedAtUtc { get; set; }
    public DateTimeOffset? ProcessedAtUtc { get; set; }
    public string Status { get; set; } = "Pending";
    public int Attempts { get; set; }
    public DateTimeOffset NextAttemptAtUtc { get; set; }
    public string? LockOwner { get; set; }
    public DateTimeOffset? LockedUntilUtc { get; set; }
    public string? LastError { get; set; }
}
