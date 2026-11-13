namespace Workshop.Messaging.Reliability;

/// <summary>Retry-topic topology. Kafka has no native per-message delay; a retry dispatcher
/// must hold records until x-not-before-utc, or use an external scheduler.</summary>
public static class RetryTopicPolicy
{
    public static readonly TimeSpan[] Delays =
    [
        TimeSpan.FromMinutes(1),
        TimeSpan.FromMinutes(5),
        TimeSpan.FromMinutes(15),
        TimeSpan.FromHours(1)
    ];

    public static DateTimeOffset NextVisibleAt(int attempt, DateTimeOffset now) =>
        now + Delays[Math.Clamp(attempt - 1, 0, Delays.Length - 1)];
}
