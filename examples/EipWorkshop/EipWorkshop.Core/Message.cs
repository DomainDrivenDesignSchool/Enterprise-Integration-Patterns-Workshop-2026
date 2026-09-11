namespace EipWorkshop.Core;

public sealed class Message<T>
{
    public string Id { get; init; } = Guid.NewGuid().ToString("N");
    public string? CorrelationId { get; set; }
    public string? CausationId { get; init; }
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;
    public Dictionary<string, string> Headers { get; init; } = new();
    public T Payload { get; set; } = default!;
    public int DeliveryAttempt { get; set; }

    public Message<T> WithHeader(string key, string value)
    {
        Headers[key] = value;
        return this;
    }
}

public static class Message
{
    // Message Factory
    public static Message<T> Create<T>(T payload, string? correlationId = null)
        => new() { Payload = payload, CorrelationId = correlationId };
}