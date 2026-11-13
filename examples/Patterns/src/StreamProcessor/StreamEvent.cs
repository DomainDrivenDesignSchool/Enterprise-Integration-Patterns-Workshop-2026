namespace Workshop.StreamProcessor;

public sealed record StreamEvent(string Key, decimal Amount, DateTimeOffset EventTimeUtc, DateTimeOffset ProcessingTimeUtc);
