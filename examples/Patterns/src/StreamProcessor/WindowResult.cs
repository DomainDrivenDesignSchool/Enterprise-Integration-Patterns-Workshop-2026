namespace Workshop.StreamProcessor;

public sealed record WindowResult(string Key, DateTimeOffset StartUtc, DateTimeOffset EndUtc, int Count, decimal Sum, bool IsLate);
