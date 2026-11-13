namespace Workshop.RetryScheduler;

public sealed partial class Worker
{
    private sealed record RetryRow(Guid Id, string Topic, int Partition, string? Key, byte[] Payload, string HeadersJson, int Attempt);
}
