namespace Workshop.EventStore;

public sealed class AggregateStreamHead
{
    public Guid AggregateId { get; set; }
    public long CurrentVersion { get; set; }
    public byte[] RowVersion { get; set; } = [];
}
