namespace Workshop.EventStore;

public sealed class AggregateSnapshotRow
{
    public Guid AggregateId { get; set; }
    public long Version { get; set; }
    public string State { get; set; } = "";
    public DateTimeOffset CreatedAtUtc { get; set; }
}
