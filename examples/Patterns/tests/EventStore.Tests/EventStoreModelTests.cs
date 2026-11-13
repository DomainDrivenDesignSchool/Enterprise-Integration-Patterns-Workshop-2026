using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Workshop.EventStore;
using Xunit;

public sealed class EventStoreModelTests
{
    [Fact]
    public void MapsAppendOnlyEventsAndUniqueAggregateVersion()
    {
        var options = new DbContextOptionsBuilder<EventStoreDbContext>()
            .UseSqlServer("Server=(local);Database=EventStoreModelOnly;Trusted_Connection=True;TrustServerCertificate=True")
            .Options;
        using var db = new EventStoreDbContext(options);
        var entity = db.Model.FindEntityType(typeof(DomainEventRow))!;
        var table = StoreObjectIdentifier.Table("domain_events", "dbo");
        var index = entity.GetIndexes().Single(x => x.IsUnique &&
            x.Properties.Select(p => p.Name).SequenceEqual([nameof(DomainEventRow.AggregateId), nameof(DomainEventRow.Version)]));

        Assert.Equal("domain_events", entity.GetTableName());
        Assert.Equal("dbo", entity.GetSchema());
        Assert.Equal("ux_domain_events_aggregate_version", index.GetDatabaseName());
        Assert.Equal("stream_version", entity.FindProperty(nameof(DomainEventRow.Version))!.GetColumnName(table));

        var head = db.Model.FindEntityType(typeof(AggregateStreamHead))!;
        var rowVersion = head.FindProperty(nameof(AggregateStreamHead.RowVersion))!;
        Assert.True(rowVersion.IsConcurrencyToken);
        Assert.Equal(Microsoft.EntityFrameworkCore.Metadata.ValueGenerated.OnAddOrUpdate, rowVersion.ValueGenerated);
    }
}
