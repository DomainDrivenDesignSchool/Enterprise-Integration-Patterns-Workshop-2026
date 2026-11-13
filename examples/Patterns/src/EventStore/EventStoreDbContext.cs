using Microsoft.EntityFrameworkCore;

namespace Workshop.EventStore;

public sealed class EventStoreDbContext(DbContextOptions<EventStoreDbContext> options) 
    : DbContext(options)
{
    public DbSet<DomainEventRow> Events => Set<DomainEventRow>();
    public DbSet<AggregateStreamHead> StreamHeads => Set<AggregateStreamHead>();
    public DbSet<AggregateSnapshotRow> Snapshots => Set<AggregateSnapshotRow>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        var events = modelBuilder.Entity<DomainEventRow>();
        events.ToTable("domain_events", "dbo");
        events.HasKey(x => x.EventId);
        events.Property(x => x.EventId).HasColumnName("event_id");
        events.Property(x => x.AggregateId).HasColumnName("aggregate_id");
        events.Property(x => x.Version).HasColumnName("stream_version");
        events.Property(x => x.EventType).HasColumnName("event_type").HasMaxLength(200).IsRequired();
        events.Property(x => x.SchemaVersion).HasColumnName("schema_version");
        events.Property(x => x.Payload).HasColumnName("payload_json").HasColumnType("nvarchar(max)").IsRequired();
        events.Property(x => x.OccurredAtUtc).HasColumnName("occurred_at_utc");
        events.Property(x => x.CorrelationId).HasColumnName("correlation_id");
        events.Property(x => x.CausationId).HasColumnName("causation_id");
        events.HasIndex(x => new { x.AggregateId, x.Version }).IsUnique()
            .HasDatabaseName("ux_domain_events_aggregate_version");
        events.HasIndex(x => new { x.AggregateId, x.OccurredAtUtc })
            .HasDatabaseName("ix_domain_events_aggregate_time");
        events.HasOne<AggregateStreamHead>().WithMany().HasForeignKey(x => x.AggregateId)
            .OnDelete(DeleteBehavior.Restrict);

        var heads = modelBuilder.Entity<AggregateStreamHead>();
        heads.ToTable("aggregate_stream_heads", "dbo");
        heads.HasKey(x => x.AggregateId);
        heads.Property(x => x.AggregateId).HasColumnName("aggregate_id");
        heads.Property(x => x.CurrentVersion).HasColumnName("current_version");
        heads.Property(x => x.RowVersion).HasColumnName("row_version").IsRowVersion();

        var snapshots = modelBuilder.Entity<AggregateSnapshotRow>();
        snapshots.ToTable("aggregate_snapshots", "dbo");
        snapshots.HasKey(x => new { x.AggregateId, x.Version });
        snapshots.Property(x => x.AggregateId).HasColumnName("aggregate_id");
        snapshots.Property(x => x.Version).HasColumnName("stream_version");
        snapshots.Property(x => x.State).HasColumnName("state_json").HasColumnType("nvarchar(max)").IsRequired();
        snapshots.Property(x => x.CreatedAtUtc).HasColumnName("created_at_utc");
        snapshots.HasOne<AggregateStreamHead>().WithMany().HasForeignKey(x => x.AggregateId)
            .OnDelete(DeleteBehavior.Restrict);
    }
}
