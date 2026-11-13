using Microsoft.EntityFrameworkCore;

namespace Workshop.PaymentWorker;

public sealed class PaymentWebhookDbContext(DbContextOptions<PaymentWebhookDbContext> options) : DbContext(options)
{
    public DbSet<PspWebhookInboxRow> WebhookInbox => Set<PspWebhookInboxRow>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        var entity = modelBuilder.Entity<PspWebhookInboxRow>();
        entity.ToTable("psp_webhook_inbox", "dbo");
        entity.HasKey(x => x.Id);
        entity.Property(x => x.Id).HasColumnName("id").UseIdentityColumn();
        entity.Property(x => x.Provider).HasColumnName("provider").HasMaxLength(100).IsRequired();
        entity.Property(x => x.ProviderEventId).HasColumnName("provider_event_id").HasMaxLength(200).IsRequired();
        entity.Property(x => x.PayloadHash).HasColumnName("payload_hash").HasColumnType("char(64)").IsRequired();
        entity.Property(x => x.Payload).HasColumnName("payload").HasColumnType("nvarchar(max)").IsRequired();
        entity.Property(x => x.EventType).HasColumnName("event_type").HasColumnType("varchar(40)").IsRequired();
        entity.Property(x => x.IdempotencyKey).HasColumnName("idempotency_key").HasMaxLength(600).IsRequired();
        entity.Property(x => x.ProviderReference).HasColumnName("provider_reference").HasMaxLength(500);
        entity.Property(x => x.OccurredAtUtc).HasColumnName("occurred_at_utc");
        entity.Property(x => x.ReceivedAtUtc).HasColumnName("received_at_utc").HasDefaultValueSql("SYSUTCDATETIME()").ValueGeneratedOnAdd();
        entity.Property(x => x.ProcessedAtUtc).HasColumnName("processed_at_utc");
        entity.Property(x => x.Status).HasColumnName("status").HasColumnType("varchar(20)").HasDefaultValue("Pending");
        entity.Property(x => x.Attempts).HasColumnName("attempts").HasDefaultValue(0);
        entity.Property(x => x.NextAttemptAtUtc).HasColumnName("next_attempt_at_utc").HasDefaultValueSql("SYSUTCDATETIME()").ValueGeneratedOnAdd();
        entity.Property(x => x.LockOwner).HasColumnName("lock_owner").HasMaxLength(200);
        entity.Property(x => x.LockedUntilUtc).HasColumnName("locked_until_utc");
        entity.Property(x => x.LastError).HasColumnName("last_error").HasMaxLength(1000);
        entity.HasIndex(x => new { x.Provider, x.ProviderEventId }).IsUnique()
            .HasDatabaseName("UQ_psp_webhook_provider_event");
        entity.HasIndex(x => new { x.Status, x.NextAttemptAtUtc, x.ReceivedAtUtc })
            .HasDatabaseName("IX_psp_webhook_pending")
            .HasFilter("[status] <> 'Processed' AND [status] <> 'Conflict' AND [status] <> 'Dead'");
    }
}
