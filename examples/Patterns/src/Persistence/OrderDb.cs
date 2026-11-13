using Microsoft.EntityFrameworkCore;
namespace Workshop.Persistence;

public sealed class OrderDb(DbContextOptions<OrderDb> o) : DbContext(o)
{
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<OrderItem> OrderItems => Set<OrderItem>();
    public DbSet<OutboxMessage> Outbox => Set<OutboxMessage>();
    public DbSet<OrderCommandInbox> CommandInbox => Set<OrderCommandInbox>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<Order>().ToTable("orders").HasKey(x => x.Id);
        b.Entity<Order>().Property(x => x.Id).HasColumnName("id"); b.Entity<Order>().Property(x => x.CustomerId).HasColumnName("customer_id");
        b.Entity<Order>().Property(x => x.TotalAmount).HasColumnName("total_amount").HasPrecision(18, 2); b.Entity<Order>().Property(x => x.Currency).HasColumnName("currency");
        b.Entity<Order>().Property(x => x.Status).HasColumnName("status"); b.Entity<Order>().Property(x => x.CreatedAt).HasColumnName("created_at_utc");
        b.Entity<Order>().HasMany(x => x.Items).WithOne().HasForeignKey(x => x.OrderId);

        b.Entity<OrderItem>().ToTable("order_items").HasKey(x => x.Id); b.Entity<OrderItem>().Property(x => x.Id).HasColumnName("id");
        b.Entity<OrderItem>().Property(x => x.OrderId).HasColumnName("order_id"); b.Entity<OrderItem>().Property(x => x.Sku).HasColumnName("sku");
        b.Entity<OrderItem>().Property(x => x.Quantity).HasColumnName("quantity"); b.Entity<OrderItem>().Property(x => x.UnitPrice).HasColumnName("unit_price").HasPrecision(18, 2);

        b.Entity<OutboxMessage>().ToTable("outbox_messages").HasKey(x => x.Id); b.Entity<OutboxMessage>().Property(x => x.Id).HasColumnName("id");
        b.Entity<OutboxMessage>().Property(x => x.AggregateId).HasColumnName("aggregate_id");
        b.Entity<OutboxMessage>().Property(x => x.EventType).HasColumnName("event_type"); b.Entity<OutboxMessage>().Property(x => x.Payload).HasColumnName("payload").HasColumnType("nvarchar(max)");
        b.Entity<OutboxMessage>().Property(x => x.OccurredAt).HasColumnName("occurred_at_utc"); b.Entity<OutboxMessage>().Property(x => x.PublishedAt).HasColumnName("published_at_utc");
        b.Entity<OutboxMessage>().Property(x => x.Attempts).HasColumnName("attempts"); b.Entity<OutboxMessage>().Property(x => x.NextAttemptAt).HasColumnName("next_attempt_at_utc");
        b.Entity<OutboxMessage>().Property(x => x.LockedUntil).HasColumnName("locked_until_utc"); b.Entity<OutboxMessage>().Property(x => x.LockOwner).HasColumnName("lock_owner");
        b.Entity<OutboxMessage>().HasIndex(x => x.NextAttemptAt).HasFilter("published_at_utc IS NULL");

        b.Entity<OrderCommandInbox>().ToTable("order_command_inbox").HasKey(x => x.EventId);
        b.Entity<OrderCommandInbox>().Property(x => x.EventId).HasColumnName("event_id");
        b.Entity<OrderCommandInbox>().Property(x => x.PayloadHash).HasColumnName("payload_hash");
        b.Entity<OrderCommandInbox>().Property(x => x.ProcessedAt).HasColumnName("processed_at_utc");
    }
}
