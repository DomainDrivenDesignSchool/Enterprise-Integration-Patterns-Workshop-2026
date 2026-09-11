using Microsoft.EntityFrameworkCore;

namespace EipWorkshop.Reliability;

public sealed class AppDbContext : DbContext
{
    public DbSet<Order> Orders => Set<Order>();

    public DbSet<OutboxMessage> Outbox => Set<OutboxMessage>();
    public DbSet<InboxMessage> Inbox => Set<InboxMessage>();

    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }
}
