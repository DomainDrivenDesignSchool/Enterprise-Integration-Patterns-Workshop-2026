using Microsoft.EntityFrameworkCore;
namespace Workshop.Persistence;

public sealed class ConsumerDb(DbContextOptions<ConsumerDb> o):DbContext(o) {
 public DbSet<ProcessedEvent> Processed=>Set<ProcessedEvent>();
 protected override void OnModelCreating(ModelBuilder b){ b.Entity<ProcessedEvent>().ToTable("processed_events").HasKey(x=>new{x.ConsumerName,x.EventId}); b.Entity<ProcessedEvent>().Property(x=>x.ConsumerName).HasColumnName("consumer_name"); b.Entity<ProcessedEvent>().Property(x=>x.EventId).HasColumnName("event_id"); b.Entity<ProcessedEvent>().Property(x=>x.PayloadHash).HasColumnName("payload_hash"); b.Entity<ProcessedEvent>().Property(x=>x.ProcessedAt).HasColumnName("processed_at_utc"); }
}
