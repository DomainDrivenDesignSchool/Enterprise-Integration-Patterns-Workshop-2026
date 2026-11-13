using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Workshop.Contracts;
using Workshop.Messaging;
using Workshop.Persistence;

public sealed class OutboxPublisher(IServiceScopeFactory scopes, IEventPublisher publisher, IConfiguration config, ILogger<OutboxPublisher> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stop)
    {
        var topic = config["Kafka:OrdersTopic"] ?? "commerce.orders"; var owner = $"{Environment.MachineName}-{Guid.NewGuid():N}";
        while (!stop.IsCancellationRequested) try
            {
                using var scope = scopes.CreateScope(); var db = scope.ServiceProvider.GetRequiredService<OrderDb>();
                var now = DateTimeOffset.UtcNow;
                // Atomic claim: READPAST skips rows claimed by another publisher instance.
                List<OutboxMessage> claimed;
                // The row locks are held only while claiming and persisting the lease. Publishing happens
                // after commit, so a slow broker never holds a SQL Server transaction open.
                await using (var claimTx = await db.Database.BeginTransactionAsync(stop))
                {
                    claimed = await db.Outbox.FromSqlInterpolated(@$"""SELECT TOP (30) current_row.* FROM dbo.outbox_messages current_row WITH (UPDLOCK,READPAST,ROWLOCK)
      WHERE current_row.published_at_utc IS NULL AND current_row.next_attempt_at_utc<={now}
        AND (current_row.locked_until_utc IS NULL OR current_row.locked_until_utc<{now})
        AND NOT EXISTS (SELECT 1 FROM dbo.outbox_messages earlier
          WHERE earlier.aggregate_id=current_row.aggregate_id AND earlier.published_at_utc IS NULL
            AND (earlier.occurred_at_utc<current_row.occurred_at_utc OR
              (earlier.occurred_at_utc=current_row.occurred_at_utc AND earlier.id<current_row.id)))
      ORDER BY current_row.occurred_at_utc,current_row.id""").ToListAsync(stop);
                    foreach (var x in claimed) { x.LockOwner = owner; x.LockedUntil = now.AddMinutes(2); }
                    await db.SaveChangesAsync(stop);
                    await claimTx.CommitAsync(stop);
                }
                if (claimed.Count == 0) { await Task.Delay(400, stop); continue; }
                foreach (var x in claimed)
                {
                    try
                    {
                        var evt = JsonSerializer.Deserialize<IntegrationEvent<OrderCreated>>(x.Payload, new JsonSerializerOptions(JsonSerializerDefaults.Web)) ?? throw new JsonException("Invalid outbox event");
                        var key = evt.Payload.OrderId.ToString("N");
                        await publisher.PublishAsync(topic, key, evt, stop);
                        var publishedAt = DateTimeOffset.UtcNow;
                        var marked = await db.Database.ExecuteSqlInterpolatedAsync($"UPDATE dbo.outbox_messages SET published_at_utc={publishedAt},locked_until_utc=NULL,lock_owner=NULL WHERE id={x.Id} AND lock_owner={owner} AND published_at_utc IS NULL", stop);
                        if (marked == 0) log.LogWarning("Outbox {Id} was published but this worker no longer owns its lease; duplicate delivery is possible", x.Id);
                    }
                    catch (Exception ex)
                    {
                        var nextAttempt = DateTimeOffset.UtcNow.AddMilliseconds(Random.Shared.Next(250, (int)Math.Min(300000, 1000 * Math.Pow(2, Math.Min(x.Attempts + 1, 8)))));
                        var released = await db.Database.ExecuteSqlInterpolatedAsync($"UPDATE dbo.outbox_messages SET attempts=attempts+1,next_attempt_at_utc={nextAttempt},locked_until_utc=NULL,lock_owner=NULL WHERE id={x.Id} AND lock_owner={owner} AND published_at_utc IS NULL", stop);
                        if (released == 0) log.LogWarning("Outbox {Id} failed after lease ownership changed; recovery will be handled by the current lease owner", x.Id);
                        log.LogError(ex, "Outbox publish failed {Id}", x.Id);
                    }
                }
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested) { break; }
            catch (Exception ex) { log.LogError(ex, "Outbox loop failure"); await Task.Delay(2000, stop); }
    }
}
