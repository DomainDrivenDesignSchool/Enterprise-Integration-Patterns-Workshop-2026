using System.Text.Json;
using System.Security.Cryptography;
using System.Globalization;
using Confluent.Kafka;
using Microsoft.Data.SqlClient;
using Workshop.Messaging.Reliability;
namespace Workshop.AnalyticsWorker;

public sealed class Worker(AnalyticsOptions options, ILogger<Worker> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stop)
    {
        using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig { BootstrapServers = options.BootstrapServers, GroupId = options.GroupId, EnableAutoCommit = false, EnableAutoOffsetStore = false, AutoOffsetReset = AutoOffsetReset.Earliest, IsolationLevel = IsolationLevel.ReadCommitted }).Build();
        using var failureRouter = new KafkaTransactionalFailureRouter(options.BootstrapServers, $"analytics-failure-{Environment.MachineName}-{Guid.NewGuid():N}", "commerce.retry", "commerce.dlq", 5);
        consumer.Subscribe(options.InputTopic);
        try
        {
            while (!stop.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]> r;

                try
                {
                    r = consumer.Consume(stop);
                }
                catch (OperationCanceledException)
                    when (stop.IsCancellationRequested)
                {
                    break;
                }

                try
                {
                    await ApplyAsync(r, stop); consumer.Commit(r);
                }
                catch (Exception ex)
                {
                    log.LogError(ex, "Analytics projection failed at {Tpo}", r.TopicPartitionOffset); try { await failureRouter.RouteFailureAndCommitAsync(consumer, r, ex, stop); } catch (Exception route) { log.LogCritical(route, "Analytics failure routing failed; stopping consumer to avoid committing past it"); throw; }
                }
            }
        }
        finally { consumer.Close(); }
    }

    private async Task ApplyAsync(ConsumeResult<string, byte[]> r, CancellationToken ct)
    {
        using var d = JsonDocument.Parse(r.Message.Value);
        var root = d.RootElement;
        var eventId = Guid.Parse(Get(root, "eventId"));

        var p = root.GetProperty("payload");
        var customer = Get(p, "customerKey");

        var currency = Get(p, "currency");

        var start = DateTimeOffset.Parse(Get(p, "windowStartUtc"));

        var end = DateTimeOffset.Parse(Get(p, "windowEndUtc"));

        var count = long.Parse(Get(p, "changeCount"));
        var total = decimal.Parse(Get(p, "netPaidAmount"), CultureInfo.InvariantCulture);

        await using var cn = new SqlConnection(options.ConnectionString);
        await cn.OpenAsync(ct);
        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);
        var hash = Convert.ToHexString(SHA256.HashData(r.Message.Value));

        await using (var inbox = new SqlCommand("IF NOT EXISTS (SELECT 1 FROM dbo.analytics_inbox WITH (UPDLOCK,HOLDLOCK) WHERE event_id=@id) INSERT INTO dbo.analytics_inbox(event_id,payload_hash,processed_at_utc) VALUES(@id,@hash,SYSUTCDATETIME())", cn, tx))
        {
            inbox.Parameters.AddWithValue("id", eventId);
            inbox.Parameters.AddWithValue("hash", hash);
            if (await inbox.ExecuteNonQueryAsync(ct) == 0)
            {
                await using var old = new SqlCommand("SELECT payload_hash FROM dbo.analytics_inbox WHERE event_id=@id", cn, tx);

                old.Parameters.AddWithValue("id", eventId);
                var previous = (string?)await old.ExecuteScalarAsync(ct);

                if (!StringComparer.Ordinal.Equals(previous, hash))
                    throw new InvalidOperationException("Analytics EventId collision with different payload.");

                await tx.CommitAsync(ct);
                return;
            }
        }
        await using (var upsert = new SqlCommand("""
   MERGE dbo.customer_order_windows WITH (HOLDLOCK) AS target
   USING (SELECT @customer AS customer_key,@currency AS currency,@start AS window_start_utc) AS source
   ON target.customer_key=source.customer_key AND target.currency=source.currency AND target.window_start_utc=source.window_start_utc
   WHEN MATCHED THEN UPDATE SET window_end_utc=@end,change_count=@count,net_paid_amount=@total,last_event_id=@event,updated_at_utc=SYSUTCDATETIME()
   WHEN NOT MATCHED THEN INSERT(customer_key,currency,window_start_utc,window_end_utc,change_count,net_paid_amount,last_event_id,updated_at_utc)
     VALUES(@customer,@currency,@start,@end,@count,@total,@event,SYSUTCDATETIME());
   """, cn, tx))
        {
            upsert.Parameters.AddWithValue("customer", customer);

            upsert.Parameters.AddWithValue("currency", currency);
            upsert.Parameters.AddWithValue("start", start);
            upsert.Parameters.AddWithValue("end", end);
            upsert.Parameters.AddWithValue("count", count);
            upsert.Parameters.AddWithValue("total", total);
            upsert.Parameters.AddWithValue("event", eventId);
            await upsert.ExecuteNonQueryAsync(ct);
        }

        await tx.CommitAsync(ct);
    }
    private static string Get(JsonElement e, string n)
        => e.TryGetProperty(n, out var v)
        ? v.ToString()
        : throw new JsonException($"Missing {n}");
}
