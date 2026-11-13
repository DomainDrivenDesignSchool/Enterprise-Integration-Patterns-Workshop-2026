using System.Security.Cryptography;
using System.Text;
using System.Globalization;
using Confluent.Kafka;
using Microsoft.Data.SqlClient;
namespace Workshop.RetryScheduler;

public sealed partial class Worker(RetryOptions o, ILogger<Worker> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stop)
    {
        using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig { BootstrapServers = o.BootstrapServers, GroupId = o.GroupId, EnableAutoCommit = false, EnableAutoOffsetStore = false, AutoOffsetReset = AutoOffsetReset.Earliest, IsolationLevel = IsolationLevel.ReadCommitted }).Build();
        using var producer = new ProducerBuilder<string, byte[]>(new ProducerConfig { BootstrapServers = o.BootstrapServers, EnableIdempotence = true, Acks = Acks.All }).Build(); consumer.Subscribe(o.RetryTopics.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)); var dispatcher = DispatchAsync(producer, stop);
        try { while (!stop.IsCancellationRequested) { ConsumeResult<string, byte[]> r; try { r = consumer.Consume(stop); } catch (OperationCanceledException) when (stop.IsCancellationRequested) { break; } try { await PersistAsync(r, stop); consumer.Commit(r); } catch (Exception ex) { log.LogCritical(ex, "Retry schedule persistence failed at {Tpo}; stopping consumer to avoid committing past it", r.TopicPartitionOffset); throw; } } } finally { consumer.Close(); }
        await dispatcher;
    }
    private async Task PersistAsync(ConsumeResult<string, byte[]> r, CancellationToken ct)
    {
        var h = r.Message.Headers ?? new Headers();
        var originalTopic = Header(h, "x-original-topic")
            ?? throw new InvalidOperationException("x-original-topic missing");
        var partition = int.Parse(Header(h, "x-original-partition") ?? r.Partition.Value.ToString(CultureInfo.InvariantCulture)); var due = DateTimeOffset.Parse(Header(h, "x-not-before-utc") ?? DateTimeOffset.UtcNow.ToString("O"), CultureInfo.InvariantCulture); var attempt = int.Parse(Header(h, "x-retry-attempt") ?? "1"); var sourceOffset = long.Parse(Header(h, "x-original-offset") ?? r.Offset.Value.ToString(CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);

        var id = Id($"{r.Topic}:{r.Partition.Value}:{r.Offset.Value}:{attempt}");

        await using var cn = new SqlConnection(o.ConnectionString);
        await cn.OpenAsync(ct);
        await using var cmd = new SqlCommand("IF NOT EXISTS (SELECT 1 FROM dbo.scheduled_retries WITH (UPDLOCK,HOLDLOCK) WHERE id=@id) INSERT INTO dbo.scheduled_retries(id,original_topic,original_partition,source_offset,message_key,payload,headers_json,attempt,due_at_utc,created_at_utc) VALUES(@id,@topic,@partition,@source_offset,@key,@payload,@headers,@attempt,@due,SYSUTCDATETIME())", cn);
        cmd.Parameters.AddWithValue("id", id); cmd.Parameters.AddWithValue("topic", originalTopic);
        cmd.Parameters.AddWithValue("partition", partition);
        cmd.Parameters.AddWithValue("source_offset", sourceOffset);
        cmd.Parameters.AddWithValue("key", (object?)r.Message.Key ?? DBNull.Value);
        cmd.Parameters.AddWithValue("payload", r.Message.Value);
        cmd.Parameters.AddWithValue("headers",
            System.Text.Json.JsonSerializer.Serialize(h.Select(x => new
            {
                x.Key,
                Value = Convert.ToBase64String(x.GetValueBytes() ?? [])

            })));
        cmd.Parameters.AddWithValue("attempt", attempt); 
        cmd.Parameters.AddWithValue("due", due); 
        
        await cmd.ExecuteNonQueryAsync(ct);
    }
    private async Task DispatchAsync(IProducer<string, byte[]> producer, CancellationToken ct)
    {
        var owner = $"retry-{Environment.MachineName}-{Guid.NewGuid():N}";

        while (!ct.IsCancellationRequested) try
            {
                await using var cn = new SqlConnection(o.ConnectionString);
                await cn.OpenAsync(ct);

                var now = DateTimeOffset.UtcNow;
                var rows = new List<RetryRow>();
                await using (var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct))
                {
                    await using var q = new SqlCommand("""
     ;WITH picked AS (
       SELECT TOP (50) current_row.id FROM dbo.scheduled_retries current_row WITH (UPDLOCK,READPAST,ROWLOCK)
       WHERE current_row.published_at_utc IS NULL AND current_row.due_at_utc<=@now
         AND (current_row.locked_until_utc IS NULL OR current_row.locked_until_utc<@now)
         AND NOT EXISTS (
           SELECT 1 FROM dbo.scheduled_retries earlier
           WHERE earlier.original_topic=current_row.original_topic
             AND earlier.original_partition=current_row.original_partition
             AND (earlier.message_key=current_row.message_key OR (earlier.message_key IS NULL AND current_row.message_key IS NULL))
             AND earlier.published_at_utc IS NULL
             AND earlier.source_offset < current_row.source_offset
         )
       ORDER BY current_row.due_at_utc,current_row.source_offset
     )
     UPDATE target SET lock_owner=@owner,locked_until_utc=@lease
     OUTPUT inserted.id,inserted.original_topic,inserted.original_partition,inserted.message_key,inserted.payload,inserted.headers_json,inserted.attempt
     FROM dbo.scheduled_retries target INNER JOIN picked ON target.id=picked.id
     """, cn, tx);
                    q.Parameters.AddWithValue("now", now);
                    q.Parameters.AddWithValue("owner", owner);
                    q.Parameters.AddWithValue("lease", now.AddMinutes(2));

                    await using (var r = await q.ExecuteReaderAsync(ct))
                        while (await r.ReadAsync(ct))
                            rows.Add(new(r.GetGuid(0),
                                r.GetString(1), r.GetInt32(2),
                                r.IsDBNull(3) ? null : r.GetString(3),
                                (byte[])r[4],
                                r.GetString(5),
                                r.GetInt32(6)));
                    await tx.CommitAsync(ct);
                }
                foreach (var x in rows) try
                    {
                        await producer.ProduceAsync(new TopicPartition(x.Topic,
                            new Partition(x.Partition)),
                            new Message<string, byte[]>
                            {
                                Key = x.Key,
                                Value = x.Payload,
                                Headers = RestoreHeaders(x.HeadersJson)
                            }, ct);

                        await using var mark
                            = new SqlCommand("UPDATE dbo.scheduled_retries SET published_at_utc=SYSUTCDATETIME(),locked_until_utc=NULL,lock_owner=NULL WHERE id=@id AND lock_owner=@owner AND published_at_utc IS NULL", cn); mark.Parameters.AddWithValue("id", x.Id); mark.Parameters.AddWithValue("owner", owner); var marked = await mark.ExecuteNonQueryAsync(ct); if (marked == 0) log.LogWarning("Retry {RetryId} was published but lease ownership changed before mark; duplicate replay is possible", x.Id);
                    }
                    catch (Exception ex)
                    {
                        log.LogError(ex, "Retry publish failed {RetryId}", x.Id);

                        await using var unlock = new SqlCommand("UPDATE dbo.scheduled_retries SET locked_until_utc=NULL,lock_owner=NULL WHERE id=@id AND lock_owner=@owner", cn); unlock.Parameters.AddWithValue("id", x.Id); unlock.Parameters.AddWithValue("owner", owner); await unlock.ExecuteNonQueryAsync(ct);
                    }
                if (rows.Count == 0) await Task.Delay(250, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Retry scheduler dispatch loop failed");

                await Task.Delay(1000, ct);
            }
    }

    private static string? Header(Headers? h, string key)
        => h?.Where(x => x.Key == key).LastOrDefault()?.GetValueBytes() is { } b ? Encoding.UTF8.GetString(b) : null;
    private static Guid Id(string s)
        => new(SHA256.HashData(Encoding.UTF8.GetBytes(s))[..16]);
    private static Headers RestoreHeaders(string json)
    {
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        var headers = new Headers();
        foreach (var item in doc.RootElement.EnumerateArray())
            headers.Add(item.GetProperty("Key").GetString()!,
                Convert.FromBase64String(item.GetProperty("Value").GetString()!));
        return headers;
    }
}
