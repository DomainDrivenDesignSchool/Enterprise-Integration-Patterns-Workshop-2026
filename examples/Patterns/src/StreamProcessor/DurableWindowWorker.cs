using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Data.SqlClient;
using Workshop.Messaging.Reliability;

namespace Workshop.StreamProcessor;

public sealed class DurableWindowWorker(StreamOptions options, ILogger<DurableWindowWorker> log) : BackgroundService
{
    private static readonly TimeSpan Window = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan AllowedLateness = TimeSpan.FromMinutes(2);
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    protected override async Task ExecuteAsync(CancellationToken stop)
    {
        using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.GroupId,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            IsolationLevel = IsolationLevel.ReadCommitted
        }).Build();

        using var failureRouter = new KafkaTransactionalFailureRouter(options.BootstrapServers,
            $"stream-failure-{Environment.MachineName}-{Guid.NewGuid():N}", "commerce.retry", "commerce.dlq", maxAttempts: 5);

        using var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers =
            options.BootstrapServers,
            EnableIdempotence = true,
            Acks = Acks.All
        }).Build();

        consumer.Subscribe(options.InputTopic);

        var outbox = DispatchOutboxAsync(producer, stop);
        try
        {
            while (!stop.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]> record;
                try
                {
                    record = consumer.Consume(stop);
                }
                catch (OperationCanceledException)
                when (stop.IsCancellationRequested)
                {
                    break;
                }

                try
                {
                    await ApplyAsync(record, stop);

                    consumer.Commit(record);
                }
                catch (JsonException ex)
                {
                    log.LogError(ex, "Poison stream event at {Tpo}; routing to retry/DLQ", record.TopicPartitionOffset);
                    await failureRouter.RouteFailureAndCommitAsync(consumer, record, ex, stop);
                }
                catch (Exception ex)
                {
                    log.LogCritical(ex, "Stream state/infrastructure failure at {Tpo}; stopping to preserve event-time ordering", record.TopicPartitionOffset);
                    throw;
                }
            }
        }
        finally
        {
            consumer.Close();
        }
        await outbox;
    }

    private async Task ApplyAsync(ConsumeResult<string, byte[]> record, CancellationToken ct)
    {
        using var doc = JsonDocument.Parse(record.Message.Value);

        var root = doc.RootElement;
        var eventId = Guid.Parse(Get(root, "eventId"));
        var customer = Get(root, "customerId");
        var currency = Get(root, "currency");

        var amount = decimal.Parse(Get(root, "paidAmountDelta"), CultureInfo.InvariantCulture);

        var eventTime = DateTimeOffset.TryParse(Get(root, "occurredAtUtc"),
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal, out var parsed)
            ? parsed : record.Message.Timestamp.UtcDateTime == DateTime.MinValue
                ? DateTimeOffset.UtcNow : new DateTimeOffset(record.Message.Timestamp.UtcDateTime, TimeSpan.Zero);

        var windowStart = new DateTimeOffset(eventTime.UtcDateTime.Ticks / Window.Ticks * Window.Ticks, TimeSpan.Zero);
        var windowEnd = windowStart + Window;

        var payloadHash = Convert.ToHexString(SHA256.HashData(record.Message.Value));

        await using var cn = new SqlConnection(options.ConnectionString);
        await cn.OpenAsync(ct);

        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);

        await using (var inbox = new SqlCommand("""
            IF NOT EXISTS (SELECT 1 FROM dbo.stream_inbox WITH (UPDLOCK,HOLDLOCK)
              WHERE event_id=@id OR (topic=@topic AND partition_id=@partition AND offset_value=@offset))
              INSERT INTO dbo.stream_inbox(event_id,topic,partition_id,offset_value,payload_hash,processed_at_utc)
              VALUES(@id,@topic,@partition,@offset,@hash,SYSUTCDATETIME());
            """, cn, tx))
        {
            inbox.Parameters.AddWithValue("id", eventId);
            inbox.Parameters.AddWithValue("topic", record.Topic);
            inbox.Parameters.AddWithValue("partition", record.Partition.Value);
            inbox.Parameters.AddWithValue("offset", record.Offset.Value);

            inbox.Parameters.AddWithValue("hash", payloadHash);

            if (await inbox.ExecuteNonQueryAsync(ct) == 0)
            {
                await using var existing = new SqlCommand("""
                    SELECT COUNT_BIG(*) FROM dbo.stream_inbox
                    WHERE (event_id=@id OR (topic=@topic AND partition_id=@partition AND offset_value=@offset))
                      AND payload_hash<>@hash
                    """, cn, tx);

                existing.Parameters.AddWithValue("id", eventId);
                existing.Parameters.AddWithValue("topic", record.Topic);
                existing.Parameters.AddWithValue("partition", record.Partition.Value);
                existing.Parameters.AddWithValue("offset", record.Offset.Value);
                existing.Parameters.AddWithValue("hash", payloadHash);

                var conflicts = Convert.ToInt64(await existing.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
                if (conflicts != 0)
                    throw new InvalidOperationException("Stream inbox identity collision: same EventId/offset with different payload.");
                await tx.CommitAsync(ct); return;
            }
        }

        DateTimeOffset maxEvent;
        await using (var state = new SqlCommand("""
            IF EXISTS (SELECT 1 FROM dbo.stream_key_state WITH (UPDLOCK,HOLDLOCK)
              WHERE topic=@topic AND customer_key=@customer AND currency=@currency)
              UPDATE dbo.stream_key_state SET max_event_time_utc=
                CASE WHEN max_event_time_utc<@time THEN @time ELSE max_event_time_utc END
              WHERE topic=@topic AND customer_key=@customer AND currency=@currency;
            ELSE
              INSERT INTO dbo.stream_key_state(topic,customer_key,currency,max_event_time_utc)
              VALUES(@topic,@customer,@currency,@time);
            SELECT max_event_time_utc FROM dbo.stream_key_state
              WHERE topic=@topic AND customer_key=@customer AND currency=@currency;
            """, cn, tx))
        {
            state.Parameters.AddWithValue("topic", record.Topic);
            state.Parameters.AddWithValue("customer", customer);
            state.Parameters.AddWithValue("currency", currency);
            state.Parameters.AddWithValue("time", eventTime);

            var scalar = await state.ExecuteScalarAsync(ct);
            maxEvent = scalar is DateTimeOffset dto
                ? dto
                : new DateTimeOffset((DateTime)scalar!, TimeSpan.Zero);
        }

        var watermark = maxEvent - AllowedLateness;
        if (eventTime < watermark)
        {
            await using var late = new SqlCommand("""
                IF NOT EXISTS (SELECT 1 FROM dbo.stream_late_events WITH (UPDLOCK,HOLDLOCK) WHERE event_id=@id)
                  INSERT INTO dbo.stream_late_events(event_id,customer_key,currency,event_time_utc,amount,reason,recorded_at_utc)
                  VALUES(@id,@key,@currency,@time,@amount,'BeyondAllowedLateness',SYSUTCDATETIME())
                """, cn, tx);
            late.Parameters.AddWithValue("id", eventId);
            late.Parameters.AddWithValue("key", customer);
            late.Parameters.AddWithValue("currency", currency);
            late.Parameters.AddWithValue("time", eventTime);

            late.Parameters.AddWithValue("amount", amount);

            await late.ExecuteNonQueryAsync(ct);
        }
        else if (amount != 0m)
        {
            await using var upsert = new SqlCommand("""
                MERGE dbo.stream_window_state WITH (HOLDLOCK) AS target
                USING (SELECT @key AS customer_key,@currency AS currency,@start AS window_start_utc) AS source
                ON target.customer_key=source.customer_key AND target.currency=source.currency AND target.window_start_utc=source.window_start_utc
                WHEN MATCHED THEN UPDATE SET event_count=target.event_count+1,
                  total_amount=target.total_amount+@amount,
                  last_event_time_utc=CASE WHEN target.last_event_time_utc<@time THEN @time ELSE target.last_event_time_utc END
                WHEN NOT MATCHED THEN INSERT(customer_key,currency,window_start_utc,window_end_utc,event_count,total_amount,last_event_time_utc,emitted)
                  VALUES(@key,@currency,@start,@end,1,@amount,@time,0);
                """, cn, tx);
            upsert.Parameters.AddWithValue("key", customer);
            upsert.Parameters.AddWithValue("currency", currency);
            upsert.Parameters.AddWithValue("start", windowStart);
            upsert.Parameters.AddWithValue("end", windowEnd);
            upsert.Parameters.AddWithValue("amount", amount);
            upsert.Parameters.AddWithValue("time", eventTime);

            await upsert.ExecuteNonQueryAsync(ct);
        }

        var ready = new List<WindowRow>();
        await using (var close = new SqlCommand("""
            ;WITH ready AS (
              SELECT TOP (500) * FROM dbo.stream_window_state WITH (UPDLOCK,READPAST,ROWLOCK)
              WHERE emitted=0 AND customer_key=@customer AND currency=@currency AND window_end_utc<=@watermark
              ORDER BY window_end_utc
            )
            UPDATE ready SET emitted=1
            OUTPUT inserted.customer_key,inserted.currency,inserted.window_start_utc,inserted.window_end_utc,
              inserted.event_count,inserted.total_amount;
            """, cn, tx))
        {
            close.Parameters.AddWithValue("watermark", watermark);
            close.Parameters.AddWithValue("customer", customer);
            close.Parameters.AddWithValue("currency", currency);
            await using var reader = await close.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                ready.Add(new WindowRow(reader.GetString(0), reader.GetString(1).Trim(),
                    reader.GetFieldValue<DateTimeOffset>(2), reader.GetFieldValue<DateTimeOffset>(3),
                    reader.GetInt64(4), reader.GetDecimal(5)));
        }

        foreach (var row in ready)
        {
            var id = DeterministicGuid($"window:{row.CustomerKey}:{row.Currency}:{row.WindowStartUtc:O}");
            var payload = JsonSerializer.Serialize(new
            {
                eventId = id,
                eventType = "CustomerOrderWindowClosed",
                eventVersion = 1,
                occurredAt = DateTimeOffset.UtcNow,
                payload = new
                {
                    customerKey = row.CustomerKey,
                    currency = row.Currency,
                    windowStartUtc = row.WindowStartUtc,
                    windowEndUtc = row.WindowEndUtc,
                    changeCount = row.EventCount,
                    netPaidAmount = row.TotalAmount
                }
            }, Json);
            await using var insert = new SqlCommand("""
                IF NOT EXISTS (SELECT 1 FROM dbo.stream_outbox WITH (UPDLOCK,HOLDLOCK) WHERE id=@id)
                  INSERT INTO dbo.stream_outbox(id,customer_key,currency,payload,created_at_utc)
                  VALUES(@id,@customer,@currency,@payload,SYSUTCDATETIME())
                """, cn, tx);

            insert.Parameters.AddWithValue("id", id);
            insert.Parameters.AddWithValue("customer", row.CustomerKey);
            insert.Parameters.AddWithValue("currency", row.Currency);
            insert.Parameters.AddWithValue("payload", payload);

            await insert.ExecuteNonQueryAsync(ct);
        }
        await tx.CommitAsync(ct);
    }

    private async Task DispatchOutboxAsync(IProducer<string, string> producer, CancellationToken ct)
    {
        var owner = $"stream-{Environment.MachineName}-{Guid.NewGuid():N}";
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await using var cn = new SqlConnection(options.ConnectionString); await cn.OpenAsync(ct);
                await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);
                var rows = new List<(Guid Id, string Key, string Payload)>();
                await using (var q = new SqlCommand("""
                    ;WITH picked AS (
                      SELECT TOP (50) current_row.id FROM dbo.stream_outbox current_row WITH (UPDLOCK,READPAST,ROWLOCK)
                      WHERE current_row.published_at_utc IS NULL
                        AND (current_row.locked_until_utc IS NULL OR current_row.locked_until_utc<@now)
                        AND NOT EXISTS (SELECT 1 FROM dbo.stream_outbox earlier
                          WHERE earlier.customer_key=current_row.customer_key AND earlier.currency=current_row.currency
                            AND earlier.published_at_utc IS NULL
                            AND (earlier.created_at_utc<current_row.created_at_utc OR
                              (earlier.created_at_utc=current_row.created_at_utc AND earlier.id<current_row.id)))
                      ORDER BY current_row.created_at_utc,current_row.id
                    )
                    UPDATE target SET lock_owner=@owner,locked_until_utc=@lease
                    OUTPUT inserted.id,inserted.customer_key+':'+inserted.currency,inserted.payload
                    FROM dbo.stream_outbox target INNER JOIN picked ON target.id=picked.id
                    """, cn, tx))
                {
                    q.Parameters.AddWithValue("now", DateTimeOffset.UtcNow);
                    q.Parameters.AddWithValue("owner", owner);
                    q.Parameters.AddWithValue("lease", DateTimeOffset.UtcNow.AddMinutes(2));
                    await using var r = await q.ExecuteReaderAsync(ct);

                    while (await r.ReadAsync(ct)) rows.Add((r.GetGuid(0), r.GetString(1), r.GetString(2)));
                }
                await tx.CommitAsync(ct);
                foreach (var row in rows)
                {
                    try
                    {
                        await producer.ProduceAsync(options.OutputTopic,
                            new Message<string, string> { Key = row.Key, Value = row.Payload }, ct);
                        await using var mark = new SqlCommand("UPDATE dbo.stream_outbox SET published_at_utc=SYSUTCDATETIME(),locked_until_utc=NULL,lock_owner=NULL WHERE id=@id AND lock_owner=@owner", cn);
                        mark.Parameters.AddWithValue("id", row.Id); mark.Parameters.AddWithValue("owner", owner);
                        await mark.ExecuteNonQueryAsync(ct);
                    }
                    catch
                    {
                        await using var unlock = new SqlCommand("UPDATE dbo.stream_outbox SET locked_until_utc=NULL,lock_owner=NULL WHERE id=@id AND lock_owner=@owner", cn);
                        unlock.Parameters.AddWithValue("id", row.Id);
                        unlock.Parameters.AddWithValue("owner", owner);

                        await unlock.ExecuteNonQueryAsync(ct); throw;
                    }
                }
                if (rows.Count == 0) await Task.Delay(300, ct);
            }
            catch (OperationCanceledException)
            when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Stream outbox dispatch failed");

                await Task.Delay(1000, ct);
            }
        }
    }

    private sealed record WindowRow(string CustomerKey, string Currency, DateTimeOffset WindowStartUtc,
        DateTimeOffset WindowEndUtc, long EventCount, decimal TotalAmount);
    private static string Get(JsonElement e, string name)
        => e.TryGetProperty(name, out var v)
        ? v.ToString() : throw new JsonException($"Missing {name}");
    private static Guid DeterministicGuid(string value)
        => new(SHA256.HashData(Encoding.UTF8.GetBytes(value))[..16]);
}
