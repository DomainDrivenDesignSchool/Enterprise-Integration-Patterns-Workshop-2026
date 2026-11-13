using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Data.SqlClient;
using Workshop.Messaging.Reliability;
namespace Workshop.ShippingWorker;

public sealed class Worker(ShippingOptions options, ILogger<Worker> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stop)
    {
        using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig { BootstrapServers = options.BootstrapServers, GroupId = options.GroupId, EnableAutoCommit = false, EnableAutoOffsetStore = false, AutoOffsetReset = AutoOffsetReset.Earliest, IsolationLevel = IsolationLevel.ReadCommitted }).Build();
        using var failureRouter = new KafkaTransactionalFailureRouter(options.BootstrapServers, $"shipping-failure-{Environment.MachineName}-{Guid.NewGuid():N}", "commerce.retry", "commerce.dlq", maxAttempts: 5);
        using var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = options.BootstrapServers, EnableIdempotence = true, Acks = Acks.All }).Build(); consumer.Subscribe(options.CommandTopic);

        var outbox = DispatchOutboxAsync(producer, stop);

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
                    using var d = JsonDocument.Parse(r.Message.Value);

                    if (Get(d.RootElement, "eventType") == "RequestShipment")
                        await CreateShipmentAsync(d.RootElement.GetProperty("payload"), stop);
                    consumer.Commit(r);
                }
                catch (Exception ex) { log.LogError(ex, "Shipping command failed at {Tpo}; routing to retry/DLQ", r.TopicPartitionOffset); try { await failureRouter.RouteFailureAndCommitAsync(consumer, r, ex, stop); } catch (Exception routeError) { log.LogCritical(routeError, "Could not route failed shipping message; stopping consumer to avoid committing past it"); throw; } }
            }
        }
        finally
        {
            consumer.Close();
        }
        await outbox;
    }
    private async Task CreateShipmentAsync(JsonElement p, CancellationToken ct)
    {
        var order = Guid.Parse(Get(p, "orderId"));
        var reservation = Guid.Parse(Get(p, "reservationId"));
        var shipment = Id($"shipment:{order:N}");

        await using var cn = new SqlConnection(options.ConnectionString);
        await cn.OpenAsync(ct);
        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);

        await using (var existing = new SqlCommand("SELECT reservation_id FROM dbo.shipments WITH (UPDLOCK,HOLDLOCK) WHERE order_id=@order", cn, tx))
        {
            existing.Parameters.AddWithValue("order", order);
            await using var reader = await existing.ExecuteReaderAsync(ct);

            if (await reader.ReadAsync(ct))
            {
                var existingReservation = reader.GetGuid(0);
                if (existingReservation != reservation)
                    throw new InvalidOperationException("Idempotency conflict: shipment for this order is already bound to a different inventory reservation.");
                await reader.CloseAsync();

                await tx.CommitAsync(ct); return;
            }
        }
        await using (var ins = new SqlCommand("INSERT INTO dbo.shipments(id,order_id,reservation_id,status,created_at_utc) VALUES(@id,@order,@reservation,'Created',SYSUTCDATETIME())",
            cn, tx))
        {
            ins.Parameters.AddWithValue("id", shipment);
            ins.Parameters.AddWithValue("order", order);
            ins.Parameters.AddWithValue("reservation", reservation);

            await ins.ExecuteNonQueryAsync(ct);
        }
        await using (var outbox = new SqlCommand("IF NOT EXISTS (SELECT 1 FROM dbo.shipping_outbox WITH (UPDLOCK,HOLDLOCK) WHERE id=@id) INSERT INTO dbo.shipping_outbox(id,order_id,event_type,payload,created_at_utc) VALUES(@id,@order,'ShipmentCreated',@payload,SYSUTCDATETIME())", cn, tx)) { var eventId = Id($"ShipmentCreated:{order:N}"); var payload = JsonSerializer.Serialize(new { eventId, eventType = "ShipmentCreated", eventVersion = 1, occurredAt = DateTimeOffset.UtcNow, producer = "shipping-service", correlationId = order, causationId = (Guid?)null, payload = new { orderId = order, shipmentId = shipment } }, new JsonSerializerOptions(JsonSerializerDefaults.Web)); outbox.Parameters.AddWithValue("id", eventId); outbox.Parameters.AddWithValue("order", order); outbox.Parameters.AddWithValue("payload", payload); await outbox.ExecuteNonQueryAsync(ct); }
        await tx.CommitAsync(ct);
    }
    private async Task DispatchOutboxAsync(IProducer<string, string> producer, CancellationToken ct)
    {
        var owner = $"shipping-{Environment.MachineName}-{Guid.NewGuid():N}";
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await using var cn = new SqlConnection(options.ConnectionString);
                await cn.OpenAsync(ct);
                await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);

                var rows = new List<(Guid Id, string Key, string Payload)>();

                await using (var q = new SqlCommand("""
                    ;WITH picked AS (
                      SELECT TOP (50) current_row.id FROM dbo.shipping_outbox current_row WITH (UPDLOCK,READPAST,ROWLOCK)
                      WHERE current_row.published_at_utc IS NULL
                        AND (current_row.locked_until_utc IS NULL OR current_row.locked_until_utc < @now)
                        AND NOT EXISTS (
                          SELECT 1 FROM dbo.shipping_outbox earlier
                          WHERE earlier.order_id = current_row.order_id
                            AND earlier.published_at_utc IS NULL
                            AND (earlier.created_at_utc < current_row.created_at_utc OR
                                (earlier.created_at_utc = current_row.created_at_utc AND earlier.id < current_row.id))
                        )
                      ORDER BY current_row.created_at_utc, current_row.id
                    )
                    UPDATE target SET lock_owner=@owner,locked_until_utc=@lease
                    OUTPUT inserted.id,inserted.order_id,inserted.payload
                    FROM dbo.shipping_outbox target INNER JOIN picked ON target.id=picked.id
                    """, cn, tx))
                {

                    q.Parameters.AddWithValue("now", DateTimeOffset.UtcNow);
                    q.Parameters.AddWithValue("owner", owner);
                    q.Parameters.AddWithValue("lease", DateTimeOffset.UtcNow.AddMinutes(2));

                    await using var r = await q.ExecuteReaderAsync(ct);
                    while (await r.ReadAsync(ct))
                        rows.Add((r.GetGuid(0), r.GetValue(1).ToString()!, r.GetString(2)));
                }
                await tx.CommitAsync(ct);
                foreach (var x in rows)
                {
                    try
                    {
                        await producer.ProduceAsync(options.SagaEventsTopic, new Message<string, string> { Key = x.Key, Value = x.Payload }, ct);
                        await using var mark = new SqlCommand("UPDATE dbo.shipping_outbox SET published_at_utc=SYSUTCDATETIME(),locked_until_utc=NULL,lock_owner=NULL WHERE id=@id AND lock_owner=@owner", cn);
                        mark.Parameters.AddWithValue("id", x.Id);
                        mark.Parameters.AddWithValue("owner", owner);

                        await mark.ExecuteNonQueryAsync(ct);
                    }
                    catch
                    {
                        await using var unlock = new SqlCommand("UPDATE dbo.shipping_outbox SET locked_until_utc=NULL,lock_owner=NULL WHERE id=@id AND lock_owner=@owner", cn);
                        unlock.Parameters.AddWithValue("id", x.Id);
                        unlock.Parameters.AddWithValue("owner", owner);

                        await unlock.ExecuteNonQueryAsync(ct);
                        throw;
                    }
                }
                if (rows.Count == 0) await Task.Delay(300, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Shipping outbox dispatch failed");

                await Task.Delay(1000, ct);
            }
        }
    }

    private static string Get(JsonElement e, string n)
        => e.TryGetProperty(n, out var v) ? v.ToString() : throw new JsonException($"Missing {n}"); private static Guid Id(string s) => new(SHA256.HashData(Encoding.UTF8.GetBytes(s))[..16]);
}
