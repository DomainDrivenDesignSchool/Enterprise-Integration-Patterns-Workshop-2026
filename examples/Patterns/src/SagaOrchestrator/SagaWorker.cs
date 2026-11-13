using System.Text;
using System.Text.Json;
using System.Security.Cryptography;
using Confluent.Kafka;
using Microsoft.Data.SqlClient;
using Workshop.Messaging.Reliability;

namespace Workshop.SagaOrchestrator;

public sealed record SagaOptions(string BootstrapServers, string InputTopic, string CommandTopic, string SagaEventsTopic,
    string GroupId, string ConnectionString);

public sealed class SagaWorker(SagaOptions options, ILogger<SagaWorker> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stop)
    {
        var producerConfig = new ProducerConfig { BootstrapServers = options.BootstrapServers,
            EnableIdempotence = true, Acks = Acks.All };
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();
        using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers, GroupId = options.GroupId,
            EnableAutoCommit = false, EnableAutoOffsetStore = false, AutoOffsetReset = AutoOffsetReset.Earliest,
            IsolationLevel = IsolationLevel.ReadCommitted
        }).Build();
        using var failureRouter = new KafkaTransactionalFailureRouter(options.BootstrapServers, $"saga-failure-{Environment.MachineName}-{Guid.NewGuid():N}", "commerce.retry", "commerce.dlq", maxAttempts: 5);
        consumer.Subscribe(options.InputTopic.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries));

        var outboxTask = DispatchOutboxAsync(producer, stop);
        try
        {
            while (!stop.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]> record;
                try { record = consumer.Consume(stop); }
                catch (OperationCanceledException) when (stop.IsCancellationRequested) { break; }
                try
                {
                    await ApplyEventAsync(record.Message.Value, stop);
                    // DB inbox + state + outbox are committed before Kafka offset.
                    // Re-delivery after a crash is harmless due to saga_inbox uniqueness.
                    consumer.Commit(record);
                }
                catch (Exception ex) when (ex is JsonException or InvalidOperationException)
                {
                    log.LogError(ex, "Invalid saga event/transition at {Tpo}; routing to retry/DLQ", record.TopicPartitionOffset);
                    await failureRouter.RouteFailureAndCommitAsync(consumer, record, ex, stop);
                }
                catch (Exception ex)
                {
                    log.LogCritical(ex, "Saga infrastructure failure at {Tpo}; stopping to preserve aggregate event order", record.TopicPartitionOffset);
                    throw;
                }
            }
        }
        finally { consumer.Close(); }
        await outboxTask;
    }

    private async Task ApplyEventAsync(byte[] bytes, CancellationToken ct)
    {
        using var doc = JsonDocument.Parse(bytes);
        var root = doc.RootElement;
        var eventId = Guid.Parse(Required(root, "eventId"));
        var eventType = Required(root, "eventType");
        if (eventType is "OrderCompleted" or "OrderFailed") return; // terminal lifecycle events emitted by this orchestrator
        var payload = root.GetProperty("payload");
        var orderId = Guid.Parse(Required(payload, "orderId"));
        var now = DateTimeOffset.UtcNow;
        await using var cn = new SqlConnection(options.ConnectionString);
        await cn.OpenAsync(ct);
        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);

        var payloadHash = Convert.ToHexString(SHA256.HashData(bytes));
        await using (var inbox = new SqlCommand("IF NOT EXISTS (SELECT 1 FROM dbo.saga_inbox WITH (UPDLOCK,HOLDLOCK) WHERE event_id=@id) INSERT INTO dbo.saga_inbox(event_id,payload_hash,received_at_utc) VALUES(@id,@hash,@now)", cn, tx))
        {
            inbox.Parameters.AddWithValue("id", eventId); inbox.Parameters.AddWithValue("hash", payloadHash); inbox.Parameters.AddWithValue("now", now);
            if (await inbox.ExecuteNonQueryAsync(ct) == 0)
            {
                await using var existing = new SqlCommand("SELECT payload_hash FROM dbo.saga_inbox WHERE event_id=@id", cn, tx);
                existing.Parameters.AddWithValue("id", eventId);
                var previous = (string?)await existing.ExecuteScalarAsync(ct);
                if (!StringComparer.Ordinal.Equals(previous, payloadHash)) throw new InvalidOperationException("Saga EventId collision: same ID with different payload.");
                await tx.CommitAsync(ct); return;
            }
        }

        await using (var create = new SqlCommand("""
            IF NOT EXISTS (SELECT 1 FROM dbo.saga_instances WITH (UPDLOCK,HOLDLOCK) WHERE order_id=@order)
              INSERT INTO dbo.saga_instances(order_id,status,version,created_at_utc,updated_at_utc,order_payload)
              VALUES(@order,'Pending',0,@now,@now,@payload)
            """, cn, tx))
        { create.Parameters.AddWithValue("order", orderId); create.Parameters.AddWithValue("now", now); create.Parameters.AddWithValue("payload", payload.GetRawText()); await create.ExecuteNonQueryAsync(ct); }

        string commandType;
        object commandPayload;
        int aggregateVersion;
        await using (var select = new SqlCommand("SELECT status,payment_reference,reservation_id,version,order_payload FROM dbo.saga_instances WITH (UPDLOCK,HOLDLOCK) WHERE order_id=@order", cn, tx))
        {
            select.Parameters.AddWithValue("order", orderId);
            await using var reader = await select.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct)) throw new InvalidOperationException("Saga row disappeared.");
            var status = reader.GetString(0); var paymentRef = reader.IsDBNull(1) ? null : reader.GetString(1);
            var reservationId = reader.IsDBNull(2) ? null : reader.GetGuid(2).ToString("D"); var version = reader.GetInt32(3); aggregateVersion = version + 1; using var originalOrder = JsonDocument.Parse(reader.GetString(4));
            await reader.CloseAsync();

            (string nextStatus, string? nextPayment, string? nextReservation, string command) = eventType switch
            {
                "OrderCreated" when status == "Pending" => ("AwaitingPayment", paymentRef, reservationId, "RequestPayment"),
                "PaymentAuthorized" when status == "AwaitingPayment" => ("AwaitingInventory", Required(payload,"paymentReference"), reservationId, "ReserveInventory"),
                "PaymentRejected" when status == "AwaitingPayment" => ("Failed", paymentRef, reservationId, "OrderFailed"),
                "InventoryReserved" when status == "AwaitingInventory" => ("AwaitingShipping", paymentRef, Required(payload,"reservationId"), "RequestShipment"),
                "InventoryRejected" when status == "AwaitingInventory" => (paymentRef is null ? "Failed" : "Compensating", paymentRef, reservationId, paymentRef is null ? "OrderFailed" : "RefundPayment"),
                "ShipmentCreated" when status == "AwaitingShipping" => ("Completed", paymentRef, reservationId, "OrderCompleted"),
                "ShipmentFailed" when status == "AwaitingShipping" => ("Compensating", paymentRef, reservationId, reservationId is not null ? "ReleaseInventory" : paymentRef is not null ? "RefundPayment" : "OrderFailed"),
                "InventoryReleased" when status == "Compensating" => (paymentRef is null ? "Failed" : "Compensating", paymentRef, reservationId, paymentRef is null ? "OrderFailed" : "RefundPayment"),
                "PaymentRefunded" when status == "Compensating" => ("Failed", paymentRef, reservationId, "OrderFailed"),
                _ => throw new InvalidOperationException($"Illegal saga transition: {eventType} while {status} (order {orderId}).")
            };
            commandType = command;
            commandPayload = command switch
            {
                "RequestPayment" => new { orderId, amount = payload.TryGetProperty("totalAmount", out var a) ? a.GetDecimal() : 0m, currency = payload.TryGetProperty("currency", out var c) ? c.GetString() : "IRR", idempotencyKey = $"pay:{orderId:N}" },
                "ReserveInventory" => new { orderId, lines = originalOrder.RootElement.TryGetProperty("lines", out var lines) ? lines.Clone() : JsonDocument.Parse("[]").RootElement.Clone() },
                "RequestShipment" => new { orderId, reservationId = nextReservation },
                "RefundPayment" => new { orderId, paymentReference = nextPayment, reason = eventType },
                "ReleaseInventory" => new { orderId, reservationId = nextReservation },
                _ => new { orderId, reason = eventType }
            };
            var awaitsReply = command is "RequestPayment" or "ReserveInventory" or "RequestShipment" or "RefundPayment" or "ReleaseInventory";
            await using var update = new SqlCommand("""
                UPDATE dbo.saga_instances SET status=@status,payment_reference=@payment,reservation_id=@reservation,version=@version,
                    pending_command_type=@pending_command,pending_since_utc=@pending_since,recovery_attempts=0,
                    recovery_lock_owner=NULL,recovery_locked_until_utc=NULL,dead_at_utc=NULL,dead_reason=NULL,updated_at_utc=@now
                WHERE order_id=@order
                """, cn, tx);
            update.Parameters.AddWithValue("status", nextStatus); update.Parameters.AddWithValue("payment", (object?)nextPayment ?? DBNull.Value);
            update.Parameters.AddWithValue("reservation", (object?)nextReservation ?? DBNull.Value); update.Parameters.AddWithValue("version", version + 1);
            update.Parameters.AddWithValue("pending_command", awaitsReply ? command : DBNull.Value);
            update.Parameters.AddWithValue("pending_since", awaitsReply ? now : DBNull.Value);
            update.Parameters.AddWithValue("now", now); update.Parameters.AddWithValue("order", orderId); await update.ExecuteNonQueryAsync(ct);
        }

        var commandId = Guid.NewGuid();
        var envelope = JsonSerializer.Serialize(new { eventId = commandId, eventType = commandType, eventVersion = 1,
            occurredAt = now, producer = "saga-orchestrator", correlationId = orderId, causationId = eventId, payload = commandPayload });
        await using (var outbox = new SqlCommand("INSERT INTO dbo.saga_outbox(id,aggregate_id,aggregate_version,event_type,payload,created_at_utc) VALUES(@id,@order,@aggregate_version,@type,@payload,@now)", cn, tx))
        {
            outbox.Parameters.AddWithValue("id", commandId); outbox.Parameters.AddWithValue("order", orderId);
            outbox.Parameters.AddWithValue("aggregate_version", aggregateVersion);
            outbox.Parameters.AddWithValue("type", commandType); outbox.Parameters.AddWithValue("payload", envelope);
            outbox.Parameters.AddWithValue("now", now); await outbox.ExecuteNonQueryAsync(ct);
        }
        await tx.CommitAsync(ct);
        log.LogInformation("Saga {OrderId}: {EventType} -> {Command}", orderId, eventType, commandType);
    }

    private async Task DispatchOutboxAsync(IProducer<string, string> producer, CancellationToken ct)
    {
        var owner = $"saga-{Environment.MachineName}-{Guid.NewGuid():N}";
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await using var cn = new SqlConnection(options.ConnectionString);
                await cn.OpenAsync(ct);
                await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);
                var rows = new List<(Guid Id, Guid Aggregate, string EventType, string Payload)>();
                await using (var select = new SqlCommand("""
                    ;WITH picked AS (
                      SELECT TOP (50) current_row.id FROM dbo.saga_outbox current_row WITH (UPDLOCK,READPAST,ROWLOCK)
                      WHERE current_row.published_at_utc IS NULL
                        AND (current_row.locked_until_utc IS NULL OR current_row.locked_until_utc < @now)
                        AND NOT EXISTS (
                          SELECT 1 FROM dbo.saga_outbox earlier
                          WHERE earlier.aggregate_id = current_row.aggregate_id
                            AND earlier.published_at_utc IS NULL
                            AND earlier.aggregate_version < current_row.aggregate_version
                        )
                      ORDER BY current_row.aggregate_version, current_row.created_at_utc, current_row.id
                    )
                    UPDATE target SET lock_owner=@owner, locked_until_utc=@lease
                    OUTPUT inserted.id,inserted.aggregate_id,inserted.event_type,inserted.payload
                    FROM dbo.saga_outbox target INNER JOIN picked ON target.id=picked.id
                    """, cn, tx))
                {
                    select.Parameters.AddWithValue("now", DateTimeOffset.UtcNow);
                    select.Parameters.AddWithValue("owner", owner);
                    select.Parameters.AddWithValue("lease", DateTimeOffset.UtcNow.AddMinutes(2));
                    await using var reader = await select.ExecuteReaderAsync(ct);
                    while (await reader.ReadAsync(ct)) rows.Add((reader.GetGuid(0), reader.GetGuid(1), reader.GetString(2), reader.GetString(3)));
                }
                await tx.CommitAsync(ct);
                foreach (var row in rows)
                {
                    try
                    {
                        var destinationTopic = row.EventType is "OrderCompleted" or "OrderFailed" ? options.SagaEventsTopic : options.CommandTopic;
                        await producer.ProduceAsync(destinationTopic, new Message<string,string> { Key = row.Aggregate.ToString("N"), Value = row.Payload }, ct);
                        await using var mark = new SqlCommand("UPDATE dbo.saga_outbox SET published_at_utc=SYSUTCDATETIME(),locked_until_utc=NULL,lock_owner=NULL WHERE id=@id AND lock_owner=@owner", cn);
                        mark.Parameters.AddWithValue("id", row.Id); mark.Parameters.AddWithValue("owner", owner); await mark.ExecuteNonQueryAsync(ct);
                    }
                    catch
                    {
                        await using var unlock = new SqlCommand("UPDATE dbo.saga_outbox SET locked_until_utc=NULL,lock_owner=NULL WHERE id=@id AND lock_owner=@owner", cn);
                        unlock.Parameters.AddWithValue("id", row.Id); unlock.Parameters.AddWithValue("owner", owner); await unlock.ExecuteNonQueryAsync(ct);
                        throw;
                    }
                }
                if (rows.Count == 0) await Task.Delay(300, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex) { log.LogError(ex, "Saga outbox dispatch failed"); await Task.Delay(1000, ct); }
        }
    }

    private static string Required(JsonElement e, string name) => e.TryGetProperty(name, out var v) ? v.ToString() : throw new JsonException($"Missing {name}");
}
