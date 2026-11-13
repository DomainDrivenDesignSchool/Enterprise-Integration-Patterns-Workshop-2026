using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Data.SqlClient;
using Workshop.Messaging.Reliability;

namespace Workshop.InventoryWorker;

public sealed class Worker(InventoryOptions options, ILogger<Worker> log) : BackgroundService
{
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

        using var failureRouter = new KafkaTransactionalFailureRouter(options.BootstrapServers, $"inventory-failure-{Environment.MachineName}-{Guid.NewGuid():N}",
            "commerce.retry", "commerce.dlq", maxAttempts: 5);

        using var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            EnableIdempotence = true,
            Acks = Acks.All
        }).Build();

        consumer.Subscribe(options.CommandTopic);

        var outbox = DispatchOutboxAsync(producer, stop);
        try
        {
            while (!stop.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]> record;
                try { record = consumer.Consume(stop); }
                catch (OperationCanceledException)
                when (stop.IsCancellationRequested)
                {
                    break;
                }
                try
                {
                    using var doc = JsonDocument.Parse(record.Message.Value);
                    var type = Get(doc.RootElement, "eventType");
                    var payload = doc.RootElement.GetProperty("payload");

                    if (type == "ReserveInventory")
                        await ReserveAsync(payload, stop);

                    else if (type == "ReleaseInventory")
                        await ReleaseAsync(payload, stop);

                    consumer.Commit(record);
                }
                catch (Exception ex)
                {
                    log.LogError(ex, "Inventory command failed at {Tpo}; routing to retry/DLQ",
                        record.TopicPartitionOffset);
                    try
                    {
                        await failureRouter.RouteFailureAndCommitAsync(consumer, record, ex, stop);
                    }
                    catch (Exception routeError)
                    {
                        log.LogCritical(routeError, "Could not route failed inventory message; stopping consumer to avoid committing past it");
                        throw;
                    }
                }
            }
        }
        finally { consumer.Close(); }
        await outbox;
    }

    private async Task ReserveAsync(JsonElement p, CancellationToken ct)
    {
        var orderId = Guid.Parse(Get(p, "orderId"));
        var lines = InventoryRequestFingerprint.Normalize(p.GetProperty("lines").EnumerateArray()
            .Select(x => new InventoryLine(Get(x, "sku"),
            int.Parse(Get(x, "quantity")))));

        if (lines.Count == 0)
            throw new InvalidOperationException("Cannot reserve an order without lines.");

        var requestHash = InventoryRequestFingerprint.Compute(lines);
        var reservationId = DeterministicGuid($"reservation:{orderId:N}");

        await using var cn = new SqlConnection(options.ConnectionString);
        await cn.OpenAsync(ct);
        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);
        // HOLDLOCK takes a serializable key-range lock on the unique order_id index,
        // including when no reservation row exists yet.
        await using (var existing = new SqlCommand("SELECT status,request_hash FROM dbo.stock_reservations WITH (UPDLOCK,HOLDLOCK) WHERE order_id=@order", cn, tx))
        {
            existing.Parameters.AddWithValue("order", orderId);
            await using var reader = await existing.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                var status = reader.GetString(0);
                var previousHash = reader.IsDBNull(1)
                    ? null
                    : reader.GetString(1);
                await reader.CloseAsync();

                if (status is "Reserved" or "Rejected")
                {
                    if (previousHash is not null && !StringComparer.Ordinal.Equals(previousHash, requestHash))
                        throw new InvalidOperationException("Idempotency conflict: the same order was retried with different inventory lines.");

                    if (previousHash is null)
                    {
                        await using var backfill = new SqlCommand("UPDATE dbo.stock_reservations SET request_hash=@hash WHERE order_id=@order AND request_hash IS NULL", cn, tx);
                        backfill.Parameters.AddWithValue("hash", requestHash);
                        backfill.Parameters.AddWithValue("order", orderId);

                        await backfill.ExecuteNonQueryAsync(ct);
                    }
                    await tx.CommitAsync(ct); return;
                }

                if (status == "Released")
                    throw new InvalidOperationException("Reservation was already released; a new order workflow is required.");

                if (status == "Reserving")
                    throw new InvalidOperationException("A persisted Reserving state requires reconciliation; refusing to decrement stock a second time.");

                throw new InvalidOperationException($"Unknown reservation state '{status}'.");
            }
        }
        await using (var create = new SqlCommand("IF NOT EXISTS (SELECT 1 FROM dbo.stock_reservations WITH (UPDLOCK,HOLDLOCK) WHERE order_id=@order) INSERT INTO dbo.stock_reservations(id,order_id,status,request_hash,created_at_utc) VALUES(@id,@order,'Reserving',@hash,SYSUTCDATETIME())", cn, tx))
        {
            create.Parameters.AddWithValue("id", reservationId);
            create.Parameters.AddWithValue("order", orderId);
            create.Parameters.AddWithValue("hash", requestHash);

            if (await create.ExecuteNonQueryAsync(ct) != 1)
                throw new InvalidOperationException("A concurrent reservation won the order idempotency race; reload reservation state before continuing.");
        }

        // Lock all required SKU rows in a stable order before deciding. This avoids both
        // overselling and partial decrements when one line is unavailable.
        var stockBySku = new Dictionary<string, int>(StringComparer.Ordinal);
        var skuParameters = lines.Select((line, index) => $"@sku{index}").ToArray();

        await using (var stock = new SqlCommand($"SELECT sku,available FROM dbo.stock_items WITH (UPDLOCK,HOLDLOCK,ROWLOCK) WHERE sku IN ({string.Join(',', skuParameters)}) ORDER BY sku", cn, tx))
        {
            for (var i = 0; i < lines.Count; i++)
                stock.Parameters.AddWithValue($"sku{i}", lines[i].Sku);

            await using var reader = await stock.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                stockBySku[reader.GetString(0)] = reader.GetInt32(1);
        }

        var unavailable = lines.FirstOrDefault(line => !stockBySku.TryGetValue(line.Sku, out var available) || available < line.Quantity);
        if (unavailable is not null)
        {
            await using (var rejected = new SqlCommand("UPDATE dbo.stock_reservations SET status='Rejected' WHERE id=@id", cn, tx))
            {
                rejected.Parameters.AddWithValue("id", reservationId);
                await rejected.ExecuteNonQueryAsync(ct);
            }
            await AddOutboxAsync(cn, tx, orderId, "InventoryRejected",
                new
                {
                    orderId,
                    reason = $"Insufficient stock for SKU {unavailable.Sku}"
                }, ct);
            await tx.CommitAsync(ct);
            return;
        }

        foreach (var line in lines)
        {
            await using var update = new SqlCommand("UPDATE dbo.stock_items SET available=available-@qty,reserved=reserved+@qty,version=version+1 WHERE sku=@sku AND available>=@qty", cn, tx);
            update.Parameters.AddWithValue("qty", line.Quantity);
            update.Parameters.AddWithValue("sku", line.Sku);

            if (await update.ExecuteNonQueryAsync(ct) != 1)
                throw new InvalidOperationException($"Stock row {line.Sku} changed despite holding its row lock.");

            await using var detail = new SqlCommand("INSERT INTO dbo.stock_reservation_lines(reservation_id,sku,quantity) VALUES(@id,@sku,@qty)", cn, tx);
            detail.Parameters.AddWithValue("id", reservationId);
            detail.Parameters.AddWithValue("sku", line.Sku);
            detail.Parameters.AddWithValue("qty", line.Quantity);

            await detail.ExecuteNonQueryAsync(ct);
        }

        await using (var done = new SqlCommand("UPDATE dbo.stock_reservations SET status='Reserved' WHERE id=@id", cn, tx))
        {
            done.Parameters.AddWithValue("id", reservationId);
            await done.ExecuteNonQueryAsync(ct);
        }
        await AddOutboxAsync(cn, tx, orderId, "InventoryReserved", new
        {
            orderId,
            reservationId
        }, ct);

        await tx.CommitAsync(ct);
    }

    private async Task ReleaseAsync(JsonElement p, CancellationToken ct)
    {
        var orderId = Guid.Parse(Get(p, "orderId"));

        var reservationId = Guid.Parse(Get(p, "reservationId"));
        await using var cn = new SqlConnection(options.ConnectionString);

        await cn.OpenAsync(ct);

        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);

        string? status;

        Guid storedOrderId;

        await using (var check = new SqlCommand("SELECT order_id,status FROM dbo.stock_reservations WITH (UPDLOCK,HOLDLOCK) WHERE id=@id", cn, tx))
        {
            check.Parameters.AddWithValue("id", reservationId);

            await using var reader = await check.ExecuteReaderAsync(ct);

            if (!await reader.ReadAsync(ct))
                throw new InvalidOperationException("Reservation does not exist.");

            storedOrderId = reader.GetGuid(0);
            status = reader.GetString(1);
        }

        if (storedOrderId != orderId)
            throw new InvalidOperationException("Reservation does not belong to the supplied orderId.");

        if (status == "Released")
        {
            await tx.CommitAsync(ct);
            return;
        }

        if (status != "Reserved")
            throw new InvalidOperationException($"Cannot release reservation in state {status}.");

        await using (var restore = new SqlCommand("UPDATE s SET available=s.available+l.quantity,reserved=s.reserved-l.quantity,version=s.version+1 FROM dbo.stock_items s INNER JOIN dbo.stock_reservation_lines l ON l.sku=s.sku WHERE l.reservation_id=@id", cn, tx))
        {
            restore.Parameters.AddWithValue("id", reservationId);
            await restore.ExecuteNonQueryAsync(ct);
        }

        await using (var mark = new SqlCommand("UPDATE dbo.stock_reservations SET status='Released',released_at_utc=SYSUTCDATETIME() WHERE id=@id", cn, tx))
        {
            mark.Parameters.AddWithValue("id", reservationId);
            await mark.ExecuteNonQueryAsync(ct);
        }

        await AddOutboxAsync(cn, tx, orderId, "InventoryReleased",
            new
            {
                orderId,
                reservationId
            }, ct);

        await tx.CommitAsync(ct);
    }

    private async Task AddOutboxAsync(SqlConnection cn, SqlTransaction tx, Guid orderId, string type, object payload, CancellationToken ct)
    {
        var id = DeterministicGuid($"{type}:{orderId:N}");
        var json = JsonSerializer.Serialize(new
        {
            eventId = id,
            eventType = type,
            eventVersion = 1,
            occurredAt = DateTimeOffset.UtcNow,
            producer = "inventory-service",
            correlationId = orderId,
            causationId = (Guid?)null,
            payload
        }, Json);
        await using var cmd = new SqlCommand("IF NOT EXISTS (SELECT 1 FROM dbo.inventory_outbox WITH (UPDLOCK,HOLDLOCK) WHERE id=@id) INSERT INTO dbo.inventory_outbox(id,aggregate_id,event_type,payload,created_at_utc) VALUES(@id,@order,@type,@payload,SYSUTCDATETIME())", cn, tx);

        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("order", orderId);
        cmd.Parameters.AddWithValue("type", type);
        cmd.Parameters.AddWithValue("payload", json);

        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task DispatchOutboxAsync(IProducer<string, string> producer, CancellationToken ct)
    {
        var owner = $"inventory-{Environment.MachineName}-{Guid.NewGuid():N}";
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
                      SELECT TOP (50) current_row.id FROM dbo.inventory_outbox current_row WITH (UPDLOCK,READPAST,ROWLOCK)
                      WHERE current_row.published_at_utc IS NULL
                        AND (current_row.locked_until_utc IS NULL OR current_row.locked_until_utc < @now)
                        AND NOT EXISTS (
                          SELECT 1 FROM dbo.inventory_outbox earlier
                          WHERE earlier.aggregate_id = current_row.aggregate_id
                            AND earlier.published_at_utc IS NULL
                            AND (earlier.created_at_utc < current_row.created_at_utc OR
                                (earlier.created_at_utc = current_row.created_at_utc AND earlier.id < current_row.id))
                        )
                      ORDER BY current_row.created_at_utc, current_row.id
                    )
                    UPDATE target SET lock_owner=@owner,locked_until_utc=@lease
                    OUTPUT inserted.id,inserted.aggregate_id,inserted.payload
                    FROM dbo.inventory_outbox target INNER JOIN picked ON target.id=picked.id
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
                        await using var mark = new SqlCommand("UPDATE dbo.inventory_outbox SET published_at_utc=SYSUTCDATETIME(),locked_until_utc=NULL,lock_owner=NULL WHERE id=@id AND lock_owner=@owner", cn);

                        mark.Parameters.AddWithValue("id", x.Id);
                        mark.Parameters.AddWithValue("owner", owner);

                        await mark.ExecuteNonQueryAsync(ct);
                    }
                    catch
                    {
                        await using var unlock = new SqlCommand("UPDATE dbo.inventory_outbox SET locked_until_utc=NULL,lock_owner=NULL WHERE id=@id AND lock_owner=@owner", cn);
                        unlock.Parameters.AddWithValue("id", x.Id);

                        unlock.Parameters.AddWithValue("owner", owner);

                        await unlock.ExecuteNonQueryAsync(ct); throw;
                    }
                }
                if (rows.Count == 0) await Task.Delay(300, ct);
            }
            catch (OperationCanceledException)
            when (ct.IsCancellationRequested)
            { break; }
            catch (Exception ex)
            {
                log.LogError(ex, "Inventory outbox dispatch failed");

                await Task.Delay(1000, ct);
            }
        }
    }

    private static string Get(JsonElement e, string name)
        => e.TryGetProperty(name, out var v)
        ? v.ToString()
        : throw new JsonException($"Missing {name}");
    private static Guid DeterministicGuid(string value)
        => new(SHA256.HashData(Encoding.UTF8.GetBytes(value))[..16]);

}
