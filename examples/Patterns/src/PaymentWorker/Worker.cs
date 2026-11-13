using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Data.SqlClient;
using Workshop.Messaging.Reliability;

namespace Workshop.PaymentWorker;

public sealed class Worker(PaymentOptions options, IPaymentGateway gateway, ILogger<Worker> log) : BackgroundService
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

        using var failureRouter = new KafkaTransactionalFailureRouter(options.BootstrapServers, $"payment-failure-{Environment.MachineName}-{Guid.NewGuid():N}", "commerce.retry", "commerce.dlq", maxAttempts: 5);
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
                catch (OperationCanceledException) when (stop.IsCancellationRequested) { break; }
                try
                {
                    using var doc = JsonDocument.Parse(record.Message.Value);
                    var type = Get(doc.RootElement, "eventType");
                    var payload = doc.RootElement.GetProperty("payload");
                    if (type == "RequestPayment") await ChargeAsync(payload, stop);
                    else if (type == "RefundPayment") await RefundAsync(payload, stop);
                    else { log.LogDebug("Payment worker ignores command {Type}", type); }
                    consumer.Commit(record);
                }
                catch (Exception ex)
                {
                    log.LogError(ex, "Payment command failed at {Tpo}; routing to retry/DLQ", record.TopicPartitionOffset);
                    try { await failureRouter.RouteFailureAndCommitAsync(consumer, record, ex, stop); }
                    catch (Exception routeError) { log.LogCritical(routeError, "Could not route failed payment message; stopping consumer to avoid committing past it"); throw; }
                }
            }
        }
        finally { consumer.Close(); }
        await outbox;
    }

    private async Task ChargeAsync(JsonElement p, CancellationToken ct)
    {
        var orderId = Guid.Parse(Get(p, "orderId")); var amount = decimal.Parse(Get(p, "amount"), System.Globalization.CultureInfo.InvariantCulture);
        var currency = Get(p, "currency"); var key = Get(p, "idempotencyKey");

        await using var cn = new SqlConnection(options.ConnectionString); await cn.OpenAsync(ct);
        string attemptStatus;
        await using (var guardTx = (SqlTransaction)await cn.BeginTransactionAsync(ct))
        {
            await using (var create = new SqlCommand("""
                IF NOT EXISTS (SELECT 1 FROM dbo.payment_attempts WITH (UPDLOCK,HOLDLOCK) WHERE idempotency_key=@key)
                  INSERT INTO dbo.payment_attempts(id,order_id,idempotency_key,amount,currency,status,created_at_utc)
                  VALUES(@id,@order,@key,@amount,@currency,'Initiated',SYSUTCDATETIME())
                """, cn, guardTx))
            {
                create.Parameters.AddWithValue("id", DeterministicGuid(key));
                create.Parameters.AddWithValue("order", orderId);
                create.Parameters.AddWithValue("key", key);
                create.Parameters.AddWithValue("amount", amount);
                create.Parameters.AddWithValue("currency", currency);

                await create.ExecuteNonQueryAsync(ct);
            }
            await using (var guard = new SqlCommand("SELECT order_id,amount,currency,status FROM dbo.payment_attempts WITH (UPDLOCK,HOLDLOCK) WHERE idempotency_key=@key", cn, guardTx))
            {
                guard.Parameters.AddWithValue("key", key);
                await using var reader = await guard.ExecuteReaderAsync(ct);

                if (!await reader.ReadAsync(ct))
                    throw new InvalidOperationException("Payment attempt disappeared after idempotent insert.");

                var storedOrder = reader.GetGuid(0); var storedAmount = reader.GetDecimal(1);
                var storedCurrency = reader.GetString(2).Trim(); attemptStatus = reader.GetString(3);

                if (storedOrder != orderId || storedAmount != amount || !StringComparer.Ordinal.Equals(storedCurrency, currency))
                    throw new InvalidOperationException("Idempotency key reuse with a different order, amount or currency is forbidden.");
            }
            await guardTx.CommitAsync(ct);
        }
        // A terminal or ambiguous attempt must never issue a second charge command.
        // Unknown is resolved only by the reconciliation worker via provider inquiry.
        if (attemptStatus is "Authorized" or "Rejected" or "Unknown" or "ManualReview") return;
        // Persist intent before calling the remote provider. A process crash from this point
        // is ambiguous; the reconciliation worker will query by the same provider idempotency key.
        GatewayResult result;
        try
        {
            result = await gateway.ChargeAsync(orderId, amount, currency, key, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
        catch (Exception ex)
        {
            await MarkUnknownAsync(cn, key, ex.Message, ct);
            log.LogWarning(ex, "Payment outcome is ambiguous for {OrderId}; reconciliation will query provider", orderId);
            return; // DB has durably accepted responsibility for resolving this attempt.
        }
        if (result.Approved && (string.IsNullOrWhiteSpace(result.ProviderReference) || result.ProviderReference.Length > 500))
        {
            await MarkUnknownAsync(cn, key, "Provider approved without a valid reference (empty or longer than 500 characters)", ct);
            return;
        }
        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);
        await using (var update = new SqlCommand("UPDATE dbo.payment_attempts SET status=@status,provider_reference=@reference,decline_reason=@reason,next_reconcile_at_utc=NULL,reconciliation_owner=NULL,reconciliation_locked_until_utc=NULL,updated_at_utc=SYSUTCDATETIME() WHERE idempotency_key=@key AND status IN ('Initiated','Unknown')", cn, tx))
        {
            update.Parameters.AddWithValue("status", result.Approved ? "Authorized" : "Rejected");
            update.Parameters.AddWithValue("reference", (object?)result.ProviderReference ?? DBNull.Value);
            update.Parameters.AddWithValue("reason", (object?)(result.DeclineReason is { Length: > 1000 }
            ? result.DeclineReason[..1000] : result.DeclineReason)
            ?? DBNull.Value); update.Parameters.AddWithValue("key", key);

            if (await update.ExecuteNonQueryAsync(ct) != 1)
            {
                await tx.CommitAsync(ct); return;
            }
        }

        if (result.Approved)
        {
            await using var ledger = new SqlCommand("""
                IF NOT EXISTS (SELECT 1 FROM dbo.payment_ledger WITH (UPDLOCK,HOLDLOCK) WHERE id=@id OR (reference=@ref AND entry_type='Charge'))
                  INSERT INTO dbo.payment_ledger(id,order_id,entry_type,amount,currency,reference,created_at_utc)
                  VALUES(@id,@order,'Charge',@amount,@currency,@ref,SYSUTCDATETIME())
                """, cn, tx);

            ledger.Parameters.AddWithValue("id", DeterministicGuid($"ledger:{key}"));
            ledger.Parameters.AddWithValue("order", orderId);
            ledger.Parameters.AddWithValue("amount", amount);
            ledger.Parameters.AddWithValue("currency", currency);
            ledger.Parameters.AddWithValue("ref", result.ProviderReference!);

            await ledger.ExecuteNonQueryAsync(ct);
            await AddOutboxAsync(cn, tx, orderId, "PaymentAuthorized", new
            {
                orderId,
                paymentReference = result.ProviderReference,
                amount,
                currency
            }, key, ct);
        }
        else await AddOutboxAsync(cn, tx, orderId, "PaymentRejected", new
        {
            orderId,
            reason = result.DeclineReason ?? "Declined"
        }, key, ct);

        await tx.CommitAsync(ct);
    }

    private static async Task MarkUnknownAsync(SqlConnection cn, string key, string error, CancellationToken ct)
    {
        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);
        await using var cmd = new SqlCommand("""
            UPDATE dbo.payment_attempts
            SET status='Unknown', last_error=@error, next_reconcile_at_utc=DATEADD(SECOND,15,SYSUTCDATETIME()),
                reconciliation_attempts=0, updated_at_utc=SYSUTCDATETIME()
            WHERE idempotency_key=@key AND status NOT IN ('Authorized','Rejected','Refunded')
            """, cn, tx);

        cmd.Parameters.AddWithValue("key", key);
        cmd.Parameters.AddWithValue("error", error.Length > 2000 ? error[..2000] : error);

        await cmd.ExecuteNonQueryAsync(ct);
        await tx.CommitAsync(ct);
    }

    private async Task RefundAsync(JsonElement p, CancellationToken ct)
    {
        var orderId = Guid.Parse(Get(p, "orderId"));

        var reference = Get(p, "paymentReference");
        var key = $"refund:{reference}:{orderId:N}";
        await using var cn = new SqlConnection(options.ConnectionString);
        await cn.OpenAsync(ct);
        Guid refundId; decimal amount; string currency; string status;

        await using (var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct))
        {
            await using (var exists = new SqlCommand("SELECT CAST(CASE WHEN EXISTS(SELECT 1 FROM dbo.payment_attempts WHERE order_id=@order AND provider_reference=@reference AND status='Authorized') THEN 1 ELSE 0 END AS bit)", cn, tx))
            {
                exists.Parameters.AddWithValue("order", orderId); exists.Parameters.AddWithValue("reference", reference);
                if (!(bool)(await exists.ExecuteScalarAsync(ct) ?? false))
                    throw new InvalidOperationException("Cannot refund: no authorized payment matches the order and provider reference.");
            }
            await using (var create = new SqlCommand("""
                IF NOT EXISTS (SELECT 1 FROM dbo.payment_refund_attempts WITH (UPDLOCK,HOLDLOCK) WHERE idempotency_key=@key)
                  INSERT INTO dbo.payment_refund_attempts(id,order_id,payment_reference,idempotency_key,amount,currency,status,created_at_utc)
                  SELECT @id,@order,@reference,@key,amount,currency,'Initiated',SYSUTCDATETIME()
                  FROM dbo.payment_attempts WHERE order_id=@order AND provider_reference=@reference AND status='Authorized' 
                """, cn, tx))
            {
                create.Parameters.AddWithValue("id", DeterministicGuid(key));
                create.Parameters.AddWithValue("order", orderId);
                create.Parameters.AddWithValue("reference", reference);
                create.Parameters.AddWithValue("key", key);

                await create.ExecuteNonQueryAsync(ct);
            }

            await using (var read = new SqlCommand("SELECT id,order_id,payment_reference,status,amount,currency FROM dbo.payment_refund_attempts WITH (UPDLOCK,HOLDLOCK) WHERE idempotency_key=@key", cn, tx))
            {
                read.Parameters.AddWithValue("key", key);
                await using var r = await read.ExecuteReaderAsync(ct);

                if (!await r.ReadAsync(ct))
                    throw new InvalidOperationException("Refund attempt disappeared.");

                refundId = r.GetGuid(0);

                if (r.GetGuid(1) != orderId || r.GetString(2) != reference)
                    throw new InvalidOperationException("Refund idempotency key reused with different payment.");
                status = r.GetString(3); amount = r.GetDecimal(4);

                currency = r.GetString(5).Trim();
            }
            await tx.CommitAsync(ct);
        }

        if (status is "Refunded" or "Rejected" or "Unknown" or "ManualReview")
            return;

        bool refunded;
        try
        {
            refunded = await gateway.RefundAsync(reference, key, ct);
        }

        catch (OperationCanceledException)
        when (ct.IsCancellationRequested)
        { throw; }

        catch (Exception ex)
        {
            await MarkRefundUnknownAsync(cn, key, ex.Message, ct);
            log.LogWarning(ex, "Refund outcome is ambiguous for {OrderId}; reconciliation will query provider", orderId);
            return;
        }

        await using var tx2 = (SqlTransaction)await cn.BeginTransactionAsync(ct);
        var finalStatus = refunded ? "Refunded" : "Rejected";
        await using (var update = new SqlCommand("UPDATE dbo.payment_refund_attempts SET status=@status,last_error=@error,updated_at_utc=SYSUTCDATETIME(),next_reconcile_at_utc=NULL,reconciliation_owner=NULL,reconciliation_locked_until_utc=NULL WHERE id=@id AND status IN ('Initiated','Unknown')", cn, tx2))
        {
            update.Parameters.AddWithValue("status", finalStatus);
            update.Parameters.AddWithValue("error", refunded
                ? DBNull.Value
                : "Provider refused refund.");

            update.Parameters.AddWithValue("id", refundId);


            if (await update.ExecuteNonQueryAsync(ct) != 1)
            {
                await tx2.CommitAsync(ct); return;
            }
        }
        if (refunded)
        {
            await using var ledger = new SqlCommand("""
                IF NOT EXISTS (SELECT 1 FROM dbo.payment_ledger WITH (UPDLOCK,HOLDLOCK) WHERE id=@id OR (reference=@ref AND entry_type='Refund'))
                  INSERT INTO dbo.payment_ledger(id,order_id,entry_type,amount,currency,reference,created_at_utc)
                  VALUES(@id,@order,'Refund',-@amount,@currency,@ref,SYSUTCDATETIME())
                """, cn, tx2);

            ledger.Parameters.AddWithValue("id", DeterministicGuid(key));
            ledger.Parameters.AddWithValue("order", orderId);
            ledger.Parameters.AddWithValue("amount", amount);
            ledger.Parameters.AddWithValue("currency", currency);
            ledger.Parameters.AddWithValue("ref", reference);

            await ledger.ExecuteNonQueryAsync(ct);

            await AddOutboxAsync(cn, tx2, orderId, "PaymentRefunded",
                new
                {
                    orderId,
                    paymentReference = reference
                }, key, ct);
        }
        await tx2.CommitAsync(ct);
    }

    private static async Task MarkRefundUnknownAsync(SqlConnection cn, string key, string error, CancellationToken ct)
    {
        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);
        await using var cmd = new SqlCommand("""
            UPDATE dbo.payment_refund_attempts SET status='Unknown',last_error=@error,
              next_reconcile_at_utc=DATEADD(SECOND,15,SYSUTCDATETIME()),reconciliation_attempts=0,updated_at_utc=SYSUTCDATETIME()
            WHERE idempotency_key=@key AND status NOT IN ('Refunded','Rejected')
            """, cn, tx);

        cmd.Parameters.AddWithValue("key", key);
        cmd.Parameters.AddWithValue("error", error.Length > 2000 ? error[..2000] : error);

        await cmd.ExecuteNonQueryAsync(ct);
        await tx.CommitAsync(ct);
    }

    private static async Task AddOutboxAsync(SqlConnection cn, SqlTransaction tx, Guid orderId,
        string type, object payload, string sourceKey, CancellationToken ct)
    {
        var id = DeterministicGuid($"event:{type}:{sourceKey}");
        var json = JsonSerializer.Serialize(new
        {
            eventId = id,
            eventType = type,
            eventVersion = 1,
            occurredAt = DateTimeOffset.UtcNow,
            producer = "payment-service",
            correlationId = orderId,
            causationId = (Guid?)null,
            payload
        }, Json);

        await using var cmd = new SqlCommand("IF NOT EXISTS (SELECT 1 FROM dbo.payment_outbox WITH (UPDLOCK,HOLDLOCK) WHERE id=@id) INSERT INTO dbo.payment_outbox(id,aggregate_id,event_type,payload,created_at_utc) VALUES(@id,@order,@type,@payload,SYSUTCDATETIME())", cn, tx);

        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("order", orderId);
        cmd.Parameters.AddWithValue("type", type);
        cmd.Parameters.AddWithValue("payload", json);

        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task DispatchOutboxAsync(IProducer<string, string> producer, CancellationToken ct)
    {
        var owner = $"payment-{Environment.MachineName}-{Guid.NewGuid():N}";
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await using var cn = new SqlConnection(options.ConnectionString); await cn.OpenAsync(ct);
                await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);
                var rows = new List<(Guid Id, string Key, string Payload)>();

                await using (var q = new SqlCommand("""
                    ;WITH picked AS (
                      SELECT TOP (50) current_row.id FROM dbo.payment_outbox current_row WITH (UPDLOCK,READPAST,ROWLOCK)
                      WHERE current_row.published_at_utc IS NULL
                        AND (current_row.locked_until_utc IS NULL OR current_row.locked_until_utc < @now)
                        AND NOT EXISTS (
                          SELECT 1 FROM dbo.payment_outbox earlier
                          WHERE earlier.aggregate_id = current_row.aggregate_id
                            AND earlier.published_at_utc IS NULL
                            AND (earlier.created_at_utc < current_row.created_at_utc OR
                                (earlier.created_at_utc = current_row.created_at_utc AND earlier.id < current_row.id))
                        )
                      ORDER BY current_row.created_at_utc, current_row.id
                    )
                    UPDATE target SET lock_owner=@owner,locked_until_utc=@lease
                    OUTPUT inserted.id,inserted.aggregate_id,inserted.payload
                    FROM dbo.payment_outbox target INNER JOIN picked ON target.id=picked.id
                    """, cn, tx))
                {
                    q.Parameters.AddWithValue("now", DateTimeOffset.UtcNow);
                    q.Parameters.AddWithValue("owner", owner);

                    q.Parameters.AddWithValue("lease", DateTimeOffset.UtcNow.AddMinutes(2));
                    await using var r = await q.ExecuteReaderAsync(ct);

                    while (await r.ReadAsync(ct))
                        rows.Add((r.GetGuid(0),
                            r.GetValue(1).ToString()!, r.GetString(2)));
                }
                await tx.CommitAsync(ct);
                foreach (var x in rows)
                {
                    try
                    {
                        await producer.ProduceAsync(options.SagaEventsTopic, new Message<string, string> { Key = x.Key, Value = x.Payload }, ct);
                        await using var mark = new SqlCommand("UPDATE dbo.payment_outbox SET published_at_utc=SYSUTCDATETIME(),locked_until_utc=NULL,lock_owner=NULL WHERE id=@id AND lock_owner=@owner", cn);
                        mark.Parameters.AddWithValue("id", x.Id); mark.Parameters.AddWithValue("owner", owner); await mark.ExecuteNonQueryAsync(ct);
                    }
                    catch
                    {
                        await using var unlock = new SqlCommand("UPDATE dbo.payment_outbox SET locked_until_utc=NULL,lock_owner=NULL WHERE id=@id AND lock_owner=@owner", cn);
                        unlock.Parameters.AddWithValue("id", x.Id); unlock.Parameters.AddWithValue("owner", owner); await unlock.ExecuteNonQueryAsync(ct); throw;
                    }
                }
                if (rows.Count == 0) await Task.Delay(300, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex) { log.LogError(ex, "Payment outbox dispatch failed"); await Task.Delay(1000, ct); }
        }
    }

    private static string Get(JsonElement e, string name) 
        => e.TryGetProperty(name, out var v) 
        ? v.ToString() 
        : throw new JsonException($"Missing {name}");

    private static Guid DeterministicGuid(string value)
        => new(SHA256.HashData(Encoding.UTF8.GetBytes(value))[..16]);
}
