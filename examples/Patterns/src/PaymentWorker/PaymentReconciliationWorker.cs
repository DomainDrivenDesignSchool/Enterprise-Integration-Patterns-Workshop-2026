using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;

namespace Workshop.PaymentWorker;

/// <summary>
/// Resolves payment attempts whose remote outcome is unknown. Provider inquiry is authoritative;
/// this worker never interprets a timeout as a decline and never blindly sends another charge.
/// </summary>
public sealed class PaymentReconciliationWorker(
    PaymentOptions options,
    IPaymentGateway gateway,
    ILogger<PaymentReconciliationWorker> log) : BackgroundService
{
    private const int BatchSize = 25;
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private readonly string _owner = $"payment-recon-{Environment.MachineName}-{Guid.NewGuid():N}";

    private sealed record Candidate(Guid Id, Guid OrderId, string Key, decimal Amount, string Currency,
        string Status, int Attempts);
    private sealed record RefundCandidate(Guid Id, Guid OrderId, string PaymentReference, string Key,
        decimal Amount, string Currency, int Attempts);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var batch = await ClaimAsync(stoppingToken);
                foreach (var candidate in batch)
                {
                    stoppingToken.ThrowIfCancellationRequested();
                    await ReconcileOneAsync(candidate, stoppingToken);
                }
                var refunds = await ClaimRefundsAsync(stoppingToken);
                foreach (var refund in refunds)
                {
                    stoppingToken.ThrowIfCancellationRequested();
                    await ReconcileRefundAsync(refund, stoppingToken);
                }
                if (batch.Count == 0 && refunds.Count == 0)
                    await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                log.LogError(ex, "Payment reconciliation loop failed");
                await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
            }
        }
    }

    private async Task<List<Candidate>> ClaimAsync(CancellationToken ct)
    {
        await using var cn = new SqlConnection(options.ConnectionString);
        await cn.OpenAsync(ct);
        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);
        var rows = new List<Candidate>();
        await using (var cmd = new SqlCommand("""
            ;WITH picked AS (
                SELECT TOP (@limit) * FROM dbo.payment_attempts WITH (UPDLOCK, READPAST, ROWLOCK)
                WHERE status IN ('Unknown','Initiated')
                  AND (next_reconcile_at_utc IS NULL OR next_reconcile_at_utc <= SYSUTCDATETIME())
                  AND (reconciliation_locked_until_utc IS NULL OR reconciliation_locked_until_utc < SYSUTCDATETIME())
                  AND (status <> 'Initiated' OR created_at_utc < DATEADD(SECOND,-30,SYSUTCDATETIME()))
                ORDER BY COALESCE(next_reconcile_at_utc, created_at_utc), created_at_utc
            )
            UPDATE picked SET reconciliation_owner=@owner,
                reconciliation_locked_until_utc=DATEADD(SECOND,60,SYSUTCDATETIME())
            OUTPUT inserted.id,inserted.order_id,inserted.idempotency_key,inserted.amount,inserted.currency,inserted.status,inserted.reconciliation_attempts
            """, cn, tx))
        {
            cmd.Parameters.AddWithValue("limit", BatchSize);
            cmd.Parameters.AddWithValue("owner", _owner);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                rows.Add(new Candidate(reader.GetGuid(0), reader.GetGuid(1), reader.GetString(2),
                    reader.GetDecimal(3), reader.GetString(4).Trim(), reader.GetString(5), reader.GetInt32(6)));
        }
        await tx.CommitAsync(ct);
        return rows;
    }

    private async Task<List<RefundCandidate>> ClaimRefundsAsync(CancellationToken ct)
    {
        await using var cn = new SqlConnection(options.ConnectionString);
        await cn.OpenAsync(ct);
        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);
        var rows = new List<RefundCandidate>();
        await using (var cmd = new SqlCommand("""
            ;WITH picked AS (
                SELECT TOP (@limit) * FROM dbo.payment_refund_attempts WITH (UPDLOCK, READPAST, ROWLOCK)
                WHERE status IN ('Unknown','Initiated')
                  AND (next_reconcile_at_utc IS NULL OR next_reconcile_at_utc <= SYSUTCDATETIME())
                  AND (reconciliation_locked_until_utc IS NULL OR reconciliation_locked_until_utc < SYSUTCDATETIME())
                  AND (status <> 'Initiated' OR created_at_utc < DATEADD(SECOND,-30,SYSUTCDATETIME()))
                ORDER BY COALESCE(next_reconcile_at_utc, created_at_utc), created_at_utc
            )
            UPDATE picked SET reconciliation_owner=@owner,
                reconciliation_locked_until_utc=DATEADD(SECOND,60,SYSUTCDATETIME())
            OUTPUT inserted.id,inserted.order_id,inserted.payment_reference,inserted.idempotency_key,inserted.amount,inserted.currency,inserted.reconciliation_attempts
            """, cn, tx))
        {
            cmd.Parameters.AddWithValue("limit", BatchSize); cmd.Parameters.AddWithValue("owner", _owner);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                rows.Add(new RefundCandidate(reader.GetGuid(0), reader.GetGuid(1), reader.GetString(2),
                    reader.GetString(3), reader.GetDecimal(4), reader.GetString(5).Trim(), reader.GetInt32(6)));
        }
        await tx.CommitAsync(ct);
        return rows;
    }

    private async Task ReconcileRefundAsync(RefundCandidate item, CancellationToken ct)
    {
        ProviderPaymentStatus status;
        try
        {
            status = await gateway.QueryRefundAsync(item.Key, ct);
        }
        catch (OperationCanceledException)
        when (ct.IsCancellationRequested)
        {
            throw;
        }

        catch (Exception ex)
        {
            await RescheduleRefundAsync(item, $"Provider refund inquiry failed: {ex.Message}", ct); return;
        }

        if (status.State == ProviderPaymentState.Authorized)
        {
            await CompleteRefundAsync(item, ct);
            log.LogInformation("Refund {RefundId} reconciled as Refunded", item.Id);
        }
        else if (status.State == ProviderPaymentState.Declined)
            await RejectRefundAsync(item, status.DeclineReason ?? "Provider declined refund", ct);
        else
            await RescheduleRefundAsync(item, $"Provider refund state: {status.State}", ct);
    }

    private async Task CompleteRefundAsync(RefundCandidate item, CancellationToken ct)
    {
        await using var cn = new SqlConnection(options.ConnectionString); await cn.OpenAsync(ct);
        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);

        await using (var update = new SqlCommand("""
            UPDATE dbo.payment_refund_attempts SET status='Refunded',last_error=NULL,next_reconcile_at_utc=NULL,
              reconciliation_owner=NULL,reconciliation_locked_until_utc=NULL,updated_at_utc=SYSUTCDATETIME()
            WHERE id=@id AND reconciliation_owner=@owner AND status IN ('Unknown','Initiated')
            """, cn, tx))
        {
            update.Parameters.AddWithValue("id", item.Id); update.Parameters.AddWithValue("owner", _owner);
            if (await update.ExecuteNonQueryAsync(ct) != 1) { await tx.RollbackAsync(ct); return; }
        }


        await using (var ledger = new SqlCommand("""
            IF NOT EXISTS (SELECT 1 FROM dbo.payment_ledger WITH (UPDLOCK,HOLDLOCK) WHERE id=@id OR (reference=@reference AND entry_type='Refund'))
              INSERT INTO dbo.payment_ledger(id,order_id,entry_type,amount,currency,reference,created_at_utc)
              VALUES(@id,@order,'Refund',-@amount,@currency,@reference,SYSUTCDATETIME())
            """, cn, tx))
        {
            ledger.Parameters.AddWithValue("id", DeterministicGuid(item.Key)); ledger.Parameters.AddWithValue("order", item.OrderId);
            ledger.Parameters.AddWithValue("amount", item.Amount); ledger.Parameters.AddWithValue("currency", item.Currency);
            ledger.Parameters.AddWithValue("reference", item.PaymentReference); await ledger.ExecuteNonQueryAsync(ct);
        }
        await AddRefundOutboxAsync(cn, tx, item, ct);
        await tx.CommitAsync(ct);
    }

    private async Task RejectRefundAsync(RefundCandidate item, string reason, CancellationToken ct)
    {
        await using var cn = new SqlConnection(options.ConnectionString); await cn.OpenAsync(ct);
        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);

        await using var cmd = new SqlCommand("""
            UPDATE dbo.payment_refund_attempts SET status='Rejected',last_error=@error,next_reconcile_at_utc=NULL,
              reconciliation_owner=NULL,reconciliation_locked_until_utc=NULL,updated_at_utc=SYSUTCDATETIME()
            WHERE id=@id AND reconciliation_owner=@owner
            """, cn, tx);

        cmd.Parameters.AddWithValue("error", Truncate(reason)); cmd.Parameters.AddWithValue("id", item.Id); cmd.Parameters.AddWithValue("owner", _owner);

        await cmd.ExecuteNonQueryAsync(ct); await tx.CommitAsync(ct);
    }

    private async Task RescheduleRefundAsync(RefundCandidate item, string error, CancellationToken ct)
    {
        await using var cn = new SqlConnection(options.ConnectionString);
        await cn.OpenAsync(ct);
        var next = item.Attempts + 1;

        var terminal = PaymentReconciliationPolicy.RequiresManualReview(next);
        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);

        await using var cmd = new SqlCommand(terminal ? """
            UPDATE dbo.payment_refund_attempts SET status='ManualReview',reconciliation_attempts=@attempts,last_error=@error,
              next_reconcile_at_utc=NULL,reconciliation_owner=NULL,reconciliation_locked_until_utc=NULL,updated_at_utc=SYSUTCDATETIME()
            WHERE id=@id AND reconciliation_owner=@owner
            """ : """
            UPDATE dbo.payment_refund_attempts SET reconciliation_attempts=@attempts,last_error=@error,
              next_reconcile_at_utc=DATEADD(SECOND,CONVERT(int,@delay),SYSUTCDATETIME()),reconciliation_owner=NULL,
              reconciliation_locked_until_utc=NULL,updated_at_utc=SYSUTCDATETIME()
            WHERE id=@id AND reconciliation_owner=@owner
            """, cn, tx);

        cmd.Parameters.AddWithValue("attempts", next);
        cmd.Parameters.AddWithValue("error", Truncate(error));

        cmd.Parameters.AddWithValue("id", item.Id);
        cmd.Parameters.AddWithValue("owner", _owner);


        if (!terminal)
            cmd.Parameters.AddWithValue("delay", PaymentReconciliationPolicy.DelayForAttempt(next).TotalSeconds);

        await cmd.ExecuteNonQueryAsync(ct); await tx.CommitAsync(ct);

        if (terminal)
            log.LogError("Refund {RefundId} moved to ManualReview after {Attempts} inquiries", item.Id, next);
    }

    private static async Task AddRefundOutboxAsync(SqlConnection cn, SqlTransaction tx, RefundCandidate item, CancellationToken ct)
    {
        var id = DeterministicGuid($"event:PaymentRefunded:{item.Key}");
        var json = JsonSerializer.Serialize(new
        {
            eventId = id,
            eventType = "PaymentRefunded",
            eventVersion = 1,
            occurredAt = DateTimeOffset.UtcNow,
            producer = "payment-service",
            correlationId = item.OrderId,
            causationId = (Guid?)null,
            payload = new { orderId = item.OrderId, paymentReference = item.PaymentReference }
        }, Json);

        await using var cmd = new SqlCommand("IF NOT EXISTS (SELECT 1 FROM dbo.payment_outbox WITH (UPDLOCK,HOLDLOCK) WHERE id=@id) INSERT INTO dbo.payment_outbox(id,aggregate_id,event_type,payload,created_at_utc) VALUES(@id,@order,'PaymentRefunded',@payload,SYSUTCDATETIME())", cn, tx);

        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("order", item.OrderId);
        cmd.Parameters.AddWithValue("payload", json);

        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task ReconcileOneAsync(Candidate item, CancellationToken ct)
    {
        ProviderPaymentStatus status;
        try
        {
            status = await gateway.QueryAsync(item.Key, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
        catch (Exception ex)
        {
            await RescheduleAsync(item, $"Provider inquiry failed: {ex.Message}", ct);
            return;
        }

        switch (status.State)
        {
            case ProviderPaymentState.Authorized:
                if (string.IsNullOrWhiteSpace(status.ProviderReference) || status.ProviderReference.Length > 500)
                {
                    await RescheduleAsync(item, "Provider reports authorization without a valid reference", ct);
                    return;
                }
                await CompleteAsync(item, true, status.ProviderReference, null, ct);
                log.LogInformation("Payment {PaymentId} reconciled as Authorized", item.Id);
                return;
            case ProviderPaymentState.Declined:
                await CompleteAsync(item, false, null, status.DeclineReason ?? "Provider declined", ct);
                log.LogInformation("Payment {PaymentId} reconciled as Rejected", item.Id);
                return;
            case ProviderPaymentState.Pending:
            case ProviderPaymentState.Unknown:
                await RescheduleAsync(item, $"Provider state: {status.State}", ct);
                return;
            case ProviderPaymentState.NotFound:
                // A not-found response is not necessarily proof of non-charge when provider
                // inquiry is eventually consistent. Never charge again automatically here.
                await RescheduleAsync(item, "Provider has no visible record for this idempotency key; awaiting consistent inquiry", ct);
                return;
            default:
                await RescheduleAsync(item, "Unsupported provider state", ct);
                return;
        }
    }

    private async Task CompleteAsync(Candidate item, bool approved, string? reference, string? reason, CancellationToken ct)
    {
        await using var cn = new SqlConnection(options.ConnectionString);
        await cn.OpenAsync(ct);
        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);

        string current;
        await using (var read = new SqlCommand("SELECT status FROM dbo.payment_attempts WITH (UPDLOCK,HOLDLOCK) WHERE id=@id AND reconciliation_owner=@owner", cn, tx))
        {
            read.Parameters.AddWithValue("id", item.Id);
            read.Parameters.AddWithValue("owner", _owner);
            var value = await read.ExecuteScalarAsync(ct);

            if (value is null)
            {
                await tx.RollbackAsync(ct);
                return;
            }

            current = (string)value;
        }

        if (current is "Authorized" or "Rejected" or "Refunded")
        {
            await tx.CommitAsync(ct); return;
        }

        var finalStatus = approved ? "Authorized" : "Rejected";
        await using (var update = new SqlCommand("""
            UPDATE dbo.payment_attempts SET status=@status,provider_reference=@reference,decline_reason=@reason,
                last_error=NULL,next_reconcile_at_utc=NULL,reconciliation_owner=NULL,
                reconciliation_locked_until_utc=NULL,updated_at_utc=SYSUTCDATETIME()
            WHERE id=@id AND reconciliation_owner=@owner
            """, cn, tx))
        {
            update.Parameters.AddWithValue("status", finalStatus);
            update.Parameters.AddWithValue("reference", (object?)reference ?? DBNull.Value);
            update.Parameters.AddWithValue("reason", (object?)(reason is { Length: > 1000 }
            ? reason[..1000] : reason)
            ?? DBNull.Value);

            update.Parameters.AddWithValue("id", item.Id);

            update.Parameters.AddWithValue("owner", _owner);

            if (await update.ExecuteNonQueryAsync(ct) != 1)
            {
                await tx.RollbackAsync(ct); return;
            }
        }
        if (approved)
        {
            await using var ledger = new SqlCommand("""
                IF NOT EXISTS (SELECT 1 FROM dbo.payment_ledger WITH (UPDLOCK,HOLDLOCK) WHERE id=@id OR (reference=@reference AND entry_type='Charge'))
                  INSERT INTO dbo.payment_ledger(id,order_id,entry_type,amount,currency,reference,created_at_utc)
                  VALUES(@id,@order,'Charge',@amount,@currency,@reference,SYSUTCDATETIME())
                """, cn, tx);

            ledger.Parameters.AddWithValue("id", DeterministicGuid($"ledger:{item.Key}"));
            ledger.Parameters.AddWithValue("order", item.OrderId);
            ledger.Parameters.AddWithValue("amount", item.Amount);
            ledger.Parameters.AddWithValue("currency", item.Currency);
            ledger.Parameters.AddWithValue("reference", reference!);

            await ledger.ExecuteNonQueryAsync(ct);
            await AddOutboxAsync(cn, tx, item, "PaymentAuthorized", new { orderId = item.OrderId, paymentReference = reference, amount = item.Amount, currency = item.Currency }, ct);
        }
        else
        {
            await AddOutboxAsync(cn, tx, item, "PaymentRejected", new { orderId = item.OrderId, reason }, ct);
        }
        await tx.CommitAsync(ct);
    }

    private async Task RescheduleAsync(Candidate item, string error, CancellationToken ct)
    {
        await using var cn = new SqlConnection(options.ConnectionString);
        await cn.OpenAsync(ct);
        var nextAttempt = item.Attempts + 1;
        var delaySeconds = PaymentReconciliationPolicy.DelayForAttempt(nextAttempt).TotalSeconds;
        var terminal = PaymentReconciliationPolicy.RequiresManualReview(nextAttempt);
        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);

        if (terminal)
        {
            await using var manual = new SqlCommand("""
                UPDATE dbo.payment_attempts SET status='ManualReview', reconciliation_attempts=@attempts,
                    last_error=@error,next_reconcile_at_utc=NULL,reconciliation_owner=NULL,
                    reconciliation_locked_until_utc=NULL,updated_at_utc=SYSUTCDATETIME()
                WHERE id=@id AND reconciliation_owner=@owner AND status IN ('Unknown','Initiated')
                """, cn, tx);

            manual.Parameters.AddWithValue("attempts", nextAttempt);
            manual.Parameters.AddWithValue("error", Truncate(error));
            manual.Parameters.AddWithValue("id", item.Id);
            manual.Parameters.AddWithValue("owner", _owner);

            await manual.ExecuteNonQueryAsync(ct);
            log.LogError("Payment {PaymentId} moved to ManualReview after {Attempts} inquiry attempts", item.Id, nextAttempt);
        }
        else
        {
            await using var update = new SqlCommand("""
                UPDATE dbo.payment_attempts SET reconciliation_attempts=@attempts,last_error=@error,
                    next_reconcile_at_utc=DATEADD(SECOND,CONVERT(int,@delay),SYSUTCDATETIME()),
                    reconciliation_owner=NULL,reconciliation_locked_until_utc=NULL,updated_at_utc=SYSUTCDATETIME()
                WHERE id=@id AND reconciliation_owner=@owner AND status IN ('Unknown','Initiated')
                """, cn, tx);
            update.Parameters.AddWithValue("attempts", nextAttempt);
            update.Parameters.AddWithValue("error", Truncate(error));
            update.Parameters.AddWithValue("delay", delaySeconds);
            update.Parameters.AddWithValue("id", item.Id);

            update.Parameters.AddWithValue("owner", _owner);

            await update.ExecuteNonQueryAsync(ct);
        }
        await tx.CommitAsync(ct);
    }

    private static async Task AddOutboxAsync(SqlConnection cn, SqlTransaction tx, Candidate item,
        string eventType, object payload, CancellationToken ct)
    {
        var id = DeterministicGuid($"event:{eventType}:{item.Key}");
        var json = JsonSerializer.Serialize(new
        {
            eventId = id,
            eventType,
            eventVersion = 1,
            occurredAt = DateTimeOffset.UtcNow,
            producer = "payment-service",
            correlationId = item.OrderId,
            causationId = (Guid?)null,
            payload
        }, Json);

        await using var cmd = new SqlCommand("""
            IF NOT EXISTS (SELECT 1 FROM dbo.payment_outbox WITH (UPDLOCK,HOLDLOCK) WHERE id=@id)
              INSERT INTO dbo.payment_outbox(id,aggregate_id,event_type,payload,created_at_utc)
              VALUES(@id,@order,@type,@payload,SYSUTCDATETIME())
            """, cn, tx);

        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("order", item.OrderId);

        cmd.Parameters.AddWithValue("type", eventType); 
        cmd.Parameters.AddWithValue("payload", json);

        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static string Truncate(string value)
        => value.Length > 2000 ? value[..2000] : value;
    private static Guid DeterministicGuid(string value) 
        => new(SHA256.HashData(Encoding.UTF8.GetBytes(value))[..16]);
}
