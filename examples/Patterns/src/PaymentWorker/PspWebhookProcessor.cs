using System.Text.Json;
using Microsoft.Data.SqlClient;

namespace Workshop.PaymentWorker;

/// <summary>Durable Inbox consumer. Callback payloads are signals; conflicting/late signals never rewrite terminal financial state.</summary>
public sealed class PspWebhookProcessor(PaymentOptions options, ILogger<PspWebhookProcessor> log) : BackgroundService
{
    private readonly string _owner = $"psp-webhook-{Environment.MachineName}-{Guid.NewGuid():N}";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var item = await ClaimAsync(stoppingToken);
                if (item is null)
                {
                    await Task.Delay(500, stoppingToken);
                    continue;
                }
                await ProcessAsync(item, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "PSP webhook processing loop failed");
                await Task.Delay(1000, stoppingToken);
            }
        }
    }

    private async Task<WebhookItem?> ClaimAsync(CancellationToken ct)
    {
        await using var cn = new SqlConnection(options.ConnectionString); await cn.OpenAsync(ct);
        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);
        await using var cmd = new SqlCommand("""
            ;WITH candidate AS (
                SELECT TOP (1) id FROM dbo.psp_webhook_inbox WITH (UPDLOCK,READPAST,ROWLOCK)
                WHERE status IN ('Pending','Retry','Processing') AND next_attempt_at_utc<=SYSUTCDATETIME()
                  AND (locked_until_utc IS NULL OR locked_until_utc<SYSUTCDATETIME())
                ORDER BY received_at_utc,id
            )
            UPDATE target SET status='Processing',lock_owner=@owner,locked_until_utc=DATEADD(SECOND,60,SYSUTCDATETIME()),attempts=attempts+1
            OUTPUT inserted.id,inserted.provider,inserted.provider_event_id,inserted.event_type,inserted.idempotency_key,
                   inserted.provider_reference,inserted.occurred_at_utc,inserted.payload_hash
            FROM dbo.psp_webhook_inbox target JOIN candidate ON target.id=candidate.id;
            """, cn, tx);
        cmd.Parameters.AddWithValue("owner", _owner);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        WebhookItem? item = null;
        if (await reader.ReadAsync(ct)) item = new WebhookItem(reader.GetInt64(0), reader.GetString(1), reader.GetString(2), reader.GetString(3),
            reader.GetString(4), reader.IsDBNull(5) ? null : reader.GetString(5), reader.GetDateTimeOffset(6), reader.GetString(7));
        await reader.CloseAsync(); await tx.CommitAsync(ct); return item;
    }

    private async Task ProcessAsync(WebhookItem item, CancellationToken ct)
    {
        await using var cn = new SqlConnection(options.ConnectionString); await cn.OpenAsync(ct);
        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);
        try
        {
            await using var lease = new SqlCommand("SELECT status,lock_owner FROM dbo.psp_webhook_inbox WITH (UPDLOCK,HOLDLOCK) WHERE id=@id", cn, tx);
            lease.Parameters.AddWithValue("id", item.Id);
            await using (var reader = await lease.ExecuteReaderAsync(ct))
            {
                if (!await reader.ReadAsync(ct) || reader.GetString(0) != "Processing" || reader.GetString(1) != _owner)
                { await tx.RollbackAsync(ct); return; }
            }

            var terminalStatus = item.EventType.StartsWith("payment.", StringComparison.Ordinal) ?
                await ApplyPaymentEventAsync(cn, tx, item, ct) : await ApplyRefundEventAsync(cn, tx, item, ct);
            await using var mark = new SqlCommand("UPDATE dbo.psp_webhook_inbox SET status=@status,processed_at_utc=SYSUTCDATETIME(),locked_until_utc=NULL,lock_owner=NULL,last_error=@error WHERE id=@id AND lock_owner=@owner", cn, tx);
            mark.Parameters.AddWithValue("status", terminalStatus);
            mark.Parameters.AddWithValue("error", terminalStatus == "Conflict" ? "Callback conflicts with a terminal or incompatible payment state; provider inquiry/manual review required." : DBNull.Value);
            mark.Parameters.AddWithValue("id", item.Id); mark.Parameters.AddWithValue("owner", _owner);
            await mark.ExecuteNonQueryAsync(ct);
            await tx.CommitAsync(ct);
        }
        catch (WebhookNotReadyException ex)
        {
            await tx.RollbackAsync(ct);
            await using var retry = new SqlCommand("UPDATE dbo.psp_webhook_inbox SET status=CASE WHEN attempts>=20 THEN 'Dead' ELSE 'Retry' END,next_attempt_at_utc=DATEADD(SECOND,CASE WHEN attempts>=20 THEN 0 ELSE CASE WHEN POWER(CAST(2 AS float),CASE WHEN attempts>12 THEN 12 ELSE attempts END)>3600 THEN 3600 ELSE CAST(POWER(CAST(2 AS float),CASE WHEN attempts>12 THEN 12 ELSE attempts END) AS int) END END,SYSUTCDATETIME()),locked_until_utc=NULL,lock_owner=NULL,last_error=@error WHERE id=@id AND lock_owner=@owner", cn);
            retry.Parameters.AddWithValue("error", ex.Message.Length > 1000 ? ex.Message[..1000] : ex.Message); retry.Parameters.AddWithValue("id", item.Id); retry.Parameters.AddWithValue("owner", _owner);
            await retry.ExecuteNonQueryAsync(ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            await tx.RollbackAsync(CancellationToken.None);
            throw;
        }
        catch (Exception ex)
        {
            await tx.RollbackAsync(ct);
            await using var retry = new SqlCommand("UPDATE dbo.psp_webhook_inbox SET status=CASE WHEN attempts>=20 THEN 'Dead' ELSE 'Retry' END,next_attempt_at_utc=DATEADD(SECOND,CASE WHEN attempts>=20 THEN 0 ELSE CASE WHEN POWER(CAST(2 AS float),CASE WHEN attempts>12 THEN 12 ELSE attempts END)>3600 THEN 3600 ELSE CAST(POWER(CAST(2 AS float),CASE WHEN attempts>12 THEN 12 ELSE attempts END) AS int) END END,SYSUTCDATETIME()),locked_until_utc=NULL,lock_owner=NULL,last_error=@error WHERE id=@id AND lock_owner=@owner", cn);
            retry.Parameters.AddWithValue("error", ex.Message.Length > 1000 ? ex.Message[..1000] : ex.Message); retry.Parameters.AddWithValue("id", item.Id); retry.Parameters.AddWithValue("owner", _owner);
            await retry.ExecuteNonQueryAsync(ct);
            log.LogError(ex, "Webhook Inbox item {InboxId} failed; it will be retried or dead-lettered", item.Id);
        }
    }

    private static async Task<string> ApplyPaymentEventAsync(SqlConnection cn, SqlTransaction tx, WebhookItem item, CancellationToken ct)
    {
        string status;
        await using (var read = new SqlCommand("SELECT status FROM dbo.payment_attempts WITH (UPDLOCK,HOLDLOCK) WHERE idempotency_key=@key", cn, tx))
        {
            read.Parameters.AddWithValue("key", item.IdempotencyKey);
            await using var r = await read.ExecuteReaderAsync(ct);
            if (!await r.ReadAsync(ct)) throw new WebhookNotReadyException("Payment attempt is not persisted yet.");
            status = r.GetString(0);
        }

        // A callback is only a wake-up signal. Inquiry is authoritative, which prevents
        // two out-of-order contradictory callbacks from choosing the financial outcome.
        if (status is "Authorized" or "Rejected" or "Refunded" or "ManualReview")
            return (item.EventType == "payment.authorized" && status == "Authorized") ||
                   (item.EventType == "payment.declined" && status == "Rejected") ? "Processed" : "Conflict";
        await using var wake = new SqlCommand("""
            UPDATE dbo.payment_attempts SET status='Unknown',next_reconcile_at_utc=SYSUTCDATETIME(),
                reconciliation_owner=NULL,reconciliation_locked_until_utc=NULL,
                last_error='Verified PSP webhook received; provider inquiry scheduled',updated_at_utc=SYSUTCDATETIME()
            WHERE idempotency_key=@key AND status IN ('Initiated','Unknown')
            """, cn, tx);
        wake.Parameters.AddWithValue("key", item.IdempotencyKey);
        return await wake.ExecuteNonQueryAsync(ct) == 1 ? "Processed" : "Conflict";
    }

    private static async Task<string> ApplyRefundEventAsync(SqlConnection cn, SqlTransaction tx, WebhookItem item, CancellationToken ct)
    {
        string status;
        await using (var read = new SqlCommand("SELECT status FROM dbo.payment_refund_attempts WITH (UPDLOCK,HOLDLOCK) WHERE idempotency_key=@key", cn, tx))
        {
            read.Parameters.AddWithValue("key", item.IdempotencyKey);
            var value = await read.ExecuteScalarAsync(ct);
            if (value is null) throw new WebhookNotReadyException("Refund attempt is not persisted yet.");
            status = (string)value;
        }
        var success = item.EventType == "refund.succeeded";
        if (status is "Refunded" or "Rejected" or "ManualReview")
            return (success && status == "Refunded") || (!success && status == "Rejected") ? "Processed" : "Conflict";

        await using var wake = new SqlCommand("""
            UPDATE dbo.payment_refund_attempts SET status='Unknown',next_reconcile_at_utc=SYSUTCDATETIME(),
                reconciliation_owner=NULL,reconciliation_locked_until_utc=NULL,
                last_error='Verified PSP webhook received; provider inquiry scheduled',updated_at_utc=SYSUTCDATETIME()
            WHERE idempotency_key=@key AND status IN ('Initiated','Unknown')
            """, cn, tx);
        wake.Parameters.AddWithValue("key", item.IdempotencyKey);
        return await wake.ExecuteNonQueryAsync(ct) == 1 ? "Processed" : "Conflict";
    }

    private sealed record WebhookItem(long Id, string Provider, string ProviderEventId, string EventType, string IdempotencyKey, string? ProviderReference, DateTimeOffset OccurredAt, string PayloadHash);
    private sealed class WebhookNotReadyException(string message) : Exception(message);
}
