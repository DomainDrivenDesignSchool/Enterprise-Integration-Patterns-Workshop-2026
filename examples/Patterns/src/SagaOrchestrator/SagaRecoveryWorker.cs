using Microsoft.Data.SqlClient;

namespace Workshop.SagaOrchestrator;

/// <summary>Re-publishes the exact last command (same event ID and idempotency key) after a response timeout.</summary>
public sealed class SagaRecoveryWorker(SagaOptions options, ILogger<SagaRecoveryWorker> log) : BackgroundService
{
    private readonly string _owner = $"saga-recovery-{Environment.MachineName}-{Guid.NewGuid():N}";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var candidate = await ClaimAsync(stoppingToken);
                if (candidate is null) { await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken); continue; }
                await RecoverAsync(candidate, stoppingToken);
            }
            catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Saga recovery scan failed");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }

    private async Task<RecoveryCandidate?> ClaimAsync(CancellationToken ct)
    {
        await using var cn = new SqlConnection(options.ConnectionString); await cn.OpenAsync(ct);
        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);
        await using var cmd = new SqlCommand("""
            ;WITH due AS (
              SELECT TOP (1) order_id FROM dbo.saga_instances WITH (UPDLOCK,READPAST,ROWLOCK)
              WHERE status NOT IN ('Completed','Failed','DeadSaga') AND pending_command_type IS NOT NULL
                AND pending_since_utc<=DATEADD(MINUTE,-2,SYSUTCDATETIME())
                AND (recovery_locked_until_utc IS NULL OR recovery_locked_until_utc<SYSUTCDATETIME())
              ORDER BY pending_since_utc,order_id
            )
            UPDATE s SET recovery_lock_owner=@owner,recovery_locked_until_utc=DATEADD(SECOND,60,SYSUTCDATETIME())
            OUTPUT inserted.order_id,inserted.pending_command_type,inserted.recovery_attempts
            FROM dbo.saga_instances s JOIN due ON due.order_id=s.order_id;
            """, cn, tx);
        cmd.Parameters.AddWithValue("owner", _owner);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        RecoveryCandidate? result = null;
        if (await reader.ReadAsync(ct)) result = new RecoveryCandidate(reader.GetGuid(0), reader.GetString(1), reader.GetInt32(2));
        await reader.CloseAsync(); await tx.CommitAsync(ct); return result;
    }

    private async Task RecoverAsync(RecoveryCandidate candidate, CancellationToken ct)
    {
        await using var cn = new SqlConnection(options.ConnectionString); await cn.OpenAsync(ct);
        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);
        await using var verify = new SqlCommand("SELECT status,pending_command_type,recovery_attempts FROM dbo.saga_instances WITH (UPDLOCK,HOLDLOCK) WHERE order_id=@order AND recovery_lock_owner=@owner", cn, tx);
        verify.Parameters.AddWithValue("order", candidate.OrderId); verify.Parameters.AddWithValue("owner", _owner);
        string status; string? command; int attempts;
        await using (var reader = await verify.ExecuteReaderAsync(ct))
        {
            if (!await reader.ReadAsync(ct)) { await tx.RollbackAsync(ct); return; }
            status = reader.GetString(0); command = reader.IsDBNull(1) ? null : reader.GetString(1); attempts = reader.GetInt32(2);
        }
        if (status is "Completed" or "Failed" or "DeadSaga" || command is null)
        {
            await ReleaseLeaseAsync(cn, tx, candidate.OrderId, ct);
            await tx.CommitAsync(ct);
            return;
        }

        Guid? latestOutboxId = null;
        DateTimeOffset? publishedAt = null;
        await using (var latest = new SqlCommand("SELECT TOP (1) id,published_at_utc FROM dbo.saga_outbox WITH (UPDLOCK,HOLDLOCK) WHERE aggregate_id=@order AND event_type=@command ORDER BY aggregate_version DESC", cn, tx))
        {
            latest.Parameters.AddWithValue("order", candidate.OrderId); latest.Parameters.AddWithValue("command", command);
            await using var reader = await latest.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                latestOutboxId = reader.GetGuid(0);
                publishedAt = reader.IsDBNull(1) ? null : reader.GetDateTimeOffset(1);
            }
        }

        if (latestOutboxId is null)
        {
            await MoveToDeadSagaAsync(cn, tx, candidate.OrderId, "Pending command outbox record is missing", ct);
            await AuditAsync(cn, tx, candidate.OrderId, "AutoDeadSaga", "Pending command outbox row missing", ct);
        }
        else if (publishedAt is null)
        {
            // Do not count an attempt until Kafka acknowledged the original command.
            await ReleaseLeaseAsync(cn, tx, candidate.OrderId, ct);
        }
        else if (attempts >= 8)
        {
            await MoveToDeadSagaAsync(cn, tx, candidate.OrderId, "No response after 8 durable command re-publications", ct);
            await AuditAsync(cn, tx, candidate.OrderId, "AutoDeadSaga", "Recovery attempts exhausted", ct);
            log.LogWarning("Saga {OrderId} moved to DeadSaga after {Attempts} recoveries", candidate.OrderId, attempts);
        }
        else
        {
            await using var republish = new SqlCommand("UPDATE dbo.saga_outbox SET published_at_utc=NULL,lock_owner=NULL,locked_until_utc=NULL WHERE id=@id AND published_at_utc IS NOT NULL", cn, tx);
            republish.Parameters.AddWithValue("id", latestOutboxId.Value);
            var rows = await republish.ExecuteNonQueryAsync(ct);
            if (rows == 1)
            {
                await using var update = new SqlCommand("UPDATE dbo.saga_instances SET recovery_attempts=recovery_attempts+1,last_recovery_at_utc=SYSUTCDATETIME(),pending_since_utc=SYSUTCDATETIME(),recovery_lock_owner=NULL,recovery_locked_until_utc=NULL,updated_at_utc=SYSUTCDATETIME() WHERE order_id=@order AND recovery_lock_owner=@owner", cn, tx);
                update.Parameters.AddWithValue("order", candidate.OrderId); update.Parameters.AddWithValue("owner", _owner); await update.ExecuteNonQueryAsync(ct);
                await AuditAsync(cn, tx, candidate.OrderId, "CommandRepublished", $"Re-published {command}; attempt {attempts + 1}", ct);
            }
            else await ReleaseLeaseAsync(cn, tx, candidate.OrderId, ct);
        }
        await tx.CommitAsync(ct);
    }

    private async Task MoveToDeadSagaAsync(SqlConnection cn, SqlTransaction tx, Guid orderId, string reason, CancellationToken ct)
    {
        await using var dead = new SqlCommand("UPDATE dbo.saga_instances SET status='DeadSaga',dead_at_utc=SYSUTCDATETIME(),dead_reason=@reason,recovery_lock_owner=NULL,recovery_locked_until_utc=NULL,updated_at_utc=SYSUTCDATETIME() WHERE order_id=@order AND recovery_lock_owner=@owner", cn, tx);
        dead.Parameters.AddWithValue("reason", reason); dead.Parameters.AddWithValue("order", orderId); dead.Parameters.AddWithValue("owner", _owner);
        await dead.ExecuteNonQueryAsync(ct);
    }

    private static async Task ReleaseLeaseAsync(SqlConnection cn, SqlTransaction tx, Guid orderId, CancellationToken ct)
    {
        await using var cmd = new SqlCommand("UPDATE dbo.saga_instances SET recovery_lock_owner=NULL,recovery_locked_until_utc=NULL WHERE order_id=@order", cn, tx);
        cmd.Parameters.AddWithValue("order", orderId); await cmd.ExecuteNonQueryAsync(ct);
    }

    internal static async Task AuditAsync(SqlConnection cn, SqlTransaction tx, Guid orderId, string action, string detail, CancellationToken ct)
    {
        await using var cmd = new SqlCommand("INSERT INTO dbo.saga_operator_audit(id,order_id,actor,action,detail,created_at_utc) VALUES(@id,@order,@actor,@action,@detail,SYSUTCDATETIME())", cn, tx);
        cmd.Parameters.AddWithValue("id", Guid.NewGuid()); cmd.Parameters.AddWithValue("order", orderId); cmd.Parameters.AddWithValue("actor", "saga-recovery-worker");
        cmd.Parameters.AddWithValue("action", action); cmd.Parameters.AddWithValue("detail", detail); await cmd.ExecuteNonQueryAsync(ct);
    }

    private sealed record RecoveryCandidate(Guid OrderId, string CommandType, int Attempts);
}
