using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;

namespace Workshop.SagaOrchestrator;

public static class SagaOperationsEndpoints
{
    public static void MapSagaOperations(this WebApplication app)
    {
        app.MapGet("/ops/sagas/dead", ListDeadAsync);
        app.MapPost("/ops/sagas/{orderId:guid}/retry", RetryDeadAsync);
        app.MapGet("/health/live", () => Results.Ok(new { status = "live" }));
    }

    private static bool Authorized(HttpContext context, SagaOperationsOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.ApiKey) || !context.Request.Headers.TryGetValue("X-Operator-Key", out var supplied)) return false;
        var expectedBytes = Encoding.UTF8.GetBytes(options.ApiKey); var suppliedBytes = Encoding.UTF8.GetBytes(supplied.ToString());
        return expectedBytes.Length == suppliedBytes.Length && CryptographicOperations.FixedTimeEquals(expectedBytes, suppliedBytes);
    }

    private static async Task<IResult> ListDeadAsync(HttpContext context, SagaOptions saga, SagaOperationsOptions ops, CancellationToken ct)
    {
        if (!Authorized(context, ops)) return Results.Unauthorized();

        await using var cn = new SqlConnection(saga.ConnectionString);
        await cn.OpenAsync(ct);

        await using var cmd = new SqlCommand("SELECT TOP (200) order_id,status,pending_command_type,recovery_attempts,dead_at_utc,dead_reason,updated_at_utc FROM dbo.saga_instances WHERE status='DeadSaga' ORDER BY dead_at_utc DESC", cn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var rows = new List<object>();
        while (await reader.ReadAsync(ct)) rows.Add(new
        {
            orderId = reader.GetGuid(0),
            status = reader.GetString(1),
            pendingCommand = reader.IsDBNull(2) ? null : reader.GetString(2),
            attempts = reader.GetInt32(3),
            deadAtUtc = reader.GetDateTimeOffset(4),
            reason = reader.IsDBNull(5) ? null : reader.GetString(5),
            updatedAtUtc = reader.GetDateTimeOffset(6)
        });
        return Results.Ok(rows);
    }

    private static async Task<IResult> RetryDeadAsync(Guid orderId, HttpContext context, SagaOptions saga, SagaOperationsOptions ops, CancellationToken ct)
    {
        if (!Authorized(context, ops)) return Results.Unauthorized();

        var body = await context.Request.ReadFromJsonAsync<SagaRetryRequest>(cancellationToken: ct);

        if (body is null || string.IsNullOrWhiteSpace(body.Reason) || body.Reason.Length > 1000)
            return Results.BadRequest(new { error = "A non-empty operator reason (max 1000 chars) is required." });

        await using var cn = new SqlConnection(saga.ConnectionString); await cn.OpenAsync(ct);
        await using var tx = (SqlTransaction)await cn.BeginTransactionAsync(ct);

        string? command;
        await using (var read = new SqlCommand("SELECT pending_command_type FROM dbo.saga_instances WITH (UPDLOCK,HOLDLOCK) WHERE order_id=@order AND status='DeadSaga'", cn, tx))
        {
            read.Parameters.AddWithValue("order", orderId);
            command = (string?)await read.ExecuteScalarAsync(ct);
        }
        if (command is null)
        {
            await tx.RollbackAsync(ct);
            return Results.NotFound(new { error = "Dead Saga not found or no pending command is recorded." });
        }

        var resumeStatus = command switch
        {
            "RequestPayment" => "AwaitingPayment",
            "ReserveInventory" => "AwaitingInventory",
            "RequestShipment" => "AwaitingShipping",
            "RefundPayment" or "ReleaseInventory" => "Compensating",
            _ => null
        };

        if (resumeStatus is null)
        {
            await tx.RollbackAsync(ct);

            return Results.Conflict(new { error = "Pending command cannot be resumed automatically." });
        }
        await using var republish = new SqlCommand("""
            ;WITH latest AS (SELECT TOP (1) id FROM dbo.saga_outbox WITH (UPDLOCK,HOLDLOCK) WHERE aggregate_id=@order AND event_type=@command ORDER BY aggregate_version DESC)
            UPDATE o SET published_at_utc=NULL,lock_owner=NULL,locked_until_utc=NULL FROM dbo.saga_outbox o JOIN latest ON latest.id=o.id;
            SELECT @@ROWCOUNT;
            """, cn, tx);

        republish.Parameters.AddWithValue("order", orderId);
        republish.Parameters.AddWithValue("command", command);

        if (Convert.ToInt32(await republish.ExecuteScalarAsync(ct)) != 1)
        {
            await tx.RollbackAsync(ct); return Results.Conflict(new
            {
                error = "The original command cannot be found in the outbox."
            });
        }

        await using var update = new SqlCommand("UPDATE dbo.saga_instances SET status=@status,recovery_attempts=0,pending_since_utc=SYSUTCDATETIME(),last_recovery_at_utc=SYSUTCDATETIME(),dead_at_utc=NULL,dead_reason=NULL,recovery_lock_owner=NULL,recovery_locked_until_utc=NULL,updated_at_utc=SYSUTCDATETIME() WHERE order_id=@order AND status='DeadSaga'", cn, tx);

        update.Parameters.AddWithValue("status", resumeStatus);
        update.Parameters.AddWithValue("order", orderId);

        await update.ExecuteNonQueryAsync(ct);

        await using var audit = new SqlCommand("INSERT INTO dbo.saga_operator_audit(id,order_id,actor,action,detail,created_at_utc) VALUES(@id,@order,@actor,'ManualRetry',@detail,SYSUTCDATETIME())", cn, tx);
        audit.Parameters.AddWithValue("id", Guid.NewGuid());
        audit.Parameters.AddWithValue("order", orderId);

        audit.Parameters.AddWithValue("actor", context.User.Identity?.Name
            ?? context.Connection.RemoteIpAddress?.ToString() ?? "operator");

        audit.Parameters.AddWithValue("detail", body.Reason);

        await audit.ExecuteNonQueryAsync(ct);
        await tx.CommitAsync(ct);

        return Results.Accepted(value: new
        {
            orderId,
            status = resumeStatus,
            command,
            requeued = true
        });
    }

}
