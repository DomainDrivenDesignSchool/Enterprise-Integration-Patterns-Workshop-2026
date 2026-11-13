namespace Workshop.Shared.Sharding;

/// <summary>Routes tenant-scoped reads and writes to the same owning shard.</summary>
public sealed class TenantCommandRouter<TSession>(IShardMap map, IShardSessionFactory<TSession> sessions)
    where TSession : IAsyncDisposable
{
    public async Task<TResult> ExecuteCommandAsync<TResult>(Guid tenantId,
        Func<TSession, CancellationToken, Task<TResult>> command, CancellationToken ct)
    {
        var endpoint = map.Resolve(tenantId);
        await using var session = await sessions.OpenAsync(endpoint, ct);
        return await command(session, ct);
    }

    public async Task<TResult> ExecuteQueryAsync<TResult>(Guid tenantId,
        Func<TSession, CancellationToken, Task<TResult>> query, CancellationToken ct)
    {
        var endpoint = map.Resolve(tenantId);
        await using var session = await sessions.OpenAsync(endpoint, ct);
        return await query(session, ct);
    }
}
