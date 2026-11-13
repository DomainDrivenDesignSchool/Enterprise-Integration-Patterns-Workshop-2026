namespace Workshop.Shared.Resilience;

public sealed class AsyncBulkhead : IDisposable
{
    private readonly SemaphoreSlim _slots;
    public AsyncBulkhead(int concurrency) => _slots = new SemaphoreSlim(concurrency, concurrency);
    public async Task<T> ExecuteAsync<T>(Func<CancellationToken, Task<T>> action, TimeSpan timeout, CancellationToken ct)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct);
        linked.CancelAfter(timeout);

        await _slots.WaitAsync(linked.Token);
        try
        {
            return await action(linked.Token);
        }
        finally
        {
            _slots.Release();
        }
    }
    public void Dispose() => _slots.Dispose();
}
