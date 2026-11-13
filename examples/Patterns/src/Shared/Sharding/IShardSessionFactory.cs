namespace Workshop.Shared.Sharding;

public interface IShardSessionFactory<TSession> where TSession : IAsyncDisposable
{
    ValueTask<TSession> OpenAsync(ShardEndpoint endpoint, CancellationToken cancellationToken);
}
