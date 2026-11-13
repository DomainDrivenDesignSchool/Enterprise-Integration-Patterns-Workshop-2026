using System.Threading.RateLimiting;

namespace Workshop.Shared.Resilience;

public sealed class TokenBucketLimiter : IAsyncDisposable
{
    private readonly TokenBucketRateLimiter _limiter;

    public TokenBucketLimiter(int tokenLimit, TimeSpan replenishmentPeriod, int tokensPerPeriod)
    {
        _limiter = new TokenBucketRateLimiter(new TokenBucketRateLimiterOptions
        {
            TokenLimit = tokenLimit, TokensPerPeriod = tokensPerPeriod,
            ReplenishmentPeriod = replenishmentPeriod, AutoReplenishment = true,
            QueueLimit = tokenLimit, QueueProcessingOrder = QueueProcessingOrder.OldestFirst
        });
    }
    public ValueTask<RateLimitLease> AcquireAsync(int permits, CancellationToken ct) => _limiter.AcquireAsync(permits, ct);
    public ValueTask DisposeAsync() => _limiter.DisposeAsync();
}
