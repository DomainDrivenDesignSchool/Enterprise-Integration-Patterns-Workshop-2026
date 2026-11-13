namespace Workshop.Shared.Resilience;

public static class RetryPolicy
{
    public static TimeSpan Delay(int attempt, TimeSpan baseDelay, TimeSpan maxDelay, Random? random = null)
    {
        if (attempt < 1) 
            throw new ArgumentOutOfRangeException(nameof(attempt));

        random ??= Random.Shared;

        var exponent = Math.Min(attempt - 1, 20);

        var rawMs = Math.Min(maxDelay.TotalMilliseconds, baseDelay.TotalMilliseconds * Math.Pow(2, exponent));
        // Full jitter avoids synchronized retry waves.
        return TimeSpan.FromMilliseconds(random.NextDouble() * rawMs);
    }

    public static bool IsTransient(Exception ex) =>
        ex is TimeoutException or IOException or HttpRequestException;
}
