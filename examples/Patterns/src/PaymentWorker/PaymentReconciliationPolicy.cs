namespace Workshop.PaymentWorker;

/// <summary>Pure policy so retry timing and escalation can be verified without Kafka or SQL Server.</summary>
public static class PaymentReconciliationPolicy
{
    public const int MaxAttempts = 20;
    public static TimeSpan DelayForAttempt(int attempt)
    {
        if (attempt < 1) throw new ArgumentOutOfRangeException(nameof(attempt));
        var exponent = Math.Min(attempt - 1, 8);
        return TimeSpan.FromSeconds(Math.Min(3600, 15 * Math.Pow(2, exponent)));
    }
    public static bool RequiresManualReview(int attempt) => attempt >= MaxAttempts;
}
