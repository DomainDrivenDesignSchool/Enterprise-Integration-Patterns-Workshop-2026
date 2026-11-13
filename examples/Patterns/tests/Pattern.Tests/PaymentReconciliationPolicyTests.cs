using Workshop.PaymentWorker;
using Xunit;

namespace Workshop.Pattern.Tests;

public sealed class PaymentReconciliationPolicyTests
{
    [Theory]
    [InlineData(1, 15)]
    [InlineData(2, 30)]
    [InlineData(3, 60)]
    [InlineData(8, 1920)]
    [InlineData(9, 3600)]
    [InlineData(20, 3600)]
    public void Delay_IsExponentialAndBounded(int attempt, double expectedSeconds)
        => Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), PaymentReconciliationPolicy.DelayForAttempt(attempt));

    [Fact]
    public void ManualReview_IsRequiredAtConfiguredLimit()
    {
        Assert.False(PaymentReconciliationPolicy.RequiresManualReview(19));
        Assert.True(PaymentReconciliationPolicy.RequiresManualReview(20));
        Assert.True(PaymentReconciliationPolicy.RequiresManualReview(21));
    }

    [Fact]
    public void Delay_RejectsInvalidAttempt()
        => Assert.Throws<ArgumentOutOfRangeException>(() => PaymentReconciliationPolicy.DelayForAttempt(0));
}
