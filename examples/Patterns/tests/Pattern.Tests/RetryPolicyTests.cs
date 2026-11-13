using Workshop.Shared.Resilience;
using Xunit;
public class RetryPolicyTests
{
    [Fact] public void Delay_IsBoundedByMax() {
        var d = RetryPolicy.Delay(20, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5), new Random(1));
        Assert.InRange(d, TimeSpan.Zero, TimeSpan.FromSeconds(5));
    }
    [Fact] public void PermanentValidation_IsNotTransient() =>
        Assert.False(RetryPolicy.IsTransient(new ArgumentException()));
}
