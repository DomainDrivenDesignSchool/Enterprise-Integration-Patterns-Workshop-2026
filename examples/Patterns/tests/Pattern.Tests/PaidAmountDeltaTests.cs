using Workshop.CdcEnricher;
using Xunit;
public class PaidAmountDeltaTests
{
    [Fact] public void PendingToPaid_EmitsPositiveAmount() =>
        Assert.Equal(125m, PaidAmountDeltaCalculator.Calculate("u", "Pending", 125m, "Paid", 125m));

    [Fact] public void PaidAmountChange_EmitsOnlyDifference() =>
        Assert.Equal(25m, PaidAmountDeltaCalculator.Calculate("u", "Paid", 100m, "Paid", 125m));

    [Fact] public void PaidToCancelled_EmitsNegativeCompensation() =>
        Assert.Equal(-100m, PaidAmountDeltaCalculator.Calculate("u", "Paid", 100m, "Cancelled", 100m));

    [Fact] public void DeletePaidOrder_EmitsNegativeAmount() =>
        Assert.Equal(-100m, PaidAmountDeltaCalculator.Calculate("d", "Paid", 100m, null, 100m));

    [Fact] public void PaidToCompleted_DoesNotReverseRevenue() =>
        Assert.Equal(0m, PaidAmountDeltaCalculator.Calculate("u", "Paid", 100m, "Completed", 100m));

    [Fact] public void CompletedToFailedAfterRefund_EmitsNegativeAmount() =>
        Assert.Equal(-100m, PaidAmountDeltaCalculator.Calculate("u", "Completed", 100m, "Failed", 100m));

    [Fact] public void NonFinancialStatusChange_EmitsZero() =>
        Assert.Equal(0m, PaidAmountDeltaCalculator.Calculate("u", "Pending", 100m, "Processing", 100m));
}
