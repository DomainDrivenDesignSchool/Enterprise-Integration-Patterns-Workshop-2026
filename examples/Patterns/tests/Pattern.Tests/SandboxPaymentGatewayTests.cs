using Workshop.PaymentWorker;
using Xunit;

namespace Workshop.Pattern.Tests;

public sealed class SandboxPaymentGatewayTests
{
    [Fact]
    public async Task LostChargeResponse_LeavesProviderAuthorizedAndQueryable()
    {
        var gateway = new SandboxPaymentGateway(loseChargeResponseOnce: true);
        var key = $"charge:{Guid.NewGuid():N}";

        await Assert.ThrowsAsync<TimeoutException>(() => gateway.ChargeAsync(Guid.NewGuid(), 250m, "EUR", key, CancellationToken.None));
        var state = await gateway.QueryAsync(key, CancellationToken.None);

        Assert.Equal(ProviderPaymentState.Authorized, state.State);
        Assert.False(string.IsNullOrWhiteSpace(state.ProviderReference));
    }

    [Fact]
    public async Task LostRefundResponse_LeavesRefundAuthorizedAndQueryable()
    {
        var gateway = new SandboxPaymentGateway(loseRefundResponseOnce: true);
        var key = $"refund:{Guid.NewGuid():N}";

        await Assert.ThrowsAsync<TimeoutException>(() => gateway.RefundAsync("provider-ref-1", key, CancellationToken.None));
        var state = await gateway.QueryRefundAsync(key, CancellationToken.None);

        Assert.Equal(ProviderPaymentState.Authorized, state.State);
    }
}
