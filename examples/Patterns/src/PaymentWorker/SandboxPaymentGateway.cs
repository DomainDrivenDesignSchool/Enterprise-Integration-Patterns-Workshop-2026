namespace Workshop.PaymentWorker;

/// <summary>Deterministic sandbox adapter. Replace with a PSP adapter; always pass the same idempotency key on retries.</summary>
public sealed class SandboxPaymentGateway : IPaymentGateway
{
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, ProviderPaymentStatus> _payments = new(StringComparer.Ordinal);
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, ProviderPaymentStatus> _refunds = new(StringComparer.Ordinal);
    private readonly bool _loseChargeResponseOnce;
    private readonly bool _loseRefundResponseOnce;
    private int _chargeFaultInjected;
    private int _refundFaultInjected;
    public SandboxPaymentGateway(bool loseChargeResponseOnce = false, bool loseRefundResponseOnce = false)
    {
        _loseChargeResponseOnce = loseChargeResponseOnce;
        _loseRefundResponseOnce = loseRefundResponseOnce;
    }
    public Task<GatewayResult> ChargeAsync(Guid orderId, decimal amount, string currency, string idempotencyKey, CancellationToken ct)
    {
        var approved = amount > 0 && amount <= 1_000_000m && currency is "IRR" or "USD" or "EUR";
        
        var result = approved
            ? new GatewayResult(true, $"sandbox-{orderId:N}", null)
            : new GatewayResult(false, null, "Sandbox risk rule declined amount or currency.");
       
        _payments[idempotencyKey] = approved
            ? new ProviderPaymentStatus(ProviderPaymentState.Authorized, result.ProviderReference)
            : new ProviderPaymentStatus(ProviderPaymentState.Declined, null, result.DeclineReason);
        // Fault injection: provider commits the charge, but the response is lost in transit.
        
        if (_loseChargeResponseOnce && Interlocked.Exchange(ref _chargeFaultInjected, 1) == 0)
            throw new TimeoutException("Injected fault: provider accepted charge but client did not receive response.");
        return Task.FromResult(result);
    }

    public Task<ProviderPaymentStatus> QueryAsync(string idempotencyKey, CancellationToken ct)
        => Task.FromResult(_payments.TryGetValue(idempotencyKey, out var value)
            ? value 
            : new ProviderPaymentStatus(ProviderPaymentState.NotFound));

    public Task<bool> RefundAsync(string paymentReference, string idempotencyKey, CancellationToken ct)
    {
        _refunds[idempotencyKey] = new ProviderPaymentStatus(ProviderPaymentState.Authorized, $"sandbox-refund-{paymentReference}");
        
        if (_loseRefundResponseOnce && Interlocked.Exchange(ref _refundFaultInjected, 1) == 0)
            throw new TimeoutException("Injected fault: provider accepted refund but client did not receive response.");
        return Task.FromResult(true);
    }
    public Task<ProviderPaymentStatus> QueryRefundAsync(string idempotencyKey, CancellationToken ct)
        => Task.FromResult(_refunds.TryGetValue(idempotencyKey, out var value) ? value : new ProviderPaymentStatus(ProviderPaymentState.NotFound));
}
