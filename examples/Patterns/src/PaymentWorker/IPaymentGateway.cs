namespace Workshop.PaymentWorker;

public interface IPaymentGateway
{
    Task<GatewayResult> ChargeAsync(Guid orderId, decimal amount, string currency, string idempotencyKey, CancellationToken ct);
    Task<ProviderPaymentStatus> QueryAsync(string idempotencyKey, CancellationToken ct);
    Task<bool> RefundAsync(string paymentReference, string idempotencyKey, CancellationToken ct);
    Task<ProviderPaymentStatus> QueryRefundAsync(string idempotencyKey, CancellationToken ct);
}
