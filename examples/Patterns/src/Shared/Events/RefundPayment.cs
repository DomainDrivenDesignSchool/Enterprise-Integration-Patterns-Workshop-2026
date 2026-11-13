namespace Workshop.Shared.Events;

public sealed record RefundPayment(Guid OrderId, string PaymentReference, string Reason);
