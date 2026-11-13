namespace Workshop.Shared.Events;

public sealed record PaymentRequested(Guid OrderId, decimal Amount, string Currency, string PaymentIdempotencyKey);
