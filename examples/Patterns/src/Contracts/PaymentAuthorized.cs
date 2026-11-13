namespace Workshop.Contracts;

public sealed record PaymentAuthorized(Guid OrderId, string PaymentReference, decimal Amount, string Currency);
