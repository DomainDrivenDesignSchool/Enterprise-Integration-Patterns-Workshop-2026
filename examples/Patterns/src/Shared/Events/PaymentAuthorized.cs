namespace Workshop.Shared.Events;

public sealed record PaymentAuthorized(Guid OrderId, string PaymentReference, decimal Amount);
