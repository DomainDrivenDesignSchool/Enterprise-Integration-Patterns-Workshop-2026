namespace Workshop.Shared.Events;

public sealed record PaymentRejected(Guid OrderId, string Reason);
