namespace Workshop.Contracts;

public sealed record PaymentRejected(Guid OrderId, string Reason);
