namespace Workshop.Shared.Events;

public sealed record OrderCreatedV1(Guid OrderId, string CustomerId, decimal Total, string Currency, DateTimeOffset CreatedAtUtc);
