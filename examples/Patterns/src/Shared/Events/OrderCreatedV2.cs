namespace Workshop.Shared.Events;

public sealed record OrderCreatedV2(Guid OrderId, string CustomerId, decimal Total, string Currency, DateTimeOffset CreatedAtUtc, string Region, IReadOnlyList<OrderLine> Lines);
