namespace Workshop.Shared.Events;

public sealed record OrderSnapshot(Guid OrderId, string CustomerId, decimal Total, string Currency, IReadOnlyList<OrderLine> Lines);
