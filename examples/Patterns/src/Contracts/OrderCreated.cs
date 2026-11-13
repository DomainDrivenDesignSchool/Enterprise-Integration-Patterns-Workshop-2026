namespace Workshop.Contracts;

public sealed record OrderCreated(Guid OrderId, Guid CustomerId, decimal TotalAmount, string Currency,
    IReadOnlyList<OrderLine>? Lines = null);
