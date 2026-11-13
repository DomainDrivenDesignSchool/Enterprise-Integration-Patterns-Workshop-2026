public sealed record CreateOrderRequest(Guid CustomerId, IReadOnlyList<CreateOrderLineRequest> Lines, string Currency = "IRR", Guid CorrelationId = default, decimal TotalAmount = 0);
