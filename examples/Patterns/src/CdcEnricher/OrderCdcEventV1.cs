namespace Workshop.CdcEnricher;

public sealed record OrderCdcEventV1(Guid EventId, string EventType, int SchemaVersion,
    DateTimeOffset OccurredAtUtc, string Operation, string SourceTable, string SourceLsn, Guid OrderId,
    Guid CustomerId, string? BeforeStatus, string Status, decimal TotalAmount, decimal PaidAmountDelta, string Currency,
    string? CustomerTier, string? CustomerRegion, string? ShippingZone);
