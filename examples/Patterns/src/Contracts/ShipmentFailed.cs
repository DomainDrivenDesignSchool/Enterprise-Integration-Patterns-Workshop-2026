namespace Workshop.Contracts;

public sealed record ShipmentFailed(Guid OrderId, string Reason);
