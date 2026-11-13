namespace Workshop.Shared.Events;

public sealed record ShipmentCreated(Guid OrderId, string ShipmentId);
