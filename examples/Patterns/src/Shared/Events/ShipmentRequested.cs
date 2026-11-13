namespace Workshop.Shared.Events;

public sealed record ShipmentRequested(Guid OrderId, string ReservationId, OrderSnapshot Snapshot);
