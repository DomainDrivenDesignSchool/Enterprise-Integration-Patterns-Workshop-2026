namespace Workshop.Contracts;

public sealed record InventoryReleased(Guid OrderId, string ReservationId);
