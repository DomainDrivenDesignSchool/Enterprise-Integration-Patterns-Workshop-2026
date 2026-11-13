namespace Workshop.Shared.Events;

public sealed record InventoryReserved(Guid OrderId, string ReservationId);
