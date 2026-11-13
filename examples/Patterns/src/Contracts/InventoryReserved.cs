namespace Workshop.Contracts;

public sealed record InventoryReserved(Guid OrderId, string ReservationId);
