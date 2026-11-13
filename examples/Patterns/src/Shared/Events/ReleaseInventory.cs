namespace Workshop.Shared.Events;

public sealed record ReleaseInventory(Guid OrderId, string ReservationId);
