namespace Workshop.Shared.Events;

public sealed record InventoryReservationRequested(Guid OrderId, IReadOnlyList<OrderLine> Lines);
