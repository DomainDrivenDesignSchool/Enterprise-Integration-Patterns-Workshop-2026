namespace Workshop.Shared.Events;

public sealed record InventoryRejected(Guid OrderId, string Reason);
