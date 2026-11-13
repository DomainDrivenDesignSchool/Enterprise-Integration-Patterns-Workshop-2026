namespace Workshop.Contracts;

public sealed record InventoryRejected(Guid OrderId, string Reason);
