namespace Workshop.EventStore;

public sealed record SalesOrderLineAdded(string Sku, int Quantity, decimal UnitPrice);
