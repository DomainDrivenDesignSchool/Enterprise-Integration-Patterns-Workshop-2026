namespace Workshop.EventStore;

public sealed record SalesOrderCreated(Guid CustomerId, string Currency);
