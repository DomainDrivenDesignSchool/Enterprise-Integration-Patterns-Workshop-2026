namespace Workshop.Shared.Events;

public sealed record OrderLine(string Sku, int Quantity, decimal UnitPrice);
