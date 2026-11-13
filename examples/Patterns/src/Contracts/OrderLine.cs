namespace Workshop.Contracts;

public sealed record OrderLine(string Sku, int Quantity, decimal UnitPrice);
