namespace EipWorkshop.Reliability;

public sealed class Order
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string CustomerId { get; set; } = default!;
    public decimal Amount { get; set; }
    public string Status { get; set; } = "New";
}

