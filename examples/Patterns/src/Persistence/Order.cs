namespace Workshop.Persistence;

public sealed class Order { public Guid Id {get;set;} public Guid CustomerId {get;set;} public decimal TotalAmount {get;set;} public string Currency {get;set;}="IRR"; public string Status {get;set;}="Pending"; public DateTimeOffset CreatedAt {get;set;} public List<OrderItem> Items {get;set;}=[]; }
