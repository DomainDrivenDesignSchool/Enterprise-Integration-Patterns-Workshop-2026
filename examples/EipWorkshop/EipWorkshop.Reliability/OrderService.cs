using System.Text.Json;
using Microsoft.EntityFrameworkCore;

namespace EipWorkshop.Reliability;

public sealed class OrderService
{
    private readonly AppDbContext _db;

    public OrderService(AppDbContext db) => _db = db;

    public async Task<Guid> PlaceOrderAsync(string customerId, decimal amount)
    {
        await using var tx = await _db.Database.BeginTransactionAsync();

        var order = new Order { CustomerId = customerId, Amount = amount };
        _db.Orders.Add(order);

        _db.Outbox.Add(new OutboxMessage
        {
            Type = "OrderPlaced",
            Payload = JsonSerializer.Serialize(new { order.Id, order.CustomerId, order.Amount })
        });

        await _db.SaveChangesAsync();
        await tx.CommitAsync(); 
        return order.Id;
    }
}