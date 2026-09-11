using System.Collections.Concurrent;
using EipWorkshop.Core;

namespace EipWorkshop.Transformation;

public sealed record GetOrderQuery(string Id, string CustomerId, decimal Amount);
public sealed record CustomerInfo(string Id, string Name, string Tier);
public sealed record EnrichedOrder(string Id, string CustomerName, string Tier, decimal Amount, decimal Discount);

public sealed class CachedCustomerService
{

    // ACL, (Adapter + Trasnlation)
    private readonly Func<string, Task<CustomerInfo>> _loader;
    private readonly ConcurrentDictionary<string, CustomerInfo> _cache = new();

    public CachedCustomerService(Func<string, Task<CustomerInfo>> loader) => _loader = loader;

    public async Task<CustomerInfo> GetAsync(string id)
    {
        if (_cache.TryGetValue(id, out var cached)) return cached;
        var info = await _loader(id);
        _cache[id] = info;
        return info;
    }
}

public sealed class OrderEnricher
{
    private readonly CachedCustomerService _customers;

    public OrderEnricher(CachedCustomerService customers) => _customers = customers;

    public async Task<Message<EnrichedOrder>> EnrichAsync(Message<GetOrderQuery> order)
    {
        var customer = await _customers.GetAsync(order.Payload.CustomerId);
        var discount = customer.Tier switch
        {
            "Gold" => 0.10m,
            "Silver" => 0.05m,
            _ => 0m
        };

        // auto-mapper

        return new Message<EnrichedOrder>
        {
            CorrelationId = order.CorrelationId,
            CausationId = order.Id,
            Payload = new EnrichedOrder(
                order.Payload.Id, customer.Name, customer.Tier,
                order.Payload.Amount, order.Payload.Amount * discount)
        };
    }
}