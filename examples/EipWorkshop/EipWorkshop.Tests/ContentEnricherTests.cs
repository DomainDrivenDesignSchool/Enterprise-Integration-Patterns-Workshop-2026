using EipWorkshop.Core;
using EipWorkshop.Transformation;
using Xunit;

namespace EipWorkshop.Tests;

public class ContentEnricherTests
{
    [Fact]
    public async Task Enricher_AddsCustomerData()
    {
        var svc = new CachedCustomerService(id =>
            Task.FromResult(new CustomerInfo(id, $"Name-{id}", "Gold")));
        var enricher = new OrderEnricher(svc);

        var result = await enricher.EnrichAsync(Message.Create(new GetOrderQuery("O1", "C1", 100)));

        Assert.Equal("Name-C1", result.Payload.CustomerName);
        Assert.Equal("Gold", result.Payload.Tier);
        Assert.Equal(100, result.Payload.Amount);
    }

    [Fact]
    public async Task Enricher_GoldTier_10PercentDiscount()
    {
        var svc = new CachedCustomerService(id =>
            Task.FromResult(new CustomerInfo(id, "Ali", "Gold")));
        var enricher = new OrderEnricher(svc);

        var result = await enricher.EnrichAsync(Message.Create(new GetOrderQuery("O1", "C1", 100)));

        Assert.Equal(10m, result.Payload.Discount);
    }

    [Fact]
    public async Task Enricher_SilverTier_5PercentDiscount()
    {
        var svc = new CachedCustomerService(id =>
            Task.FromResult(new CustomerInfo(id, "Reza", "Silver")));
        var enricher = new OrderEnricher(svc);

        var result = await enricher.EnrichAsync(Message.Create(new GetOrderQuery("O1", "C1", 200)));

        Assert.Equal(10m, result.Payload.Discount);
    }

    [Fact]
    public async Task Enricher_UnknownTier_NoDiscount()
    {
        var svc = new CachedCustomerService(id =>
            Task.FromResult(new CustomerInfo(id, "Sara", "Bronze")));
        var enricher = new OrderEnricher(svc);

        var result = await enricher.EnrichAsync(Message.Create(new GetOrderQuery("O1", "C1", 100)));

        Assert.Equal(0m, result.Payload.Discount);
    }

    [Fact]
    public async Task Enricher_CachesCustomerLookups()
    {
        int calls = 0;
        var svc = new CachedCustomerService(id =>
        {
            calls++;
            return Task.FromResult(new CustomerInfo(id, "Name", "Gold"));
        });
        var enricher = new OrderEnricher(svc);

        await enricher.EnrichAsync(Message.Create(new GetOrderQuery("O1", "C1", 100)));
        await enricher.EnrichAsync(Message.Create(new GetOrderQuery("O2", "C1", 200)));
        await enricher.EnrichAsync(Message.Create(new GetOrderQuery("O3", "C1", 300)));

        Assert.Equal(1, calls);   // C1 loaded once
    }

    [Fact]
    public async Task Enricher_CacheDistinguishesCustomers()
    {
        int calls = 0;
        var svc = new CachedCustomerService(id =>
        {
            calls++;
            return Task.FromResult(new CustomerInfo(id, $"Name-{id}", "Gold"));
        });
        var enricher = new OrderEnricher(svc);

        await enricher.EnrichAsync(Message.Create(new GetOrderQuery("O1", "C1", 100)));
        await enricher.EnrichAsync(Message.Create(new GetOrderQuery("O2", "C2", 200)));

        Assert.Equal(2, calls);
    }

    [Fact]
    public async Task Enricher_PreservesCorrelationId()
    {
        var svc = new CachedCustomerService(id =>
            Task.FromResult(new CustomerInfo(id, "Name", "Gold")));
        var enricher = new OrderEnricher(svc);

        var input = Message.Create(new GetOrderQuery("O1", "C1", 100), correlationId: "corr-123");
        var result = await enricher.EnrichAsync(input);

        Assert.Equal("corr-123", result.CorrelationId);
    }

    [Fact]
    public async Task Enricher_SetsCausationIdToInputId()
    {
        var svc = new CachedCustomerService(id =>
            Task.FromResult(new CustomerInfo(id, "Name", "Gold")));
        var enricher = new OrderEnricher(svc);

        var input = Message.Create(new GetOrderQuery("O1", "C1", 100));
        var result = await enricher.EnrichAsync(input);

        Assert.Equal(input.Id, result.CausationId);
    }

    [Fact]
    public async Task Enricher_DoesNotCopyHeaders_CurrentBehavior()
    {
        var svc = new CachedCustomerService(id =>
            Task.FromResult(new CustomerInfo(id, "Name", "Gold")));
        var enricher = new OrderEnricher(svc);

        var input = Message.Create(new GetOrderQuery("O1", "C1", 100));
        input.Headers["tenant"] = "acme";

        var result = await enricher.EnrichAsync(input);

        // ❌ Documenting the bug: headers are lost
        Assert.False(result.Headers.ContainsKey("tenant"));
    }

    [Fact]
    public async Task Enricher_SupportsAsyncLoader()
    {
        var svc = new CachedCustomerService(async id =>
        {
            await Task.Delay(10);
            return new CustomerInfo(id, "Async", "Silver");
        });
        var enricher = new OrderEnricher(svc);

        var result = await enricher.EnrichAsync(Message.Create(new GetOrderQuery("O1", "C1", 100)));

        Assert.Equal("Async", result.Payload.CustomerName);
    }

    [Fact]
    public async Task Enricher_PropagatesLoaderException()
    {
        var svc = new CachedCustomerService(_ =>
            throw new HttpRequestException("customer service down"));
        var enricher = new OrderEnricher(svc);

        await Assert.ThrowsAsync<HttpRequestException>(() =>
            enricher.EnrichAsync(Message.Create(new GetOrderQuery("O1", "C1", 100))));
    }
}