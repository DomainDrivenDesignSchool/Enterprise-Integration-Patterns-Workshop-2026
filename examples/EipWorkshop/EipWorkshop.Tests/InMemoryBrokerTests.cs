using EipWorkshop.Core;
using EipWorkshop.Messaging;
using Xunit;

namespace EipWorkshop.Tests;

public class InMemoryBrokerTests
{
    [Fact]
    public async Task Broker_ConsumerGroups_LoadBalanceAndFanOut()
    {
        var broker = new InMemoryBroker();
        await broker.StartAsync();

        var groupA = new System.Collections.Concurrent.ConcurrentBag<string>();
        var groupB = new System.Collections.Concurrent.ConcurrentBag<string>();

        await broker.SubscribeAsync<string>("orders", "A", "A1",
            (m, _) => { groupA.Add($"A1:{m.Payload}"); return Task.CompletedTask; });
        await broker.SubscribeAsync<string>("orders", "A", "A2",
            (m, _) => { groupA.Add($"A2:{m.Payload}"); return Task.CompletedTask; });
        await broker.SubscribeAsync<string>("orders", "B", "B1",
            (m, _) => { groupB.Add($"B1:{m.Payload}"); return Task.CompletedTask; });

        for (int i = 0; i < 6; i++)
            await broker.PublishAsync("orders", Message.Create($"M{i}"));

        await Task.Delay(300);

        // Group A: 6 messages split between A1 and A2
        Assert.Equal(6, groupA.Count);
        Assert.Equal(6, groupA.Select(x => x.Split(':')[1]).Distinct().Count());

        // Group B: all 6 messages
        Assert.Equal(6, groupB.Count);
    }

    [Fact]
    public async Task Broker_RoundRobin_WithinGroup()
    {
        var broker = new InMemoryBroker();
        await broker.StartAsync();

        var a1 = new System.Collections.Concurrent.ConcurrentBag<string>();
        var a2 = new System.Collections.Concurrent.ConcurrentBag<string>();

        await broker.SubscribeAsync<string>("orders", "A", "A1",
            (m, _) => { a1.Add(m.Payload!); return Task.CompletedTask; });
        await broker.SubscribeAsync<string>("orders", "A", "A2",
            (m, _) => { a2.Add(m.Payload!); return Task.CompletedTask; });

        for (int i = 0; i < 10; i++)
            await broker.PublishAsync("orders", Message.Create($"M{i}"));

        await Task.Delay(300);

        // With round-robin, each consumer gets ~5 messages
        Assert.Equal(5, a1.Count);
        Assert.Equal(5, a2.Count);
    }

    [Fact]
    public async Task Broker_SingleConsumer_ReceivesAll()
    {
        var broker = new InMemoryBroker();
        await broker.StartAsync();

        var received = new System.Collections.Concurrent.ConcurrentBag<string>();
        await broker.SubscribeAsync<string>("orders", "A", "A1",
            (m, _) => { received.Add(m.Payload!); return Task.CompletedTask; });

        for (int i = 0; i < 5; i++)
            await broker.PublishAsync("orders", Message.Create($"M{i}"));

        await Task.Delay(200);

        Assert.Equal(5, received.Count);
    }

    [Fact]
    public async Task Broker_PublishWithNoSubscribers_NoOp()
    {
        var broker = new InMemoryBroker();
        await broker.StartAsync();

        await broker.PublishAsync("nobody-listens", Message.Create("X"));

        Assert.True(true);   // no exception
    }


    [Fact]
    public async Task Broker_TopicsAreIsolated()
    {
        var broker = new InMemoryBroker();
        await broker.StartAsync();

        var orders = new System.Collections.Concurrent.ConcurrentBag<string>();
        var payments = new System.Collections.Concurrent.ConcurrentBag<string>();

        await broker.SubscribeAsync<string>("orders", "A", "O1",
            (m, _) => { orders.Add(m.Payload!); return Task.CompletedTask; });
        await broker.SubscribeAsync<string>("payments", "A", "P1",
            (m, _) => { payments.Add(m.Payload!); return Task.CompletedTask; });

        await broker.PublishAsync("orders", Message.Create("o-1"));
        await broker.PublishAsync("payments", Message.Create("p-1"));
        await broker.PublishAsync("orders", Message.Create("o-2"));

        await Task.Delay(200);

        Assert.Equal(2, orders.Count);
        Assert.Single(payments);
    }


    [Fact]
    public async Task Broker_RequiresStart()
    {
        var broker = new InMemoryBroker();
        // Not calling StartAsync

        var received = new System.Collections.Concurrent.ConcurrentBag<string>();
        await broker.SubscribeAsync<string>("orders", "A", "A1",
            (m, _) => { received.Add(m.Payload!); return Task.CompletedTask; });

        await broker.PublishAsync("orders", Message.Create("X"));

        await Task.Delay(100);

        // No worker started, so nothing is processed
        Assert.Empty(received);
    }
}