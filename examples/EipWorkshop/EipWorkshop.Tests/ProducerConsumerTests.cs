using EipWorkshop.Core;
using EipWorkshop.Messaging;
using Xunit;

namespace EipWorkshop.Tests;

public class ProducerConsumerTests
{
    [Fact]
    public async Task ConsumerGroups_EachGroupGetsAllMessages_WithinGroupOnlyOne()
    {
        var broker = new InMemoryBroker();
        await broker.StartAsync();

        var groupA = new List<string>();
        var groupB = new List<string>();

        // consumer (load balancing)
        await broker.SubscribeAsync<string>("orders", "A", "A1",
            (m, _) => { lock (groupA) groupA.Add($"A1:{m.Payload}"); return Task.CompletedTask; });
        await broker.SubscribeAsync<string>("orders", "A", "A2",
            (m, _) => { lock (groupA) groupA.Add($"A2:{m.Payload}"); return Task.CompletedTask; });

        //consumer (fan-out )
        await broker.SubscribeAsync<string>("orders", "B", "B1",
            (m, _) => { lock (groupB) groupB.Add($"B1:{m.Payload}"); return Task.CompletedTask; });

        for (int i = 0; i < 6; i++)
            await broker.PublishAsync("orders", Message.Create($"M{i}"));

        await Task.Delay(300);

        Assert.Equal(6, groupA.Count);
        Assert.Equal(6, groupA.Distinct().Count());

        Assert.Equal(6, groupB.Count);
    }
}