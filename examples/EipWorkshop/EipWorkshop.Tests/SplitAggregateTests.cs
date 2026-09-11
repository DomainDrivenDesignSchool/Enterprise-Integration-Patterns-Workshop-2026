using EipWorkshop.Core;
using EipWorkshop.Routing;
using Xunit;

namespace EipWorkshop.Tests;

public class SplitAggregateTests
{
    [Fact]
    public async Task Splitter_Aggregator_ReconstructsBatch()
    {
        var splitter = new OrderBatchSplitter();
        var aggregator = new Aggregator<BatchItem, string>(
            items => string.Join("|", items.Select(i => i.Payload.Data)),
            timeout: TimeSpan.FromSeconds(2));

        var batch = Message.Create(new OrderBatch("B1", new() { "A", "B", "C" }));
        batch.CorrelationId = "corr-1";

        var aggregationTask = Task.Run(async () =>
        {
            await Task.Delay(100);
            return await Task.FromResult<Message<string>?>(null);
        });

        var parts = splitter.Split(batch).ToList();
        var aggTasks = parts.Select(p => aggregator.AddAsync(p)).ToList();
        var result = await aggTasks.Last();

        Assert.Equal("A|B|C", result.Payload);
    }

    // stream
    // duration
    // aggregattion
    // time-box-window, 5minute . (12:00-12:05), (12:05-12:10)
    // (12:00-12:05), (12:01-12:06)
    [Fact]
    public async Task Aggregator_TimesOutWhenNotAllPartsArrive()
    {
        var aggregator = new Aggregator<BatchItem, string>(
            items => string.Join(",", items.Select(i => i.Payload.Data)),
            timeout: TimeSpan.FromMilliseconds(200));

        var part = new Message<BatchItem>
        {
            CorrelationId = "corr-x",
            Headers = { ["total"] = "3", ["index"] = "0" },
            Payload = new BatchItem("B1", 0, 3, "A")
        };

        var task = aggregator.AddAsync(part);
        await Assert.ThrowsAsync<TimeoutException>(() => task);
    }


    // microservices
    // get order query => microservice order
    // order, line item , 
    // get order query aggregator 
    // get order => order microservice => order vm
    // get customer info => custoemr mgmt => cusotmer vm
    // get product info => product mgmt => product vm
    
    // {
    //   order : ordervm,
    //   customer: customervm,
    //   product: product vm
    // }
    //  aggregate

    [Fact]
    public async Task ScatterGather_CollectsResponses()
    {
        var sg = new ScatterGather<string, string>(new List<Func<Message<string>, Task<Message<string>>>>
        {
            _ => Task.FromResult(Message.Create("pricing:100")),
            _ => Task.FromResult(Message.Create("inventory:yes")),
            async _ => { await Task.Delay(5000); return Message.Create("slow"); }
            // timeout
        }, timeout: TimeSpan.FromMilliseconds(500));

        var results = await sg.GatherAsync(Message.Create("sku-1"));
        Assert.Equal(2, results.Count);
    }
}