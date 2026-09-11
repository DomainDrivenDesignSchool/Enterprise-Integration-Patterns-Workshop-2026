using EipWorkshop.Core;
using EipWorkshop.Routing;
using Xunit;

namespace EipWorkshop.Tests;

public class AggregatorTests
{
    private static Message<string> Part(string correlation, int index, int total, string data)
        => new()
        {
            CorrelationId = correlation,
            Headers =
            {
                ["index"] = index.ToString(),
                ["total"] = total.ToString()
            },
            Payload = data
        };

    [Fact]
    public async Task Aggregator_ReconstructsInOrder()
    {
        var agg = new Aggregator<string, string>(
            parts => string.Join("|", parts.Select(p => p.Payload)),
            timeout: TimeSpan.FromSeconds(5));

        var t1 = agg.AddAsync(Part("c1", 0, 3, "A"));
        var t2 = agg.AddAsync(Part("c1", 1, 3, "B"));
        var t3 = agg.AddAsync(Part("c1", 2, 3, "C"));

        var result = await t3;   // all three return the same task

        Assert.Equal("A|B|C", result.Payload);
    }

    [Fact]
    public async Task Aggregator_AllCallersGetSameResult()
    {
        var agg = new Aggregator<int, int>(
            parts => parts.Sum(p => p.Payload),
            timeout: TimeSpan.FromSeconds(5));

        var t1 = agg.AddAsync(new Message<int>
        {
            CorrelationId = "c1",
            Headers = { ["index"] = "0", ["total"] = "3" },
            Payload = 10
        });
        var t2 = agg.AddAsync(new Message<int>
        {
            CorrelationId = "c1",
            Headers = { ["index"] = "1", ["total"] = "3" },
            Payload = 20
        });
        var t3 = agg.AddAsync(new Message<int>
        {
            CorrelationId = "c1",
            Headers = { ["index"] = "2", ["total"] = "3" },
            Payload = 30
        });

        var results = await Task.WhenAll(t1, t2, t3);

        Assert.All(results, r => Assert.Equal(60, r.Payload));
    }

    [Fact]
    public async Task Aggregator_ReordersOutOfOrderParts()
    {
        var agg = new Aggregator<string, string>(
            parts => string.Join("", parts.Select(p => p.Payload)),
            timeout: TimeSpan.FromSeconds(5));

        var t1 = agg.AddAsync(Part("c1", 2, 3, "C"));
        var t2 = agg.AddAsync(Part("c1", 0, 3, "A"));
        var t3 = agg.AddAsync(Part("c1", 1, 3, "B"));

        var result = await t3;

        Assert.Equal("ABC", result.Payload);
    }

    [Fact]
    public async Task Aggregator_PreservesCorrelationId()
    {
        var agg = new Aggregator<string, string>(
            parts => string.Join("", parts.Select(p => p.Payload)),
            timeout: TimeSpan.FromSeconds(5));

        var t1 = agg.AddAsync(Part("corr-xyz", 0, 2, "A"));
        var t2 = agg.AddAsync(Part("corr-xyz", 1, 2, "B"));

        var result = await t2;

        Assert.Equal("corr-xyz", result.CorrelationId);
    }

    [Fact]
    public async Task Aggregator_Throws_WhenCorrelationIdMissing()
    {
        var agg = new Aggregator<string, string>(
            parts => string.Join("", parts.Select(p => p.Payload)));

        var part = new Message<string>
        {
            Headers = { ["index"] = "0", ["total"] = "1" },
            Payload = "A"
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() => agg.AddAsync(part));
    }

    [Fact]
    public async Task Aggregator_Throws_WhenTotalHeaderMissing()
    {
        var agg = new Aggregator<string, string>(
            parts => string.Join("", parts.Select(p => p.Payload)));

        var part = new Message<string>
        {
            CorrelationId = "c1",
            Headers = { ["index"] = "0" },
            Payload = "A"
        };

        await Assert.ThrowsAsync<KeyNotFoundException>(() => agg.AddAsync(part));
    }

    [Fact]
    public async Task Aggregator_HandlesTwoConcurrentAggregations()
    {
        var agg = new Aggregator<string, string>(
            parts => string.Join("", parts.Select(p => p.Payload)),
            timeout: TimeSpan.FromSeconds(5));

        var t1 = agg.AddAsync(Part("c1", 0, 2, "A"));
        var t2 = agg.AddAsync(Part("c2", 0, 2, "X"));
        var t3 = agg.AddAsync(Part("c1", 1, 2, "B"));
        var t4 = agg.AddAsync(Part("c2", 1, 2, "Y"));

        var r1 = await t3;
        var r2 = await t4;

        Assert.Equal("AB", r1.Payload);
        Assert.Equal("XY", r2.Payload);
    }

    [Fact]
    public async Task Aggregator_SortsNumerically_NotLexicographically()
    {
        var agg = new Aggregator<int, string>(
            parts => string.Join(",", parts.Select(p => p.Payload)),
            timeout: TimeSpan.FromSeconds(5));

        // 11 parts so indexes reach "10"
        var tasks = Enumerable.Range(0, 11)
            .Select(i => agg.AddAsync(new Message<int>
            {
                CorrelationId = "c1",
                Headers = { ["index"] = i.ToString(), ["total"] = "11" },
                Payload = i
            }))
            .ToList();

        var result = await tasks.Last();

        Assert.Equal("0,1,2,3,4,5,6,7,8,9,10", result.Payload);
    }

    [Fact]
    public async Task Aggregator_SinglePart_CompletesImmediately()
    {
        var agg = new Aggregator<string, string>(
            parts => parts.Single().Payload,
            timeout: TimeSpan.FromSeconds(5));

        var result = await agg.AddAsync(Part("c1", 0, 1, "only"));

        Assert.Equal("only", result.Payload);
    }

    [Fact]
    public async Task Aggregator_IsThreadSafe_UnderConcurrentAdds()
    {
        var agg = new Aggregator<int, int>(
            parts => parts.Sum(p => p.Payload),
            timeout: TimeSpan.FromSeconds(10));

        var tasks = Enumerable.Range(0, 100)
            .Select(i => Task.Run(() => agg.AddAsync(new Message<int>
            {
                CorrelationId = "c1",
                Headers = { ["index"] = i.ToString(), ["total"] = "100" },
                Payload = i
            })))
            .ToList();

        var all = await Task.WhenAll(tasks);
        var result = all.Last();

        Assert.Equal(Enumerable.Range(0, 100).Sum(), result.Payload);
    }

    [Fact]
    public async Task Splitter_Aggregator_RoundTrip()
    {
        var splitter = new OrderBatchSplitter();
        var agg = new Aggregator<BatchItem, string>(
            parts => string.Join("|", parts.Select(p => p.Payload.Data)),
            timeout: TimeSpan.FromSeconds(5));

        var batch = new Message<OrderBatch>
        {
            CorrelationId = "corr-1",
            Payload = new OrderBatch("B1", new List<string> { "A", "B", "C" })
        };

        var parts = splitter.Split(batch).ToList();
        var tasks = parts.Select(p => agg.AddAsync(p)).ToList();
        var result = await tasks.Last();

        Assert.Equal("A|B|C", result.Payload);
        Assert.Equal("corr-1", result.CorrelationId);
    }
}