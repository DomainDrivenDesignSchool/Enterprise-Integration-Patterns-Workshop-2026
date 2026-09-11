using EipWorkshop.Core;
using EipWorkshop.Routing;
using Xunit;

namespace EipWorkshop.Tests;

public class SplitterTests
{
    [Fact]
    public void Split_ProducesOneMessagePerItem()
    {
        var splitter = new OrderBatchSplitter();
        var batch = new Message<OrderBatch>
        {
            CorrelationId = "corr-1",
            Payload = new OrderBatch("B1", new List<string> { "A", "B", "C" })
        };

        var parts = splitter.Split(batch).ToList();

        Assert.Equal(3, parts.Count);
    }

    [Fact]
    public void Split_AllPartsShareCorrelationId()
    {
        var splitter = new OrderBatchSplitter();
        var batch = new Message<OrderBatch>
        {
            CorrelationId = "corr-42",
            Payload = new OrderBatch("B1", new List<string> { "A", "B", "C" })
        };

        var parts = splitter.Split(batch).ToList();

        Assert.All(parts, p => Assert.Equal("corr-42", p.CorrelationId));
    }

    [Fact]
    public void Split_GeneratesCorrelationId_WhenMissing()
    {
        var splitter = new OrderBatchSplitter();
        var batch = new Message<OrderBatch>
        {
            Payload = new OrderBatch("B1", new List<string> { "A", "B" })
        };

        var parts = splitter.Split(batch).ToList();

        Assert.All(parts, p => Assert.NotNull(p.CorrelationId));
        Assert.Single(parts.Select(p => p.CorrelationId).Distinct());
    }

    [Fact]
    public void Split_SetsCausationIdToBatchId()
    {
        var splitter = new OrderBatchSplitter();
        var batch = new Message<OrderBatch>
        {
            CorrelationId = "corr-1",
            Payload = new OrderBatch("B1", new List<string> { "A", "B" })
        };

        var parts = splitter.Split(batch).ToList();

        Assert.All(parts, p => Assert.Equal(batch.Id, p.CausationId));
    }

    [Fact]
    public void Split_SetsIndexAndTotalHeaders()
    {
        var splitter = new OrderBatchSplitter();
        var batch = new Message<OrderBatch>
        {
            CorrelationId = "corr-1",
            Payload = new OrderBatch("B1", new List<string> { "A", "B", "C" })
        };

        var parts = splitter.Split(batch).ToList();

        Assert.Equal("0", parts[0].Headers["index"]);
        Assert.Equal("1", parts[1].Headers["index"]);
        Assert.Equal("2", parts[2].Headers["index"]);

        Assert.All(parts, p => Assert.Equal("3", p.Headers["total"]));
        Assert.All(parts, p => Assert.Equal("B1", p.Headers["batchId"]));
    }

    [Fact]
    public void Split_PreservesExistingHeaders()
    {
        var splitter = new OrderBatchSplitter();
        var batch = new Message<OrderBatch>
        {
            CorrelationId = "corr-1",
            Payload = new OrderBatch("B1", new List<string> { "A" })
        };
        batch.Headers["tenant"] = "acme";
        batch.Headers["trace-id"] = "trace-9";

        var parts = splitter.Split(batch).ToList();

        Assert.Equal("acme", parts[0].Headers["tenant"]);
        Assert.Equal("trace-9", parts[0].Headers["trace-id"]);
    }

    [Fact]
    public void Split_CopiesPayloadData()
    {
        var splitter = new OrderBatchSplitter();
        var batch = new Message<OrderBatch>
        {
            CorrelationId = "corr-1",
            Payload = new OrderBatch("B1", new List<string> { "A", "B", "C" })
        };

        var parts = splitter.Split(batch).ToList();

        Assert.Equal("A", parts[0].Payload.Data);
        Assert.Equal("B", parts[1].Payload.Data);
        Assert.Equal("C", parts[2].Payload.Data);
    }

    [Fact]
    public void Split_EmptyBatch_ProducesNoMessages()
    {
        var splitter = new OrderBatchSplitter();
        var batch = new Message<OrderBatch>
        {
            CorrelationId = "corr-1",
            Payload = new OrderBatch("B1", new List<string>())
        };

        var parts = splitter.Split(batch).ToList();

        Assert.Empty(parts);
    }

    [Fact]
    public void Split_IsLazy()
    {
        var splitter = new OrderBatchSplitter();
        var batch = new Message<OrderBatch>
        {
            CorrelationId = "corr-1",
            Payload = new OrderBatch("B1", new List<string> { "A", "B", "C" })
        };

        var enumerable = splitter.Split(batch);   // nothing happens yet
        var first = enumerable.First();            // only first message materialized

        Assert.Equal("A", first.Payload.Data);
    }
}