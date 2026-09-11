using EipWorkshop.Core;
using EipWorkshop.Routing;
using Xunit;

namespace EipWorkshop.Tests;

public class ScatterGatherTests
{
    [Fact]
    public async Task ScatterGather_CollectsAllResponses()
    {
        var sg = new ScatterGather<string, string>(new List<Func<Message<string>, Task<Message<string>>>>
        {
            _ => Task.FromResult(Message.Create("pricing:100")),
            _ => Task.FromResult(Message.Create("inventory:yes")),
            _ => Task.FromResult(Message.Create("shipping:2d"))
        }, timeout: TimeSpan.FromSeconds(2));

        var results = await sg.GatherAsync(Message.Create("sku-1"));

        Assert.Equal(3, results.Count);
        Assert.Contains(results, r => r.Payload == "pricing:100");
        Assert.Contains(results, r => r.Payload == "inventory:yes");
        Assert.Contains(results, r => r.Payload == "shipping:2d");
    }

    [Fact]
    public async Task ScatterGather_ExcludesSlowRecipient()
    {
        var sg = new ScatterGather<string, string>(new List<Func<Message<string>, Task<Message<string>>>>
        {
            _ => Task.FromResult(Message.Create("fast-1")),
            _ => Task.FromResult(Message.Create("fast-2")),
            async _ => { await Task.Delay(5000); return Message.Create("slow"); }
        }, timeout: TimeSpan.FromMilliseconds(300));

        var results = await sg.GatherAsync(Message.Create("sku-1"));

        Assert.Equal(2, results.Count);
        Assert.DoesNotContain(results, r => r.Payload == "slow");
    }

    [Fact]
    public async Task ScatterGather_ExcludesFaultedRecipient()
    {
        var sg = new ScatterGather<string, string>(new List<Func<Message<string>, Task<Message<string>>>>
        {
            _ => Task.FromResult(Message.Create("ok")),
            _ => throw new InvalidOperationException("recipient down")
        }, timeout: TimeSpan.FromSeconds(2));

        var results = await sg.GatherAsync(Message.Create("sku-1"));

        Assert.Single(results);
        Assert.Equal("ok", results[0].Payload);
    }

    [Fact]
    public async Task ScatterGather_NoRecipients_ReturnsEmpty()
    {
        var sg = new ScatterGather<string, string>(
            new List<Func<Message<string>, Task<Message<string>>>>(),
            timeout: TimeSpan.FromMilliseconds(100));

        var results = await sg.GatherAsync(Message.Create("sku-1"));

        Assert.Empty(results);
    }

    [Fact]
    public async Task ScatterGather_AllFail_ReturnsEmpty()
    {
        var sg = new ScatterGather<string, string>(new List<Func<Message<string>, Task<Message<string>>>>
        {
            _ => throw new InvalidOperationException("down-1"),
            _ => throw new InvalidOperationException("down-2")
        }, timeout: TimeSpan.FromSeconds(1));

        var results = await sg.GatherAsync(Message.Create("sku-1"));

        Assert.Empty(results);
    }

    [Fact]
    public async Task ScatterGather_RespectsCancellation()
    {
        var sg = new ScatterGather<string, string>(new List<Func<Message<string>, Task<Message<string>>>>
        {
            async _ => { await Task.Delay(5000); return Message.Create("slow"); }
        }, timeout: TimeSpan.FromSeconds(10));

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => sg.GatherAsync(Message.Create("sku-1"), cts.Token));
    }

    [Fact]
    public async Task ScatterGather_RunsRecipientsInParallel()
    {
        var startedAt = DateTimeOffset.UtcNow;

        var sg = new ScatterGather<string, string>(new List<Func<Message<string>, Task<Message<string>>>>
        {
            async _ => { await Task.Delay(300); return Message.Create("r1"); },
            async _ => { await Task.Delay(300); return Message.Create("r2"); },
            async _ => { await Task.Delay(300); return Message.Create("r3"); }
        }, timeout: TimeSpan.FromSeconds(2));

        await sg.GatherAsync(Message.Create("sku-1"));

        var elapsed = DateTimeOffset.UtcNow - startedAt;

        // If sequential: 900ms. If parallel: ~300ms.
        Assert.True(elapsed < TimeSpan.FromMilliseconds(600),
            $"Expected parallel execution, took {elapsed.TotalMilliseconds}ms");
    }

    [Fact]
    public async Task ScatterGather_RealisticProductLookup()
    {
        var sg = new ScatterGather<string, string>(new List<Func<Message<string>, Task<Message<string>>>>
        {
            // pricing service — fast
            _ => Task.FromResult(Message.Create("price=99.99")),

            // inventory service — fast
            _ => Task.FromResult(Message.Create("stock=42")),

            // shipping service — slow, will time out
            async _ => { await Task.Delay(2000); return Message.Create("eta=5d"); }
        }, timeout: TimeSpan.FromMilliseconds(500));

        var results = await sg.GatherAsync(Message.Create("product-123"));

        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.Payload.StartsWith("price="));
        Assert.Contains(results, r => r.Payload.StartsWith("stock="));
    }
}