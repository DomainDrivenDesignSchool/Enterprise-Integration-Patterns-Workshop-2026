using EipWorkshop.Core;
using EipWorkshop.PipesAndFilters;
using Xunit;

namespace EipWorkshop.Tests;

public class PipelineTests
{
    [Fact]
    public async Task Pipeline_Map_Filter_Tap_Sink()
    {
        var log = new List<string>();
        var results = new List<string>();

        // muplication
        // filter
        // maximum

        // input
        // input * 2
        // input > 2
        //   then continue 
        //           select the top
        // else return response
        
        // filter
        // pipe(s)
        
        // input, output

        // filter<input type, output type>

        var pipeline = Pipeline.Start<int>()
            .Then(new MapFilter<int, int>(x => x * 2))
            .Then(new WhereFilter<int>(m => m.Payload > 2))
            .Then(new TapFilter<int>(m => { log.Add($"tap:{m.Payload}"); return Task.CompletedTask; }))
            .ToFunc((msg, _) => { results.Add($"out:{msg.Payload}"); return Task.CompletedTask; });

        foreach (var i in new[] { 1, 2, 3 })
            await pipeline.ExecuteAsync(Message.Create(i));

        Assert.Equal(new[] { "out:4", "out:6" }, results);
        Assert.Equal(new[] { "tap:4", "tap:6" }, log);
    }

    [Fact]
    public void Start_CreatesPipelineBuilder()
    {
        var builder = Pipeline.Start<int>();

        Assert.NotNull(builder);
        Assert.IsType<Pipeline.PipelineBuilder<int, int>>(builder);
    }

    [Fact]
    public void Then_ReturnsNewBuilder_NotSameInstance()
    {
        var builder1 = Pipeline.Start<int>();
        var builder2 = builder1.Then(new MapFilter<int, int>(x => x + 1));

        Assert.NotSame(builder1, builder2);
        Assert.IsType<Pipeline.PipelineBuilder<int, int>>(builder2);
    }

    [Fact]
    public void Then_ChangesTypeParameter()
    {
        var builder = Pipeline.Start<int>()
            .Then(new MapFilter<int, string>(x => x.ToString()));

        Assert.IsType<Pipeline.PipelineBuilder<int, string>>(builder);
    }

    [Fact]
    public void ToFunc_ReturnsPipeline()
    {
        var pipeline = Pipeline.Start<int>()
            .ToFunc((_, _) => Task.CompletedTask);

        Assert.NotNull(pipeline);
        Assert.IsType<Pipeline<int, int>>(pipeline);
    }

    [Fact]
    public async Task ExecuteAsync_EmptyPipeline_PassesMessageThrough()
    {
        Message<int>? captured = null;

        var pipeline = Pipeline.Start<int>()
            .ToFunc((msg, _) => { captured = msg; return Task.CompletedTask; });

        await pipeline.ExecuteAsync(Message.Create(42));

        Assert.NotNull(captured);
        Assert.Equal(42, captured!.Payload);
    }

    [Fact]
    public async Task ExecuteAsync_DefaultCancellationToken_Works()
    {
        var called = false;

        var pipeline = Pipeline.Start<int>()
            .ToFunc((_, _) => { called = true; return Task.CompletedTask; });

        await pipeline.ExecuteAsync(Message.Create(1));

        Assert.True(called);
    }

    [Fact]
    public async Task ExecuteAsync_WithCancellationToken_PassesToFilters()
    {
        using var cts = new CancellationTokenSource();
        CancellationToken receivedCt = default;

        var pipeline = Pipeline.Start<int>()
            .ToFunc((_, ct) => { receivedCt = ct; return Task.CompletedTask; });

        await pipeline.ExecuteAsync(Message.Create(1), cts.Token);

        Assert.Equal(cts.Token, receivedCt);
    }

    [Fact]
    public async Task ExecuteAsync_CanBeReusedForMultipleMessages()
    {
        var results = new List<int>();

        var pipeline = Pipeline.Start<int>()
            .Then(new MapFilter<int, int>(x => x * x))
            .ToFunc((msg, _) => { results.Add(msg.Payload); return Task.CompletedTask; });

        for (int i = 1; i <= 5; i++)
            await pipeline.ExecuteAsync(Message.Create(i));

        Assert.Equal(new[] { 1, 4, 9, 16, 25 }, results);
    }

    [Fact]
    public async Task ExecuteAsync_ThreadSafe_UnderConcurrentExecution()
    {
        var results = new System.Collections.Concurrent.ConcurrentBag<int>();

        var pipeline = Pipeline.Start<int>()
            .Then(new MapFilter<int, int>(x => x + 1))
            .ToFunc((msg, _) => { results.Add(msg.Payload); return Task.CompletedTask; });

        await Task.WhenAll(Enumerable.Range(0, 500)
            .Select(i => pipeline.ExecuteAsync(Message.Create(i))));

        Assert.Equal(500, results.Count);
        Assert.Equal(500, results.Distinct().Count());
    }

    [Fact]
    public async Task ExecuteAsync_PropagatesExceptionFromFilter()
    {
        var pipeline = Pipeline.Start<int>()
            .Then(new MapFilter<int, int>(x =>
                x == 5 ? throw new InvalidOperationException("boom") : x))
            .ToFunc((_, _) => Task.CompletedTask);

        await pipeline.ExecuteAsync(Message.Create(1));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => pipeline.ExecuteAsync(Message.Create(5)));

        Assert.Equal("boom", ex.Message);
    }

    [Fact]
    public async Task ExecuteAsync_LongChainOfFilters_Works()
    {
        var pipeline = Pipeline.Start<int>()
            .Then(new MapFilter<int, int>(x => x + 1))
            .Then(new MapFilter<int, int>(x => x * 2))
            .Then(new MapFilter<int, int>(x => x + 3))
            .Then(new MapFilter<int, int>(x => x - 4))
            .Then(new MapFilter<int, int>(x => x * 10))
            .ToFunc((_, _) => Task.CompletedTask);

        int? result = null;
        var pipeline2 = Pipeline.Start<int>()
            .Then(new MapFilter<int, int>(x => x + 1))
            .Then(new MapFilter<int, int>(x => x * 2))
            .Then(new MapFilter<int, int>(x => x + 3))
            .Then(new MapFilter<int, int>(x => x - 4))
            .Then(new MapFilter<int, int>(x => x * 10))
            .ToFunc((msg, _) => { result = msg.Payload; return Task.CompletedTask; });

        await pipeline2.ExecuteAsync(Message.Create(1));

        // 1 → 2 → 4 → 7 → 3 → 30
        Assert.Equal(30, result);
    }
}