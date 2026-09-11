using EipWorkshop.Saga;
using Xunit;

namespace EipWorkshop.Tests;

public class SagaTests
{
    [Fact]
    public async Task Saga_HappyPath_NoCompensation()
    {
        var log = new List<string>();
        var saga = new SagaOrchestrator()
            .Add(new SagaStep("reserve",
                _ => { log.Add("do:reserve"); return Task.CompletedTask; },
                _ => { log.Add("undo:reserve"); return Task.CompletedTask; }))
            .Add(new SagaStep("charge",
                _ => { log.Add("do:charge"); return Task.CompletedTask; },
                _ => { log.Add("undo:charge"); return Task.CompletedTask; }));

        var result = await saga.ExecuteAsync();
        Assert.True(result.Success);
        Assert.DoesNotContain("undo:reserve", log);
        Assert.DoesNotContain("undo:charge", log);
    }

    [Fact]
    public async Task Saga_FailureCompensatesInReverseOrder()
    {
        var log = new List<string>();
        var saga = new SagaOrchestrator()
            .Add(new SagaStep("reserve",
                _ => { log.Add("do:reserve"); return Task.CompletedTask; },
                _ => { log.Add("undo:reserve"); return Task.CompletedTask; }))
            .Add(new SagaStep("charge",
                _ => { log.Add("do:charge"); throw new Exception("bank down"); },
                _ => { log.Add("undo:charge"); return Task.CompletedTask; }))
            .Add(new SagaStep("ship",
                _ => { log.Add("do:ship"); return Task.CompletedTask; },
                _ => { log.Add("undo:ship"); return Task.CompletedTask; }));

        var result = await saga.ExecuteAsync();
        Assert.False(result.Success);
        Assert.Equal(new[] { "do:reserve", "do:charge", "undo:reserve" }, log);
    }
}