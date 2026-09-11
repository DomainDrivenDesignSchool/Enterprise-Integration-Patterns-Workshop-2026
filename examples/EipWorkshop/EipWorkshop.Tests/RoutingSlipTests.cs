using EipWorkshop.Core;
using EipWorkshop.Routing;
using Xunit;

namespace EipWorkshop.Tests;

public class RoutingSlipTests
{

    [Fact]
    public async Task RoutingSlip_ExecutesStepsInOrder()
    {


        // bpm
        // a , b, c, d
        // a, b parallel
        // c
        // d
        // 
        //  Message =>  header Step
        //  fetch header,
        //  step by step
        //      a,b,c
        //   b,c

        // message
        // comamnd => palce order
        
        // step 1,
        // step 2,
        // 
        // step n

        var log = new List<string>();

        // routing slip
        // interface IRouringSlip
        // OrderAmoutHandler:IRouringSlip {  // TODO }
        // register OrderAmoutHandler into IoC 

        var slip = new RoutingSlip<int>()
            .AddStep("a", m => { log.Add("a"); return Task.FromResult(m); })
            .AddStep("b", m => { log.Add("b"); return Task.FromResult(m); })
            .AddStep("c", m => { log.Add("c"); return Task.FromResult(m); });


        var msg = Message.Create(1);
        msg.Headers["routing-slip"] = "a,b,c";

        await slip.ExecuteAsync(msg);

        Assert.Equal(new[] { "a", "b", "c" }, log);
    }

    [Fact]
    public async Task RoutingSlip_StepsCanMutateMessage()
    {

        // bad smell
        var slip = new RoutingSlip<int>()
            .AddStep("add10", m => { m.Payload += 10; return Task.FromResult(m); })
            .AddStep("mul2", m => { m.Payload *= 2; return Task.FromResult(m); });

        var msg = Message.Create(5);
        msg.Headers["routing-slip"] = "add10,mul2";

        var result = await slip.ExecuteAsync(msg);

        // (5 + 10) * 2 = 30
        Assert.Equal(30, result.Payload);
    }

    [Fact]
    public async Task RoutingSlip_UpdatesHeaderAfterEachStep()
    {
        var headersDuringExecution = new List<string>();
        var slip = new RoutingSlip<int>()
            .AddStep("a", m => { headersDuringExecution.Add(m.Headers["routing-slip"]); return Task.FromResult(m); })
            .AddStep("b", m => { headersDuringExecution.Add(m.Headers["routing-slip"]); return Task.FromResult(m); })
            .AddStep("c", m => { headersDuringExecution.Add(m.Headers["routing-slip"]); return Task.FromResult(m); });

        var msg = Message.Create(1);
        msg.Headers["routing-slip"] = "a,b,c";

        await slip.ExecuteAsync(msg);

        Assert.Equal(new[] { "a,b,c", "b,c", "c" }, headersDuringExecution);
    }

    [Fact]
    public async Task RoutingSlip_HeaderIsEmptyAfterCompletion()
    {
        var slip = new RoutingSlip<int>()
            .AddStep("only", m => Task.FromResult(m));

        var msg = Message.Create(1);
        msg.Headers["routing-slip"] = "only";

        var result = await slip.ExecuteAsync(msg);

        Assert.Equal("", result.Headers["routing-slip"]);
    }

    [Fact]
    public async Task RoutingSlip_NoHeader_ReturnsMessageUnchanged()
    {
        var slip = new RoutingSlip<int>()
            .AddStep("a", m => { m.Payload = 999; return Task.FromResult(m); });

        var msg = Message.Create(42);
        // No routing-slip header

        var result = await slip.ExecuteAsync(msg);

        Assert.Equal(42, result.Payload);
        Assert.Same(msg, result);
    }

    [Fact]
    public async Task RoutingSlip_EmptyHeader_ReturnsMessageUnchanged()
    {
        var slip = new RoutingSlip<int>()
            .AddStep("a", m => { m.Payload = 999; return Task.FromResult(m); });

        var msg = Message.Create(42);
        msg.Headers["routing-slip"] = "";

        var result = await slip.ExecuteAsync(msg);

        Assert.Equal(42, result.Payload);
    }

    [Fact]
    public async Task RoutingSlip_UnknownStep_Throws()
    {
        var slip = new RoutingSlip<int>()
            .AddStep("a", m => Task.FromResult(m));

        var msg = Message.Create(1);
        msg.Headers["routing-slip"] = "a,nonexistent";

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            slip.ExecuteAsync(msg));

        Assert.Contains("nonexistent", ex.Message);
    }

    [Fact]
    public async Task RoutingSlip_StepFailure_StopsChain()
    {
        var log = new List<string>();
        var slip = new RoutingSlip<int>()
            .AddStep("a", m => { log.Add("a"); return Task.FromResult(m); })
            .AddStep("b", m => throw new InvalidOperationException("b failed"))
            .AddStep("c", m => { log.Add("c"); return Task.FromResult(m); });

        var msg = Message.Create(1);
        msg.Headers["routing-slip"] = "a,b,c";

        await Assert.ThrowsAsync<InvalidOperationException>(() => slip.ExecuteAsync(msg));

        Assert.Equal(new[] { "a" }, log);   // c never ran
    }

    [Fact]
    public async Task RoutingSlip_IgnoresEmptyEntries()
    {
        var log = new List<string>();
        var slip = new RoutingSlip<int>()
            .AddStep("a", m => { log.Add("a"); return Task.FromResult(m); })
            .AddStep("b", m => { log.Add("b"); return Task.FromResult(m); });

        var msg = Message.Create(1);
        msg.Headers["routing-slip"] = "a,,b,";

        await slip.ExecuteAsync(msg);

        Assert.Equal(new[] { "a", "b" }, log);
    }

    [Fact]
    public async Task RoutingSlip_TrimsWhitespace()
    {
        var log = new List<string>();
        var slip = new RoutingSlip<int>()
            .AddStep("a", m => { log.Add("a"); return Task.FromResult(m); })
            .AddStep("b", m => { log.Add("b"); return Task.FromResult(m); });

        var msg = Message.Create(1);
        msg.Headers["routing-slip"] = " a , b ";

        await slip.ExecuteAsync(msg);

        Assert.Equal(new[] { "a", "b" }, log);
    }

    [Fact]
    public async Task RoutingSlip_SingleStep()
    {
        var slip = new RoutingSlip<int>()
            .AddStep("only", m => { m.Payload = 100; return Task.FromResult(m); });

        var msg = Message.Create(1);
        msg.Headers["routing-slip"] = "only";

        var result = await slip.ExecuteAsync(msg);

        Assert.Equal(100, result.Payload);
    }

    [Fact]
    public void RoutingSlip_AddStepIsFluent()
    {
        var slip = new RoutingSlip<int>();
        var result = slip.AddStep("a", m => Task.FromResult(m))
                         .AddStep("b", m => Task.FromResult(m));

        Assert.Same(slip, result);
    }

    [Fact]
    public async Task RoutingSlip_RealisticOrderWorkflow()
    {
        var log = new List<string>();
        var slip = new RoutingSlip<OrderState>()
            .AddStep("validate", m =>
            {
                log.Add("validate");
                if (m.Payload.Amount <= 0) throw new InvalidOperationException("invalid amount");
                return Task.FromResult(m);
            })
            .AddStep("charge", m =>
            {
                log.Add("charge");
                m.Payload = m.Payload with { Charged = true };
                return Task.FromResult(m);
            })
            .AddStep("ship", m =>
            {
                log.Add("ship");
                m.Payload = m.Payload with { Shipped = true };
                return Task.FromResult(m);
            });

        var msg = Message.Create(new OrderState("O1", 100, false, false));
        msg.Headers["routing-slip"] = "validate,charge,ship";

        var result = await slip.ExecuteAsync(msg);

        Assert.True(result.Payload.Charged);
        Assert.True(result.Payload.Shipped);
        Assert.Equal(new[] { "validate", "charge", "ship" }, log);
    }

    public sealed record OrderState(string Id, decimal Amount, bool Charged, bool Shipped);
}