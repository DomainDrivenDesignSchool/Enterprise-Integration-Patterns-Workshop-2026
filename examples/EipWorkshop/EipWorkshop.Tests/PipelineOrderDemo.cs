using EipWorkshop.Core;
using EipWorkshop.PipesAndFilters;

namespace EipWorkshop.Tests;

public sealed record RawOrder(string Id, decimal Amount, string CustomerId);
public sealed record ValidOrder(string Id, decimal Amount, string CustomerId, decimal Tax);

public static class PipelineOrderDemo
{
    public static async Task RunAsync()
    {
        // per-condition application-level

        var pipeline = Pipeline.Start<RawOrder>()
            .Then(new WhereFilter<RawOrder>(m => m.Payload.Amount > 0))
            .Then(new MapFilter<RawOrder, ValidOrder>(o =>
                new ValidOrder(o.Id, o.Amount, o.CustomerId, o.Amount * 0.09m)))
            .ToFunc((msg, _) =>
            {
                Console.WriteLine($"[Order] {msg.Payload.Id} amount={msg.Payload.Amount} tax={msg.Payload.Tax}");
                return Task.CompletedTask;
            });

        await pipeline.ExecuteAsync(Message.Create(new RawOrder("O1", 100, "C1")));
        await pipeline.ExecuteAsync(Message.Create(new RawOrder("O2", -5, "C2"))); // drop
        await pipeline.ExecuteAsync(Message.Create(new RawOrder("O3", 200, "C3")));
    }
}