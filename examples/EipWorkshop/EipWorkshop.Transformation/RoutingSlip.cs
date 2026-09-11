using EipWorkshop.Core;

namespace EipWorkshop.Routing;

public sealed class RoutingSlip<T>
{
    private readonly Dictionary<string, Func<Message<T>, Task<Message<T>>>> _steps = new();

    public RoutingSlip<T> AddStep(string name, Func<Message<T>, Task<Message<T>>> step)
    {
        _steps[name] = step;
        return this;
    }

    public async Task<Message<T>> ExecuteAsync(Message<T> message, CancellationToken ct = default)
    {
        if (!message.Headers.TryGetValue("routing-slip", out var slipStr) || string.IsNullOrEmpty(slipStr))
            return message;

        var steps = slipStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var current = message;
        var remaining = new Queue<string>(steps);

        while (remaining.Count > 0)
        {
            var step = remaining.Dequeue();
            if (!_steps.TryGetValue(step, out var handler))
                throw new InvalidOperationException($"Unknown step: {step}");

            current = await handler(current);
            current.Headers["routing-slip"] = string.Join(",", remaining);
            Console.WriteLine($"[RoutingSlip] executed '{step}', remaining: [{string.Join(",", remaining)}]");
        }
        return current;
    }
}