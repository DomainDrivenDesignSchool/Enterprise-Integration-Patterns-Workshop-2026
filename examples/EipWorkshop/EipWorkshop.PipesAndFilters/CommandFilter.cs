
using EipWorkshop.Core;

namespace EipWorkshop.PipesAndFilters;

public sealed class MapFilter<TIn, TOut> : IPipe<TIn, TOut>
{
    private readonly Func<TIn, TOut> _map;
    public MapFilter(Func<TIn, TOut> map) => _map = map;

    public Task InvokeAsync(Message<TIn> input, Next<TOut> next, CancellationToken ct)
    {
        var outMsg = new Message<TOut>
        {
            CorrelationId = input.CorrelationId,
            CausationId = input.Id,
            Headers = new Dictionary<string, string>(input.Headers),
            Payload = _map(input.Payload)
        };
        return next(outMsg, ct);
    }
}

public sealed class WhereFilter<T> : IPipe<T, T>
{
    private readonly Func<Message<T>, bool> _predicate;
    public WhereFilter(Func<Message<T>, bool> predicate) => _predicate = predicate;

    public Task InvokeAsync(Message<T> input, Next<T> next, CancellationToken ct) =>
        _predicate(input) ? next(input, ct) : Task.CompletedTask;
}

public sealed class TapFilter<T> : IPipe<T, T>
{
    private readonly Func<Message<T>, Task> _tap;
    public TapFilter(Func<Message<T>, Task> tap) => _tap = tap;

    public async Task InvokeAsync(Message<T> input, Next<T> next, CancellationToken ct)
    {
        await _tap(input);
        await next(input, ct);
    }
}