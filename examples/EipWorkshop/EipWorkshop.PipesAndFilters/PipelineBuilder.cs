using EipWorkshop.Core;

namespace EipWorkshop.PipesAndFilters;

//public sealed class PipelineBuilder<TStart>
//{
//    private readonly Func<Message<TStart>, CancellationToken, Task> _entry;

//    internal PipelineBuilder(Func<Message<TStart>, CancellationToken, Task> entry) => _entry = entry;

//    public Task ExecuteAsync(Message<TStart> message, CancellationToken ct = default) =>
//        _entry(message, ct);
//}

public sealed class PipelineBuilder<TStart>
{
    private Func<Message<TStart>, CancellationToken, Task> _entry =
        (_, _) => Task.CompletedTask;

    public PipelineBuilder(Func<Message<TStart>, CancellationToken, Task> entry)
    {
        Entry = entry;
    }

    public Func<Message<TStart>, CancellationToken, Task> Entry { get; }

    public PipelineBuilder<TStart> AddFilter<TIn, TOut>(IFilter<TIn, TOut> filter)
    {
        var prev = _entry;
        _entry = async (msg, ct) =>
        {
            var typed = (Message<TIn>)(object)msg!;
            var output = await filter.ProcessAsync(typed, ct);
            if (output is not null)
                await PipelineChain<TOut>.ContinueAsync(output, ct);
        };
        return this;
    }

    public PipelineBuilder<TStart> Build() => new(_entry);
}

internal static class PipelineChain<T>
{
    internal static Func<Message<T>, CancellationToken, Task>? Next;

    internal static Task ContinueAsync(Message<T> msg, CancellationToken ct) =>
        Next?.Invoke(msg, ct) ?? Task.CompletedTask;
}