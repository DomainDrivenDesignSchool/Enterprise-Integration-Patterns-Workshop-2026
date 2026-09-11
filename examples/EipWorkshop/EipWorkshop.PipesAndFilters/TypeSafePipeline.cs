using EipWorkshop.Core;

namespace EipWorkshop.PipesAndFilters;

public delegate Task Next<TOut>(Message<TOut> message, CancellationToken ct);

public interface IPipe<TIn, TOut>
{
    Task InvokeAsync(Message<TIn> input, Next<TOut> next, CancellationToken ct);
}

public sealed class Pipeline<TIn, TOut>
{
    private readonly IPipe<TIn, TOut> _root;
    private readonly Next<TOut> _terminal;
    public Pipeline(IPipe<TIn, TOut> root, Next<TOut> terminal)
    {
        _root = root;
        _terminal = terminal;
    }

    public Task ExecuteAsync(Message<TIn> message, CancellationToken ct = default)
    {
        return _root.InvokeAsync(message, _terminal, ct);
    }
}

public static class Pipeline
{
    public static PipelineBuilder<TIn, TIn> Start<TIn>() => new();

    public sealed class PipelineBuilder<TIn, TCur>
    {
        private readonly IPipe<TIn, TCur> _pipe;

        internal PipelineBuilder() => _pipe = new IdentityPipe<TIn, TCur>();

        private PipelineBuilder(IPipe<TIn, TCur> pipe) => _pipe = pipe;

        public PipelineBuilder<TIn, TNext> Then<TNext>(IPipe<TCur, TNext> next) =>
            new(new ComposePipe<TIn, TCur, TNext>(_pipe, next));

        public Pipeline<TIn, TCur> ToSink(ISink<TCur> sink) =>
            new(_pipe, (msg, ct) => sink.ConsumeAsync(msg, ct));

        public Pipeline<TIn, TCur> ToFunc(Func<Message<TCur>, CancellationToken, Task> terminal) =>
            new(_pipe, (msg, ct) => terminal(msg, ct));
    }

    private sealed class IdentityPipe<TIn, TOut> : IPipe<TIn, TOut>
    {
        public Task InvokeAsync(Message<TIn> input, Next<TOut> next, CancellationToken ct) =>
            next((Message<TOut>)(object)input!, ct);
    }

    private sealed class ComposePipe<TIn, TMid, TOut> : IPipe<TIn, TOut>
    {
        private readonly IPipe<TIn, TMid> _first;
        private readonly IPipe<TMid, TOut> _second;

        public ComposePipe(IPipe<TIn, TMid> first, IPipe<TMid, TOut> second)
        {
            _first = first; _second = second;
        }

        public Task InvokeAsync(Message<TIn> input, Next<TOut> next, CancellationToken ct) =>
            _first.InvokeAsync(input, (mid, c) => _second.InvokeAsync(mid, next, c), ct);
    }
}