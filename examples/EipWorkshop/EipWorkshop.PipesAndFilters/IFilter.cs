using EipWorkshop.Core;

namespace EipWorkshop.PipesAndFilters;

public interface IFilter<TIn, TOut>
{
    Task<Message<TOut>?> ProcessAsync(Message<TIn> input, CancellationToken ct = default);
}

/// Splitter
public interface IMultiOutputFilter<TIn, TOut>
{
    IAsyncEnumerable<Message<TOut>> ProcessAsync(Message<TIn> input, CancellationToken ct = default);
}

/// Sink 
public interface ISink<T>
{
    Task ConsumeAsync(Message<T> message, CancellationToken ct = default);
}