using System.Collections.Concurrent;
using EipWorkshop.Core;

namespace EipWorkshop.Routing;

public sealed class Aggregator<TIn, TOut>
{
    private sealed class State
    {
        public int Expected { get; init; }
        public List<Message<TIn>> Received { get; } = new();
        public TaskCompletionSource<Message<TOut>> Tcs { get; init; }
            = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public DateTimeOffset Started { get; init; }
    }

    private readonly ConcurrentDictionary<string, State> _states = new();
    private readonly Func<IReadOnlyList<Message<TIn>>, TOut> _aggregate;
    private readonly TimeSpan _timeout;

    public Aggregator(Func<IReadOnlyList<Message<TIn>>, TOut> aggregate, TimeSpan? timeout = null)
    {
        _aggregate = aggregate;
        _timeout = timeout ?? TimeSpan.FromSeconds(5);
    }

    public Task<Message<TOut>> AddAsync(Message<TIn> part)
    {
        var correlation = part.CorrelationId
            ?? throw new InvalidOperationException("CorrelationId required");

        var expected = int.Parse(part.Headers["total"]);

        var state = _states.GetOrAdd(correlation, _ =>
        {
            var s = new State
            {
                Expected = expected,
                Started = DateTimeOffset.UtcNow
            };

            //_ = Task.Delay(_timeout).ContinueWith(t =>
            //{
            //    if (_states.TryRemove(correlation, out var st) && !st.Tcs.Task.IsCompleted)
            //    {
            //        st.Tcs.TrySetException(new TimeoutException(
            //            $"Aggregation timeout for {correlation}. " +
            //            $"Got {st.Received.Count}/{st.Expected}"));
            //    }
            //});

            return s;
        });

        lock (state)
        {
            state.Received.Add(part);

            if (state.Received.Count == state.Expected)
            {
                _states.TryRemove(correlation, out _);

                // message sequence
                // upload, 
                // event 1
                // event n, headers["end"]

                // event => headers ["x-index"],["x-start"] , ["x-end"]
                
                var ordered = state.Received
                    .OrderBy(m => int.Parse(m.Headers["index"]))
                    .ToList();

                var result = new Message<TOut>
                {
                    CorrelationId = correlation,
                    Payload = _aggregate(ordered)
                };

                state.Tcs.TrySetResult(result);
            }
        }

        return state.Tcs.Task;
    }
}