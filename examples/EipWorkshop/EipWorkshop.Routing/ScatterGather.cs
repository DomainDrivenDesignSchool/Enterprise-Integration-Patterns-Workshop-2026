using EipWorkshop.Core;

namespace EipWorkshop.Routing;

public sealed class ScatterGather<TRequest, TResponse>
{
    private readonly IReadOnlyList<Func<Message<TRequest>, Task<Message<TResponse>>>> _recipients;
    private readonly TimeSpan _timeout;

    public ScatterGather(
        IReadOnlyList<Func<Message<TRequest>, Task<Message<TResponse>>>> recipients,
        TimeSpan? timeout = null)
    {
        _recipients = recipients;
        _timeout = timeout ?? TimeSpan.FromSeconds(3);
    }

    public async Task<IReadOnlyList<Message<TResponse>>> GatherAsync(
        Message<TRequest> request, CancellationToken ct = default)
    {
        var tasks = _recipients.Select(r => r(request)).ToList();
        var all = Task.WhenAll(tasks);
        var completed = await Task.WhenAny(all, Task.Delay(_timeout, ct));

        var results = new List<Message<TResponse>>();
        foreach (var t in tasks.Where(t => t.IsCompletedSuccessfully))
            results.Add(t.Result);

        return results;
    }
}