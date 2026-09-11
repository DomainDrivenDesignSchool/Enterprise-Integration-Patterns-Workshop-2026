using EipWorkshop.Core;

namespace EipWorkshop.Routing;

public sealed class RecipientList<T>
{
    private readonly Dictionary<string, Func<Message<T>, Task>> _recipients = new();

    public RecipientList<T> Add(string name, Func<Message<T>, Task> handler)
    {
        _recipients[name] = handler;
        return this;
    }

    public async Task DispatchAsync(Message<T> message, IEnumerable<string> recipientNames,
        CancellationToken ct = default)
    {
        var tasks = recipientNames
            .Where(_recipients.ContainsKey)
            .Select(n => _recipients[n](message));
        await Task.WhenAll(tasks);
    }
}