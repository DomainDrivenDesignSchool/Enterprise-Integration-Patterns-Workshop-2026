using System.Collections.Concurrent;
using System.Threading.Channels;
using EipWorkshop.Core;

namespace EipWorkshop.Messaging;

public sealed class InMemoryBroker : IBroker
{
    private sealed class Subscription
    {
        public required string ConsumerId { get; init; }
        public required Channel<object> Channel { get; init; }
        public required Type MessageType { get; init; }
    }

    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, List<Subscription>>> _topics = new();
    // topic -> groupId -> list of subscriptions
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, int>> _rrCursor = new();
    private readonly ConcurrentBag<Task> _workers = new();
    private CancellationTokenSource? _cts;

    public Task StartAsync(CancellationToken ct = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        return Task.CompletedTask;
    }

    public Task StopAsync()
    {
        _cts?.Cancel();
        return Task.WhenAll(_workers.ToArray());
    }

    public Task PublishAsync<T>(string topic, Message<T> message, CancellationToken ct = default)
    {

        //var headers = message.Headers;
        //var routingslipHeader = headers["routing-slip"];

        //var steps = routingslipHeader.Split(".");
        //foreach (item s in steps)
        //{
        //    var routingSplitStep =(IRoutingSplitStep) provider.ResovleByName(s);
        //    routingSplitStep.Handle(message);
        //}

        //if(routingslipHeader is not null)
        //    await HandleRoutingSlipeMessage(message, routingslipHeader);


        var groups = _topics.GetOrAdd(topic, _ => new());
        foreach (var (groupId, subs) in groups)
        {
            var cursorDict = _rrCursor.GetOrAdd(topic, _ => new());
            var idx = cursorDict.AddOrUpdate(groupId, 0, (_, v) => v + 1);
            var sub = subs[idx % subs.Count];
            sub.Channel.Writer.TryWrite(message);
        }
        return Task.CompletedTask;
    }

    public Task SubscribeAsync<T>(string topic, string groupId, string consumerId,
        Func<Message<T>, CancellationToken, Task> handler, CancellationToken ct = default)
    {
        var sub = new Subscription
        {
            ConsumerId = consumerId,
            MessageType = typeof(T),
            Channel = Channel.CreateBounded<object>(new BoundedChannelOptions(100)
            {
                FullMode = BoundedChannelFullMode.Wait
            })
        };

        var groups = _topics.GetOrAdd(topic, _ => new());
        var subs = groups.GetOrAdd(groupId, _ => new());
        lock (subs) subs.Add(sub);

        var worker = Task.Run(async () =>
        {
            try
            {
                await foreach (var obj in sub.Channel.Reader.ReadAllAsync(_cts!.Token))
                {
                    if (obj is Message<T> typed)
                        await handler(typed, _cts.Token);
                }
            }
            catch (OperationCanceledException) { }
        });
        _workers.Add(worker);

        return Task.CompletedTask;
    }
}