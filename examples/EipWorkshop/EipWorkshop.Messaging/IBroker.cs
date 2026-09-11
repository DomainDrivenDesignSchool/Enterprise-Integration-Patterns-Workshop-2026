using EipWorkshop.Core;

namespace EipWorkshop.Messaging;

public interface IBroker
{
    Task PublishAsync<T>(string topic, Message<T> message, CancellationToken ct = default);
    Task SubscribeAsync<T>(string topic, string groupId, string consumerId,
        Func<Message<T>, CancellationToken, Task> handler, CancellationToken ct = default);
    Task StartAsync(CancellationToken ct = default);
    Task StopAsync();
}