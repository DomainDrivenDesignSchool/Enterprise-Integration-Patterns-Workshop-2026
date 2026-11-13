using Workshop.Contracts;
namespace Workshop.Messaging;

public interface IEventPublisher { Task PublishAsync<T>(string topic,string key,IntegrationEvent<T> evt,CancellationToken ct); }
