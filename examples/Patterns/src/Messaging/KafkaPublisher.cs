using System.Text.Json;
using Confluent.Kafka;
using Workshop.Contracts;
namespace Workshop.Messaging;

public sealed class KafkaPublisher : IEventPublisher, IDisposable
{
    readonly IProducer<string, string> p;
    static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    public KafkaPublisher(KafkaOptions o) => p = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = o.BootstrapServers, EnableIdempotence = true, Acks = Acks.All, CompressionType = CompressionType.Snappy }).Build();
    public async Task PublishAsync<T>(string topic, string key, IntegrationEvent<T> evt, CancellationToken ct) => await p.ProduceAsync(topic, new Message<string, string> { Key = key, Value = JsonSerializer.Serialize(evt, Json) }, ct);
    public void Dispose() { p.Flush(TimeSpan.FromSeconds(10)); p.Dispose(); }
}
