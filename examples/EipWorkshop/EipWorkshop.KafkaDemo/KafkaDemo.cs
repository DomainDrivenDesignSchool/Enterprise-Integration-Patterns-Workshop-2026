using Confluent.Kafka;

namespace EipWorkshop.KafkaDemo;

public static class KafkaDemo
{
    public static async Task RunAsync(string bootstrap = "localhost:9092")
    {
        const string topic = "orders";

        // Producer
        using var producer = new ProducerBuilder<string, string>(
            new ProducerConfig { BootstrapServers = bootstrap }).Build();

        for (int i = 0; i < 10; i++)
        {
            await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = $"customer-{i % 3}",       // partitioning by key
                Value = $"{{\"orderId\":\"O{i}\",\"amount\":{100 + i}}}"
            });
        }
        producer.Flush(TimeSpan.FromSeconds(5));

        // Consumer Group A - 2 instances
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        var t1 = ConsumeAsync(bootstrap, topic, "group-A", "consumer-A1", cts.Token);
        var t2 = ConsumeAsync(bootstrap, topic, "group-A", "consumer-A2", cts.Token);

        await Task.WhenAll(t1, t2);
    }

    private static Task ConsumeAsync(string bootstrap, string topic, string group, string consumerId, CancellationToken ct)
    {
        return Task.Run(() =>
        {
            using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
            {
                BootstrapServers = bootstrap,
                GroupId = group,
                ClientId = consumerId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true
            }).Build();

            consumer.Subscribe(topic);

            try
            {
                while (!ct.IsCancellationRequested)
                {
                    var cr = consumer.Consume(ct);
                    Console.WriteLine($"[{consumerId}] partition={cr.Partition.Value} key={cr.Message.Key} value={cr.Message.Value}");
                }
            }
            catch (OperationCanceledException) { }
            finally
            {
                consumer.Close();
            }
        }, ct);
    }
}