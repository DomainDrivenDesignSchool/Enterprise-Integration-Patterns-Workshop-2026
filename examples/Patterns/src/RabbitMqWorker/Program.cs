using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

var host = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost";
var factory = new ConnectionFactory
{
    HostName = host
};

await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

await channel.ExchangeDeclareAsync("workshop.events", ExchangeType.Topic, durable: true);
await channel.ExchangeDeclareAsync("workshop.fanout", ExchangeType.Fanout, durable: true);
await channel.ExchangeDeclareAsync("workshop.direct", ExchangeType.Direct, durable: true);
await channel.ExchangeDeclareAsync("workshop.headers", ExchangeType.Headers, durable: true);

await channel.QueueDeclareAsync("payment.commands", durable: true, exclusive: false, autoDelete: false);
await channel.QueueBindAsync("payment.commands", "workshop.events", "order.payment.*");
await channel.QueueDeclareAsync("analytics.events", durable: true, exclusive: false, autoDelete: false);
await channel.QueueBindAsync("analytics.events", "workshop.fanout", "");
await channel.QueueDeclareAsync("retry.payment.10s", durable: true, exclusive: false, autoDelete: false,
    arguments: new Dictionary<string, object?> { ["x-message-ttl"] = 10000, ["x-dead-letter-exchange"] = "workshop.events", ["x-dead-letter-routing-key"] = "order.payment.retry" });


var consumer = new AsyncEventingBasicConsumer(channel);

consumer.ReceivedAsync += async (_, ea) =>
{
    try
    {
        var body = Encoding.UTF8.GetString(ea.Body.ToArray());
        Console.WriteLine($"deliveryTag={ea.DeliveryTag} routingKey={ea.RoutingKey} body={body}");
        await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
    }
    catch
    {
        await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
    }
};
await channel.BasicConsumeAsync("payment.commands", autoAck: false, consumer);
Console.WriteLine("RabbitMQ routing lab running. Press Ctrl+C to stop.");
await Task.Delay(Timeout.Infinite);
