using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace EipWorkshop.RabbitDemo;

public static class RabbitDemo
{
    public static async Task RunAsync(string host = "localhost")
    {
        var factory = new ConnectionFactory { HostName = host, UserName = "guest", Password = "guest" };
        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        // --- DLX ---
        channel.ExchangeDeclare("orders.dlx", ExchangeType.Direct, durable: true);
        channel.QueueDeclare("orders.dlq", durable: true, exclusive: false, autoDelete: false);
        channel.QueueBind("orders.dlq", "orders.dlx", "orders");

        // --- Main Queue با DLX و TTL ---
        var args = new Dictionary<string, object>
        {
            ["x-dead-letter-exchange"] = "orders.dlx",
            ["x-dead-letter-routing-key"] = "orders",
            ["x-message-ttl"] = 60_000
        };
        channel.QueueDeclare("orders", durable: true, exclusive: false, autoDelete: false, arguments: args);

        // --- Publisher ---
        for (int i = 0; i < 6; i++)
        {
            var body = Encoding.UTF8.GetBytes($"{{\"orderId\":\"O{i}\"}}");
            var props = channel.CreateBasicProperties();
            props.Persistent = true;
            props.CorrelationId = Guid.NewGuid().ToString("N");
            channel.BasicPublish("", "orders", props, body);
            Console.WriteLine($"[Producer] sent O{i}");
        }

        // --- Consumer با Nack برای DLQ ---
        var consumer = new EventingBasicConsumer(channel);
        consumer.Received += (s, ea) =>
        {
            var msg = Encoding.UTF8.GetString(ea.Body.ToArray());
            Console.WriteLine($"[Consumer] received: {msg}");

            // شبیه‌سازی خطا برای O3
            if (msg.Contains("O3"))
            {
                Console.WriteLine("[Consumer] ✖ failing -> DLQ");
                channel.BasicNack(ea.DeliveryTag, false, requeue: false);
            }
            else
            {
                channel.BasicAck(ea.DeliveryTag, false);
            }
        };
        channel.BasicConsume("orders", autoAck: false, consumer: consumer);

        await Task.Delay(3000);

        // --- خواندن از DLQ ---
        var dlqConsumer = new EventingBasicConsumer(channel);
        dlqConsumer.Received += (s, ea) =>
            Console.WriteLine($"[DLQ] {Encoding.UTF8.GetString(ea.Body.ToArray())}");
        channel.BasicConsume("orders.dlq", autoAck: true, consumer: dlqConsumer);

        await Task.Delay(2000);
    }
}