using System.Security.Cryptography;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Workshop.Messaging;
using Workshop.Persistence;

public sealed class OrderLifecycleWorker(
    KafkaOptions options,
    IServiceScopeFactory scopes,
    IConfiguration configuration,
    ILogger<OrderLifecycleWorker> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stop)
    {
        var sagaEvents = configuration["Kafka:SagaEventsTopic"] ?? "commerce.saga-events";

        using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = configuration["Kafka:LifecycleGroupId"] ?? "order-lifecycle-v1",
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            IsolationLevel = IsolationLevel.ReadCommitted
        }).Build();

        consumer.Subscribe(sagaEvents);
        try
        {
            while (!stop.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]> record;
                try { record = consumer.Consume(stop); }

                catch (OperationCanceledException)
                when (stop.IsCancellationRequested)
                {
                    break;
                }

                try
                {
                    var applied = await ApplyAsync(record.Message.Value, stop);

                    if (applied)
                        log.LogInformation("Order lifecycle updated from {Topic} at {Tpo}", record.Topic, record.TopicPartitionOffset);

                    consumer.Commit(record);
                }
                catch (Exception ex)
                {
                    log.LogCritical(ex, "Order lifecycle message failed at {Tpo}; stopping this consumer to preserve per-order ordering", record.TopicPartitionOffset);
                    throw;
                }
            }
        }
        finally { consumer.Close(); }
    }

    private async Task<bool> ApplyAsync(byte[] bytes, CancellationToken ct)
    {
        using var document = JsonDocument.Parse(bytes);
        var root = document.RootElement;
        var eventType = Required(root, "eventType");
        if (eventType is not ("PaymentAuthorized" or "OrderCompleted" or "OrderFailed")) return false;

        var eventId = Guid.Parse(Required(root, "eventId"));
        var payload = root.GetProperty("payload");
        var orderId = Guid.Parse(Required(payload, "orderId"));
        var hash = Convert.ToHexString(SHA256.HashData(bytes));

        await using var scope = scopes.CreateAsyncScope();
        
        var db = scope.ServiceProvider.GetRequiredService<OrderDb>();
        
        await using var transaction = await db.Database.BeginTransactionAsync(ct);

        var existing = await db.CommandInbox.SingleOrDefaultAsync(x => x.EventId == eventId, ct);
        if (existing is not null)
        {
            if (!StringComparer.Ordinal.Equals(existing.PayloadHash, hash))
                throw new InvalidOperationException("Order lifecycle EventId collision: same ID with different payload.");
            await transaction.CommitAsync(ct);
            return false;
        }

        var order = await db.Orders.SingleOrDefaultAsync(x => x.Id == orderId, ct)
            ?? throw new InvalidOperationException($"Order {orderId} was not found for {eventType}.");
        
        switch (eventType)
        {
            case "PaymentAuthorized":
                if (order.Status == "Pending") order.Status = "Paid";
                else if (order.Status is not ("Paid" or "Completed"))
                    throw new InvalidOperationException($"Cannot mark order {orderId} paid from state {order.Status}.");
                break;
            case "OrderCompleted":
                if (order.Status == "Paid") order.Status = "Completed";
                else if (order.Status != "Completed")
                    throw new InvalidOperationException($"Cannot complete order {orderId} from state {order.Status}.");
                break;
            case "OrderFailed":
                if (order.Status == "Completed")
                    throw new InvalidOperationException($"Completed order {orderId} cannot transition to Failed.");
                order.Status = "Failed";
                break;
        }

        db.CommandInbox.Add(new OrderCommandInbox { EventId = eventId, PayloadHash = hash, ProcessedAt = DateTimeOffset.UtcNow });
        await db.SaveChangesAsync(ct);
        await transaction.CommitAsync(ct);
        return true;
    }

    private static string Required(JsonElement element, string name) =>
        element.TryGetProperty(name, out var value) ? value.ToString() : throw new JsonException($"Missing '{name}'.");
}
