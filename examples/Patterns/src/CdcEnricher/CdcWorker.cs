using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Globalization;
using Confluent.Kafka;
using Microsoft.Data.SqlClient;
using Workshop.Messaging.Reliability;

namespace Workshop.CdcEnricher;

public sealed class CdcWorker(CdcOptions options, ILogger<CdcWorker> logger) : BackgroundService
{
    private static readonly JsonSerializerOptions OutputJson = new(JsonSerializerDefaults.Web);
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.GroupId,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            IsolationLevel = IsolationLevel.ReadCommitted,
            AllowAutoCreateTopics = false
        };

        using var consumer = new ConsumerBuilder<string, byte[]>(consumerConfig).Build();

        using var forwarder = new KafkaTransactionalForwarder(options.BootstrapServers, options.TransactionalId);

        using var failureRouter = new KafkaTransactionalFailureRouter(options.BootstrapServers, options.TransactionalId + "-failure", "commerce.retry", "commerce.dlq", maxAttempts: 5);
        consumer.Subscribe(options.InputTopic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]> record;
                try
                {
                    record = consumer.Consume(stoppingToken);
                }
                catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }

                try
                {
                    var evt = await TransformAsync(record.Message.Value, stoppingToken);
                    var bytes = JsonSerializer.SerializeToUtf8Bytes(evt, OutputJson);

                    await forwarder.ForwardAndCommitAsync(consumer, record, options.OutputTopic, bytes, stoppingToken, evt.OrderId.ToString("N"));
                    logger.LogInformation("CDC forwarded {OrderId} from LSN {Lsn}", evt.OrderId, evt.SourceLsn);
                }
                catch (JsonException ex)
                {
                    logger.LogError(ex, "Poison CDC payload at {Tpo}; route transactionally to retry/DLQ", record.TopicPartitionOffset);
                    await failureRouter.RouteFailureAndCommitAsync(consumer, record, ex, stoppingToken);
                }
                catch (Exception ex)
                {
                    logger.LogCritical(ex, "CDC infrastructure failure at {Tpo}; stopping to preserve source partition order", record.TopicPartitionOffset);
                    throw;
                }
            }
        }
        finally
        {
            consumer.Close();
        }
    }

    private async Task<OrderCdcEventV1> TransformAsync(byte[] raw, CancellationToken ct)
    {
        using var doc = JsonDocument.Parse(raw);
        var payload = doc.RootElement.TryGetProperty("payload", out var p)
            ? p
            : doc.RootElement;

        var op = GetString(payload, "op");

        if (op is not ("c" or "u" or "d" or "r"))
            throw new JsonException($"Unsupported Debezium operation '{op ?? "<null>"}'. Expected c/u/d/r row events.");

        var imageName = op == "d" ? "before" : "after";

        if (!payload.TryGetProperty(imageName, out var after)
            || after.ValueKind != JsonValueKind.Object)
            throw new JsonException($"CDC payload.{imageName} is missing; verify Debezium snapshot and delete settings.");

        JsonElement before = default;

        var hasBefore = payload.TryGetProperty("before", out before)
            && before.ValueKind == JsonValueKind.Object;

        var orderId = Guid.Parse(GetString(after, "id")
            ?? throw new JsonException("orders.id missing"));

        var customerId = Guid.Parse(GetString(after, "customer_id")
            ?? throw new JsonException("orders.customer_id missing"));

        if (op == "u" && !hasBefore)
            throw new JsonException("UPDATE before-image is missing. verify that the SQL Server CDC connector supplies before images for this capture mode.");

        var beforeStatus = hasBefore
            ? GetString(before, "status")
            ?? throw new JsonException("before.status missing; verify the SQL Server CDC before-image configuration") : null;

        var status = op == "d"
            ? "Deleted"
            : GetString(after, "status") ?? "Unknown";

        var amount = GetDecimal(after, "total_amount");
        var beforeAmount = hasBefore
            ? GetDecimal(before, "total_amount")
            : 0m;

        var currency = GetString(after, "currency")
            ?? throw new JsonException("orders.currency missing");

        var beforeCurrency = hasBefore
            ? GetString(before, "currency")
            : null;

        if (op == "u" && beforeStatus is ("Paid" or "Captured" or "Completed")
            && status is ("Paid" or "Captured" or "Completed")
            && !StringComparer.Ordinal.Equals(beforeCurrency, currency))
            throw new JsonException("Currency is immutable after an order is paid; cross-currency deltas require a separate FX event.");

        var paidDelta = PaidAmountDeltaCalculator.Calculate(op, beforeStatus, beforeAmount, status, amount);

        var lsn = "unknown"; var txId = "unknown";
        if (payload.TryGetProperty("source", out var source))
        {
            if (source.TryGetProperty("lsn", out var lsnElement))
                lsn = lsnElement.ToString();
            else if (source.TryGetProperty("commit_lsn", out var commitLsn))
                lsn = commitLsn.ToString();
            else if (source.TryGetProperty("change_lsn", out var changeLsn))
                lsn = changeLsn.ToString();
            if (source.TryGetProperty("txId", out var txElement))
                txId = txElement.ToString();
            else if (source.TryGetProperty("event_serial_no", out var serial))
                txId = serial.ToString();
        }

        var totalOrder = payload.TryGetProperty("transaction", out var transaction)
            && transaction.TryGetProperty("total_order", out var orderElement)
            ? orderElement.ToString() : txId;

        await using var connection = new SqlConnection(options.EnrichmentConnectionString);
        await connection.OpenAsync(ct);

        await using var cmd = new SqlCommand("""
            SELECT tier, region, shipping_zone
            FROM dbo.customer_enrichment
            WHERE customer_id = @customer_id AND is_active = 1
            """, connection);

        cmd.Parameters.AddWithValue("customer_id", customerId);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        string? tier = null, region = null, zone = null;

        if (await reader.ReadAsync(ct))
        {
            tier = reader.IsDBNull(0)
                ? null
                : reader.GetString(0);
            region = reader.IsDBNull(1)
                ? null
                : reader.GetString(1);
            zone = reader.IsDBNull(2)
                ? null
                : reader.GetString(2);
        }

        var occurredAt = payload.TryGetProperty("ts_ms", out var ts)
            && long.TryParse(ts.ToString(), out var millis)
            ? DateTimeOffset.FromUnixTimeMilliseconds(millis)
            : DateTimeOffset.UtcNow;

        var eventId = DeterministicId($"orders|{orderId:N}|{lsn}|{txId}|{totalOrder}|{op}");

        return new(eventId, op == "d" ? "OrderDeleted" : "OrderChanged", 1, occurredAt, op, "dbo.orders", lsn,
            orderId, customerId, beforeStatus, status, amount, paidDelta, currency, tier, region, zone);
    }

    private static Guid DeterministicId(string value)
        => new(SHA256.HashData(Encoding.UTF8.GetBytes(value))[..16]);
    private static string? GetString(JsonElement e, string name)
        => e.TryGetProperty(name, out var v) && v.ValueKind != JsonValueKind.Null ? v.ToString() : null;
    private static decimal GetDecimal(JsonElement e, string name)
    {
        if (!e.TryGetProperty(name, out var value) || !decimal.TryParse(value.ToString(), NumberStyles.Number, CultureInfo.InvariantCulture, out var amount))
            throw new JsonException($"Missing or invalid decimal field '{name}'.");
        return amount;
    }
}
