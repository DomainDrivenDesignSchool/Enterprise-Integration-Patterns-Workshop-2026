using System.Globalization;
using System.Text;
using Confluent.Kafka;
using Workshop.Messaging.Reliability;

var bootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
var dlqTopic = Environment.GetEnvironmentVariable("DLQ_TOPIC") ?? "commerce.dlq";

var groupId = Environment.GetEnvironmentVariable("DLQ_REPLAY_GROUP") ?? $"dlq-replay-{Environment.MachineName}";
var transactionalId = Environment.GetEnvironmentVariable("DLQ_REPLAY_TRANSACTIONAL_ID")
    ?? throw new InvalidOperationException("Set DLQ_REPLAY_TRANSACTIONAL_ID to a stable, unique value for this replay operator instance.");

var maxMessages = int.TryParse(Environment.GetEnvironmentVariable("DLQ_REPLAY_MAX_MESSAGES"), out var max)
    ? Math.Clamp(max, 1, 10000)
    : 100;
var ratePerSecond = int.TryParse(Environment.GetEnvironmentVariable("DLQ_REPLAY_RATE_PER_SECOND"), out var rate)
    ? Math.Clamp(rate, 1, 1000)
    : 10;
var idleSeconds = int.TryParse(Environment.GetEnvironmentVariable("DLQ_REPLAY_IDLE_SECONDS"), out var idle)
    ? Math.Clamp(idle, 1, 300)
    : 5;

using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
{
    BootstrapServers = bootstrap,
    GroupId = groupId,
    EnableAutoCommit = false,
    EnableAutoOffsetStore = false,
    AutoOffsetReset = AutoOffsetReset.Earliest,
    IsolationLevel = IsolationLevel.ReadCommitted,
    AllowAutoCreateTopics = false
}).Build();
using var forwarder = new KafkaTransactionalForwarder(bootstrap, transactionalId);
consumer.Subscribe(dlqTopic);

var replayed = 0;
var idl = 0;
Console.WriteLine($"DLQ replay started. source={dlqTopic}, group={groupId}, max={maxMessages}, rate={ratePerSecond}/s");
try
{
    while (replayed < maxMessages && idle < idleSeconds)
    {
        var record = consumer.Consume(TimeSpan.FromSeconds(1));
        if (record is null)
        {
            idle++;
            continue;
        }
        idle = 0;
        var originalTopic = ReadHeader(record.Message.Headers, "x-original-topic")
            ?? throw new InvalidOperationException($"DLQ record {record.TopicPartitionOffset} has no x-original-topic header; refusing blind replay.");

        if (StringComparer.Ordinal.Equals(originalTopic, dlqTopic))
            throw new InvalidOperationException("Refusing to replay a DLQ record back into the DLQ topic.");

        var headers = BuildReplayHeaders(record.Message.Headers, originalTopic);
        await forwarder.ForwardAndCommitAsync(
            consumer,
            record,
            originalTopic,
            record.Message.Value,
            CancellationToken.None,
            record.Message.Key,
            headers);

        replayed++;
        Console.WriteLine($"Replayed {record.TopicPartitionOffset} -> {originalTopic}; key={record.Message.Key ?? "<null>"}; count={replayed}");
        await Task.Delay(TimeSpan.FromSeconds(1d / ratePerSecond));
    }
}
finally
{
    consumer.Close();
}

Console.WriteLine($"DLQ replay finished. replayed={replayed}; max={maxMessages}; idle-stop={idleSeconds}s");

static Headers BuildReplayHeaders(Headers? source, string originalTopic)
{
    var result = new Headers();
    if (source is not null)
    {
        foreach (var header in source)
        {
            if (header.Key is "x-retry-attempt" or "x-not-before-utc" or "x-failure-type" or "x-failure-message" or "x-failed-at-utc")
                continue;
            result.Add(header.Key, header.GetValueBytes());
        }
    }

    result.Add("x-original-topic", Encoding.UTF8.GetBytes(originalTopic));
    result.Add("x-replayed-from-dlq", Encoding.UTF8.GetBytes("true"));
    result.Add("x-replay-id", Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("N")));
    result.Add("x-replayed-at-utc", Encoding.UTF8.GetBytes(DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture)));
    return result;
}

static string? ReadHeader(Headers? headers, string name)
{
    var value = headers?.Where(x => x.Key == name).LastOrDefault()?.GetValueBytes();
    return value is null ? null : Encoding.UTF8.GetString(value);
}
