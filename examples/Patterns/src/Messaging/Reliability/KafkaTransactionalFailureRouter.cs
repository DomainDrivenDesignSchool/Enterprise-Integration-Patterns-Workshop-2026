using System.Globalization;
using System.Text;
using Confluent.Kafka;

namespace Workshop.Messaging.Reliability;

/// <summary>
/// Atomically forwards a consumed Kafka record to a retry/DLQ topic and advances
/// the source consumer-group offset in the SAME Kafka transaction.
/// This guarantee applies only when source and destination topics are on the same cluster.
/// It does not make an external database/API side effect part of the Kafka transaction.
/// </summary>
public sealed class KafkaTransactionalFailureRouter : IDisposable
{
    private readonly IProducer<string, byte[]> _producer;
    private readonly string _retryPrefix;
    private readonly string _dlqTopic;
    private readonly int _maxAttempts;
    private readonly TimeSpan _transactionTimeout;

    public KafkaTransactionalFailureRouter(
        string bootstrapServers,
        string transactionalId,
        string retryTopicPrefix,
        string dlqTopic,
        int maxAttempts = 5,
        TimeSpan? transactionTimeout = null)
    {
        if (maxAttempts < 1) throw new ArgumentOutOfRangeException(nameof(maxAttempts));
        _retryPrefix = retryTopicPrefix;
        _dlqTopic = dlqTopic;
        _maxAttempts = maxAttempts;
        _transactionTimeout = transactionTimeout ?? TimeSpan.FromSeconds(30);
        _producer = new ProducerBuilder<string, byte[]>(new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            TransactionalId = transactionalId,
            EnableIdempotence = true,
            Acks = Acks.All,
            MaxInFlight = 5,
            TransactionTimeoutMs = (int)_transactionTimeout.TotalMilliseconds
        }).Build();
        _producer.InitTransactions(_transactionTimeout);
    }

    public async Task RouteFailureAndCommitAsync(
        IConsumer<string, byte[]> consumer,
        ConsumeResult<string, byte[]> source,
        Exception failure,
        CancellationToken cancellationToken)
    {
        var previousAttempt = ReadAttempt(source.Message.Headers);
        var attempt = previousAttempt + 1;
        var isTerminal = attempt >= _maxAttempts;
        var destination = isTerminal ? _dlqTopic : RetryTopic(attempt);

        var headers = CopyHeaders(source.Message.Headers);

        headers.Add("x-retry-attempt", Encoding.UTF8.GetBytes(attempt.ToString(CultureInfo.InvariantCulture)));
        headers.Add("x-failure-type", Encoding.UTF8.GetBytes(failure.GetType().FullName ?? failure.GetType().Name));
        headers.Add("x-failure-message", Encoding.UTF8.GetBytes(Truncate(failure.Message, 1024)));
        headers.Add("x-failed-at-utc", Encoding.UTF8.GetBytes(DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture)));
        
        if (!isTerminal) 
            headers.Add("x-not-before-utc", Encoding.UTF8.GetBytes(RetryTopicPolicy.NextVisibleAt(attempt, DateTimeOffset.UtcNow).ToString("O", CultureInfo.InvariantCulture)));
        
        headers.Add("x-original-topic", Encoding.UTF8.GetBytes(source.Topic));
        headers.Add("x-original-partition", Encoding.UTF8.GetBytes(source.Partition.Value.ToString(CultureInfo.InvariantCulture)));
        headers.Add("x-original-offset", Encoding.UTF8.GetBytes(source.Offset.Value.ToString(CultureInfo.InvariantCulture)));

        _producer.BeginTransaction();

        try
        {
            await _producer.ProduceAsync(destination, new Message<string, byte[]>
            {
                Key = source.Message.Key,
                Value = source.Message.Value,
                Headers = headers,
                Timestamp = source.Message.Timestamp
            }, cancellationToken).ConfigureAwait(false);

            // Offset+1 means the next record to consume. Do NOT call consumer.Commit here.
            _producer.SendOffsetsToTransaction(
                [new TopicPartitionOffset(source.TopicPartition, source.Offset + 1)],
                consumer.ConsumerGroupMetadata,
                _transactionTimeout);

            _producer.CommitTransaction(_transactionTimeout);
        }
        catch
        {
            try { _producer.AbortTransaction(_transactionTimeout); }
            catch { /* Preserve the original exception; transaction outcome may require operator inspection. */ }
            throw;
        }
    }

    private string RetryTopic(int attempt) => attempt switch
    {
        1 => $"{_retryPrefix}.1m",
        2 => $"{_retryPrefix}.5m",
        3 => $"{_retryPrefix}.15m",
        _ => $"{_retryPrefix}.1h"
    };

    private static int ReadAttempt(Headers? headers)
    {
        var bytes = headers?.Where(x => x.Key == "x-retry-attempt").LastOrDefault()?.GetValueBytes();
        return bytes is null ? 0 : int.TryParse(Encoding.UTF8.GetString(bytes), out var value) ? value : 0;
    }

    private static Headers CopyHeaders(Headers? source)
    {
        var result = new Headers();
        if (source is null) return result;
        foreach (var header in source) result.Add(header.Key, header.GetValueBytes());
        return result;
    }

    private static string Truncate(string value, int max) => value.Length <= max ? value : value[..max];
    public void Dispose() => _producer.Dispose();
}
