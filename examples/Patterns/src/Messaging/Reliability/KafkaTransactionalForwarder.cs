using Confluent.Kafka;

namespace Workshop.Messaging.Reliability;

/// <summary>Kafka-to-Kafka exactly-once forwarding on one cluster using a Kafka transaction.</summary>
public sealed class KafkaTransactionalForwarder : IDisposable
{
    private readonly IProducer<string, byte[]> _producer;
    private readonly TimeSpan _timeout;

    public KafkaTransactionalForwarder(string bootstrapServers, string transactionalId, TimeSpan? timeout = null)
    {
        _timeout = timeout ?? TimeSpan.FromSeconds(30);

        _producer = new ProducerBuilder<string, byte[]>(new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            TransactionalId = transactionalId,
            EnableIdempotence = true,
            Acks = Acks.All,
            TransactionTimeoutMs = (int)_timeout.TotalMilliseconds
        }).Build();
        _producer.InitTransactions(_timeout);
    }

    public async Task ForwardAndCommitAsync(
        IConsumer<string, byte[]> consumer,
        ConsumeResult<string, byte[]> source,
        string destinationTopic,
        byte[] value,
        CancellationToken cancellationToken,
        string? destinationKey = null,
        Headers? destinationHeaders = null)
    {
        _producer.BeginTransaction();
        try
        {
            await _producer.ProduceAsync(destinationTopic, new Message<string, byte[]>
            {
                Key = destinationKey ?? source.Message.Key,
                Value = value,
                Headers = destinationHeaders ?? source.Message.Headers,
                Timestamp = source.Message.Timestamp
            }, cancellationToken).ConfigureAwait(false);

            _producer.SendOffsetsToTransaction(
                [new TopicPartitionOffset(source.TopicPartition, source.Offset + 1)],
                consumer.ConsumerGroupMetadata,
                _timeout);
            _producer.CommitTransaction(_timeout);
        }
        catch
        {
            try { _producer.AbortTransaction(_timeout); } catch { }
            throw;
        }
    }

    public void Dispose() => _producer.Dispose();
}
