using EipWorkshop.Core;

namespace EipWorkshop.Routing;

public sealed record BatchItem(string BatchId, int Index, int Total, string Data);

public sealed class OrderBatchSplitter
{
    public IEnumerable<Message<BatchItem>> Split(Message<OrderBatch> batch)
    {
        var correlation = batch.CorrelationId ?? Guid.NewGuid().ToString("N");
        for (int i = 0; i < batch.Payload.Items.Count; i++)
        {
            yield return new Message<BatchItem>
            {
                CorrelationId = correlation,
                CausationId = batch.Id,
                Headers = new Dictionary<string, string>(batch.Headers)
                {
                    ["batchId"] = batch.Payload.BatchId,
                    ["index"] = i.ToString(),
                    ["total"] = batch.Payload.Items.Count.ToString()
                },
                Payload = new BatchItem(batch.Payload.BatchId, i, batch.Payload.Items.Count, batch.Payload.Items[i])
            };
        }
    }
}

public sealed record OrderBatch(string BatchId, List<string> Items);