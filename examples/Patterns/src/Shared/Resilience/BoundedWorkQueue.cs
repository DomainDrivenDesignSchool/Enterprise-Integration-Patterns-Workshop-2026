using System.Threading.Channels;

namespace Workshop.Shared.Resilience;

public sealed class BoundedWorkQueue<T>
{
    private readonly Channel<T> _channel;

    public BoundedWorkQueue(int capacity)
    {
        if (capacity <= 0) throw new ArgumentOutOfRangeException(nameof(capacity));

        _channel = Channel.CreateBounded<T>(new BoundedChannelOptions(capacity)

        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });
    }

    public ValueTask WriteAsync(T item, CancellationToken ct)
        => _channel.Writer.WriteAsync(item, ct);
    public IAsyncEnumerable<T> ReadAllAsync(CancellationToken ct)
        => _channel.Reader.ReadAllAsync(ct);
    public void Complete(Exception? error = null)
        => _channel.Writer.TryComplete(error);
}
