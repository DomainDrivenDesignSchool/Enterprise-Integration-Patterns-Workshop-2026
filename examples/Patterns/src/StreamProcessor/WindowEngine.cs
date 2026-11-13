namespace Workshop.StreamProcessor;

/// <summary>
/// Single-process teaching implementation of event-time windows and watermark.
/// State is volatile; use a durable distributed stream processor for production.
/// </summary>
public sealed class WindowEngine
{
    private readonly TimeSpan _window;
    private readonly TimeSpan _slide;
    private readonly TimeSpan _allowedLateness;
    private readonly Dictionary<(string Key, long StartTicks), (int Count, decimal Sum)> _state = new();
    private DateTimeOffset _maxEventTime = DateTimeOffset.MinValue;

    public WindowEngine(TimeSpan window, TimeSpan slide, TimeSpan allowedLateness)
    {
        if (window <= TimeSpan.Zero || slide <= TimeSpan.Zero || slide > window) throw new ArgumentOutOfRangeException();
        _window = window; _slide = slide; _allowedLateness = allowedLateness;
    }

    public DateTimeOffset Watermark => _maxEventTime == DateTimeOffset.MinValue
        ? DateTimeOffset.MinValue : _maxEventTime - _allowedLateness;

    public IReadOnlyList<WindowResult> Add(StreamEvent e)
    {
        if (e.EventTimeUtc > _maxEventTime) _maxEventTime = e.EventTimeUtc;
        var watermark = Watermark;
        var latest = Floor(e.EventTimeUtc, _slide);
        if (e.EventTimeUtc < watermark)
        {
            // Surface late records explicitly; do not silently fold them into an already closed window.
            return [new WindowResult(e.Key, latest, latest + _window, 1, e.Amount, true)];
        }

        var starts = new List<DateTimeOffset>();
        for (var start = latest; start + _window > e.EventTimeUtc && start <= e.EventTimeUtc; start -= _slide)
        {
            starts.Add(start);
            if (starts.Count > 1000) throw new InvalidOperationException("Window configuration creates excessive fan-out.");
        }

        foreach (var start in starts)
        {
            var key = (e.Key, start.UtcDateTime.Ticks);
            var old = _state.GetValueOrDefault(key);
            _state[key] = (old.Count + 1, old.Sum + e.Amount);
        }
        return EvictClosed();
    }

    public IReadOnlyList<WindowResult> AddCountWindow(IEnumerable<StreamEvent> events, int count)
    {
        if (count <= 0) throw new ArgumentOutOfRangeException(nameof(count));
        return events.Chunk(count).Select(chunk => new WindowResult(
            chunk[^1].Key, chunk[0].EventTimeUtc, chunk[^1].EventTimeUtc,
            chunk.Length, chunk.Sum(x => x.Amount), false)).ToArray();
    }

    private IReadOnlyList<WindowResult> EvictClosed()
    {
        var ready = new List<WindowResult>();
        foreach (var pair in _state.ToArray())
        {
            var start = new DateTimeOffset(pair.Key.StartTicks, TimeSpan.Zero);
            var end = start + _window;
            if (end <= Watermark)
            {
                ready.Add(new(pair.Key.Key, start, end, pair.Value.Count, pair.Value.Sum, false));
                _state.Remove(pair.Key);
            }
        }
        return ready;
    }

    private static DateTimeOffset Floor(DateTimeOffset time, TimeSpan size)
    {
        var ticks = time.UtcDateTime.Ticks / size.Ticks * size.Ticks;
        return new DateTimeOffset(ticks, TimeSpan.Zero);
    }
}
