using Workshop.StreamProcessor;
using Xunit;
public class WindowEngineTests
{
    [Fact] public void CountWindow_EmitsFixedChunks() {
        var now = DateTimeOffset.UtcNow;
        var engine = new WindowEngine(TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(10));
        var result = engine.AddCountWindow(Enumerable.Range(1,5).Select(i => new StreamEvent("k", i, now.AddSeconds(i), now)), 2);
        Assert.Equal(3, result.Count);
        Assert.Equal(5, result.Sum(x => x.Count));
    }

    [Fact] public void EventTimeWindow_ClosesOnlyAfterWatermarkPassesWindowEnd()
    {
        var start = new DateTimeOffset(2026, 11, 11, 10, 0, 0, TimeSpan.Zero);
        var engine = new WindowEngine(TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(10));
        Assert.Empty(engine.Add(new StreamEvent("customer-1", 100m, start.AddSeconds(5), start)));
        var closed = engine.Add(new StreamEvent("customer-1", 50m, start.AddSeconds(75), start.AddSeconds(75)));
        Assert.Single(closed);
        Assert.Equal(start, closed[0].StartUtc);
        Assert.Equal(100m, closed[0].Sum);
        Assert.False(closed[0].IsLate);
    }

    [Fact] public void EventBeyondWatermark_IsReturnedAsLateInsteadOfBeingSilentlyAggregated()
    {
        var start = new DateTimeOffset(2026, 11, 11, 10, 0, 0, TimeSpan.Zero);
        var engine = new WindowEngine(TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(10));
        engine.Add(new StreamEvent("customer-1", 10m, start.AddSeconds(5), start));
        engine.Add(new StreamEvent("customer-1", 10m, start.AddSeconds(75), start.AddSeconds(75)));
        var late = engine.Add(new StreamEvent("customer-1", 25m, start.AddSeconds(20), start.AddSeconds(80)));
        Assert.Single(late);
        Assert.True(late[0].IsLate);
        Assert.Equal(25m, late[0].Sum);
    }
}
