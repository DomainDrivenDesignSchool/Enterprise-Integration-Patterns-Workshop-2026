using EipWorkshop.Core;
using EipWorkshop.Routing;
using Xunit;

namespace EipWorkshop.Tests;

public class RecipientListTests
{
    [Fact]
    public async Task RecipientList_DispatchesToAll()
    {
        var log = new List<string>();

        // topic palce-order-topic

        // audit mgm => subsctribe palce-order-topic
        // email mgm => subsctribe palce-order-topic
        // sms mgm => subsctribe palce-order-topic

        // order-topic, God-Topic

        // bootstraper(orger mgmt, )

        var list = new RecipientList<string>()
            .Add("audit", m => { log.Add($"audit:{m.Payload}"); return Task.CompletedTask; })
            .Add("email", m => { log.Add($"email:{m.Payload}"); return Task.CompletedTask; })
            .Add("sms", m => { log.Add($"sms:{m.Payload}"); return Task.CompletedTask; });

        // send
        // broker.send
        // publisher.send(message, list)

        // action
        await list.DispatchAsync(Message.Create("OrderPlaced"), new[] { "audit", "email", "sms" });

        Assert.Equal(3, log.Count);
    }

    [Fact]
    public async Task RecipientList_OnlySelectedRecipients()
    {
        var log = new List<string>();
        var list = new RecipientList<string>()
            .Add("audit", _ => { log.Add("audit"); return Task.CompletedTask; })
            .Add("email", _ => { log.Add("email"); return Task.CompletedTask; })
            .Add("sms", _ => { log.Add("sms"); return Task.CompletedTask; });

        await list.DispatchAsync(Message.Create("X"), new[] { "audit", "sms" });

        Assert.Equal(2, log.Count);
        Assert.Contains("audit", log);
        Assert.Contains("sms", log);
        Assert.DoesNotContain("email", log);
    }

    [Fact]
    public async Task RecipientList_SkipsUnknownRecipients()
    {
        var log = new List<string>();
        var list = new RecipientList<string>()
            .Add("audit", _ => { log.Add("audit"); return Task.CompletedTask; });

        await list.DispatchAsync(Message.Create("X"), new[] { "audit", "nonexistent" });

        Assert.Single(log);
        Assert.Equal("audit", log[0]);
    }

    [Fact]
    public async Task RecipientList_EmptyList_NoOp()
    {
        var log = new List<string>();
        var list = new RecipientList<string>()
            .Add("audit", _ => { log.Add("audit"); return Task.CompletedTask; });

        await list.DispatchAsync(Message.Create("X"), Array.Empty<string>());

        Assert.Empty(log);
    }

    [Fact]
    public async Task RecipientList_RunsRecipientsInParallel()
    {
        var startedAt = DateTimeOffset.UtcNow;

        var list = new RecipientList<string>()
            .Add("a", async _ => await Task.Delay(200))
            .Add("b", async _ => await Task.Delay(200))
            .Add("c", async _ => await Task.Delay(200));

        await list.DispatchAsync(Message.Create("X"), new[] { "a", "b", "c" });

        var elapsed = DateTimeOffset.UtcNow - startedAt;

        // Sequential would be ~600ms, parallel ~200ms
        Assert.True(elapsed < TimeSpan.FromMilliseconds(400),
            $"Expected parallel, took {elapsed.TotalMilliseconds}ms");
    }

    [Fact]
    public async Task RecipientList_DuplicateNames_DispatchesTwice()
    {
        int count = 0;
        var list = new RecipientList<string>()
            .Add("audit", _ => { count++; return Task.CompletedTask; });

        // idempotent, statfull
        await list.DispatchAsync(Message.Create("X"), new[] { "audit", "audit" });

        Assert.Equal(2, count);
    }

    [Fact]
    public void RecipientList_AddIsFluent()
    {
        var list = new RecipientList<string>();
        var result = list.Add("a", _ => Task.CompletedTask)
                         .Add("b", _ => Task.CompletedTask);

        Assert.Same(list, result);
    }

    [Fact]
    public async Task RecipientList_PropagatesFirstException()
    {
        var list = new RecipientList<string>()
            .Add("good", _ => Task.CompletedTask)
            .Add("bad", _ => throw new InvalidOperationException("recipient down"));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            list.DispatchAsync(Message.Create("X"), new[] { "good", "bad" }));
    }
}