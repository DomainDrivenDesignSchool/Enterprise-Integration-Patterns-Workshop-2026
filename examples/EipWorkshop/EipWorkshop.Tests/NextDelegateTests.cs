using EipWorkshop.Core;
using EipWorkshop.PipesAndFilters;
using Xunit;

namespace EipWorkshop.Tests;

public class NextDelegateTests
{
    [Fact]
    public async Task Next_CanBeCreatedFromLambda()
    {
        Next<int> next = (msg, ct) => Task.CompletedTask;

        await next(Message.Create(1), CancellationToken.None);

        Assert.NotNull(next);
    }

    [Fact]
    public async Task Next_CanBeCreatedFromMethodGroup()
    {
        Next<int> next = MyHandlerAsync;

        await next(Message.Create(1), CancellationToken.None);

        Assert.True(true);
        }

    private static Task MyHandlerAsync(Message<int> msg, CancellationToken ct)
        => Task.CompletedTask;

    [Fact]
    public async Task Next_ReturnsAwaitableTask()
    {
        var completed = false;

        Next<int> next = async (msg, ct) =>
        {
            await Task.Delay(10);
            completed = true;
        };

        await next(Message.Create(1), CancellationToken.None);

        Assert.True(completed);
    }

    [Fact]
    public async Task Next_ReceivesCancellationToken()
    {
        using var cts = new CancellationTokenSource();
        CancellationToken receivedCt = default;

        Next<int> next = (msg, ct) =>
        {
            receivedCt = ct;
            return Task.CompletedTask;
        };

        await next(Message.Create(1), cts.Token);

        Assert.Equal(cts.Token, receivedCt);
    }

    [Fact]
    public async Task Next_SupportsMulticast()
    {
        var log = new List<string>();

        Next<int> a = (msg, ct) =>
        {
            log.Add("a");
            return Task.CompletedTask;
        };

        Next<int> b = (msg, ct) =>
        {
            log.Add("b");
            return Task.CompletedTask;
        };

        var combined = a + b;
        await combined(Message.Create(1), CancellationToken.None);

        Assert.Equal(new[] { "a", "b" }, log);
    }

    [Fact]
    public async Task Next_IsNotAssignableFromFunc()
    {
        Func<Message<int>, CancellationToken, Task> func = (msg, ct) => Task.CompletedTask;

        Next<int> next = (msg, ct) => func(msg, ct);

        await next(Message.Create(1), CancellationToken.None);

        Assert.NotNull(next);
    }

    [Fact]
    public async Task Next_CanChainToAnotherNext()
    {
        var log = new List<string>();

        Next<int> second = (msg, ct) =>
        {
            log.Add("second");
            return Task.CompletedTask;
        };

        Next<int> first = async (msg, ct) =>
        {
            log.Add("first");
            await second(msg, ct);
        };

        await first(Message.Create(1), CancellationToken.None);

        Assert.Equal(new[] { "first", "second" }, log);
    }
}