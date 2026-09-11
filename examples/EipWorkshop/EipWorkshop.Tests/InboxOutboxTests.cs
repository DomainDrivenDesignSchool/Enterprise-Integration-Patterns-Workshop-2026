using EipWorkshop.Reliability;
using Microsoft.EntityFrameworkCore;
using System;
using Xunit;

namespace EipWorkshop.Tests;

public class InboxOutboxTests
{
    private static AppDbContext NewDb() =>
        new(new DbContextOptionsBuilder<AppDbContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString())
            .Options);

    [Fact]
    public async Task PlaceOrder_WritesOrderAndOutboxAtomically()
    {
        var db = NewDb();
        var svc = new OrderService(db);

        var id = await svc.PlaceOrderAsync("C1", 100);

        Assert.Equal(1, await db.Orders.CountAsync());
        Assert.Equal(1, await db.Outbox.CountAsync());
        Assert.Equal("OrderPlaced", (await db.Outbox.FirstAsync()).Type);
    }

    [Fact]
    public async Task InboxProcessor_DropsDuplicateMessage()
    {
        var db = NewDb();
        var processor = new InboxProcessor(db, "consumer-1");
        var msgId = Guid.NewGuid();
        int count = 0;

        Assert.True(await processor.TryProcessAsync(msgId, () => { count++; return Task.CompletedTask; }));
        Assert.False(await processor.TryProcessAsync(msgId, () => { count++; return Task.CompletedTask; }));
        Assert.Equal(1, count);
    }
}