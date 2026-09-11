using Microsoft.EntityFrameworkCore;

namespace EipWorkshop.Reliability;

public sealed class InboxProcessor
{
    private readonly AppDbContext _db;
    private readonly string _consumerId;

    public InboxProcessor(AppDbContext db, string consumerId)
    {
        _db = db; _consumerId = consumerId;
    }

    public async Task<bool> TryProcessAsync(Guid messageId, Func<Task> handler)
    {
        if (await _db.Inbox.AnyAsync(i => i.Id == messageId && i.ConsumerId == _consumerId))
            return false;

        await using var tx = await _db.Database.BeginTransactionAsync();
        await handler();
        _db.Inbox.Add(new InboxMessage { Id = messageId, ConsumerId = _consumerId });
        await _db.SaveChangesAsync();
        await tx.CommitAsync();
        return true;
    }
}