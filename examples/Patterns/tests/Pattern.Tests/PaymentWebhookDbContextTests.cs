using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Workshop.PaymentWorker;
using Xunit;

public sealed class PaymentWebhookDbContextTests
{
    [Fact]
    public void MapsProviderEventUniquenessAndSqlServerDefaults()
    {
        var options = new DbContextOptionsBuilder<PaymentWebhookDbContext>()
            .UseSqlServer("Server=(local);Database=PaymentModelOnly;Trusted_Connection=True;TrustServerCertificate=True")
            .Options;
        using var db = new PaymentWebhookDbContext(options);
        var entity = db.Model.FindEntityType(typeof(PspWebhookInboxRow))!;
        var unique = entity.GetIndexes().Single(x => x.IsUnique &&
            x.Properties.Select(p => p.Name).SequenceEqual([
                nameof(PspWebhookInboxRow.Provider), nameof(PspWebhookInboxRow.ProviderEventId)]));

        Assert.Equal("psp_webhook_inbox", entity.GetTableName());
        Assert.Equal("UQ_psp_webhook_provider_event", unique.GetDatabaseName());
        Assert.NotNull(entity.FindProperty(nameof(PspWebhookInboxRow.ReceivedAtUtc))!.GetDefaultValueSql());
        Assert.NotNull(entity.FindProperty(nameof(PspWebhookInboxRow.NextAttemptAtUtc))!.GetDefaultValueSql());
        Assert.Equal("char(64)", entity.FindProperty(nameof(PspWebhookInboxRow.PayloadHash))!.GetColumnType());
    }
}
