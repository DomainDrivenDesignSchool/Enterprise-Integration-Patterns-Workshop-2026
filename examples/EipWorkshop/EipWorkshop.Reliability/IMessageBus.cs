using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EipWorkshop.Reliability;

public interface IMessageBus
{
    Task PublishAsync(string type, string payload, CancellationToken ct = default);
}

public sealed class OutboxPublisher : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IMessageBus _bus;
    private readonly ILogger<OutboxPublisher> _logger;

    public OutboxPublisher(IServiceScopeFactory scopeFactory, IMessageBus bus, ILogger<OutboxPublisher> logger)
    {
        _scopeFactory = scopeFactory; _bus = bus; _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            using var scope = _scopeFactory.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

            var pending = await db.Outbox
                .Where(o => o.PublishedAt == null && o.Attempts < 5)
                .OrderBy(o => o.CreatedAt)
                .Take(50)
                .ToListAsync(stoppingToken);

            foreach (var msg in pending)
            {
                try
                {
                    await _bus.PublishAsync(msg.Type, msg.Payload, stoppingToken);
                    msg.PublishedAt = DateTimeOffset.UtcNow;
                    _logger.LogInformation("Published outbox {Id}", msg.Id);
                }
                catch (Exception ex)
                {
                    msg.Attempts++;
                    _logger.LogError(ex, "Failed to publish {Id}", msg.Id);
                }
            }

            if (pending.Count > 0) await db.SaveChangesAsync(stoppingToken);
            await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
        }
    }
}