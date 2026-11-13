using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<WorkshopWorker>();
await builder.Build().RunAsync();

sealed class WorkshopWorker(ILogger<WorkshopWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("NotificationWorker started; configure its broker consumer and isolated database before running the lab.");
        while (!stoppingToken.IsCancellationRequested)
            await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
    }
}
