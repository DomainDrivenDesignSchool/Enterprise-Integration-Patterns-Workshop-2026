using Microsoft.AspNetCore.Builder;
using Microsoft.EntityFrameworkCore;
using Workshop.PaymentWorker;

var builder = WebApplication.CreateBuilder(args);
var cfg = builder.Configuration;

var paymentConnectionString = cfg.GetConnectionString("Payment");

if (string.IsNullOrWhiteSpace(paymentConnectionString))
    throw new InvalidOperationException("ConnectionStrings:Payment must be configured for SQL Server.");

builder.Services.AddDbContextFactory<PaymentWebhookDbContext>(options => options.UseSqlServer(paymentConnectionString));

builder.Services.AddSingleton(new PaymentOptions(
    cfg["Kafka:BootstrapServers"] ?? "localhost:9092",
    cfg["Kafka:CommandTopic"] ?? "commerce.commands",
    cfg["Kafka:SagaEventsTopic"] ?? "commerce.saga-events",
    cfg["Kafka:GroupId"] ?? "payment-command-v1",
    paymentConnectionString));

builder.Services.AddSingleton<IPaymentGateway>(_ => new SandboxPaymentGateway(
    bool.TryParse(cfg["Sandbox:SimulateChargeResponseLossOnce"], out var chargeLoss) && chargeLoss,
    bool.TryParse(cfg["Sandbox:SimulateRefundResponseLossOnce"], out var refundLoss) && refundLoss));

var secrets = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

foreach (var item in cfg.GetSection("Psp:WebhookSecrets").GetChildren())
    if (!string.IsNullOrWhiteSpace(item.Value)) secrets[item.Key] = item.Value;

builder.Services.AddSingleton(new PspWebhookSettings(secrets,
    TimeSpan.FromMinutes(5), int.TryParse(cfg["Psp:MaxWebhookBodyBytes"], out var maxBytes) ? Math.Clamp(maxBytes, 1024, 1_048_576) : 262_144));

builder.Services.AddHostedService<Worker>();
builder.Services.AddHostedService<PaymentReconciliationWorker>();
builder.Services.AddHostedService<PspWebhookProcessor>();

var app = builder.Build();

app.MapPspWebhook();
await app.RunAsync();
