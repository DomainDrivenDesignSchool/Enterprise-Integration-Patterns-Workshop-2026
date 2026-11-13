using Workshop.CdcEnricher;
using Workshop.Messaging.Reliability;

var builder = Host.CreateApplicationBuilder(args);

var cfg = builder.Configuration;
var enrichmentConnectionString = cfg.GetConnectionString("Enrichment");

if (string.IsNullOrWhiteSpace(enrichmentConnectionString))
    throw new InvalidOperationException("ConnectionStrings:Enrichment must be configured for SQL Server.");

builder.Services.AddSingleton(new CdcOptions(
    cfg["Kafka:BootstrapServers"] ?? "localhost:9092",
    cfg["Kafka:InputTopic"] ?? "workshop-sqlserver.dbo.orders",
    cfg["Kafka:OutputTopic"] ?? "commerce.order-change-enriched.v1",
    cfg["Kafka:GroupId"] ?? "order-cdc-enricher-v1",
    cfg["Kafka:TransactionalId"] ?? $"cdc-enricher-{Environment.MachineName}-{Guid.NewGuid():N}",
    enrichmentConnectionString));

builder.Services.AddHostedService<CdcWorker>();
await builder.Build().RunAsync();
