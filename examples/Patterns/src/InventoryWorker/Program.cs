using Workshop.InventoryWorker;
var builder = Host.CreateApplicationBuilder(args);

var cfg = builder.Configuration;

var inventoryConnectionString = cfg.GetConnectionString("Inventory");

if (string.IsNullOrWhiteSpace(inventoryConnectionString)) 
    throw new InvalidOperationException("ConnectionStrings:Inventory must be configured for SQL Server.");

builder.Services.AddSingleton(new InventoryOptions(
    cfg["Kafka:BootstrapServers"] ?? "localhost:9092",
    cfg["Kafka:CommandTopic"] ?? "commerce.commands",
    cfg["Kafka:SagaEventsTopic"] ?? "commerce.saga-events",
    cfg["Kafka:GroupId"] ?? "inventory-command-v1",
    inventoryConnectionString));

builder.Services.AddHostedService<Worker>();

await builder.Build().RunAsync();
