using Workshop.StreamProcessor;

var builder = Host.CreateApplicationBuilder(args);
var c = builder.Configuration;

var streamConnectionString = c.GetConnectionString("StreamState");

if (string.IsNullOrWhiteSpace(streamConnectionString))
    throw new InvalidOperationException("ConnectionStrings:StreamState must be configured for SQL Server.");

builder.Services.AddSingleton(new StreamOptions(c["Kafka:BootstrapServers"] ?? "localhost:9092", c["Kafka:InputTopic"] ?? "commerce.order-change-enriched.v1", c["Kafka:OutputTopic"] ?? "analytics.customer-order-windows.v1", c["Kafka:GroupId"] ?? "customer-order-window-v1", streamConnectionString));

builder.Services.AddHostedService<DurableWindowWorker>(); 

await builder.Build().RunAsync();
